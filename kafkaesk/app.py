from .exceptions import AutoCommitError
from .exceptions import ConsumerUnhealthyException
from .exceptions import ProducerUnhealthyException
from .exceptions import SchemaConflictException
from .exceptions import StopConsumer
from .exceptions import UnhandledMessage
from .kafka import KafkaTopicManager
from .metrics import CONSUMED_MESSAGE_TIME
from .metrics import CONSUMED_MESSAGES
from .metrics import CONSUMER_REBALANCED
from .metrics import CONSUMER_TOPIC_OFFSET
from .metrics import MESSAGE_LEAD_TIME
from .metrics import NOERROR
from .metrics import PRODUCER_TOPIC_OFFSET
from .metrics import PUBLISHED_MESSAGES
from .metrics import PUBLISHED_MESSAGES_TIME
from .metrics import watch_kafka
from .metrics import watch_publish
from .retry import RetryHandler
from .retry import RetryPolicy
from .utils import resolve_dotted_name
from aiokafka.structs import TopicPartition
from asyncio.futures import Future
from functools import partial
from opentracing.scope_managers.contextvars import ContextVarsScopeManager
from pydantic import BaseModel
from pydantic import ValidationError
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

import aiokafka
import aiokafka.errors
import aiokafka.structs
import argparse
import asyncio
import fnmatch
import inspect
import logging
import opentracing
import orjson
import pydantic
import signal
import time

logger = logging.getLogger("kafkaesk")


class Subscription:
    def __init__(
        self,
        stream_id: str,
        func: Callable,
        group: str,
        *,
        retry_handlers: Optional[Dict[Type[Exception], RetryHandler]] = None,
    ):
        self.stream_id = stream_id
        self.func = func
        self.group = group
        self.retry_handlers = retry_handlers

    def __repr__(self) -> str:
        return f"<Subscription stream: {self.stream_id} >"


class SchemaRegistration:
    def __init__(
        self,
        id: str,
        version: int,
        model: Type[pydantic.BaseModel],
        retention: Optional[int] = None,
        streams: Optional[List[str]] = None,
    ):
        self.id = id
        self.version = version
        self.model = model
        self.retention = retention
        self.streams = streams

    def __repr__(self) -> str:
        return f"<SchemaRegistration id: {self.id}, version: {self.version} >"


def _pydantic_msg_handler(
    model: Type[BaseModel], record: aiokafka.structs.ConsumerRecord, data: Dict[str, Any]
) -> BaseModel:
    try:
        return model.parse_obj(data["data"])
    except ValidationError:
        # log the execption so we can see what fields failed
        logger.warning(f"Error parsing pydantic model:{model} {record}", exc_info=True)
        raise UnhandledMessage(f"Error parsing data: {model}")


def _raw_msg_handler(
    record: aiokafka.structs.ConsumerRecord, data: Dict[str, Any]
) -> Dict[str, Any]:
    return data


def _bytes_msg_handler(record: aiokafka.structs.ConsumerRecord, data: Dict[str, Any]) -> bytes:
    return record.value


def _record_msg_handler(
    record: aiokafka.structs.ConsumerRecord, data: Dict[str, Any]
) -> aiokafka.structs.ConsumerRecord:
    return record


def _published_callback(topic: str, start_time: float, fut: Future) -> None:
    # Record the metrics
    finish_time = time.time()
    exception = fut.exception()
    if exception:
        error = str(exception.__class__.__name__)
    else:
        error = NOERROR

    metadata = fut.result()
    PUBLISHED_MESSAGES.labels(stream_id=topic, partition=metadata.partition, error=error).inc()
    PRODUCER_TOPIC_OFFSET.labels(stream_id=topic, partition=metadata.partition).set(metadata.offset)
    PUBLISHED_MESSAGES_TIME.labels(stream_id=topic).observe(finish_time - start_time)


_aiokafka_consumer_settings = (
    "fetch_max_wait_ms",
    "fetch_max_bytes",
    "fetch_min_bytes",
    "max_partition_fetch_bytes",
    "request_timeout_ms",
    "auto_offset_reset",
    "metadata_max_age_ms",
    "max_poll_interval_ms",
    "rebalance_timeout_ms",
    "session_timeout_ms",
    "heartbeat_interval_ms",
    "consumer_timeout_ms",
    "max_poll_records",
    "connections_max_idle_ms",
)
_aiokafka_producer_settings = (
    "metadata_max_age_ms",
    "request_timeout_ms",
    "max_batch_size",
    "max_request_size",
    "send_backoff_ms",
    "retry_backoff_ms",
)


class SubscriptionConsumer:
    _consumer: Optional[aiokafka.AIOKafkaConsumer] = None
    _to_commit: Dict[TopicPartition, int]
    _auto_commit_task: Optional[asyncio.Task] = None

    def __init__(
        self,
        app: "Application",
        subscription: Subscription,
        event_handlers: Optional[Dict[str, List[Callable]]] = None,
    ):
        self._app = app
        self._subscription = subscription
        self._event_handlers = event_handlers or {}
        self._to_commit = {}
        self._commit_lock = asyncio.Lock()

    @property
    def consumer(self) -> aiokafka.AIOKafkaConsumer:
        if self._consumer is None:
            raise RuntimeError("Consumer not initialized")
        return self._consumer

    async def healthy(self) -> None:
        if self._consumer is None:
            return
        if self._auto_commit_task is not None and self._auto_commit_task.done():
            raise AutoCommitError(self, "No auto commit task running")
        if not await self._consumer._client.ready(self._consumer._coordinator.coordinator_id):
            raise ConsumerUnhealthyException(self, "Consumer is not ready")
        return

    async def emit(self, name: str, *args: Any, **kwargs: Any) -> None:
        for func in self._event_handlers.get(name, []):
            try:
                await func(*args, **kwargs)
            except StopConsumer:
                raise
            except Exception:
                logger.warning(f"Error emitting event: {name}: {func}", exc_info=True)

    async def __call__(self) -> None:
        await self.initialize()
        try:
            while True:
                try:
                    await self._run()
                except aiokafka.errors.KafkaConnectionError:
                    logger.warning("Connection error, retrying", exc_info=True)
                    await asyncio.sleep(0.5)
        except (RuntimeError, asyncio.CancelledError, StopConsumer):
            logger.debug("Consumer stopped, exiting")
        finally:
            await self.finalize()

    async def initialize(self) -> None:
        if self._app.auto_commit:
            self._auto_commit_task = asyncio.create_task(self._auto_commit())

    async def finalize(self) -> None:
        if (
            self._app.auto_commit
            and self._auto_commit_task is not None
            and not self._auto_commit_task.done()
        ):
            self._auto_commit_task.cancel()

    async def _auto_commit(self) -> None:
        default_to = self._app.kafka_settings.get("auto_commit_interval_ms", 2000) / 1000
        while True:
            to = default_to
            try:
                await asyncio.sleep(to)
                to = await self.commit() or default_to
            except (RuntimeError, asyncio.CancelledError):
                logger.debug("Exiting auto commit task")
                return

    async def commit(self) -> Optional[float]:
        """
        Commit current state
        """
        if len(self._to_commit) > 0:
            async with self._commit_lock:
                # it's possible to commit on exit as well
                # so let's make sure to use lock here
                return await self._commit()
        return None

    async def _commit(self) -> float:
        now = time.monotonic()
        interval = self._app.kafka_settings.get("auto_commit_interval_ms", 2000) / 1000
        backoff = self._app.kafka_settings.get("retry_backoff_ms", 200) / 1000

        to_commit = self._to_commit
        try:
            self._to_commit = {}
            assignment = self.consumer.assignment()
            for tp in list(to_commit.keys()):
                if tp not in assignment:
                    # clean if we got a new assignment
                    del to_commit[tp]
            if len(to_commit) > 0:
                await self.consumer.commit(to_commit)
                logging.debug(f"Committed offsets: {to_commit}")
        except aiokafka.errors.CommitFailedError:
            # try to commit again but we need to combine
            # what could now be in the commit from when we started
            logging.info("Failed to commit offsets, rebalanced likely. Retrying.", exc_info=True)
            for tp in to_commit.keys():
                if tp in self._to_commit:
                    self._to_commit[tp] = max(to_commit[tp], self._to_commit[tp])
                else:
                    self._to_commit[tp] = to_commit[tp]
            return backoff
        except Exception:
            logging.exception("Unhandled exception committed offsets", exc_info=True)
        return max(0, (now + interval) - time.monotonic())

    def record_commit(self, record: aiokafka.structs.ConsumerRecord) -> None:
        self._to_commit[TopicPartition(record.topic, record.partition)] = record.offset + 1

    async def _run(self) -> None:
        self._consumer = self._app.consumer_factory(self._subscription.group)
        pattern = fnmatch.translate(self._app.topic_mng.get_topic_id(self._subscription.stream_id))
        listener = CustomConsumerRebalanceListener(
            self._consumer, self._app, self._subscription.group
        )
        self._consumer.subscribe(pattern=pattern, listener=listener)

        # Initialize subscribers retry policy
        retry_policy = RetryPolicy(app=self._app, subscription=self._subscription,)
        await retry_policy.initialize()

        with watch_kafka("consumer_start"):
            await self._consumer.start()

        msg_handler = _raw_msg_handler
        sig = inspect.signature(self._subscription.func)
        param_name = [k for k in sig.parameters.keys()][0]
        annotation = sig.parameters[param_name].annotation
        if annotation and annotation != sig.empty:
            if annotation == bytes:
                msg_handler = _bytes_msg_handler  # type: ignore
            elif annotation == aiokafka.structs.ConsumerRecord:
                msg_handler = _record_msg_handler  # type: ignore
            else:
                msg_handler = partial(_pydantic_msg_handler, annotation)  # type: ignore

        await self.emit("started", subscription_consumer=self)
        try:
            # Consume messages
            async for record in self._consumer:
                tracer = opentracing.tracer
                headers = {x[0]: x[1].decode() for x in record.headers or []}
                parent = tracer.extract(opentracing.Format.TEXT_MAP, headers)
                context = tracer.start_active_span(
                    record.topic,
                    tags={
                        "message_bus.destination": record.topic,
                        "message_bus.partition": record.partition,
                        "message_bus.group_id": self._subscription.group,
                    },
                    references=[opentracing.follows_from(parent)],
                )
                CONSUMER_TOPIC_OFFSET.labels(
                    group_id=self._subscription.group,
                    stream_id=record.topic,
                    partition=record.partition,
                ).set(record.offset)
                # Calculate the time since the message is send until is successfully consumed
                lead_time = time.time() - record.timestamp / 1000  # type: ignore
                MESSAGE_LEAD_TIME.labels(
                    stream_id=record.topic,
                    partition=record.partition,
                    group_id=self._subscription.group,
                ).observe(lead_time)

                try:
                    logger.debug(f"Handling msg: {record}")
                    msg_data = orjson.loads(record.value)
                    it = iter(sig.parameters.items())
                    name, _ = next(it)
                    kwargs: Dict[str, Any] = {name: msg_handler(record, msg_data)}

                    for key, param in it:
                        if key == "schema":
                            kwargs["schema"] = msg_data["schema"]
                        elif key == "record":
                            kwargs["record"] = record
                        elif key == "app":
                            kwargs["app"] = self._app
                        elif key == "subscriber":
                            kwargs["subscriber"] = self
                        elif issubclass(param.annotation, opentracing.Span):
                            kwargs[key] = context.span

                    with CONSUMED_MESSAGE_TIME.labels(
                        stream_id=record.topic,
                        partition=record.partition,
                        group_id=self._subscription.group,
                    ).time():
                        await self._subscription.func(**kwargs)

                    # No error metric
                    CONSUMED_MESSAGES.labels(
                        stream_id=record.topic,
                        error=NOERROR,
                        partition=record.partition,
                        group_id=self._subscription.group,
                    ).inc()
                    self.record_commit(record)
                except Exception as err:
                    CONSUMED_MESSAGES.labels(
                        stream_id=record.topic,
                        partition=record.partition,
                        error=err.__class__.__name__,
                        group_id=self._subscription.group,
                    ).inc()
                    await retry_policy(record=record, exception=err)
                    # also commit after successful message retry handling
                    self.record_commit(record)
                except aiokafka.errors.ConsumerStoppedError:
                    # Consumer is closed
                    pass
                finally:
                    context.close()
                    await self.emit("message", record=record)
        finally:
            try:
                await self.commit()
            except Exception:
                logger.info("Could not commit on shutdown", exc_info=True)
            try:
                # kill retry policy now
                await retry_policy.finalize()
            except Exception:
                logger.info("Cound not properly stop retry policy", exc_info=True)
            try:
                await self._consumer.stop()
            except Exception:
                logger.warning("Could not properly stop consumer", exc_info=True)


class Router:
    """
    Application routing configuration.
    """

    def __init__(self) -> None:
        self._subscriptions: List[Subscription] = []
        self._schemas: Dict[str, SchemaRegistration] = {}
        self._event_handlers: Dict[str, List[Callable[[], Awaitable[None]]]] = {}

    @property
    def subscriptions(self) -> List[Subscription]:
        return self._subscriptions

    @property
    def schemas(self) -> Dict[str, SchemaRegistration]:
        return self._schemas

    @property
    def event_handlers(self) -> Dict[str, List[Callable[[], Awaitable[None]]]]:
        return self._event_handlers

    def on(self, name: str, handler: Callable[[], Awaitable[None]]) -> None:
        if name not in self._event_handlers:
            self._event_handlers[name] = []

        self._event_handlers[name].append(handler)

    def subscribe(
        self,
        stream_id: str,
        group: str,
        *,
        retry_handlers: Optional[Dict[Type[Exception], RetryHandler]] = None,
    ) -> Callable:
        def inner(func: Callable) -> Callable:
            subscription = Subscription(
                stream_id, func, group or func.__name__, retry_handlers=retry_handlers
            )
            self._subscriptions.append(subscription)
            return func

        return inner

    def schema(
        self,
        _id: Optional[str] = None,
        *,
        version: Optional[int] = None,
        retention: Optional[int] = None,
        streams: Optional[List[str]] = None,
    ) -> Callable:
        version = version or 1

        def inner(cls: Type[BaseModel]) -> Type[BaseModel]:
            if _id is None:
                type_id = cls.__name__
            else:
                type_id = _id
            key = f"{type_id}:{version}"
            reg = SchemaRegistration(
                id=type_id, version=version or 1, model=cls, retention=retention, streams=streams
            )
            if key in self._schemas:
                raise SchemaConflictException(self._schemas[key], reg)
            cls.__key__ = key  # type: ignore
            self._schemas[key] = reg
            return cls

        return inner


class Application(Router):
    """
    Application configuration
    """

    _producer: Optional[aiokafka.AIOKafkaProducer]

    def __init__(
        self,
        kafka_servers: Optional[List[str]] = None,
        topic_prefix: str = "",
        kafka_settings: Optional[Dict[str, Any]] = None,
        replication_factor: Optional[int] = None,
        kafka_api_version: str = "auto",
        auto_commit: bool = True,
    ):
        super().__init__()
        self._kafka_servers = kafka_servers
        self._kafka_settings = kafka_settings
        self._producer = None
        self._initialized = False
        self._locks: Dict[str, asyncio.Lock] = {}

        self._kafka_api_version = kafka_api_version
        self._topic_prefix = topic_prefix
        self._replication_factor = replication_factor
        self._topic_mng: Optional[KafkaTopicManager] = None
        self._subscription_consumers: List[SubscriptionConsumer] = []
        self._subscription_consumers_tasks: List[asyncio.Task] = []

        self.auto_commit = auto_commit

    @property
    def kafka_settings(self) -> Dict[str, Any]:
        return self._kafka_settings or {}

    def mount(self, router: Router) -> None:
        self._subscriptions.extend(router.subscriptions)
        self._schemas.update(router.schemas)
        self._event_handlers.update(router.event_handlers)

    async def health_check(self) -> None:
        for subscription_consumer in self._subscription_consumers:
            await subscription_consumer.healthy()
        if not self.producer_healthy():
            raise ProducerUnhealthyException(self._producer)  # type: ignore

    async def _call_event_handlers(self, name: str) -> None:
        handlers = self._event_handlers.get(name)

        if handlers is not None:
            for handler in handlers:
                await handler()

    @property
    def topic_mng(self) -> KafkaTopicManager:
        if self._topic_mng is None:
            self._topic_mng = KafkaTopicManager(
                cast(List[str], self._kafka_servers),
                self._topic_prefix,
                replication_factor=self._replication_factor,
                kafka_api_version=self._kafka_api_version,
            )
        return self._topic_mng

    def get_lock(self, name: str) -> asyncio.Lock:
        if name not in self._locks:
            self._locks[name] = asyncio.Lock()
        return self._locks[name]

    def configure(
        self,
        kafka_servers: Optional[List[str]] = None,
        topic_prefix: Optional[str] = None,
        kafka_settings: Optional[Dict[str, Any]] = None,
        api_version: Optional[str] = None,
        replication_factor: Optional[int] = None,
    ) -> None:
        if kafka_servers is not None:
            self._kafka_servers = kafka_servers
        if topic_prefix is not None:
            self._topic_prefix = topic_prefix
        if kafka_settings is not None:
            self._kafka_settings = kafka_settings
        if api_version is not None:
            self._kafka_api_version = api_version
        if replication_factor is not None:
            self._replication_factor = replication_factor

    async def publish_and_wait(
        self, stream_id: str, data: BaseModel, key: Optional[bytes] = None
    ) -> aiokafka.structs.ConsumerRecord:
        return await (await self.publish(stream_id, data, key))

    async def publish(
        self, stream_id: str, data: BaseModel, key: Optional[bytes] = None
    ) -> Awaitable[aiokafka.structs.ConsumerRecord]:
        if not self._initialized:
            async with self.get_lock("_"):
                await self.initialize()

        schema_key = getattr(data, "__key__", None)
        if schema_key not in self._schemas:
            # do not require key
            schema_key = f"{data.__class__.__name__}:1"
        data_ = data.dict()

        topic_id = self.topic_mng.get_topic_id(stream_id)
        async with self.get_lock(stream_id):
            if not await self.topic_mng.topic_exists(topic_id):
                reg = self.get_schema_reg(data)
                retention_ms = None
                if reg is not None and reg.retention is not None:
                    retention_ms = reg.retention * 1000
                await self.topic_mng.create_topic(
                    topic_id,
                    replication_factor=self._replication_factor,
                    retention_ms=retention_ms,
                )

        return await self.raw_publish(
            stream_id, orjson.dumps({"schema": schema_key, "data": data_}), key
        )

    async def raw_publish(
        self, stream_id: str, data: bytes, key: Optional[bytes] = None
    ) -> Awaitable[aiokafka.structs.ConsumerRecord]:
        logger.debug(f"Sending kafka msg: {stream_id}")
        producer = await self._get_producer()
        tracer = opentracing.tracer
        headers: Optional[List[Tuple[str, bytes]]] = None
        if isinstance(tracer.scope_manager, ContextVarsScopeManager):
            # This only makes sense if the context manager is asyncio aware
            if tracer.active_span:
                carrier: Dict[str, str] = {}
                tracer.inject(
                    span_context=tracer.active_span,
                    format=opentracing.Format.TEXT_MAP,
                    carrier=carrier,
                )
                headers = [(k, v.encode()) for k, v in carrier.items()]

        if not self.producer_healthy():
            raise ProducerUnhealthyException(self._producer)  # type: ignore

        topic_id = self.topic_mng.get_topic_id(stream_id)
        start_time = time.time()
        with watch_publish(topic_id):
            fut = await producer.send(topic_id, value=data, key=key, headers=headers,)

        fut.add_done_callback(partial(_published_callback, topic_id, start_time))  # type: ignore
        return fut

    async def flush(self) -> None:
        if self._producer is not None:
            await self._producer.flush()

    def get_schema_reg(self, model_or_def: BaseModel) -> Optional[SchemaRegistration]:
        try:
            key = model_or_def.__key__  # type: ignore
            return self._schemas[key]
        except (AttributeError, KeyError):
            return None

    def producer_healthy(self) -> bool:
        """
        It's possible for the producer to be unhealthy while we're still sending messages to it.
        """
        if self._producer is not None and self._producer._sender.sender_task is not None:
            return not self._producer._sender.sender_task.done()
        return True

    def consumer_factory(self, group_id: str) -> aiokafka.AIOKafkaConsumer:
        return aiokafka.AIOKafkaConsumer(
            bootstrap_servers=cast(List[str], self._kafka_servers),
            loop=asyncio.get_event_loop(),
            group_id=group_id,
            api_version=self._kafka_api_version,
            enable_auto_commit=False,
            **{k: v for k, v in self.kafka_settings.items() if k in _aiokafka_consumer_settings},
        )

    def producer_factory(self) -> aiokafka.AIOKafkaProducer:
        return aiokafka.AIOKafkaProducer(
            bootstrap_servers=cast(List[str], self._kafka_servers),
            loop=asyncio.get_event_loop(),
            api_version=self._kafka_api_version,
            **{k: v for k, v in self.kafka_settings.items() if k in _aiokafka_producer_settings},
        )

    async def _get_producer(self) -> aiokafka.AIOKafkaProducer:
        if self._producer is None:
            self._producer = self.producer_factory()
            with watch_kafka("producer_start"):
                await self._producer.start()
        return self._producer

    async def initialize(self) -> None:
        await self._call_event_handlers("initialize")

        for reg in self._schemas.values():
            # initialize topics for known streams
            for stream_id in reg.streams or []:
                topic_id = self.topic_mng.get_topic_id(stream_id)
                async with self.get_lock(stream_id):
                    if not await self.topic_mng.topic_exists(topic_id):
                        await self.topic_mng.create_topic(
                            topic_id,
                            retention_ms=reg.retention * 1000
                            if reg.retention is not None
                            else None,
                        )

        self._initialized = True

    async def finalize(self) -> None:
        await self._call_event_handlers("finalize")

        if self._producer is not None:
            with watch_kafka("producer_flush"):
                await self._producer.flush()
            with watch_kafka("producer_stop"):
                await self._producer.stop()

        if self._subscription_consumers_tasks:
            for task in self._subscription_consumers_tasks:
                if not task.done():
                    task.cancel()

            for task in self._subscription_consumers_tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    ...

        if self._topic_mng is not None:
            await self._topic_mng.finalize()

        self._producer = None
        self._initialized = False
        self._topic_mng = None

    async def __aenter__(self) -> "Application":
        await self.initialize()
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self.finalize()

    async def consume_for(self, num_messages: int, *, seconds: Optional[int] = None) -> int:

        consumed = 0

        self._subscription_consumers = []
        for subscription in self._subscriptions:

            async def on_message(record: aiokafka.structs.ConsumerRecord) -> None:
                nonlocal consumed
                consumed += 1
                if consumed >= num_messages:
                    raise StopConsumer

            consumer = SubscriptionConsumer(
                self, subscription, event_handlers={"message": [on_message]}
            )
            self._subscription_consumers.append(consumer)

        try:
            futures = [asyncio.create_task(c()) for c in self._subscription_consumers]
            future = asyncio.gather(*futures)
            if seconds is not None:
                future = asyncio.wait_for(future, seconds)

            try:
                await future
            except asyncio.TimeoutError:
                ...

        finally:
            for fut in futures:
                if not fut.done():
                    fut.cancel()
        return consumed

    async def consume_forever(self) -> None:
        self._subscription_consumers = []
        self._subscription_consumers_tasks = []

        for subscription in self._subscriptions:
            consumer = SubscriptionConsumer(self, subscription)
            self._subscription_consumers.append(consumer)

        try:
            self._subscription_consumers_tasks = [
                asyncio.create_task(c()) for c in self._subscription_consumers
            ]
            await asyncio.gather(*self._subscription_consumers_tasks)
        finally:
            for task in self._subscription_consumers_tasks:
                if not task.done():
                    task.cancel()

            for task in self._subscription_consumers_tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    ...


class CustomConsumerRebalanceListener(aiokafka.ConsumerRebalanceListener):
    def __init__(self, consumer: aiokafka.AIOKafkaConsumer, app: Application, group_id: str):
        self.consumer = consumer
        self.app = app
        self.group_id = group_id

    async def on_partitions_revoked(self, revoked: List[aiokafka.structs.TopicPartition]) -> None:
        ...

    async def on_partitions_assigned(self, assigned: List[aiokafka.structs.TopicPartition]) -> None:
        """This method will be called after partition
           re-assignment completes and before the consumer
           starts fetching data again.

        Arguments:
            assigned {TopicPartition} -- List of topics and partitions assigned
            to a given consumer.
        """
        starting_pos = await self.app.topic_mng.list_consumer_group_offsets(
            self.group_id, partitions=assigned
        )
        logger.debug(f"Partitions assigned: {assigned}")

        for tp in assigned:
            CONSUMER_REBALANCED.labels(
                stream_id=tp.topic, partition=tp.partition, group_id=self.group_id,
            ).inc()
            if tp not in starting_pos or starting_pos[tp].offset == -1:
                # detect if we've never consumed from this topic before
                # decision right now is to go back to beginning
                # and it's unclear if this is always right decision
                await self.consumer.seek_to_beginning(tp)


cli_parser = argparse.ArgumentParser(description="Run kafkaesk worker.")
cli_parser.add_argument("app", help="Application object")
cli_parser.add_argument("--kafka-servers", help="Kafka servers")
cli_parser.add_argument("--kafka-settings", help="Kafka settings")
cli_parser.add_argument("--topic-prefix", help="Topic prefix")
cli_parser.add_argument("--api-version", help="Kafka API Version")


def _close_app(app: Application, fut: asyncio.Future) -> None:
    if not fut.done():
        logger.debug("Cancelling consumer from signal")
        fut.cancel()


async def run_app(app: Application) -> None:
    async with app:
        loop = asyncio.get_event_loop()
        fut = asyncio.create_task(app.consume_forever())
        for signame in {"SIGINT", "SIGTERM"}:
            loop.add_signal_handler(getattr(signal, signame), partial(_close_app, app, fut))
        await fut
        logger.debug("Exiting consumer")


def run(app: Optional[Application] = None) -> None:
    if app is None:
        opts = cli_parser.parse_args()
        module_str, attr = opts.app.split(":")
        module = resolve_dotted_name(module_str)
        app = getattr(module, attr)

        if callable(app):
            app = app()

        app = cast(Application, app)

        if opts.kafka_servers:
            app.configure(kafka_servers=opts.kafka_servers.split(","))
        if opts.kafka_settings:
            app.configure(kafka_settings=orjson.loads(opts.kafka_settings))
        if opts.topic_prefix:
            app.configure(topic_prefix=opts.topic_prefix)
        if opts.api_version:
            app.configure(api_version=opts.api_version)

    try:
        logger.debug(f"Running kafkaesk consumer {app}")
        asyncio.run(run_app(app))
    except asyncio.CancelledError:
        logger.debug("Closing because task was exited")
