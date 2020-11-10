from .exceptions import ConsumerUnhealthyException
from .exceptions import SchemaConflictException
from .exceptions import StopConsumer
from .exceptions import UnhandledMessage
from .exceptions import UnregisteredSchemaException
from .kafka import KafkaTopicManager
from .metrics import CONSUMED_MESSAGE_TIME
from .metrics import CONSUMED_MESSAGES
from .metrics import CONSUMER_REBALANCED
from .metrics import CONSUMER_TOPIC_OFFSET
from .metrics import MESSAGE_LEAD_TIME
from .metrics import NOERROR
from .metrics import PRODUCER_TOPIC_OFFSET
from .metrics import PUBLISHED_MESSAGES
from .utils import resolve_dotted_name
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
    def __init__(self, stream_id: str, func: Callable, group: str):
        self.stream_id = stream_id
        self.func = func
        self.group = group

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


def _published_callback(topic: str, fut: Future) -> None:
    # Record the metrics
    exception = fut.exception()
    if exception:
        error = str(exception.__class__.__name__)
    else:
        error = NOERROR

    metadata = fut.result()
    PUBLISHED_MESSAGES.labels(stream_id=topic, partition=metadata.partition, error=error).inc()
    PRODUCER_TOPIC_OFFSET.labels(stream_id=topic, partition=metadata.partition).set(metadata.offset)


class SubscriptionConsumer:
    _consumer: Optional[aiokafka.AIOKafkaConsumer] = None

    def __init__(
        self,
        app: "Application",
        subscription: Subscription,
        event_handlers: Optional[Dict[str, List[Callable]]] = None,
    ):
        self._app = app
        self._subscription = subscription
        self._event_handlers = event_handlers or {}

    @property
    def consumer(self) -> Optional[aiokafka.AIOKafkaConsumer]:
        return self._consumer

    async def emit(self, name: str, *args: Any, **kwargs: Any) -> None:
        for func in self._event_handlers.get(name, []):
            try:
                await func(*args, **kwargs)
            except StopConsumer:
                raise
            except Exception:
                logger.warning(f"Error emitting event: {name}: {func}", exc_info=True)

    async def __call__(self) -> None:
        try:
            while True:
                await self.__run()
        except aiokafka.errors.UnrecognizedBrokerVersion:
            logger.error("Could not determine kafka version. Exiting")
        except aiokafka.errors.KafkaConnectionError:
            logger.warning("Connection error", exc_info=True)
        except (RuntimeError, asyncio.CancelledError, StopConsumer):
            logger.debug("Consumer stopped, exiting")

    async def __run(self) -> None:
        self._consumer = aiokafka.AIOKafkaConsumer(
            bootstrap_servers=cast(List[str], self._app._kafka_servers),
            loop=asyncio.get_event_loop(),
            group_id=self._subscription.group,
            api_version=self._app._kafka_api_version,
            **self._app._kafka_settings or {},
        )
        pattern = fnmatch.translate(self._app.topic_mng.get_topic_id(self._subscription.stream_id))
        listener = CustomConsumerRebalanceListener(
            self._consumer, self._app, self._subscription.group
        )
        self._consumer.subscribe(pattern=pattern, listener=listener)
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
                except UnhandledMessage:
                    # how should we handle this? Right now, fail hard
                    logger.warning(f"Could not process msg: {record}", exc_info=True)
                    # We increase the error counter
                    CONSUMED_MESSAGES.labels(
                        stream_id=record.topic,
                        partition=record.partition,
                        error="UnhandledMessage",
                        group_id=self._subscription.group,
                    ).inc()

                finally:
                    context.span.finish()
                    context.close()
                    await self.emit("message", record=record)
        finally:
            try:
                await self._consumer.commit()
            except Exception:
                logger.info("Could not commit current offsets", exc_info=True)
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

    def subscribe(self, stream_id: str, group: str,) -> Callable:
        def inner(func: Callable) -> Callable:
            subscription = Subscription(stream_id, func, group or func.__name__)
            self._subscriptions.append(subscription)
            return func

        return inner

    def schema(
        self,
        _id: str,
        *,
        version: Optional[int] = None,
        retention: Optional[int] = None,
        streams: Optional[List[str]] = None,
    ) -> Callable:
        version = version or 1

        def inner(cls: Type[BaseModel]) -> Type[BaseModel]:
            key = f"{_id}:{version}"
            reg = SchemaRegistration(
                id=_id, version=version or 1, model=cls, retention=retention, streams=streams
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

    def mount(self, router: Router) -> None:
        self._subscriptions.extend(router.subscriptions)
        self._schemas.update(router.schemas)
        self._event_handlers.update(router.event_handlers)

    async def health_check(self) -> None:
        for subscription_consumer in self._subscription_consumers:
            if (
                subscription_consumer.consumer is not None
                and not await subscription_consumer.consumer._client.ready(
                    subscription_consumer.consumer._coordinator.coordinator_id
                )
            ):
                raise ConsumerUnhealthyException(subscription_consumer, "Consumer is not ready")

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

    async def publish(
        self, stream_id: str, data: BaseModel, key: Optional[bytes] = None
    ) -> Awaitable[aiokafka.structs.ConsumerRecord]:
        if not self._initialized:
            async with self.get_lock("_"):
                await self.initialize()

        schema_key = getattr(data, "__key__", None)
        if schema_key not in self._schemas:
            raise UnregisteredSchemaException(model=data)
        data_ = data.dict()

        topic_id = self.topic_mng.get_topic_id(stream_id)
        async with self.get_lock(stream_id):
            if not await self.topic_mng.topic_exists(topic_id):
                reg = self.get_schema_reg(data)
                await self.topic_mng.create_topic(
                    topic_id,
                    replication_factor=self._replication_factor,
                    retention_ms=reg.retention * 1000 if reg.retention is not None else None,
                )

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

        fut = await producer.send(
            topic_id,
            value=orjson.dumps({"schema": schema_key, "data": data_}),
            key=key,
            headers=headers,
        )

        fut.add_done_callback(partial(_published_callback, topic_id))  # type: ignore
        return fut

    async def flush(self) -> None:
        if self._producer is not None:
            await self._producer.flush()

    def get_schema_reg(self, model_or_def: BaseModel) -> SchemaRegistration:
        key = model_or_def.__key__  # type: ignore
        return self._schemas[key]

    async def _get_producer(self) -> aiokafka.AIOKafkaProducer:
        if self._producer is None:
            self._producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers=cast(List[str], self._kafka_servers),
                loop=asyncio.get_event_loop(),
                api_version=self._kafka_api_version,
            )
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
            await self._producer.flush()
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

            # XXX go back one message
            # unclear if this is what we want to do...
            # try:
            #     position = await self.consumer.position(tp)
            #     offset = position - 1
            # except aiokafka.errors.IllegalStateError:
            #     offset = -1
            # if offset > 0:
            #     self.consumer.seek(tp, offset)
            # else:
            #     await self.consumer.seek_to_beginning(tp)


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
