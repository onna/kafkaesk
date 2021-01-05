from .exceptions import AppNotConfiguredException
from .exceptions import ProducerUnhealthyException
from .exceptions import SchemaConflictException
from .exceptions import StopConsumer
from .kafka import KafkaTopicManager
from .metrics import NOERROR
from .metrics import PRODUCER_TOPIC_OFFSET
from .metrics import PUBLISHED_MESSAGES
from .metrics import PUBLISHED_MESSAGES_TIME
from .metrics import watch_kafka
from .metrics import watch_publish
from .retry import RetryHandler
from .subscription import Subscription
from .subscription import SubscriptionConsumer
from .utils import resolve_dotted_name
from asyncio.futures import Future
from functools import partial
from opentracing.scope_managers.contextvars import ContextVarsScopeManager
from pydantic import BaseModel
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

import aiokafka
import aiokafka.errors
import aiokafka.structs
import argparse
import asyncio
import logging
import opentracing
import orjson
import pydantic
import signal
import time

logger = logging.getLogger("kafkaesk")


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
        return f"<SchemaRegistration {self.id}, version: {self.version} >"


def published_callback(topic: str, start_time: float, fut: Future) -> None:
    # Record the metrics
    finish_time = time.time()
    exception = fut.exception()
    if exception:
        error = str(exception.__class__.__name__)
        PUBLISHED_MESSAGES.labels(stream_id=topic, partition=-1, error=error).inc()
    else:
        metadata = fut.result()
        PUBLISHED_MESSAGES.labels(
            stream_id=topic, partition=metadata.partition, error=NOERROR
        ).inc()
        PRODUCER_TOPIC_OFFSET.labels(stream_id=topic, partition=metadata.partition).set(
            metadata.offset
        )
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

    @property
    def is_configured(self) -> bool:
        return bool(self._kafka_servers)

    async def publish_and_wait(
        self,
        stream_id: str,
        data: BaseModel,
        key: Optional[bytes] = None,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> aiokafka.structs.ConsumerRecord:
        return await (await self.publish(stream_id, data, key, headers=headers))

    async def publish(
        self,
        stream_id: str,
        data: BaseModel,
        key: Optional[bytes] = None,
        headers: Optional[Dict[str, bytes]] = None,
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
            stream_id, orjson.dumps({"schema": schema_key, "data": data_}), key, headers=headers
        )

    async def raw_publish(
        self,
        stream_id: str,
        data: bytes,
        key: Optional[bytes] = None,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> Awaitable[aiokafka.structs.ConsumerRecord]:
        logger.debug(f"Sending kafka msg: {stream_id}")
        producer = await self._get_producer()
        tracer = opentracing.tracer

        if headers is None:
            headers = {}

        if isinstance(tracer.scope_manager, ContextVarsScopeManager):
            # This only makes sense if the context manager is asyncio aware
            if tracer.active_span:
                carrier: Dict[str, str] = {}
                tracer.inject(
                    span_context=tracer.active_span,
                    format=opentracing.Format.TEXT_MAP,
                    carrier=carrier,
                )
                headers.update({k: v.encode() for k, v in carrier.items()})

        if not self.producer_healthy():
            raise ProducerUnhealthyException(self._producer)  # type: ignore

        topic_id = self.topic_mng.get_topic_id(stream_id)
        start_time = time.time()
        with watch_publish(topic_id):
            fut = await producer.send(
                topic_id,
                value=data,
                key=key,
                headers=[(k, v) for k, v in headers.items()],
            )

        fut.add_done_callback(partial(published_callback, topic_id, start_time))  # type: ignore
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
        if not self.is_configured:
            raise AppNotConfiguredException

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

        await self.stop()

        if self._producer is not None:
            with watch_kafka("producer_flush"):
                await self._producer.flush()
            with watch_kafka("producer_stop"):
                await self._producer.stop()

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

        done, pending = await asyncio.wait(
            [asyncio.create_task(c()) for c in self._subscription_consumers], timeout=seconds
        )
        await self.stop()

        # re-raise any errors so we can validate during tests
        for task in done | pending:
            exc = task.exception()
            if exc is not None:
                raise exc

        return consumed

    def consume_forever(self) -> Awaitable:
        self._subscription_consumers = []
        self._subscription_consumers_tasks = []

        for subscription in self._subscriptions:
            consumer = SubscriptionConsumer(self, subscription)
            self._subscription_consumers.append(consumer)

        self._subscription_consumers_tasks = [
            asyncio.create_task(c()) for c in self._subscription_consumers
        ]
        return asyncio.wait(self._subscription_consumers_tasks, return_when=asyncio.ALL_COMPLETED)

    async def stop(self) -> None:
        async with self.get_lock("_"):
            # do not allow stop calls at same time

            if len(self._subscription_consumers) == 0:
                return

            _, pending = await asyncio.wait(
                [c.stop() for c in self._subscription_consumers if c], timeout=5
            )
            for task in pending:
                # stop tasks that didn't finish
                task.cancel()

            for task in self._subscription_consumers_tasks:
                # make sure everything is done
                if not task.done():
                    task.cancel()

            for task in self._subscription_consumers_tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    ...


cli_parser = argparse.ArgumentParser(description="Run kafkaesk worker.")
cli_parser.add_argument("app", help="Application object")
cli_parser.add_argument("--kafka-servers", help="Kafka servers")
cli_parser.add_argument("--kafka-settings", help="Kafka settings")
cli_parser.add_argument("--topic-prefix", help="Topic prefix")
cli_parser.add_argument("--api-version", help="Kafka API Version")


def _sig_handler(app: Application) -> None:
    asyncio.create_task(app.stop())


async def run_app(app: Application) -> None:
    async with app:
        loop = asyncio.get_event_loop()
        fut = asyncio.create_task(app.consume_forever())
        for signame in {"SIGINT", "SIGTERM"}:
            loop.add_signal_handler(getattr(signal, signame), partial(_sig_handler, app))
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
        print(f"Running kafkaesk consumer {app}")
        asyncio.run(run_app(app))
    except asyncio.CancelledError:  # pragma: no cover
        logger.debug("Closing because task was exited")
