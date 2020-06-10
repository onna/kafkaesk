from .exceptions import SchemaConflictException
from .exceptions import StopConsumer
from .exceptions import UnhandledMessage
from .exceptions import UnregisteredSchemaException
from .kafka import KafkaTopicManager
from .schema import SchemaManager
from .utils import resolve_dotted_name
from functools import partial
from pydantic import BaseModel
from pydantic import ValidationError
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
import fnmatch
import inspect
import logging
import orjson
import pydantic
import signal

logger = logging.getLogger("kafkaesk")


class Subscription:
    def __init__(
        self, stream_id: str, func: Callable, group: str, max_partitions: int, max_concurrency: int
    ):
        self.stream_id = stream_id
        self.func = func
        self.group = group
        self.max_partitions = max_partitions
        self.max_concurrency = max_concurrency

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


class SubscriptionConsumer:
    def __init__(
        self,
        app: "Application",
        subscription: Subscription,
        event_handlers: Optional[Dict[str, List[Callable]]] = None,
    ):
        self._app = app
        self._subscription = subscription
        self._event_handlers = event_handlers or {}

    async def emit(self, name: str, *args: Any, **kwargs: Any) -> None:
        for func in self._event_handlers.get(name, []):
            try:
                await func(*args, **kwargs)
            except StopConsumer:
                raise
            except Exception:
                logger.warning(f"Error emitting event: {name}: {func}")

    async def __call__(self) -> None:
        try:
            while True:
                await self.__run()
        except (RuntimeError, asyncio.CancelledError, StopConsumer):
            ...

    async def __run(self) -> None:
        consumer = aiokafka.AIOKafkaConsumer(
            bootstrap_servers=cast(List[str], self._app._kafka_servers),
            loop=asyncio.get_event_loop(),
            group_id=self._subscription.group,
            **self._app._kafka_settings or {},
        )
        pattern = fnmatch.translate(self._app.topic_mng.get_topic_id(self._subscription.stream_id))
        listener = CustomConsumerRebalanceListener(consumer, self._app, self._subscription.group)
        consumer.subscribe(pattern=pattern, listener=listener)
        await consumer.start()

        msg_handler = _raw_msg_handler
        sig = inspect.signature(self._subscription.func)
        param_name = [k for k in sig.parameters.keys()][0]
        annotation = sig.parameters[param_name].annotation
        if annotation:
            if annotation == bytes:
                msg_handler = _bytes_msg_handler  # type: ignore
            elif annotation == aiokafka.structs.ConsumerRecord:
                msg_handler = _record_msg_handler  # type: ignore
            else:
                msg_handler = partial(_pydantic_msg_handler, annotation)  # type: ignore

        await self.emit("started", subscription_consumer=self)
        try:
            # Consume messages
            async for record in consumer:
                try:
                    msg_data = orjson.loads(record.value)
                    it = iter(sig.parameters.keys())
                    kwargs: Dict[str, Any] = {next(it): msg_handler(record, msg_data)}
                    for key in it:
                        if key == "schema":
                            kwargs["schema"] = msg_data["schema"]
                        elif key == "record":
                            kwargs["record"] = record
                    await self._subscription.func(**kwargs)
                except UnhandledMessage:
                    # how should we handle this? Right now, fail hard
                    logger.warning(f"Could not process msg: {record}", exc_info=True)
                finally:
                    await self.emit("message", record=record)
        finally:
            try:
                await consumer.commit()
            except Exception:
                logger.info("Could not commit current offsets", exc_info=True)
            try:
                await consumer.stop()
            except Exception:
                logger.warning("Could not properly stop consumer", exc_info=True)


class Application:
    """
    Application configuration
    """

    _producer: Optional[aiokafka.AIOKafkaProducer]

    def __init__(
        self,
        kafka_servers: Optional[List[str]] = None,
        topic_prefix: str = "",
        kafka_settings: Optional[Dict[str, Any]] = None,
    ):
        self._subscriptions: List[Subscription] = []
        self._schemas: Dict[str, SchemaRegistration] = {}
        self._kafka_servers = kafka_servers
        self._kafka_settings = kafka_settings
        self._producer = None
        self._intialized = False
        self._locks: Dict[str, asyncio.Lock] = {}

        self._topic_prefix = topic_prefix
        self._topic_mng: Optional[KafkaTopicManager] = None
        self._schema_mng: Optional[SchemaManager] = None

    @property
    def topic_mng(self) -> KafkaTopicManager:
        if self._topic_mng is None:
            self._topic_mng = KafkaTopicManager(
                cast(List[str], self._kafka_servers), self._topic_prefix
            )
        return self._topic_mng

    @property
    def schema_mng(self) -> SchemaManager:
        if self._schema_mng is None:
            self._schema_mng = SchemaManager(
                cast(List[str], self._kafka_servers), topic_prefix=self._topic_prefix
            )
        return self._schema_mng

    def get_lock(self, name: str) -> asyncio.Lock:
        if name not in self._locks:
            self._locks[name] = asyncio.Lock()
        return self._locks[name]

    def configure(
        self,
        kafka_servers: Optional[List[str]] = None,
        topic_prefix: Optional[str] = None,
        kafka_settings: Optional[Dict[str, Any]] = None,
    ) -> None:
        if kafka_servers is not None:
            self._kafka_servers = kafka_servers
        if topic_prefix is not None:
            self._topic_prefix = topic_prefix
        if kafka_settings is not None:
            self._kafka_settings = kafka_settings

    async def publish(
        self, stream_id: str, data: BaseModel, key: Optional[bytes] = None
    ) -> Awaitable[aiokafka.structs.ConsumerRecord]:
        if not self._intialized:
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
                    retention_ms=reg.retention * 1000 if reg.retention is not None else None,
                )

        logger.info(f"Sending kafka msg: {stream_id}")

        producer = await self._get_producer()
        return await producer.send(
            topic_id, value=orjson.dumps({"schema": schema_key, "data": data_}), key=key
        )

    async def flush(self) -> None:
        if self._producer is not None:
            await self._producer.flush()

    def subscribe(
        self,
        stream_id: str,
        group: Optional[str] = None,
        max_partitions: int = 40,
        max_concurrency: int = 3,
    ) -> Callable:
        def inner(func: Callable) -> Callable:
            subscription = Subscription(
                stream_id,
                func,
                group or func.__name__,
                max_partitions=max_partitions,
                max_concurrency=max_concurrency,
            )
            self._subscriptions.append(subscription)
            return func

        return inner

    def get_schema_reg(self, model_or_def: BaseModel) -> SchemaRegistration:
        key = model_or_def.__key__  # type: ignore
        return self._schemas[key]

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

    async def _get_producer(self) -> aiokafka.AIOKafkaProducer:
        if self._producer is None:
            self._producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers=cast(List[str], self._kafka_servers),
                loop=asyncio.get_event_loop(),
            )
            await self._producer.start()
        return self._producer

    async def initialize(self) -> None:

        await self.schema_mng.initialize()

        for reg in self._schemas.values():
            await self.schema_mng.register(reg)

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

        self._intialized = True

    async def finalize(self) -> None:
        if self._producer is not None:
            await self._producer.flush()
            await self._producer.stop()
        await self.schema_mng.finalize()

        if self._topic_mng is not None:
            await self._topic_mng.finalize()

        self._producer = None
        self._intialized = False
        self._topic_mng = None
        self._schema_mng = None

        self._intialized = False

    async def __aenter__(self) -> "Application":
        await self.initialize()
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self.finalize()

    async def consume_for(self, num_messages: int, *, seconds: Optional[int] = None) -> None:
        consumers = []

        consumed = 0

        for subscription in self._subscriptions:

            async def on_message(record: aiokafka.structs.ConsumerRecord) -> None:
                nonlocal consumed
                consumed += 1
                if consumed >= num_messages:
                    raise StopConsumer

            consumer = SubscriptionConsumer(
                self, subscription, event_handlers={"message": [on_message]}
            )
            consumers.append(consumer)

        try:
            futures = [asyncio.create_task(c()) for c in consumers]
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

    async def consume_forever(self) -> None:
        consumers = []

        for subscription in self._subscriptions:
            consumer = SubscriptionConsumer(self, subscription)
            consumers.append(consumer)

        try:
            futures = [asyncio.create_task(c()) for c in consumers]
            await asyncio.gather(*futures)
        finally:
            for fut in futures:
                if not fut.done():
                    fut.cancel()


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
        for tp in assigned:
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


def _close_app(app: Application, fut: asyncio.Future) -> None:
    if not fut.done():
        fut.cancel()


async def __run_app(app: Application) -> None:
    async with app:
        loop = asyncio.get_event_loop()
        fut = asyncio.create_task(app.consume_forever())
        for signame in {"SIGINT", "SIGTERM"}:
            loop.add_signal_handler(getattr(signal, signame), partial(_close_app, app, fut))
        await fut


def run() -> None:
    opts = cli_parser.parse_args()
    module_str, attr = opts.app.split(":")
    module = resolve_dotted_name(module_str)
    app = getattr(module, attr)

    if callable(app):
        app = app()

    if opts.kafka_servers:
        app.configure(kafka_servers=opts.kafka_servers.split(","))
    if opts.kafka_settings:
        app.configure(kafka_settings=orjson.loads(opts.kafka_settings))
    if opts.topic_prefix:
        app.configure(topic_prefix=opts.topic_prefix)

    try:
        asyncio.run(__run_app(app))
    except asyncio.CancelledError:
        logger.info("Closing because task was exited")
