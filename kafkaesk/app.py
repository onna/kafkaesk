import asyncio
import argparse
import fnmatch
import inspect
import logging
from typing import Any, Dict, List, Optional, Type, Union

import orjson
import pydantic
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener
from aiokafka.errors import IllegalStateError
from aiokafka.structs import TopicPartition
from pydantic import BaseModel

from .exceptions import (
    SchemaConflictException,
    StopConsumer,
    UnhandledMessage,
    UnregisteredSchemaException,
)
from .kafka import KafkaTopicManager
from .schema import SchemaManager
from functools import partial
from pydantic import ValidationError

logger = logging.getLogger("kafkaesk")


class Subscription:
    def __init__(self, stream_id: str, func, group: str):
        self.stream_id = stream_id
        self.func = func
        self.group = group

    def __repr__(self):
        return f"<Subscription stream: {self.stream_id} >"


class SchemaRegistration:
    def __init__(
        self,
        id: str,
        version: int,
        model: Optional[Type[pydantic.BaseModel]] = None,
        retention: Optional[int] = None,
        max_partitions: Optional[int] = None,
        streams: Optional[List[str]] = None,
    ):
        self.id = id
        self.version = version
        self.model = model
        self.retention = retention
        self.max_partitions = max_partitions
        self.streams = streams

    def __repr__(self):
        return f"<SchemaRegistration id: {self.id}, version: {self.version} >"


def _default_parser(v):
    return v


def _pydantic_parser(model, v):
    try:
        data = model.parse_obj(v)
    except ValidationError:
        raise UnhandledMessage(f"Error parsing data: {model}")
    return data


async def _default_handler(func, parser, msg):
    payload = orjson.loads(msg.value)
    data = parser(payload["data"])
    await func(data)


class SubscriptionConsumer:
    def __init__(
        self, app: "Application", subscription: Subscription, handler=_default_handler
    ):
        self._app = app
        self._subscription = subscription
        self._handler = handler

    async def __call__(self):
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._app._kafka_servers,
            loop=asyncio.get_event_loop(),
            group_id=self._subscription.group,
            **self._app._kafka_settings or {},
        )
        pattern = fnmatch.translate(
            self._app.topic_mng.get_topic_id(self._subscription.stream_id)
        )
        listener = CustomConsumerRebalanceListener(consumer)
        consumer.subscribe(pattern=pattern, listener=listener)
        await consumer.start()

        parser = _default_parser
        sig = inspect.signature(self._subscription.func)
        if "data" in sig.parameters:
            annotation = sig.parameters["data"].annotation
            if annotation:
                parser = partial(_pydantic_parser, annotation)

        try:
            # Consume messages
            async for msg in consumer:
                try:
                    await self._handler(self._subscription.func, parser, msg)
                except UnhandledMessage:
                    # how should we handle this? Right now, fail hard
                    logger.warning(f"Could not process msg: {msg}", exc_info=True)
                except StopConsumer:
                    return
        except (RuntimeError, asyncio.CancelledError):
            ...
        finally:
            try:
                await consumer.stop()
            except Exception:
                logger.warning("Could not properly stop consumer")


class Application:
    """
    Application configuration
    """

    _producer: Optional[AIOKafkaProducer]

    def __init__(
        self,
        kafka_servers: Optional[List[str]] = None,
        topic_prefix="",
        kafka_settings=None,
    ):
        self._subscriptions = []
        self._schemas = {}
        self._kafka_servers = kafka_servers
        self._kafka_settings = kafka_settings
        self._producer = None
        self._intialized = False
        self._locks = {}

        self._topic_prefix = topic_prefix
        self._topic_mng = None
        self._schema_mng = None

    @property
    def topic_mng(self):
        if self._topic_mng is None:
            self._topic_mng = KafkaTopicManager(self._kafka_servers, self._topic_prefix)
        return self._topic_mng

    @property
    def schema_mng(self):
        if self._schema_mng is None:
            self._schema_mng = SchemaManager(
                self._kafka_servers, topic_prefix=self._topic_prefix
            )
        return self._schema_mng

    def get_lock(self, name: str) -> asyncio.Lock:
        if name not in self._locks:
            self._locks[name] = asyncio.Lock()
        return self._locks[name]

    def configure(
        self,
        kafka_servers: Optional[List[str]] = None,
        topic_prefix=None,
        kafka_settings=None,
    ):
        if kafka_servers is not None:
            self._kafka_servers = kafka_servers
        if topic_prefix is not None:
            self._topic_prefix = topic_prefix
        if kafka_settings is not None:
            self._kafka_settings = kafka_settings

    async def publish(
        self, stream_id: str, data: BaseModel, key: Optional[bytes] = None,
    ):
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
                    retention_ms=reg.retention * 1000
                    if reg.retention is not None
                    else None,
                )

        logger.info(f"Sending kafka msg: {stream_id}")
        await self._producer.send(
            topic_id, value=orjson.dumps({"schema": schema_key, "data": data_}), key=key
        )

    async def flush(self):
        if self._producer is not None:
            await self._producer.flush()

    def subscribe(self, stream_id: str, group: Optional[str] = None):
        def inner(func):
            subscription = Subscription(stream_id, func, group or func.__name__)
            self._subscriptions.append(subscription)
            return func

        return inner

    def get_schema_reg(
        self, model_or_def: Union[Dict[str, Any], BaseModel]
    ) -> SchemaRegistration:
        if isinstance(model_or_def, BaseModel):
            key = model_or_def.__key__
        else:
            key = model_or_def["x-schema"]
        return self._schemas[key]

    def schema(
        self,
        _id: Optional[str],
        *,
        version: Optional[int] = None,
        retention: Optional[int] = None,
        streams: Optional[List[str]] = None,
    ):
        version = version or 1

        def inner(cls):
            key = f"{_id}:{version}"
            reg = SchemaRegistration(
                id=_id,
                version=version or 1,
                model=cls,
                retention=retention,
                streams=streams,
            )
            if key in self._schemas:
                raise SchemaConflictException(self._schemas[key], reg)
            cls.__key__ = key
            self._schemas[key] = reg
            return cls

        return inner

    async def initialize(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._kafka_servers, loop=asyncio.get_event_loop()
        )
        await self._producer.start()

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

    async def finalize(self):
        if self._producer is not None:
            await self._producer.flush()
            await self._producer.stop()
        await self.schema_mng.finalize()
        self._intialized = False

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.finalize()

    async def consume_for(
        self, num_messages: int, *, seconds: Optional[int] = None
    ) -> None:
        consumers = []

        for subscription in self._subscriptions:

            consumed = 0

            async def _handler(func, parser, msg):
                nonlocal consumed
                result = await _default_handler(func, parser, msg)
                consumed += 1
                if consumed >= num_messages:
                    raise StopConsumer
                return result

            consumer = SubscriptionConsumer(self, subscription, _handler)
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


class CustomConsumerRebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer: AIOKafkaConsumer):
        self.consumer = consumer

    async def on_partitions_revoked(self, revoked: List[TopicPartition]) -> None:
        ...

    async def on_partitions_assigned(self, assigned: List[TopicPartition]) -> None:
        """This method will be called after partition
           re-assignment completes and before the consumer
           starts fetching data again.

        Arguments:
            assigned {TopicPartition} -- List of topics and partitions assigned
            to a given consumer.
        """
        for tp in assigned:
            try:
                position = await self.consumer.position(tp)
                offset = position - 1
            except IllegalStateError:
                offset = -1

            if offset > 0:
                self.consumer.seek(tp, offset)
            else:
                await self.consumer.seek_to_beginning(tp)


cli_parser = argparse.ArgumentParser(description="Run kafkaesk worker.")
cli_parser.add_argument("app", help="Application object")
cli_parser.add_argument("--kafka-servers", help="Kafka servers")
cli_parser.add_argument("--kafka-settings", help="Kafka settings")
cli_parser.add_argument("--topic-prefix", help="Topic prefix")


async def __run_app(app):
    async with app:
        await app.consume_forever()


def run():
    opts = cli_parser.parse_args()
    module_str, attr = opts.app.split(":")
    module = __import__(module_str)
    app = getattr(module, attr)

    if callable(app):
        app = app()

    if opts.kafka_servers:
        app.configure(kafka_servers=opts.kafka_servers.split(","))
    if opts.kafka_settings:
        app.configure(kafka_settings=orjson.loads(opts.kafka_settings))
    if opts.topic_prefix:
        app.configure(topic_prefix=opts.topic_prefix)

    asyncio.run(__run_app(app))
