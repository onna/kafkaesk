from .exceptions import ConsumerUnhealthyException
from .exceptions import StopConsumer
from .exceptions import UnhandledMessage
from .metrics import CONSUMED_MESSAGE_TIME
from .metrics import CONSUMED_MESSAGES
from .metrics import CONSUMER_REBALANCED
from .metrics import CONSUMER_TOPIC_OFFSET
from .metrics import MESSAGE_LEAD_TIME
from .metrics import NOERROR
from .metrics import watch_kafka
from .retry import RetryHandler
from .retry import RetryPolicy
from functools import partial
from pydantic import BaseModel
from pydantic import ValidationError
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import TYPE_CHECKING

import aiokafka
import aiokafka.errors
import aiokafka.structs
import asyncio
import fnmatch
import inspect
import logging
import opentracing
import orjson
import time

if TYPE_CHECKING:  # pragma: no cover
    from .app import Application
else:
    Application = None

logger = logging.getLogger(__name__)


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


class MessageHandler:
    def __init__(self, subscription_consumer: "SubscriptionConsumer"):

        self._subscription_consumer = subscription_consumer
        handler = _raw_msg_handler
        sig = inspect.signature(self._subscription_consumer._subscription.func)
        param_name = [k for k in sig.parameters.keys()][0]
        annotation = sig.parameters[param_name].annotation
        if annotation and annotation != sig.empty:
            if annotation == bytes:
                handler = _bytes_msg_handler  # type: ignore
            elif annotation == aiokafka.structs.ConsumerRecord:
                handler = _record_msg_handler  # type: ignore
            else:
                handler = partial(_pydantic_msg_handler, annotation)  # type: ignore
        self._handler = handler

        it = iter(sig.parameters.items())
        next(it)
        kwargs: Dict[str, Any] = {}

        for key, param in it:
            if key == "schema":
                kwargs["schema"] = None
            elif key == "record":
                kwargs["record"] = None
            elif key == "app":
                kwargs["app"] = self._subscription_consumer._app
            elif key == "subscriber":
                kwargs["subscriber"] = self
            elif issubclass(param.annotation, opentracing.Span):
                kwargs[key] = opentracing.Span

        self._kwargs = kwargs

    async def handle(
        self, record: aiokafka.structs.ConsumerRecord, context: opentracing.Span
    ) -> None:
        msg_data = orjson.loads(record.value)
        data = self._handler(record, msg_data)
        handler_args = self._kwargs.copy()

        for key, param in handler_args.items():
            if key == "schema":
                handler_args["schema"] = msg_data["schema"]
            elif key == "record":
                handler_args["record"] = record
            elif param == opentracing.Span:
                handler_args[key] = context.span

        await self._subscription_consumer._subscription.func(data, **handler_args)


class SubscriptionConsumer:
    _consumer: Optional[aiokafka.AIOKafkaConsumer] = None
    _retry_policy: Optional[RetryPolicy] = None
    _handler: MessageHandler
    _close_fut: Optional[asyncio.Future] = None
    _running = False

    def __init__(
        self,
        app: Application,
        subscription: Subscription,
        event_handlers: Optional[Dict[str, List[Callable]]] = None,
    ):
        self._app = app
        self._subscription = subscription
        self._event_handlers = event_handlers or {}
        self._last_commit = time.monotonic()
        self._last_error = False
        self._needs_commit = False

    @property
    def consumer(self) -> aiokafka.AIOKafkaConsumer:
        if self._consumer is None:
            raise RuntimeError("Consumer not initialized")
        return self._consumer

    @property
    def retry_policy(self) -> RetryPolicy:
        if self._retry_policy is None:
            raise RuntimeError("Consumer not initialized")
        return self._retry_policy

    async def healthy(self) -> None:
        if self._consumer is None:
            return
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
            while self._close_fut is None:
                # if connection error, try reconnecting
                try:
                    await self._run()
                except aiokafka.errors.KafkaConnectionError:
                    logger.warning("Connection error, retrying", exc_info=True)
                    await asyncio.sleep(0.5)
        except (RuntimeError, asyncio.CancelledError, StopConsumer):
            logger.info("Consumer stopped, exiting", exc_info=True)
        finally:
            await self.finalize()

    async def initialize(self) -> None:
        self._running = True
        self._close_fut = None

        self._consumer = self._app.consumer_factory(self._subscription.group)
        pattern = fnmatch.translate(self._app.topic_mng.get_topic_id(self._subscription.stream_id))
        listener = CustomConsumerRebalanceListener(
            self._consumer, self._app, self._subscription.group
        )

        # Initialize subscribers retry policy
        self._retry_policy = RetryPolicy(
            app=self._app,
            subscription=self._subscription,
        )
        await self.retry_policy.initialize()

        self._handler = MessageHandler(self)

        self._consumer.subscribe(pattern=pattern, listener=listener)

        with watch_kafka("consumer_start"):
            await self._consumer.start()

    async def finalize(self) -> None:

        try:
            if not self._last_error:
                await self.consumer.commit()
        except Exception:
            logger.info("Could not commit on shutdown", exc_info=True)

        try:
            # kill retry policy now
            await self.retry_policy.finalize()
        except Exception:
            logger.info("Cound not properly stop retry policy", exc_info=True)

        try:
            await self.consumer.stop()
        except Exception:
            logger.warning("Could not properly stop consumer", exc_info=True)

        if self._close_fut is not None:
            # notify any stop handlers we're done here
            self._close_fut.set_result(None)

        self._close_fut = None
        self._running = False
        self._last_error = False
        self._consumer = None

    async def _maybe_commit(self) -> None:
        """
        Commit if we've eclipsed the time to commit next
        """
        interval = self._app.kafka_settings.get("auto_commit_interval_ms", 2000) / 1000
        if self._needs_commit and time.monotonic() > self._last_commit + interval:
            try:
                await self.consumer.commit()
            except aiokafka.errors.CommitFailedError:
                logger.warning("Error attempting to commit", exc_info=True)
            self._last_commit = time.monotonic()
            self._needs_commit = False

    async def _run(self) -> None:
        """
        Main entry point to consume messages
        """
        await self.emit("started", subscription_consumer=self)
        while self._close_fut is None:
            # only thing, except for an error to stop loop is for a _close_fut to be
            # created and waited on.
            # The finalize method will then handle this future
            try:
                record = await asyncio.wait_for(self.consumer.getone(), timeout=0.5)
            except asyncio.TimeoutError:
                await self._maybe_commit()
                continue
            await self._handle_message(record)

    async def _handle_message(self, record: aiokafka.structs.ConsumerRecord) -> None:
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

            with CONSUMED_MESSAGE_TIME.labels(
                stream_id=record.topic,
                partition=record.partition,
                group_id=self._subscription.group,
            ).time():
                await self._handler.handle(record, context)

            # No error metric
            CONSUMED_MESSAGES.labels(
                stream_id=record.topic,
                error=NOERROR,
                partition=record.partition,
                group_id=self._subscription.group,
            ).inc()
            self._last_error = False
            self._needs_commit = True
            await self._maybe_commit()
        except Exception as err:
            self._last_error = True
            CONSUMED_MESSAGES.labels(
                stream_id=record.topic,
                partition=record.partition,
                error=err.__class__.__name__,
                group_id=self._subscription.group,
            ).inc()
            await self.retry_policy(record=record, exception=err)
            # we didn't bubble, so no error here
            self._last_error = False
            self._needs_commit = True
            await self._maybe_commit()
        finally:
            context.close()
            await self.emit("message", record=record)

    async def stop(self) -> None:
        if not self._running:
            return

        if self._close_fut is None:
            # no need to lock on this
            loop = asyncio.get_running_loop()
            self._close_fut = loop.create_future()

        if not self._close_fut.done():
            await asyncio.wait([self._close_fut])


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
                stream_id=tp.topic,
                partition=tp.partition,
                group_id=self.group_id,
            ).inc()
            if tp not in starting_pos or starting_pos[tp].offset == -1:
                # detect if we've never consumed from this topic before
                # decision right now is to go back to beginning
                # and it's unclear if this is always right decision
                await self.consumer.seek_to_beginning(tp)
