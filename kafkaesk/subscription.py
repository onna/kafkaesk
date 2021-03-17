from .exceptions import ConsumerUnhealthyException
from .exceptions import HandlerTaskCancelled
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
        timeout_seconds: float = None,
        concurrency: int = None,
        retry_handlers: Optional[Dict[Type[Exception], RetryHandler]] = None,
    ):
        self.stream_id = stream_id
        self.func = func
        self.group = group
        self.retry_handlers = retry_handlers
        self.timeout = timeout_seconds
        self.concurrency = concurrency

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
    _slow_consumer: Optional[aiokafka.AIOKafkaConsumer] = None
    _retry_policy: Optional[RetryPolicy] = None
    _handler: MessageHandler
    _close_fut: Optional[asyncio.Future] = None
    _running = False
    _futures: Dict[Any, aiokafka.ConsumerRecord]
    _offsets: Dict[aiokafka.TopicPartition, int]

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
        self._initialized = False
        self._futures = dict()
        self._offsets = dict()

    @property
    def consumer(self) -> aiokafka.AIOKafkaConsumer:
        if self._consumer is None:
            raise RuntimeError("Consumer not initialized")
        return self._consumer

    @property
    def slow_consumer(self) -> aiokafka.AIOKafkaConsumer:
        if self._slow_consumer is None:
            raise RuntimeError("Slow consumer not initialized")
        return self._slow_consumer

    @property
    def retry_policy(self) -> RetryPolicy:
        if self._retry_policy is None:
            raise RuntimeError("Consumer not initialized")
        return self._retry_policy

    async def healthy(self) -> None:
        if not self._running:
            raise ConsumerUnhealthyException(
                self, f"Consumer '{self._subscription.stream_id}' is not running"
            )
        if self._consumer is not None and not await self._consumer._client.ready(
            self._consumer._coordinator.coordinator_id
        ):
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
        if not self._initialized:
            await self.initialize()

        try:
            while self._close_fut is None:
                # if connection error, try reconnecting
                try:
                    if self._subscription.concurrency:
                        await self._run_concurrent()
                    else:
                        await self._run_sequential()
                except aiokafka.errors.KafkaConnectionError:
                    logger.warning("Connection error, retrying", exc_info=True)
                    await asyncio.sleep(0.5)
        except (RuntimeError, asyncio.CancelledError, StopConsumer):
            logger.info("Consumer stopped, exiting", exc_info=True)
        except Exception as exc:
            logger.exception("Unmanaged consumer error, exiting", exc_info=exc)
            raise exc
        finally:
            await self.finalize()

    def consumer_factory(self, stream_id: str) -> aiokafka.AIOKafkaConsumer:
        consumer = self._app.consumer_factory(self._subscription.group)
        pattern = fnmatch.translate(self._app.topic_mng.get_topic_id(stream_id))
        listener = CustomConsumerRebalanceListener(
            consumer=consumer,
            subscription=self,
        )
        consumer.subscribe(pattern=pattern, listener=listener)
        return consumer

    async def initialize(self) -> None:
        self._running = True
        self._close_fut = None

        stream_id = self._subscription.stream_id
        self._consumer = self.consumer_factory(stream_id)
        self._slow_consumer = self.consumer_factory(f"{stream_id}-slow")
        # Initialize subscribers retry policy
        self._retry_policy = RetryPolicy(
            app=self._app,
            subscription=self._subscription,
        )
        await self.retry_policy.initialize()

        self._handler = MessageHandler(self)

        with watch_kafka("consumer_start"):
            await asyncio.wait([self._consumer.start(), self._slow_consumer.start()])
        self._initialized = True

    async def finalize(self) -> None:
        try:
            if not self._last_error:
                await self.consumer.commit()
                await self.slow_consumer.commit()
        except Exception:
            logger.info("Could not commit on shutdown", exc_info=True)

        try:
            # kill retry policy now
            await self.retry_policy.finalize()
        except Exception:
            logger.info("Cound not properly stop retry policy", exc_info=True)

        try:
            await asyncio.wait(
                [
                    self.consumer.stop(),
                    self.slow_consumer.stop(),
                ]
            )
        except Exception:
            logger.warning("Could not properly stop consumer", exc_info=True)

        if self._close_fut is not None:
            # notify any stop handlers we're done here
            self._close_fut.set_result(None)

        self._close_fut = None
        self._running = False
        self._last_error = False
        self._slow_consumer = None
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

    async def _run_sequential(self) -> None:
        """
        Main entry point to consume messages
        Note: to be replaced by concurrent consumers once is fully tested!
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

    async def _run_concurrent(self) -> None:
        # We run both the same consumer but with different timeouts
        # For the regular one we should have latencies below the selected timeout
        # Just a fraction of the records are expected to end in the slow topic.
        main = self._run_concurrent_inner(slow=False)
        slow = self._run_concurrent_inner(slow=True)
        await self.emit("started", subscription_consumer=self)
        await asyncio.gather(main, slow)

    async def _run_concurrent_inner(self, slow: bool = False) -> None:
        """
        Main entry point to consume messages
        """

        if slow:
            timeout = None
            consumer = self.slow_consumer
        else:
            timeout = self._subscription.timeout
            consumer = self.consumer

        while self._close_fut is None:
            data = await consumer.getmany(
                max_records=self._subscription.concurrency, timeout_ms=500
            )
            if data:
                for tp, records in data.items():
                    for record in records:
                        handler = self._handle_message(record, commit=False)
                        fut = asyncio.create_task(handler)
                        self._futures[fut] = record
                        self._offsets[tp] = record.offset + 1

                # Wait and check for errors
                done, pending = await asyncio.wait(
                    self._futures.keys(),
                    timeout=timeout,
                    return_when=asyncio.FIRST_EXCEPTION,
                )

                for done_task in done:
                    self._futures.pop(done_task, None)
                    # If something failed we want to hard fail and bring down the consumer
                    try:
                        # Beware!! `.exception()` call
                        # doesnt raise unless: `CancelledError` or task not done
                        # https://docs.python.org/3/library/asyncio-future.html#asyncio.Future.exception
                        if exc := done_task.exception():
                            raise exc
                    except asyncio.CancelledError:
                        continue

                # There is no point to resend to slow topic a task without timeout
                for timeout_task in pending:
                    # Do not block cancel as fast as possible!!
                    raw_record = self._futures.pop(timeout_task, None)
                    if not slow and raw_record:
                        timeout_task.cancel()
                        # And now send the record to the slow topic
                        await self.publish_to_slow_topic(raw_record)

            # Before asking for a new batch, lets persist the offsets
            try:
                offsets = {
                    tp: offset
                    for tp, offset in self._offsets.items()
                    if tp in consumer.assignment()
                }
                if offsets:
                    await consumer.commit(offsets)
            except aiokafka.errors.CommitFailedError:
                logger.exception("Problem commiting offsets.")

    async def publish_to_slow_topic(self, record: aiokafka.structs.ConsumerRecord) -> None:
        stream_id = f"{self._subscription.stream_id}-slow"
        data = record.value or b""
        key = record.key or b""
        fut = await self._app.raw_publish(
            stream_id=stream_id,
            data=data,
            key=key,
        )
        await fut

    async def _handle_message(
        self, record: aiokafka.structs.ConsumerRecord, commit: bool = True
    ) -> None:
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
            if commit:
                await self._maybe_commit()
        except BaseException as err:
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
            if commit:
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
    def __init__(
        self,
        consumer: aiokafka.AIOKafkaConsumer,
        subscription: SubscriptionConsumer,
    ):
        self.consumer = consumer
        self.subscription = subscription
        self.group_id = consumer._group_id
        self.app = subscription._app

    async def on_partitions_revoked(self, revoked: List[aiokafka.structs.TopicPartition]) -> None:
        logger.debug(f"Partitions revoked: {revoked}")
        if revoked:
            for fut, record in self.subscription._futures.items():
                tp = aiokafka.TopicPartition(record.topic, record.partition)
                if tp in revoked:
                    fut.cancel()

            for tp in revoked:
                self.subscription._offsets.pop(tp, None)

    async def on_partitions_assigned(self, assigned: List[aiokafka.structs.TopicPartition]) -> None:
        """This method will be called after partition
           re-assignment completes and before the consumer
           starts fetching data again.

        Arguments:
            assigned {TopicPartition} -- List of topics and partitions assigned
            to a given consumer.
        """
        logger.debug(f"Partitions assigned: {assigned}")

        for tp in assigned:
            CONSUMER_REBALANCED.labels(
                stream_id=tp.topic,
                partition=tp.partition,
                group_id=self.group_id,
            ).inc()
