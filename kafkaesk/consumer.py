from .exceptions import ConsumerUnhealthyException
from .exceptions import HandlerTaskCancelled
from .exceptions import StopConsumer
from .exceptions import UnhandledMessage
from .metrics import CONSUMED_MESSAGE_TIME
from .metrics import CONSUMED_MESSAGES
from .metrics import CONSUMED_MESSAGES_BATCH_SIZE
from .metrics import CONSUMER_HEALTH
from .metrics import CONSUMER_REBALANCED
from .metrics import CONSUMER_TOPIC_OFFSET
from .metrics import MESSAGE_LEAD_TIME
from .metrics import NOERROR
from kafka.structs import TopicPartition

import aiokafka
import asyncio
import fnmatch
import functools
import inspect
import logging
import opentracing
import orjson
import pydantic
import time
import typing

if typing.TYPE_CHECKING:  # pragma: no cover
    from .app import Application
else:
    Application = None


logger = logging.getLogger(__name__)


class Subscription:
    def __init__(
        self,
        consumer_id: str,
        func: typing.Callable,
        group: str,
        *,
        pattern: typing.Optional[str] = None,
        topics: typing.Optional[typing.List[str]] = None,
        timeout_seconds: float = 0.0,
        concurrency: int = None,
    ):
        self.consumer_id = consumer_id
        self.pattern = pattern
        self.topics = topics
        self.func = func
        self.group = group
        self.timeout = timeout_seconds
        self.concurrency = concurrency

    def __repr__(self) -> str:
        return f"<Subscription stream: {self.consumer_id} >"


def _pydantic_msg_handler(
    model: typing.Type[pydantic.BaseModel], record: aiokafka.ConsumerRecord
) -> pydantic.BaseModel:
    try:
        data: typing.Dict[str, typing.Any] = orjson.loads(record.value)
        return model.parse_obj(data["data"])
    except orjson.JSONDecodeError:
        # log the execption so we can see what fields failed
        logger.warning(f"Payload is not valid json: {record}", exc_info=True)
        raise UnhandledMessage("Error deserializing json")
    except pydantic.ValidationError:
        # log the execption so we can see what fields failed
        logger.warning(f"Error parsing pydantic model:{model} {record}", exc_info=True)
        raise UnhandledMessage(f"Error parsing data: {model}")
    except Exception:
        # Catch all
        logger.warning(f"Error parsing payload: {model} {record}", exc_info=True)
        raise UnhandledMessage("Error parsing payload")


def _raw_msg_handler(record: aiokafka.structs.ConsumerRecord) -> typing.Dict[str, typing.Any]:
    data: typing.Dict[str, typing.Any] = orjson.loads(record.value)
    return data


def _bytes_msg_handler(record: aiokafka.structs.ConsumerRecord) -> bytes:
    return record.value


def _record_msg_handler(record: aiokafka.structs.ConsumerRecord) -> aiokafka.structs.ConsumerRecord:
    return record


def build_handler(
    coro: typing.Callable, app: "Application", consumer: "BatchConsumer"
) -> typing.Callable:
    """Introspection on the coroutine signature to inject dependencies"""
    sig = inspect.signature(coro)
    param_name = [k for k in sig.parameters.keys()][0]
    annotation = sig.parameters[param_name].annotation
    handler = _raw_msg_handler
    if annotation and annotation != sig.empty:
        if annotation == bytes:
            handler = _bytes_msg_handler  # type: ignore
        elif annotation == aiokafka.ConsumerRecord:
            handler = _record_msg_handler  # type: ignore
        else:
            handler = functools.partial(_pydantic_msg_handler, annotation)  # type: ignore

    it = iter(sig.parameters.items())
    # first argument is required and its the payload
    next(it)
    kwargs: typing.Dict[str, typing.Any] = getattr(coro, "__extra_kwargs__", {})

    for key, param in it:
        if key == "schema":
            kwargs["schema"] = None
        elif key == "record":
            kwargs["record"] = None
        elif key == "app":
            kwargs["app"] = app
        elif key == "subscriber":
            kwargs["subscriber"] = consumer
        elif issubclass(param.annotation, opentracing.Span):
            kwargs[key] = opentracing.Span

    async def inner(record: aiokafka.ConsumerRecord, span: opentracing.Span) -> None:
        data = handler(record)
        deps = kwargs.copy()

        for key, param in kwargs.items():
            if key == "schema":
                msg = orjson.loads(record.value)
                deps["schema"] = msg["schema"]
            elif key == "record":
                deps["record"] = record
            elif param == opentracing.Span:
                deps[key] = span

        await coro(data, **deps)

    return inner


class BatchConsumer(aiokafka.ConsumerRebalanceListener):
    _subscription: Subscription
    _close: typing.Optional[asyncio.Future] = None
    _consumer: aiokafka.AIOKafkaConsumer
    _offsets: typing.Dict[aiokafka.TopicPartition, int]
    _message_handler: typing.Callable
    _initialized: bool
    _running: bool = False

    def __init__(
        self,
        subscription: Subscription,
        app: "Application",
        event_handlers: typing.Optional[typing.Dict[str, typing.List[typing.Callable]]] = None,
        auto_commit: bool = True,
    ):
        self._initialized = False
        self.stream_id = subscription.consumer_id
        self.group_id = subscription.group
        self._coro = subscription.func
        self._event_handlers = event_handlers or {}
        self._concurrency = subscription.concurrency or 1
        self._timeout = subscription.timeout
        self._subscription = subscription
        self._close = None
        self._app = app
        self._last_commit = 0.0
        self._auto_commit = auto_commit
        self._tp: typing.Dict[aiokafka.TopicPartition, int] = {}

        # We accept either pattern or a list of topics, also we might accept a single topic
        # to keep compatibility with older API
        self.pattern = subscription.pattern
        self.topics = subscription.topics

    async def __call__(self) -> None:
        if not self._initialized:
            await self.initialize()

        try:
            while not self._close:
                try:
                    if not self._consumer.assignment():
                        await asyncio.sleep(2)
                        continue
                    await self._consume()
                except aiokafka.errors.KafkaConnectionError:
                    # We retry
                    self._health_metric(False)
                    logger.info(f"Consumer {self} kafka connection error, retrying...")
                    await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            self._health_metric(False)
        except StopConsumer:
            self._health_metric(False)
            logger.info(f"Consumer {self} stopped, exiting")
        except BaseException as exc:
            logger.exception(f"Consumer {self} failed. Finalizing.", exc_info=exc)
            self._health_metric(False)
            raise
        finally:
            await self.finalize()

    def _health_metric(self, healthy: bool) -> None:
        CONSUMER_HEALTH.labels(
            group_id=self.group_id,
        ).set(healthy)

    async def emit(self, name: str, *args: typing.Any, **kwargs: typing.Any) -> None:
        for func in self._event_handlers.get(name, []):
            try:
                await func(*args, **kwargs)
            except StopConsumer:
                raise
            except Exception:
                logger.warning(f"Error emitting event: {name}: {func}", exc_info=True)

    async def initialize(self) -> None:
        self._close = None
        self._running = True
        self._processing = asyncio.Lock()
        self._consumer = await self._consumer_factory()
        await self._consumer.start()
        self._message_handler = build_handler(self._coro, self._app, self)  # type: ignore
        self._initialized = True

    async def finalize(self) -> None:
        try:
            await self._consumer.stop()
        except Exception:
            logger.info(f"[{self}] Could not commit on shutdown", exc_info=True)

        self._initialized = False
        self._running = False
        if self._close:
            self._close.set_result("done")

    async def _consumer_factory(self) -> aiokafka.AIOKafkaConsumer:
        consumer = self._app.consumer_factory(self.group_id)

        if self.pattern and self.topics:
            raise AssertionError(
                "Both of the params 'pattern' and 'topics' are not allowed. Select only one mode."
            )  # noqa

        if self.pattern:
            # This is needed in case we have a prefix
            topic_id = self._app.topic_mng.get_topic_id(self.pattern)

            if "*" in self.pattern:
                pattern = fnmatch.translate(topic_id)
                consumer.subscribe(pattern=pattern, listener=self)  # type: ignore
            else:
                consumer.subscribe(topics=[topic_id], listener=self)  # type: ignore
        elif self.topics:
            topics = [self._app.topic_mng.get_topic_id(topic) for topic in self.topics]
            consumer.subscribe(topics=topics, listener=self)  # type: ignore
        else:
            raise ValueError("Either `topics` or `pattern` should be defined")

        return consumer

    async def stop(self) -> None:
        if not self._running:
            return

        # Exit the loop, this will trigger finalize call
        loop = asyncio.get_running_loop()
        self._close = loop.create_future()
        await asyncio.wait([self._close])

    def __repr__(self) -> str:
        return f"<Consumer: {self.stream_id}, Group: {self.group_id}>"

    def _span(self, record: aiokafka.ConsumerRecord) -> opentracing.SpanContext:
        tracer = opentracing.tracer
        headers = {x[0]: x[1].decode() for x in record.headers or []}
        parent = tracer.extract(opentracing.Format.TEXT_MAP, headers)
        context = tracer.start_active_span(
            record.topic,
            tags={
                "message_bus.destination": record.topic,
                "message_bus.partition": record.partition,
                "message_bus.group_id": self.group_id,
            },
            references=[opentracing.follows_from(parent)],
        )
        return context.span

    async def _handler(self, record: aiokafka.ConsumerRecord) -> None:
        with self._span(record) as span:
            await self._message_handler(record, span)

    async def _consume(self) -> None:
        batch = await self._consumer.getmany(max_records=self._concurrency, timeout_ms=500)

        async with self._processing:
            if not batch:
                await self._maybe_commit()
            else:
                await self._consume_batch(batch)

    async def _consume_batch(
        self, batch: typing.Dict[TopicPartition, typing.List[aiokafka.ConsumerRecord]]
    ) -> None:
        futures: typing.Dict[asyncio.Future[typing.Any], aiokafka.ConsumerRecord] = dict()
        for tp, records in batch.items():
            for record in records:
                coro = self._handler(record)
                fut = asyncio.create_task(coro)
                futures[fut] = record

        # TODO: this metric is kept for backwards-compatibility, but should be revisited
        with CONSUMED_MESSAGE_TIME.labels(
            stream_id=self.stream_id,
            partition=next(iter(batch)),
            group_id=self.group_id,
        ).time():
            done, pending = await asyncio.wait(
                futures.keys(),
                timeout=self._timeout,
                return_when=asyncio.FIRST_EXCEPTION,
            )

        # Look for failures
        for task in done:
            record = futures[task]
            tp = aiokafka.TopicPartition(record.topic, record.partition)

            # Get the largest offset of the batch
            current_max = self._tp.get(tp)
            if not current_max:
                self._tp[tp] = record.offset + 1
            else:
                self._tp[tp] = max(record.offset + 1, current_max)

            try:
                if exc := task.exception():
                    self._count_message(record, error=exc.__class__.__name__)
                    await self.on_handler_failed(exc, record)
                else:
                    self._count_message(record)
            except asyncio.InvalidStateError:
                # Task didnt finish yet, we shouldnt be here since we are
                # iterating the `done` list, so just log something
                logger.warning(f"Trying to get exception from unfinished task. Record: {record}")
            except asyncio.CancelledError:
                # During task execution any exception will be returned in
                # the `done` list. But timeout exception should be captured
                # independendly, thats why we handle this condition here.
                self._count_message(record, error="cancelled")
                await self.on_handler_failed(HandlerTaskCancelled(record), record)

        # Process timeout tasks
        for task in pending:
            record = futures[task]

            try:
                # This will raise a `asyncio.CancelledError`, the consumer logic
                # is responsible to catch it.
                task.cancel()
                await task
            except asyncio.CancelledError:
                # App didnt catch this exception, so we treat it as an unmanaged one.
                await self.on_handler_timeout(record)

            self._count_message(record, error="pending")

        for tp, records in batch.items():
            CONSUMED_MESSAGES_BATCH_SIZE.labels(
                stream_id=tp.topic,
                group_id=self.group_id,
                partition=tp.partition,
            ).observe(len(records))

            for record in sorted(records, key=lambda rec: rec.offset):
                lead_time = time.time() - record.timestamp / 1000  # type: ignore
                MESSAGE_LEAD_TIME.labels(
                    stream_id=record.topic,
                    group_id=self.group_id,
                    partition=record.partition,
                ).observe(lead_time)

                CONSUMER_TOPIC_OFFSET.labels(
                    stream_id=record.topic,
                    group_id=self.group_id,
                    partition=record.partition,
                ).set(record.offset)

        # Commit first and then call the event subscribers
        await self._maybe_commit()
        for _, records in batch.items():
            for record in records:
                await self.emit("message", record=record)

    def _count_message(self, record: aiokafka.ConsumerRecord, error: str = NOERROR) -> None:
        CONSUMED_MESSAGES.labels(
            stream_id=record.topic,
            error=error,
            partition=record.partition,
            group_id=self.group_id,
        ).inc()

    @property
    def consumer(self) -> aiokafka.AIOKafkaConsumer:
        return self._consumer

    async def _maybe_commit(self, forced: bool = False) -> None:
        if not self._auto_commit:
            return

        if not self._consumer.assignment() or not self._tp:
            logger.warning("Cannot commit because no partitions are assigned!")
            return

        interval = self._app.kafka_settings.get("auto_commit_interval_ms", 5000) / 1000
        now = time.time()
        if forced or (now > (self._last_commit + interval)):
            try:
                if self._tp:
                    await self._consumer.commit(offsets=self._tp)
            except aiokafka.errors.CommitFailedError:
                logger.warning("Error attempting to commit", exc_info=True)
            self._last_commit = now

    async def publish(
        self,
        stream_id: str,
        record: aiokafka.ConsumerRecord,
        headers: typing.Optional[typing.List[typing.Tuple[str, bytes]]] = None,
    ) -> None:
        record_headers = (record.headers or []) + (headers or [])

        fut = await self._app.raw_publish(
            stream_id=stream_id, data=record.value, key=record.key, headers=record_headers
        )
        await fut

    async def healthy(self) -> None:
        if not self._running:
            self._health_metric(False)
            raise ConsumerUnhealthyException(f"Consumer '{self}' is not running")

        if self._consumer is not None and not await self._consumer._client.ready(
            self._consumer._coordinator.coordinator_id
        ):
            self._health_metric(False)
            raise ConsumerUnhealthyException(f"Consumer '{self}' is not ready")

        self._health_metric(True)
        return

    # Event handlers
    async def on_partitions_revoked(self, revoked: typing.List[aiokafka.TopicPartition]) -> None:
        if revoked:
            # Wait for the current batch to be processed
            async with self._processing:
                if self._auto_commit:
                    # And commit before releasing the partitions.
                    await self._maybe_commit(forced=True)

                for tp in revoked:
                    # Remove the partition from the dict
                    self._tp.pop(tp, None)
                    CONSUMER_REBALANCED.labels(
                        partition=tp.partition,
                        group_id=self.group_id,
                        event="revoked",
                    ).inc()
            logger.info(f"Partitions revoked to {self}: {revoked}")

    async def on_partitions_assigned(self, assigned: typing.List[aiokafka.TopicPartition]) -> None:
        if assigned:
            logger.info(f"Partitions assigned to {self}: {assigned}")

        for tp in assigned:
            position = await self._consumer.position(tp)
            self._tp[tp] = position

            CONSUMER_REBALANCED.labels(
                partition=tp.partition,
                group_id=self.group_id,
                event="assigned",
            ).inc()

    async def on_handler_timeout(self, record: aiokafka.ConsumerRecord) -> None:
        raise HandlerTaskCancelled(record)

    async def on_handler_failed(
        self, exception: BaseException, record: aiokafka.ConsumerRecord
    ) -> None:
        if isinstance(exception, UnhandledMessage):
            logger.warning("Unhandled message, ignoring...", exc_info=exception)
        else:
            raise exception
