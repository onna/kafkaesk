from .exceptions import ConsumerUnhealthyException
from .exceptions import HandlerTaskCancelled
from .exceptions import StopConsumer
from .exceptions import UnhandledMessage

import aiokafka
import asyncio
import fnmatch
import functools
import inspect
import logging
import opentracing
import orjson
import pydantic
import typing

if typing.TYPE_CHECKING:  # pragma: no cover
    from .app import Application
else:
    Application = None


logger = logging.getLogger(__name__)


def _pydantic_msg_handler(
    model: typing.Type[pydantic.BaseModel], record: aiokafka.ConsumerRecord
) -> pydantic.BaseModel:
    data: typing.Dict[str, typing.Any] = orjson.loads(record.value)

    try:
        return model.parse_obj(data["data"])
    except pydantic.ValidationError:
        # log the execption so we can see what fields failed
        logger.warning(f"Error parsing pydantic model:{model} {record}", exc_info=True)
        raise UnhandledMessage(f"Error parsing data: {model}")


def _raw_msg_handler(record: aiokafka.structs.ConsumerRecord) -> typing.Dict[str, typing.Any]:
    data: typing.Dict[str, typing.Any] = orjson.loads(record.value)
    return data


def _bytes_msg_handler(record: aiokafka.structs.ConsumerRecord) -> bytes:
    return record.value


def _record_msg_handler(record: aiokafka.structs.ConsumerRecord) -> aiokafka.structs.ConsumerRecord:
    return record


def build_handler(coro: typing.Callable, app: "Application") -> typing.Callable:
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
    kwargs: typing.Dict[str, typing.Any] = {}

    for key, param in it:
        if key == "schema":
            kwargs["schema"] = None
        elif key == "record":
            kwargs["record"] = None
        elif key == "app":
            kwargs["app"] = app
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


class ConsumerThread(aiokafka.ConsumerRebalanceListener):
    _close: typing.Optional[asyncio.Future]
    _consumer: aiokafka.AIOKafkaConsumer
    _offsets: typing.Dict[aiokafka.TopicPartition, int]
    _message_handler: typing.Callable
    _initialized: bool
    _running: bool = False

    def __init__(
        self,
        stream_id: str,
        group_id: str,
        coro: typing.Callable,
        app: "Application",
        event_handlers: typing.Optional[typing.Dict[str, typing.List[typing.Callable]]] = None,
        concurrency: int = 1,
        timeout_seconds: float = None,
    ):
        self._initialized = False
        self.stream_id = stream_id
        self.group_id = group_id
        self._coro = coro
        self._event_handlers = event_handlers or {}
        # By default "1". Analog to the sequential version
        self._concurrency = concurrency
        self._timeout = timeout_seconds
        self._close = None
        self._app = app
        self._message_handler = build_handler(coro, app)  # type: ignore

    async def __call__(self) -> None:
        if not self._initialized:
            await self.initialize()

        try:
            while not self._close:
                try:
                    if not self._consumer.assignment():
                        await asyncio.sleep(0)
                        continue
                    await self._consume()
                except aiokafka.errors.KafkaConnectionError:
                    # We retry
                    await asyncio.sleep(0.5)
        except StopConsumer:
            logger.info("Consumer stopped, exiting")
        finally:
            await self.finalize()

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
        await self._maybe_create_topic()
        self._consumer = await self._consumer_factory()
        await self._consumer.start()
        self._initialized = True

    async def finalize(self) -> None:
        try:
            await self._consumer.stop()
        except Exception:
            logger.info("Could not commit on shutdown", exc_info=True)

        self._initialized = False
        self._running = False
        if self._close:
            self._close.set_result("done")

    async def _consumer_factory(self) -> aiokafka.AIOKafkaConsumer:
        consumer = self._app.consumer_factory(self.group_id)
        # This is needed in case we have a prefix
        pattern = fnmatch.translate(self._app.topic_mng.get_topic_id(self.stream_id))
        consumer.subscribe(pattern=pattern, listener=self)
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
        if batch:
            futures: typing.Dict[asyncio.Future[typing.Any], aiokafka.ConsumerRecord] = dict()
            await self._processing.acquire()

            for tp, records in batch.items():
                for record in records:
                    coro = self._handler(record)
                    fut = asyncio.create_task(coro)
                    futures[fut] = record

            done, pending = await asyncio.wait(
                futures.keys(), timeout=self._timeout, return_when=asyncio.FIRST_EXCEPTION
            )

            # Look for failures
            for task in done:
                if r := futures.pop(task, None):
                    try:
                        if exc := task.exception():
                            await self.on_handler_failed(exc, r)
                    except asyncio.CancelledError:
                        await self.on_handler_failed(HandlerTaskCancelled(), r)

            # Process timeout tasks
            for task in pending:
                if r := futures.pop(task, None):
                    try:
                        task.cancel()
                        await task
                    except asyncio.CancelledError:
                        pass
                    await self.on_handler_timeout(r)

            # Commit first and then call the event subscribers
            await self._maybe_commit()
            for _, records in batch.items():
                for record in records:
                    await self.emit("message", record=record)

            self._processing.release()

    async def _maybe_create_topic(self) -> None:
        # TBD: should we manage this here?
        return
        if not await self._app.topic_mng.topic_exists(self.stream_id):
            await self._app.topic_mng.create_topic(topic=self.stream_id)

    async def _maybe_commit(self) -> None:
        if not self._consumer.assignment:
            logger.warning("Cannot commit because no partitions are assigned!")
            return
        await self._consumer.commit()

    async def publish(self, stream_id: str, record: aiokafka.ConsumerRecord) -> None:
        # TODO: propagate the headers as well
        fut = await self._app.raw_publish(stream_id=stream_id, data=record.value, key=record.key)
        await fut

    async def healthy(self) -> None:
        if not self._running:
            raise ConsumerUnhealthyException(f"Consumer '{self}' is not running")

        if self._consumer is not None and not await self._consumer._client.ready(
            self._consumer._coordinator.coordinator_id
        ):
            raise ConsumerUnhealthyException(f"Consumer '{self}' is not ready")
        return

    # Event handlers
    async def on_partitions_revoked(self, revoked: typing.List[aiokafka.TopicPartition]) -> None:
        # Wait for the batch to end
        # TODO: We need to add a timeout and then cancel the batch tasks
        if revoked:
            async with self._processing:
                return

    async def on_partitions_assigned(self, assigned: typing.List[aiokafka.TopicPartition]) -> None:
        for tp in assigned:
            pass

    async def on_handler_timeout(self, record: aiokafka.ConsumerRecord) -> None:
        if self._timeout:
            slow_topic = f"{self.stream_id}-slow"
            await self.publish(slow_topic, record)

    async def on_handler_failed(
        self, exception: BaseException, record: aiokafka.ConsumerRecord
    ) -> None:
        if isinstance(exception, UnhandledMessage):
            logger.warning("Unhandled message, ignoring...", exc_info=exception)
        else:
            raise exception
