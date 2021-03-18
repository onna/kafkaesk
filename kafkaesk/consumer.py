from .exceptions import HandlerTaskCancelled
from .exceptions import StopConsumer
import aiokafka
import asyncio
import fnmatch
import functools
import inspect
import logging
import pydantic
import orjson
import opentracing
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
        logger.warning(f"Error parsing pydantic model:{model} {record}", exc_info=True)
        raise Exception()


def _raw_msg_handler(
    record: aiokafka.structs.ConsumerRecord
) -> typing.Dict[str, typing.Any]:
    data: typing.Dict[str, typing.Any] = orjson.loads(record.value)
    return data


def _bytes_msg_handler(record: aiokafka.structs.ConsumerRecord) -> bytes:
    return record.value


def _record_msg_handler(
    record: aiokafka.structs.ConsumerRecord
) -> aiokafka.structs.ConsumerRecord:
    return record


def build_handler(coro: typing.Callable, app: "Application") -> typing.Callable[[aiokafka.ConsumerRecord, opentracing.Span], None]:
    """Introspection on the coroutine signature to inject dependencies"""

    sig = inspect.signature(coro)
    param_name = [k for k in sig.parameters.keys()][0]
    annotation = sig.parameters[param_name].annotation
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
    _message_handler: typing.Callable[[aiokafka.ConsumerRecord, opentracing.Span], None]
    _initialized: bool
    _running: bool

    def __init__(self,
                 stream_id: str,
                 group_id: str,
                 coro: typing.Callable,
                 app: "Application",
                 event_handlers: typing.Optional[typing.Dict[str, typing.List[typing.Callable]]] = None,
                 concurrency: int = 1,
                 timeout_seconds: float = None):
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
        self._message_handler = build_handler(coro, app)

    async def __call__(self):
        if not self._initialized:
            await self.initialize()

        while not self._close:
            try:
                await self._consume_loop()
            except aiokafka.errors.KafkaConnectionError:
                # We retry
                await asyncio.sleep(.5)
            except StopConsumer:
                logger.info("Consumer stopped, exiting")
                break
            except Exception as err:
                raise err
        await self.finalize()

    async def emit(self, name: str, *args: typing.Any, **kwargs: typing.Any) -> None:
        for func in self._event_handlers.get(name, []):
            try:
                await func(*args, **kwargs)
            except StopConsumer:
                raise
            except Exception:
                logger.warning(f"Error emitting event: {name}: {func}", exc_info=True)

    async def initialize(self):
        self._close = None
        self._running = True
        self._processing = asyncio.Lock()
        await self._maybe_create_topic()
        self._consumer = await self._consumer_factory()
        await self._consumer.start()
        self._initialized = True

    async def finalize(self):
        await self._consumer.stop()
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

    def _span(self, record: aiokafka.ConsumerRecord):
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

    async def _handler(self, record: aiokafka.ConsumerRecord):
        with self._span(record) as span:
            try:
                await self._message_handler(record, span)
            except Exception as err:
                raise err
            except asyncio.CancelledError as err:
                raise HandlerTaskCancelled() from err
        await self.emit("message", record=record)

    async def _consume_loop(self):
        batch = await self._consumer.getmany(max_records=self._concurrency, timeout_ms=500)
        if batch:
            futures = dict()
            await self._processing.acquire()

            for tp, records in batch.items():
                for record in records:
                    coro = self._handler(record)
                    fut = asyncio.create_task(coro)
                    futures[fut] = record

            done, pending = await asyncio.wait(
                futures.keys(),
                timeout=self._timeout,
                return_when=asyncio.FIRST_EXCEPTION
            )

            # Look for failures
            for task in done:
                if exc := task.exception():
                    if r := futures.pop(task, None):
                        await self.on_handler_failed(exc, r)

            # Process timeout tasks
            for task in pending:
                if r := futures.pop(task, None):
                    try:
                        task.cancel()
                        await task
                    except asyncio.CancelledError:
                        pass
                    await self.on_handler_timeout(r)

            self._processing.release()
        await self._maybe_commit()

    async def _maybe_create_topic(self):
        # TBD: should we manage this here?
        return
        if not await self._app.topic_mng.topic_exists(self.stream_id):
            await self._app.topic_mng.create_topic(
                topic=self.stream_id
            )

    async def _maybe_commit(self):
        await self._consumer.commit()

    async def publish(self, stream_id: str, record: aiokafka.ConsumerRecord) -> None:
        # TODO: propagate the headers as well
        fut = await self._app.raw_publish(stream_id=stream_id,
                                          data=record.value,
                                          key=record.key)
        await fut

    async def healthy(self):
        # TODO: implement this logic
        return

    # Event handlers
    async def on_partitions_revoked(self, revoked: typing.List[aiokafka.TopicPartition]) -> None:
        # Wait for the batch to end
        # TODO: We need to add a timeout and then cancel the batch tasks
        async with self._processing:
            return

    async def on_partitions_assigned(self, assigned: typing.List[aiokafka.TopicPartition]) -> None:
        pass

    async def on_handler_timeout(self, record: aiokafka.ConsumerRecord) -> None:
        if self._timeout:
            slow_topic = f"{self.stream_id}-slow"
            await self.publish(slow_topic, record)

    async def on_handler_failed(self, exception, record):
        raise exception
