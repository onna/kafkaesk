from .formatter import PydanticLogModel
from .record import PydanticLogRecord
from pydantic import BaseModel
from typing import IO
from typing import Optional
from typing import Tuple

import asyncio
import kafkaesk
import logging
import sys


class InvalidLogFormat(Exception):
    ...


class PydanticStreamHandler(logging.StreamHandler):
    def __init__(self, stream: Optional[IO[str]] = None):
        super().__init__(stream=stream)

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)

        if isinstance(message, BaseModel):
            message = message.json()

        return message


class KafkaeskQueue:
    def __init__(
        self, app: kafkaesk.app.Application, max_queue: int = 10000,
    ):
        self._queue: asyncio.Queue[Tuple[str, BaseModel]] = asyncio.Queue(maxsize=max_queue)
        self._app = app

        self._app.on("finalize", self.flush)

        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.get_event_loop().create_task(self._run())

    def close(self) -> None:
        if self._task is not None and not self._task._loop.is_closed():
            if not self._task.done() and not self._task.cancelled():
                self._task.cancel()

    @property
    def running(self) -> bool:
        if self._task is None:
            return False

        if self._task.done():
            return False

        return True

    async def _run(self) -> None:
        while True:
            try:
                stream, message = await asyncio.wait_for(self._queue.get(), 1)
                await self._publish(stream, message)

            except asyncio.TimeoutError:
                continue

            except asyncio.CancelledError:
                await self.flush()
                return

    async def flush(self) -> None:
        while not self._queue.empty():
            stream, message = await self._queue.get()
            await self._publish(stream, message)

    async def _publish(self, stream: str, message: BaseModel) -> None:
        if self._app._intialized:
            try:
                await self._app.publish(stream, message)
            except kafkaesk.exceptions.UnregisteredSchemaException:
                self._print_to_stderr(message, "Log schema is not registered")
            # TODO: Handle other Kafak errors that may be raised
        else:
            self._print_to_stderr(message, "Kafkaesk application is not initialized")

    def _print_to_stderr(self, message: BaseModel, error: str) -> None:
        sys.stderr.write(f"Error sending log to Kafak: \n{error}\nMessage: {message.json()}")

    def put_nowait(self, stream: str, message: PydanticLogModel) -> None:
        self._queue.put_nowait((stream, message))


class PydanticKafkaeskHandler(logging.Handler):
    def __init__(self, app: kafkaesk.Application, stream: str):
        self.app = app
        self.stream = stream

        self._queue = KafkaeskQueue(self.app)

        self._initialize_model()

        super().__init__()

    def _initialize_model(self) -> None:
        try:
            self.app.schema("PydanticLogModel")(PydanticLogModel)
        except kafkaesk.app.SchemaConflictException:
            pass

    def emit(self, record: PydanticLogRecord) -> None:  # type: ignore
        if not self._queue.running:
            self._queue.start()

        try:
            message = self.format(record)
            if not isinstance(message, BaseModel):
                raise InvalidLogFormat()

            self._queue.put_nowait(self.stream, message)
        except InvalidLogFormat:
            sys.stderr.write("PydanticKafkaeskHandler recieved non-pydantic model")
        except RuntimeError:
            sys.stderr.write("Queue No event loop running to send log to Kafka\n")
        except asyncio.QueueFull:
            sys.stderr.write("Queue hit max log queue size, discarding message\n")
        except AttributeError:
            sys.stderr.write("Queue Error sending Kafkaesk log message\n")

    def close(self) -> None:
        self.acquire()
        try:
            super().close()
            if self._queue is not None:
                self._queue.close()
        finally:
            self.release()
