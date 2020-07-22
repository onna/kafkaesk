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


class LogQueue:
    def __init__(self, app: kafkaesk.Application):
        self._queue: asyncio.Queue[Tuple[str, BaseModel]] = asyncio.Queue()
        self._app = app

    async def run(self) -> None:
        while True:
            stream, message = await self._queue.get()
            try:
                await self._app.publish(stream, message)
            except Exception as err:
                print(err)
            finally:
                self._queue.task_done()

    def put_nowait(self, stream: str, message: PydanticLogModel) -> None:
        self._queue.put_nowait((stream, message))


class PydanticKafkaeskHandler(logging.Handler):
    @property
    def _queue_initialized(self) -> bool:
        if self._queue is not None and self._queue_task is not None:
            if not self._queue_task.done():
                return True

        return False

    def __init__(
        self, app: kafkaesk.Application, stream: str, loop: asyncio.AbstractEventLoop = None
    ):
        self.app = app
        self.stream = stream
        self.loop = loop

        self._queue: Optional[LogQueue] = None
        self._queue_task: Optional[asyncio.Future] = None

        super().__init__()

    def _initialize_queue(self) -> None:
        self._queue = LogQueue(self.app)
        self._queue_task = asyncio.ensure_future(self._queue.run(), loop=self.loop)

    def emit(self, record: PydanticLogRecord) -> None:  # type: ignore
        if not self._queue_initialized:
            self._initialize_queue()
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
            if self._queue is not None and self._queue_task is not None:
                if not self._queue_task.done():
                    try:
                        self._queue_task.cancel()
                    except RuntimeError:
                        pass
        finally:
            self.release()
