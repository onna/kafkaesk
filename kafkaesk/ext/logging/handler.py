from .record import PydanticLogRecord
from datetime import datetime
from typing import Any
from typing import Dict
from typing import IO
from typing import Optional

import asyncio
import kafkaesk
import logging
import os
import pydantic
import socket
import sys
import time

NAMESPACE_FILEPATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
_not_set = object()
_K8S_NS = _not_set


def get_k8s_ns() -> Optional[str]:
    global _K8S_NS
    if _K8S_NS == _not_set:
        if os.path.exists(NAMESPACE_FILEPATH):
            with open(NAMESPACE_FILEPATH) as fi:
                _K8S_NS = fi.read().strip()
        else:
            _K8S_NS = None
    return _K8S_NS  # type: ignore


class InvalidLogFormat(Exception):
    ...


class PydanticLogModel(pydantic.BaseModel):
    class Config:
        extra = pydantic.Extra.allow


class PydanticStreamHandler(logging.StreamHandler):
    def __init__(self, stream: Optional[IO[str]] = None):
        super().__init__(stream=stream)

    def format(self, record: PydanticLogRecord) -> str:  # type: ignore
        message = super().format(record)

        for log in getattr(record, "pydantic_data", []):
            # log some attributes
            formatted_data = []
            size = 0
            for field_name in log.__fields__.keys():
                val = getattr(log, field_name)
                formatted = f"{field_name}={val}"
                size += len(formatted)
                formatted_data.append(formatted)

                if size > 256:
                    break
            message += f": {', '.join(formatted_data)}"
            break

        return message


class KafkaeskQueue:
    def __init__(
        self,
        app: kafkaesk.app.Application,
        max_queue: int = 10000,
    ):
        self._queue: Optional[asyncio.Queue] = None
        self._queue_size = max_queue

        self._app = app

        self._app.on("finalize", self.flush)

        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=self._queue_size)

        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

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
        if self._queue is None:
            raise RuntimeError("Queue must be started before workers")

        while True:
            try:
                stream, log_data = await asyncio.wait_for(asyncio.create_task(self._queue.get()), 1)
                await self._publish(stream, log_data)

            except asyncio.TimeoutError:
                continue

            except asyncio.CancelledError:
                await self.flush()
                return

    async def flush(self) -> None:
        if self._queue is not None:
            while not self._queue.empty():
                stream, message = await self._queue.get()
                await self._publish(stream, message)

    async def _publish(self, stream: str, log_data: PydanticLogModel) -> None:
        if not self._app._initialized:
            await self._app.initialize()

        await self._app.publish(stream, log_data)
        # TODO: Handle other Kafak errors that may be raised

    def put_nowait(self, stream: str, log_data: PydanticLogModel) -> None:
        if self._queue is not None:
            self._queue.put_nowait((stream, log_data))


_formatter = logging.Formatter()


class PydanticKafkaeskHandler(logging.Handler):
    def __init__(
        self, app: kafkaesk.Application, stream: str, queue: Optional[KafkaeskQueue] = None
    ):
        self.app = app
        self.stream = stream

        if queue is None:
            self._queue = KafkaeskQueue(self.app)
        else:
            self._queue = queue

        self._last_warning_sent = 0.0

        self._initialize_model()

        super().__init__()

    def clone(self) -> "PydanticKafkaeskHandler":
        return PydanticKafkaeskHandler(self.app, self.stream, queue=self._queue)

    def _initialize_model(self) -> None:
        try:
            self.app.schema("PydanticLogModel")(PydanticLogModel)
        except kafkaesk.app.SchemaConflictException:
            pass

    def _format_base_log(self, record: PydanticLogRecord) -> Dict[str, Any]:
        if record.exc_text is None and record.exc_info:
            record.exc_text = _formatter.formatException(record.exc_info)
            try:
                record.exc_type = record.exc_info[0].__name__  # type: ignore
            except (AttributeError, IndexError):  # pragma: no cover
                ...

        if record.stack_info:
            record.stack_text = _formatter.formatStack(record.stack_info)

        service_name = "unknown"
        hostname = socket.gethostname()
        dashes = hostname.count("-")
        if dashes > 0:
            # detect kubernetes service host
            service_name = "-".join(hostname.split("-")[: -min(dashes, 2)])

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "logger": record.name,
            "severity": record.levelname,
            "level": record.levelno,
            "message": record.getMessage(),
            "exception": record.exc_type,
            "trace": record.stack_text,
            "stack": record.exc_text,
            "hostname": hostname,
            "service": service_name,
            "namespace": get_k8s_ns(),
            "cluster": os.environ.get("CLUSTER"),
        }

    def _format_extra_logs(self, record: PydanticLogRecord) -> Dict[str, Any]:
        extra_logs: Dict[str, Any] = {}

        for log in getattr(record, "pydantic_data", []):
            extra_logs.update(
                log.dict(
                    exclude_none=True,
                    exclude={
                        "_is_log_model",
                    },
                )
            )

        return extra_logs

    def emit(self, record: PydanticLogRecord) -> None:  # type: ignore
        if not self._queue.running:
            try:
                self._queue.start()
            except RuntimeError:
                sys.stderr.write("RuntimeError starting kafka logging, ignoring")
                return

        try:
            raw_data = self._format_base_log(record)
            raw_data.update(self._format_extra_logs(record))
            log_data = PydanticLogModel(**raw_data)
            self._queue.put_nowait(self.stream, log_data)
        except InvalidLogFormat:  # pragma: no cover
            sys.stderr.write("PydanticKafkaeskHandler recieved non-pydantic model")
        except RuntimeError:
            sys.stderr.write("Queue No event loop running to send log to Kafka\n")
        except asyncio.QueueFull:
            if time.time() - self._last_warning_sent > 30:
                sys.stderr.write("Queue hit max log queue size, discarding message\n")
                self._last_warning_sent = time.time()
        except AttributeError:  # pragma: no cover
            sys.stderr.write("Queue Error sending Kafkaesk log message\n")

    def close(self) -> None:
        self.acquire()
        try:
            super().close()
            if self._queue is not None:
                self._queue.close()
        finally:
            self.release()
