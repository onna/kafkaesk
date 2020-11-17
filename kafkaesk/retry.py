from abc import ABC
from aiokafka.structs import ConsumerRecord
from datetime import datetime
from pydantic import BaseModel
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TYPE_CHECKING

import asyncio
import logging

if TYPE_CHECKING is True:
    from .app import Application
    from .app import Subscription

logger = logging.getLogger("kafkaesk.retry")


class Record(BaseModel):
    topic: str
    partition: int
    offset: int
    timestamp: int
    timestamp_type: int
    key: Optional[str] = None
    value: str
    checksum: Optional[int] = None
    serialized_key_size: int
    serialized_value_size: int
    headers: tuple

    @classmethod
    def from_consumer_record(cls, record: ConsumerRecord) -> "Record":
        return cls(
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            timestamp=record.timestamp,  # type: ignore
            timestamp_type=record.timestamp_type,  # type: ignore
            key=record.key,  # type: ignore
            value=record.value.decode("utf-8"),  # type: ignore
            checksum=record.checksum,  # type: ignore
            serialized_key_size=record.serialized_key_size,  # type: ignore
            serialized_value_size=record.serialized_value_size,  # type: ignore
            headers=record.headers,
        )

    def to_consumer_record(self) -> ConsumerRecord:
        # We need to convert the value back into bytes before giving this back to the consumer
        data = self.dict()
        data["value"] = data["value"].encode("utf-8")

        return ConsumerRecord(**data)  # type: ignore


class FailureInfo(BaseModel):
    error: str
    handler_key: str
    timestamp: datetime


class RetryHistory(BaseModel):
    failures: List[FailureInfo] = []


class RetryMessage(BaseModel):
    original_record: Record
    retry_history: RetryHistory


class RetryPolicy:
    def __init__(
        self,
        app: "Application",
        subscription: "Subscription",
        retry_handlers: Optional[Dict[Type[Exception], "RetryHandler"]] = None,
    ):
        self.app = app
        self.subscription = subscription

        self._handlers = retry_handlers or {}
        self._handler_cache: Dict[Type[Exception], Tuple[str, RetryHandler]] = {}

        self._initialized = False

        if "RetryMessage:1" not in self.app.schemas:
            self.app.schema("RetryMessage", version=1)(RetryMessage)

    async def add_retry_handler(self, error: Type[Exception], handler: "RetryHandler") -> None:
        if error in self._handlers:
            raise ValueError(f"{error} error handler is already set")

        self._handlers[error] = handler

        if self._initialized is True:
            await self._handlers[error].initialize()

        # Clear handler cache when handler is added
        self._handler_cache = {}

    async def remove_retry_handler(self, error: Type[Exception]) -> None:
        if error in self._handlers:
            handler = self._handlers[error]

            del self._handlers[error]

            # Clear handler cache when handler is removed
            self._handler_cache = {}

            await handler.finalize()

    async def initialize(self) -> None:
        await asyncio.gather(*[handler.initialize() for handler in self._handlers.values()])
        self._initialized = True

    async def finalize(self) -> None:
        self._initialized = False
        await asyncio.gather(*[handler.finalize() for handler in self._handlers.values()])

    async def __call__(
        self,
        record: ConsumerRecord,
        error: Exception,
        retry_history: Optional[RetryHistory] = None,
    ) -> None:
        if self._initialized is not True:
            raise RuntimeError("RetryPolicy is not initalized")

        handler_key, handler = self._get_handler(error)

        if handler is None or handler_key is None:
            raise error from error

        if retry_history is None:
            retry_history = RetryHistory()

        # Add information about this failure to the history
        retry_history.failures.append(
            FailureInfo(
                error=error.__class__.__name__, handler_key=handler_key, timestamp=datetime.now()
            )
        )

        await handler(self, handler_key, retry_history, record, error)

    def _get_handler(self, error: Exception) -> Tuple[Optional[str], Optional["RetryHandler"]]:
        error_type = error.__class__

        handler_key, handler = self._handler_cache.get(error_type, (None, None))

        if handler is None:
            handler = self._handlers.get(error_type)
            if handler is not None:
                handler_key = error_type.__name__
                self._handler_cache[error_type] = (handler_key, handler)

        if handler is None:
            for handler_error_type in self._handlers.keys():
                if isinstance(error, handler_error_type):
                    handler = self._handlers[handler_error_type]
                    handler_key = handler_error_type.__name__
                    self._handler_cache[error_type] = (handler_key, handler)
                    break

        return (handler_key, handler)


class RetryHandler(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

        self._initialized = False

    async def initialize(self) -> None:
        self._initialized = True

    async def finalize(self) -> None:
        self._initialized = False

    async def __call__(
        self,
        policy: RetryPolicy,
        handler_key: str,
        retry_history: RetryHistory,
        record: ConsumerRecord,
        error: Exception,
    ) -> None:
        raise NotImplementedError


class NoRetry(RetryHandler):
    async def __call__(
        self,
        policy: RetryPolicy,
        handler_key: str,
        retry_history: RetryHistory,
        record: ConsumerRecord,
        error: Exception,
    ) -> None:
        logger.info("NoRetry handler recieved exception, dropping message", exc_info=error)


class Forward(RetryHandler):
    async def __call__(
        self,
        policy: RetryPolicy,
        handler_key: str,
        retry_history: RetryHistory,
        record: ConsumerRecord,
        error: Exception,
    ) -> None:
        await policy.app.publish(
            self.stream_id,
            RetryMessage(
                original_record=Record.from_consumer_record(record), retry_history=retry_history
            ),
        )
