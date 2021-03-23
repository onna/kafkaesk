from .metrics import RETRY_HANDLER_DROP
from .metrics import RETRY_HANDLER_FORWARD
from .metrics import RETRY_HANDLER_RAISE
from .metrics import RETRY_POLICY
from .metrics import RETRY_POLICY_TIME
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

if TYPE_CHECKING:  # pragma: no cover
    from .app import Application
    from .app import Subscription


RETRY_HISTORY_FAILURES_MAX_SIZE = 10


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
        headers = [(k, v.decode()) for k, v in record.headers or []]
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
            headers=headers,
        )

    def to_consumer_record(self) -> ConsumerRecord:
        # We need to convert the value back into bytes before giving this back to the consumer
        data = self.dict()
        data["value"] = data["value"].encode("utf-8")
        data["headers"] = [(k, v.encode()) for k, v in data["headers"]]
        return ConsumerRecord(**data)  # type: ignore


class FailureInfo(BaseModel):
    exception: str
    handler_key: str
    timestamp: datetime


class RetryHistory(BaseModel):
    failures: List[FailureInfo] = []


class RetryMessage(BaseModel):
    original_record: Record
    retry_history: RetryHistory


class RetryPolicy:
    logger = logging.getLogger("kafkaesk.retry.retry_policy")

    def __init__(
        self,
        app: "Application",
        subscription: "Subscription",
    ):
        self.app = app
        self.subscription = subscription

        self._default_handler: "RetryHandler" = Raise()

        self._handlers = self.subscription.retry_handlers or {}
        self._handler_cache: Dict[Type[Exception], Tuple[str, RetryHandler]] = {}

        self._ready = False

        if "RetryMessage:1" not in self.app.schemas:
            self.app.schema("RetryMessage", version=1)(RetryMessage)

    async def initialize(self) -> None:
        self.logger.debug("Retry policy initializing retry handlers...")
        await asyncio.gather(*[handler.initialize(self) for handler in self._handlers.values()])
        self.logger.debug("Retry policy initialized")
        self._ready = True

    async def finalize(self) -> None:
        self._ready = False
        self.logger.debug("Retry policy finalizing retry handlers...")
        await asyncio.gather(*[handler.finalize() for handler in self._handlers.values()])
        self.logger.debug("Retry policy finalized")

    async def __call__(
        self,
        record: ConsumerRecord,
        exception: Exception,
        retry_history: Optional[RetryHistory] = None,
    ) -> None:
        if self._ready is not True:
            raise RuntimeError("RetryPolicy is not initalized")

        self.logger.debug(f"Handling msg retry: {record} {exception}")

        with RETRY_POLICY_TIME.labels(
            stream_id=record.topic,
            partition=record.partition,
            group_id=self.subscription.group,
        ).time():
            handler_key, handler = self._get_handler(exception)

            if retry_history is None:
                retry_history = RetryHistory()

            # Add information about this failure to the history
            retry_history.failures.append(
                FailureInfo(
                    exception=exception.__class__.__name__,
                    handler_key=handler_key,
                    timestamp=datetime.now(),
                )
            )
            # Enforce a maximum number of failures kept
            if len(retry_history.failures) > RETRY_HISTORY_FAILURES_MAX_SIZE:
                retry_history.failures = retry_history.failures[-RETRY_HISTORY_FAILURES_MAX_SIZE:]

            try:
                await handler(self, handler_key, retry_history, record, exception)
            finally:
                RETRY_POLICY.labels(
                    stream_id=record.topic,
                    partition=record.partition,
                    group_id=self.subscription.group,
                    handler=handler.__class__.__name__,
                    exception=exception.__class__.__name__,
                ).inc()

    def _get_handler(self, exception: Exception) -> Tuple[str, "RetryHandler"]:
        exception_type = exception.__class__

        handler_key, handler = self._handler_cache.get(exception_type, (None, None))

        if handler is None:
            handler = self._handlers.get(exception_type)
            if handler is not None:
                handler_key = exception_type.__name__
                self._handler_cache[exception_type] = (handler_key, handler)

        if handler is None:
            for handler_exception_type in self._handlers.keys():
                if isinstance(exception, handler_exception_type):
                    handler = self._handlers[handler_exception_type]
                    handler_key = handler_exception_type.__name__
                    self._handler_cache[exception_type] = (handler_key, handler)
                    break

        # Return the default handler
        if handler is None:
            handler = self._default_handler

        if handler_key is None:
            handler_key = "Exception"

        return (handler_key, handler)


def format_record(record: ConsumerRecord) -> str:
    val = repr(record)
    if len(val) > 512:
        return val[:512] + "..."
    return val


class RetryHandler(ABC):
    """Base class implementing common logic for RetryHandlers

    All RetryHandler's should implement the following metrics:

    * RETRY_HANDLER_FORWARD - Note: This is implemented by RetryHandler._forward_message
    * RETRY_HANDLER_DROP - Note: this is implemented by RetryHandler._drop_message
    * RETRY_CONSUMER_TOPIC_OFFSET
    * RETRY_CONSUMER_MESSAGE_LEAD_TIME - Note: If a RetryHandler's consumer expects a delay,
        this delay should be subtracted from the lead time
    * RETRY_CONSUMER_CONSUMED_MESSAGE_TIME
    * RETRY_CONSUMER_CONSUMED_MESSAGES

    See `kafkaesk.metrics` for more information on each of these metrics
    """

    logger = logging.getLogger("kafkaesk.retry.retry_handler")

    def __init__(self) -> None:
        self._ready = False

    async def initialize(self, policy: RetryPolicy) -> None:
        self.logger.debug(f"{self.__class__.__name__} retry handler initialized")
        self._ready = True

    async def finalize(self) -> None:
        self.logger.debug(f"{self.__class__.__name__} retry handler finalized")
        self._ready = False

    async def __call__(
        self,
        policy: RetryPolicy,
        handler_key: str,
        retry_history: RetryHistory,
        record: ConsumerRecord,
        exception: Exception,
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    async def _raise_message(
        self,
        policy: RetryPolicy,
        retry_history: RetryHistory,
        record: ConsumerRecord,
        exception: Exception,
    ) -> None:
        self.logger.critical(
            f"{self.__class__.__name__} handler recieved exception, "
            f"re-raising exception {format_record(record)}"
        )
        RETRY_HANDLER_RAISE.labels(
            stream_id=record.topic,
            partition=record.partition,
            group_id=policy.subscription.group,
            handler=self.__class__.__name__,
            exception=exception.__class__.__name__,
        ).inc()
        raise exception from exception

    async def _drop_message(
        self,
        policy: RetryPolicy,
        retry_history: RetryHistory,
        record: ConsumerRecord,
        exception: Exception,
    ) -> None:
        self.logger.warn(
            f"{self.__class__.__name__} handler recieved exception, dropping message {record}",
            exc_info=exception,
        )
        RETRY_HANDLER_DROP.labels(
            stream_id=record.topic,
            partition=record.partition,
            group_id=policy.subscription.group,
            handler=self.__class__.__name__,
            exception=exception.__class__.__name__,
        ).inc()

    async def _forward_message(
        self,
        policy: RetryPolicy,
        retry_history: RetryHistory,
        record: ConsumerRecord,
        exception: Exception,
        forward_stream_id: str,
    ) -> None:
        self.logger.info(
            f"{self.__class__.__name__} handler forwarding message "
            f"to {forward_stream_id}: {format_record(record)}"
        )
        await policy.app.publish(
            forward_stream_id,
            RetryMessage(
                original_record=Record.from_consumer_record(record), retry_history=retry_history
            ),
        )
        RETRY_HANDLER_FORWARD.labels(
            stream_id=record.topic,
            partition=record.partition,
            group_id=policy.subscription.group,
            handler=self.__class__.__name__,
            exception=exception.__class__.__name__,
            forward_stream_id=forward_stream_id,
        ).inc()


class Raise(RetryHandler):
    async def __call__(
        self,
        policy: RetryPolicy,
        handler_key: str,
        retry_history: RetryHistory,
        record: ConsumerRecord,
        exception: Exception,
    ) -> None:
        await self._raise_message(policy, retry_history, record, exception)


class Drop(RetryHandler):
    async def __call__(
        self,
        policy: RetryPolicy,
        handler_key: str,
        retry_history: RetryHistory,
        record: ConsumerRecord,
        exception: Exception,
    ) -> None:
        self.logger.error(
            f"{self.__class__.__name__}: Dropping message due to error: {format_record(record)}",
            exc_info=exception,
        )
        await self._drop_message(policy, retry_history, record, exception)


class Forward(RetryHandler):
    def __init__(self, stream_id: str):

        self.stream_id = stream_id
        super().__init__()

    async def __call__(
        self,
        policy: RetryPolicy,
        handler_key: str,
        retry_history: RetryHistory,
        record: ConsumerRecord,
        exception: Exception,
    ) -> None:
        self.logger.error(
            f"{self.__class__.__name__}: Forwarding message due to error: {format_record(record)}",
            exc_info=exception,
        )
        await self._forward_message(policy, retry_history, record, exception, self.stream_id)
