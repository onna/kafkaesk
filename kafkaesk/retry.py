from .metrics import MESSAGE_FAILED
from .metrics import MESSAGE_REQUEUED
from abc import ABC
from aiokafka.structs import ConsumerRecord
from pydantic import BaseModel
from typing import Callable
from typing import TYPE_CHECKING

import datetime

if TYPE_CHECKING:
    from .app import Application
    from .app import Subscription


class RetryInfo(BaseModel):
    retry_count: int
    retry_delay: int = 0
    retry_timestamp: datetime.datetime
    publish_timestamp: datetime.datetime
    publish_topic: str
    error: str


class RetryMessage(BaseModel):
    retry_info: RetryInfo
    original_record: ConsumerRecord

    class Config:
        arbitrary_types_allowed = True


class FailureInfo(BaseModel):
    retry_count: int
    failure_timestamp: datetime.datetime
    publish_topic: str
    error: str


class FailureMessage(BaseModel):
    failure_info: FailureInfo
    original_record: ConsumerRecord

    class Config:
        arbitrary_types_allowed = True


class RetryPolicy(ABC):
    def __init__(self) -> None:
        self._initialized = False

    async def initialize(self, app: "Application", subscription: "Subscription") -> None:

        self._app = app
        self._subscription = subscription

        self._initialized = True

    async def finalize(self) -> None:
        self._initialized = False

    async def __call__(self, record: ConsumerRecord, error: Exception) -> None:
        if self._initialized is not True:
            raise RuntimeError("RetryPolicy is not initialized")

        if self.should_retry(record, error):
            return await self._handle_retry(record, error)
        else:
            return await self._handle_failure(record, error)

    async def _handle_retry(self, record: ConsumerRecord, error: Exception) -> None:
        MESSAGE_REQUEUED.labels(
            stream_id=record.topic,
            partition=record.partition,
            error=error.__class__.__name__,
            group_id=self._subscription.group,
        ).inc()

        await self.handle_retry(record, error)

    async def _handle_failure(self, record: ConsumerRecord, error: Exception) -> None:
        MESSAGE_FAILED.labels(
            stream_id=record.topic,
            partition=record.partition,
            error=error.__class__.__name__,
            group_id=self._subscription.group,
        ).inc()

        await self.handle_failure(record, error)

    def should_retry(self, record: ConsumerRecord, error: Exception) -> bool:
        raise NotImplementedError

    async def handle_retry(self, record: ConsumerRecord, error: Exception) -> None:
        raise NotImplementedError

    async def handle_failure(self, record: ConsumerRecord, error: Exception) -> None:
        raise NotImplementedError


class NoRetry(RetryPolicy):
    def should_retry(self, record: ConsumerRecord, error: Exception) -> bool:
        return False

    async def handle_failure(self, record: ConsumerRecord, error: Exception) -> None:
        raise error


class Forward(RetryPolicy):
    async def initialize(self, app: "Application", subscription: "Subscription") -> None:
        await super().initialize(app, subscription)

        # Setup failure topic
        self.failure_topic = f"{subscription.group}__{subscription.stream_id}"

    def should_retry(self, record: ConsumerRecord, error: Exception) -> bool:
        return False

    async def handle_failure(self, record: ConsumerRecord, error: Exception) -> None:

        info = FailureInfo(
            retry_count=0,
            failure_timestamp=datetime.datetime.now(),
            publish_topic=self._subscription.stream_id,
            error=error.__class__.__name__,
        )
        await self._app.publish(
            self.failure_topic, FailureMessage(failure_info=info, original_record=record)
        )


DefaultRetryPolicyFactory = Callable[..., RetryPolicy]

_default_retry_policy: DefaultRetryPolicyFactory = NoRetry


def get_default_retry_policy() -> DefaultRetryPolicyFactory:
    global _default_retry_policy

    return _default_retry_policy


def set_default_retry_policy(policy: DefaultRetryPolicyFactory) -> None:
    global _default_retry_policy
    _default_retry_policy = policy
