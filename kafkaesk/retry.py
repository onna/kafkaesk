from .app import Application
from .app import Subscription
from .metrics import MESSAGE_FAILED
from .metrics import MESSAGE_REQUEUED
from abc import ABC
from aiokafka.structs import ConsumerRecord
from pydantic import BaseModel
from typing import Callable

import datetime


class RetryInfo(BaseModel):
    retry_count: int
    retry_delay: int = 0
    retry_timestamp: datetime.datetime
    publish_timestamp: datetime.datetime
    publish_topic: str
    error: Exception


class RetryMessage(BaseModel):
    retry_info: RetryInfo
    original_record: ConsumerRecord


class FailureInfo(BaseModel):
    retry_count: int
    failure_timestamp: datetime.datetime
    publish_topic: str
    error: Exception


class FailureMessage(BaseModel):
    failure_info: FailureInfo
    original_record: ConsumerRecord


class RetryPolicy(ABC):
    def __init__(self) -> None:
        self._initialized = False

    async def initialize(self, app: Application, subscription: Subscription) -> None:

        self._app = app
        self._subscription = subscription

        self._finalized = True

    async def finalize(self) -> None:
        self._finalized = False

    async def __call__(self, record: ConsumerRecord, error: Exception) -> None:
        if self._initialized is not True:
            raise RuntimeError("RetryPolicy is not initialized")

        if self.should_retry(record, error):
            return await self._handle_retry(record, error)
        else:
            return await self._handle_failure(record, error)

    async def _handle_retry(self, record: ConsumerRecord, error: Exception) -> None:
        await self.handle_retry(record, error)

        MESSAGE_REQUEUED.labels(
            stream_id=record.topic,
            partition=record.partition,
            error="UnhandledMessage",
            group_id=self._subscription.group,
        ).inc()

    async def _handle_failure(self, record: ConsumerRecord, error: Exception) -> None:
        await self.handle_failure(record, error)

        MESSAGE_FAILED.labels(
            stream_id=record.topic,
            partition=record.partition,
            error="UnhandledMessage",
            group_id=self._subscription.group,
        ).inc()

    def should_retry(self, record: ConsumerRecord, error: Exception) -> bool:
        raise NotImplementedError

    async def handle_retry(self, record: ConsumerRecord, error: Exception) -> None:
        raise NotImplementedError

    async def handle_failure(self, record: ConsumerRecord, error: Exception) -> None:
        raise NotImplementedError


class NoRetry(RetryPolicy):
    def should_retry(self, record: ConsumerRecord, error: Exception) -> bool:
        return False

    async def _handle_failure(self, record: ConsumerRecord, error: Exception) -> None:
        raise error


class Forward(RetryPolicy):
    async def initialize(self, app: Application, subscription: Subscription) -> None:
        super().initialize(app, subscription)

        # Setup failure topic
        self.failure_topic = f"{subscription.group}:{subscription.stream_id}"

    def should_retry(self, record: ConsumerRecord, error: Exception) -> bool:
        return False

    async def _handle_failure(self, record: ConsumerRecord, error: Exception) -> None:

        info = FailureInfo(
            retry_count=0,
            failure_timestamp=datetime.datetime.now(),
            publish_topic=self._subscription.stream_id,
            error=error,
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
