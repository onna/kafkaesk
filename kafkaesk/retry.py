from .app import Application
from .app import Subscription
from .app import SubscriptionConsumer
from abc import ABC
from kafka.consumer.fetcher import ConsumerRecord
from pydantic import BaseModel

import datetime


class RetryInfo(BaseModel):
    retry_count: int
    retry_delay: int = 0
    retry_timestamp: datetime.datetime
    publish_timestamp: datetime.datetime
    publish_topic: str
    error: Exception


class RertyMessage(BaseModel):
    retry_info: RetryInfo
    original_message: ConsumerRecord


class FailureInfo(BaseModel):
    retry_count: int
    failure_timestamp: datetime.datetime
    publish_topic: str
    error: Exception


class FailureMessage(BaseModel):
    failure_info: FailureInfo
    original_message: ConsumerRecord


class RetryPolicy(ABC):
    def __init__(self) -> None:
        self._initialized = False

    async def initialize(
        self, app: Application, subscription: Subscription, consumer: SubscriptionConsumer
    ) -> None:

        self._app = app
        self._subscription = subscription
        self._consumer = consumer

        self._finalized = True

    async def finalize(self) -> None:
        self._finalized = False

    async def handle(self, message: ConsumerRecord, error: Exception) -> None:
        if self._initialized is not True:
            raise RuntimeError("RetryPolicy is not initialized")

        return await self._handle(message, error)

    async def _handle(self, message: ConsumerRecord, error: Exception) -> None:
        raise NotImplementedError


class NoRetry(RetryPolicy):
    async def initialize(
        self, app: Application, subscription: Subscription, consumer: SubscriptionConsumer
    ) -> None:
        super().initialize(app, subscription, consumer)

        # Setup failure topic
        self.failure_topic = f"{subscription.group}:{subscription.stream_id}"

    async def finalize(self) -> None:
        pass

    async def _handle(self, message: ConsumerRecord, error: Exception) -> None:

        info = FailureInfo(
            retry_count=0,
            failure_timestamp=datetime.datetime.now(),
            publish_topic=self._subscription.stream_id,
            error=error,
        )
        await self._app.publish(
            self.failure_topic, FailureMessage(failure_info=info, original_message=message)
        )
