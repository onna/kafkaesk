from aiokafka.structs import TopicPartition
from kafkaesk import Application
from kafkaesk import Subscription
from kafkaesk import SubscriptionConsumer
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock

import aiokafka.errors
import asyncio
import kafka.errors
import logging
import pytest

pytestmark = pytest.mark.asyncio


@pytest.fixture()
def subscription():
    yield SubscriptionConsumer(Application(), Subscription("foo", lambda: 1, "group"))


def _record(topic="topic", partition=1, offset=1):
    record = Mock()
    record.topic = topic
    record.partition = partition
    record.offset = offset
    return record


async def test_consumer_property_rasises_exception(subscription):
    with pytest.raises(RuntimeError):
        subscription.consumer


async def test_lifecycle_logs_exits(subscription, caplog):
    caplog.set_level(logging.DEBUG)
    subscription._run = AsyncMock(side_effect=RuntimeError)
    await subscription()

    assert "Consumer stopped" in caplog.records[-1].message


async def test_maybe_commit(subscription):
    subscription._consumer = AsyncMock()
    subscription._last_commit = 0
    await subscription._maybe_commit()
    subscription.consumer.commit.assert_called_once()
