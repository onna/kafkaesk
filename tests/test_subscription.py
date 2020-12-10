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


async def test_consumer_property_rasises_exception(subscription):
    with pytest.raises(RuntimeError):
        subscription.consumer


async def test_lifecycle_logs_connection_error(subscription, caplog):
    async def _mock_run():
        await asyncio.sleep(0.005)
        raise aiokafka.errors.KafkaConnectionError()

    subscription._run = _mock_run
    task = asyncio.create_task(subscription())
    await asyncio.sleep(0.01)
    assert subscription._auto_commit_task is not None
    task.cancel()

    assert "Connection error" in caplog.records[-1].message


async def test_lifecycle_logs_exits(subscription, caplog):
    caplog.set_level(logging.DEBUG)
    subscription._run = AsyncMock(side_effect=RuntimeError)
    await subscription()
    assert subscription._auto_commit_task is not None
    await asyncio.sleep(0.01)  # let it cancel
    assert subscription._auto_commit_task.done()

    assert "Consumer stopped" in caplog.records[-1].message


async def test_commit(subscription):
    subscription._consumer = AsyncMock()
    subscription._consumer.assignment = MagicMock(return_value=[TopicPartition("topic", 1)])
    record = Mock()
    record.topic = "topic"
    record.partition = 1
    record.offset = 1

    subscription.record_commit(record)

    await subscription.commit()
    assert len(subscription._to_commit) == 0

    subscription.consumer.commit.assert_called_with({TopicPartition("topic", 1): 2})


async def test_do_not_commit_when_assignment_changed(subscription):
    subscription._consumer = AsyncMock()
    subscription._consumer.assignment = MagicMock(return_value=[TopicPartition("topic", 2)])
    record = Mock()
    record.topic = "topic"
    record.partition = 1
    record.offset = 1

    subscription.record_commit(record)

    await subscription.commit()

    subscription.consumer.commit.assert_not_called()


async def test_commit_failed_keeps_record(subscription):
    subscription._consumer = AsyncMock()
    subscription._consumer.assignment = MagicMock(return_value=[TopicPartition("topic", 1)])
    record = Mock()
    record.topic = "topic"
    record.partition = 1
    record.offset = 1

    subscription.record_commit(record)

    subscription.consumer.commit.side_effect = kafka.errors.CommitFailedError
    await subscription.commit()

    assert subscription._to_commit == {TopicPartition("topic", 1): 2}


async def test_commit_failed_handles_new_records(subscription):
    subscription._consumer = AsyncMock()
    subscription._consumer.assignment = MagicMock(return_value=[TopicPartition("topic", 1)])
    record = Mock()
    record.topic = "topic"
    record.partition = 1
    record.offset = 1

    subscription.record_commit(record)

    async def delayed_commit(*args):
        await asyncio.sleep(0.01)
        raise kafka.errors.CommitFailedError

    subscription.consumer.commit = delayed_commit
    task = asyncio.create_task(subscription.commit())
    await asyncio.sleep(0.005)
    # now, record new message
    record = Mock()
    record.topic = "topic"
    record.partition = 1
    record.offset = 2

    subscription.record_commit(record)

    await task

    assert subscription._to_commit == {TopicPartition("topic", 1): 3}


async def test_commit_drop_when_rebalance(subscription):
    subscription._consumer = AsyncMock()
    subscription._consumer.assignment = MagicMock(return_value=[TopicPartition("topic", 1)])
    record = Mock()
    record.topic = "topic"
    record.partition = 1
    record.offset = 1

    subscription.record_commit(record)

    subscription.consumer.commit.side_effect = kafka.errors.IllegalStateError
    await subscription.commit()

    assert len(subscription._to_commit) == 0