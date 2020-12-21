from aiokafka.structs import OffsetAndMetadata
from aiokafka.structs import TopicPartition
from kafkaesk.app import Application
from kafkaesk.subscription import CustomConsumerRebalanceListener
from tests.utils import record_factory
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import asyncio
import pytest

pytestmark = pytest.mark.asyncio


async def test_record_metric_on_rebalance():
    with patch("kafkaesk.subscription.CONSUMER_REBALANCED") as rebalance_metric:
        app_mock = AsyncMock()
        app_mock.topic_mng.list_consumer_group_offsets.return_value = {
            TopicPartition(topic="foobar", partition=0): OffsetAndMetadata(offset=0, metadata={})
        }
        rebalance_listener = CustomConsumerRebalanceListener(AsyncMock(), app_mock, "group")
        await rebalance_listener.on_partitions_assigned(
            [TopicPartition(topic="foobar", partition=0)]
        )
        rebalance_metric.labels.assert_called_with(
            stream_id="foobar",
            partition=0,
            group_id="group",
        )
        rebalance_metric.labels(
            stream_id="foobar",
            partition=0,
            group_id="group",
        ).inc.assert_called_once()


async def test_record_metric_on_publish():
    """
    this test is acting funny on github action...
    """
    with patch("kafkaesk.app.PUBLISHED_MESSAGES") as published_metric, patch(
        "kafkaesk.app.PUBLISHED_MESSAGES_TIME"
    ) as published_metric_time, patch("kafkaesk.metrics.PUBLISH_MESSAGES") as publish_metric, patch(
        "kafkaesk.metrics.PUBLISH_MESSAGES_TIME"
    ) as publish_metric_time:
        app = Application()

        async def _fake_publish(*args, **kwargs):
            async def _publish():
                return record_factory()

            return asyncio.create_task(_publish())

        producer = AsyncMock()
        producer.send.side_effect = _fake_publish
        app._get_producer = AsyncMock(return_value=producer)
        app._topic_mng = MagicMock()
        app._topic_mng.get_topic_id.return_value = "foobar"

        await (await app.raw_publish("foo", b"data"))

        published_metric.labels.assert_called_with(stream_id="foobar", partition=0, error="none")
        published_metric.labels(
            stream_id="foobar", partition=0, error="none"
        ).inc.assert_called_once()
        published_metric_time.labels.assert_called_with(stream_id="foobar")
        published_metric_time.labels(stream_id="foobar").observe.assert_called_once()

        publish_metric.labels.assert_called_with(stream_id="foobar", error="none")
        publish_metric.labels(stream_id="foobar", error="none").inc.assert_called_once()
        publish_metric_time.labels.assert_called_with(stream_id="foobar")
        publish_metric_time.labels(stream_id="foobar").observe.assert_called_once()


async def test_record_metric_error():
    """
    this test is acting funny on github action...
    """
    with patch("kafkaesk.metrics.PUBLISH_MESSAGES") as publish_metric, patch(
        "kafkaesk.metrics.PUBLISH_MESSAGES_TIME"
    ) as publish_metric_time:
        app = Application()

        producer = AsyncMock()
        producer.send.side_effect = Exception
        app._get_producer = AsyncMock(return_value=producer)
        app._topic_mng = MagicMock()
        app._topic_mng.get_topic_id.return_value = "foobar"

        with pytest.raises(Exception):
            await app.raw_publish("foo", b"data")

        publish_metric.labels.assert_called_with(stream_id="foobar", error="exception")
        publish_metric.labels(stream_id="foobar", error="none").inc.assert_called_once()
        publish_metric_time.labels.assert_called_with(stream_id="foobar")
        publish_metric_time.labels(stream_id="foobar").observe.assert_called_once()
