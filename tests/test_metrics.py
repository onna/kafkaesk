from aiokafka.structs import OffsetAndMetadata
from aiokafka.structs import TopicPartition
from kafkaesk.app import CustomConsumerRebalanceListener
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.asyncio


async def test_record_metric_on_rebalance():
    with patch("kafkaesk.app.CONSUMER_REBALANCED") as rebalance_metric:
        app_mock = AsyncMock()
        app_mock.topic_mng.list_consumer_group_offsets.return_value = {
            TopicPartition(topic="foobar", partition=0): OffsetAndMetadata(offset=0, metadata={})
        }
        rebalance_listener = CustomConsumerRebalanceListener(AsyncMock(), app_mock, "group")
        await rebalance_listener.on_partitions_assigned(
            [TopicPartition(topic="foobar", partition=0)]
        )
        rebalance_metric.labels.assert_called_with(
            stream_id="foobar", partition=0, group_id="group",
        )
        rebalance_metric.labels(
            stream_id="foobar", partition=0, group_id="group",
        ).inc.assert_called_once()
