from kafkaesk.kafka import KafkaTopicManager
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.asyncio


async def test_create_topic_uses_replication_factor_from_servers():
    mng = KafkaTopicManager(["foo", "bar"])
    with patch("kafka.admin.client.KafkaAdminClient"):
        await mng.create_topic("Foobar")
        client = await mng.get_admin_client()
        assert client.create_topics.called
        assert client.create_topics.call_args[0][0][0].replication_factor == 2


async def test_create_topic_uses_replication_factor():
    mng = KafkaTopicManager(["foo", "bar"], replication_factor=1)
    with patch("kafka.admin.client.KafkaAdminClient"):
        await mng.create_topic("Foobar")
        client = await mng.get_admin_client()
        assert client.create_topics.called
        assert client.create_topics.call_args[0][0][0].replication_factor == 1
