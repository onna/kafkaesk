from kafkaesk.kafka import KafkaTopicManager
from unittest.mock import patch

import kafka.errors
import pytest

pytestmark = pytest.mark.asyncio


async def test_create_topic_uses_replication_factor_from_servers():
    mng = KafkaTopicManager(["foo", "bar"])
    with patch("kafka.admin.client.KafkaAdminClient"):
        await mng.create_topic("Foobar")
        client = await mng.get_admin_client()
        assert client.create_topics.called
        assert client.create_topics.call_args[0][0][0].replication_factor == 2


async def test_create_topic_uses_replication_factor_from_servers_min_3():
    mng = KafkaTopicManager(["foo", "bar", "foo2", "foo3", "foo4"])
    with patch("kafka.admin.client.KafkaAdminClient"):
        await mng.create_topic("Foobar")
        client = await mng.get_admin_client()
        assert client.create_topics.called
        assert client.create_topics.call_args[0][0][0].replication_factor == 3


async def test_create_topic_uses_replication_factor():
    mng = KafkaTopicManager(["foo", "bar"], replication_factor=1)
    with patch("kafka.admin.client.KafkaAdminClient"):
        await mng.create_topic("Foobar", retention_ms=100)
        client = await mng.get_admin_client()
        assert client.create_topics.called
        assert client.create_topics.call_args[0][0][0].replication_factor == 1
        assert client.create_topics.call_args[0][0][0].topic_configs["retention.ms"] == 100


async def test_create_topic_already_exists():
    mng = KafkaTopicManager(["foo", "bar"], replication_factor=1)
    with patch("kafka.admin.client.KafkaAdminClient"):
        client = await mng.get_admin_client()
        client.create_topics.side_effect = kafka.errors.TopicAlreadyExistsError
        await mng.create_topic("Foobar")
        client.create_topics.assert_called_once()


def test_constructor_translates_api_version():
    mng = KafkaTopicManager(["foobar"], kafka_api_version="auto")
    assert mng.kafka_api_version is None

    mng = KafkaTopicManager(["foobar"], kafka_api_version="2.4.0")
    assert mng.kafka_api_version == (2, 4, 0)
