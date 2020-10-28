from aiokafka import TopicPartition
from kafkaesk.utils import run_async
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import kafka
import kafka.admin
import kafka.admin.client
import kafka.errors
import kafka.structs


class KafkaTopicManager:
    _admin_client: Optional[kafka.admin.client.KafkaAdminClient]
    _client: Optional[kafka.KafkaClient]

    def __init__(
        self,
        bootstrap_servers: List[str],
        prefix: str = "",
        replication_factor: Optional[int] = None,
    ):
        self.prefix = prefix
        self._bootstrap_servers = bootstrap_servers
        self._admin_client = self._client = None
        self._topic_cache: List[str] = []
        self._replication_factor: int = replication_factor or min(3, len(self._bootstrap_servers))

    async def finalize(self) -> None:
        if self._admin_client is not None:
            await run_async(self._admin_client.close)
            self._admin_client = None
        if self._client is not None:
            await run_async(self._client.close)
            self._client = None

    def get_topic_id(self, topic: str) -> str:
        return f"{self.prefix}{topic}"

    async def get_admin_client(self) -> kafka.admin.client.KafkaAdminClient:
        if self._admin_client is None:
            self._admin_client = await run_async(
                kafka.admin.client.KafkaAdminClient, bootstrap_servers=self._bootstrap_servers
            )
        return self._admin_client

    async def list_consumer_group_offsets(
        self, group_id: str, partitions: Optional[List[TopicPartition]] = None
    ) -> Dict[kafka.structs.TopicPartition, kafka.structs.OffsetAndMetadata]:
        client = await self.get_admin_client()
        return await run_async(client.list_consumer_group_offsets, group_id, partitions=partitions)

    async def topic_exists(self, topic: str) -> bool:
        if self._client is None:
            self._client = await run_async(
                kafka.KafkaConsumer,
                bootstrap_servers=self._bootstrap_servers,
                enable_auto_commit=False,
            )
        if topic in self._topic_cache:
            return True
        if topic in await run_async(self._client.topics):
            self._topic_cache.append(topic)
            return True
        return False

    async def create_topic(
        self,
        topic: str,
        *,
        partitions: int = 7,
        replication_factor: Optional[int] = None,
        retention_ms: Optional[int] = None,
        cleanup_policy: Optional[str] = None,
    ) -> None:
        topic_configs: Dict[str, Any] = {}
        if retention_ms is not None:
            topic_configs["retention.ms"] = retention_ms
        if cleanup_policy is not None:
            topic_configs["cleanup.policy"] = cleanup_policy
        new_topic = kafka.admin.NewTopic(
            topic,
            partitions,
            replication_factor or self._replication_factor,
            topic_configs=topic_configs,
        )
        client = await self.get_admin_client()
        try:
            await run_async(client.create_topics, [new_topic])
        except kafka.errors.TopicAlreadyExistsError:
            pass
        self._topic_cache.append(topic)
        return None
