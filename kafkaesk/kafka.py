from .metrics import watch_kafka
from aiokafka import TopicPartition
from kafkaesk.utils import run_async
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import kafka
import kafka.admin
import kafka.admin.client
import kafka.errors
import kafka.structs


class KafkaTopicManager:
    _admin_client: Optional[kafka.admin.client.KafkaAdminClient]
    _client: Optional[kafka.KafkaClient]
    _kafka_api_version: Optional[Tuple[int, ...]]

    def __init__(
        self,
        bootstrap_servers: List[str],
        prefix: str = "",
        replication_factor: Optional[int] = None,
        kafka_api_version: str = "auto",
        ssl_context: Optional[Any] = None,
        security_protocol: Optional[str] = "PLAINTEXT",
        sasl_mechanism: Optional[str] = "",
        sasl_plain_username: Optional[str] = "",
        sasl_plain_password: Optional[str] = "",
    ):
        self.prefix = prefix
        self._bootstrap_servers = bootstrap_servers
        self._admin_client = self._client = None
        self._topic_cache: List[str] = []
        self._replication_factor: int = replication_factor or min(3, len(self._bootstrap_servers))
        if kafka_api_version == "auto":
            self._kafka_api_version = None
        else:
            self._kafka_api_version = tuple([int(v) for v in kafka_api_version.split(".")])
        self.ssl_context = ssl_context
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password

    @property
    def kafka_api_version(self) -> Optional[Tuple[int, ...]]:
        return self._kafka_api_version

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
            with watch_kafka("sync_admin_connect"):
                self._admin_client = await run_async(
                    kafka.admin.client.KafkaAdminClient,
                    bootstrap_servers=self._bootstrap_servers,
                    api_version=self._kafka_api_version,
                    ssl_context=self.ssl_context,
                    security_protocol=self.security_protocol,
                    sasl_mechanism=self.sasl_mechanism,
                    sasl_plain_username=self.sasl_plain_username,
                    sasl_plain_password=self.sasl_plain_password,
                )
        return self._admin_client

    async def list_consumer_group_offsets(
        self, group_id: str, partitions: Optional[List[TopicPartition]] = None
    ) -> Dict[kafka.structs.TopicPartition, kafka.structs.OffsetAndMetadata]:
        client = await self.get_admin_client()
        return await run_async(client.list_consumer_group_offsets, group_id, partitions=partitions)

    async def topic_exists(self, topic: str) -> bool:
        if self._client is None:
            with watch_kafka("sync_consumer_connect"):
                self._client = await run_async(
                    kafka.KafkaConsumer,
                    bootstrap_servers=self._bootstrap_servers,
                    enable_auto_commit=False,
                    api_version=self._kafka_api_version,
                    ssl_context=self.ssl_context,
                    security_protocol=self.security_protocol,
                    sasl_mechanism=self.sasl_mechanism,
                    sasl_plain_username=self.sasl_plain_username,
                    sasl_plain_password=self.sasl_plain_password,
                )
        if topic in self._topic_cache:
            return True
        with watch_kafka("sync_topics"):
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
    ) -> None:
        topic_configs: Dict[str, Any] = {}
        if retention_ms is not None:
            topic_configs["retention.ms"] = retention_ms
        new_topic = kafka.admin.NewTopic(
            topic,
            partitions,
            replication_factor or self._replication_factor,
            topic_configs=topic_configs,
        )
        client = await self.get_admin_client()
        try:
            with watch_kafka("sync_create_topics"):
                await run_async(client.create_topics, [new_topic])
        except kafka.errors.TopicAlreadyExistsError:
            pass
        self._topic_cache.append(topic)
        return None
