from typing import Any
from typing import Dict
from typing import List

import kafka.structs


class NewTopic:
    def __init__(
        self, topic: str, partitions: int, replication_factor: int, topic_configs: Dict[str, Any]
    ):
        ...


class KafkaAdminClient:
    def create_topics(self, topics: List[NewTopic]) -> None:
        ...

    def close(self) -> None:
        ...

    def list_consumer_group_offsets(
        self, group_id: str
    ) -> Dict[kafka.structs.TopicPartition, kafka.structs.OffsetAndMetadata]:
        ...
