from typing import Dict, Any, List


class NewTopic:
    def __init__(
        self, topic: str, partitions: int, replicas: int, topic_configs: Dict[str, Any]
    ):
        ...


class KafkaAdminClient:
    def create_topics(self, topics: List[NewTopic]) -> None:
        ...
