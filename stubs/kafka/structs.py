from typing import List
from typing import Optional
from typing import Tuple


class ConsumerRecord:
    partition: int
    offset: int
    topic: str
    value: bytes
    key: bytes
    headers: Optional[List[Tuple[str, bytes]]]


class TopicPartition:
    topic: str
    partition: int

    def __init__(self, topic: str, partition: int):
        ...


class OffsetAndMetadata:
    offset: int
    metadata: str
