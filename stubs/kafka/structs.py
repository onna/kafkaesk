class ConsumerRecord:
    partition: int
    offset: int
    topic: str
    value: bytes


class TopicPartition:
    topic: str
    partition: int

    def __init__(self, topic: str, partition: int):
        ...


class OffsetAndMetadata:
    ...
