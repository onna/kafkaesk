from asyncio.events import AbstractEventLoop
from kafka.structs import ConsumerRecord
from kafka.structs import TopicPartition
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import List
from typing import Optional


class AIOKafkaProducer:
    def __init__(
        self,
        bootstrap_servers: List[str],
        loop: AbstractEventLoop,
        enable_auto_commit: Optional[bool] = True,
        group_id: Optional[str] = None,
    ):
        ...

    async def send(
        self, topic_id: str, value: bytes, key: Optional[bytes] = None
    ) -> Awaitable[ConsumerRecord]:
        ...

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def flush(self) -> None:
        ...


class AIOKafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: List[str],
        loop: AbstractEventLoop,
        group_id: Optional[str],
        **kwargs: Any,
    ):
        ...

    async def subscribe(
        self, pattern: Optional[str] = None, listener: Optional["ConsumerRebalanceListener"] = None
    ) -> None:
        ...

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    def __aiter__(self) -> AsyncIterator[ConsumerRecord]:
        ...

    async def __anext__(self) -> ConsumerRecord:
        ...

    async def position(self, tp: TopicPartition) -> int:
        ...

    async def seek(self, tp: TopicPartition, offset: int) -> None:
        ...

    async def seek_to_beginning(self, tp: TopicPartition) -> None:
        ...


class ConsumerRebalanceListener:
    async def on_partitions_revoked(self, revoked: List[TopicPartition]) -> None:
        ...

    async def on_partitions_assigned(self, assigned: List[TopicPartition]) -> None:
        ...
