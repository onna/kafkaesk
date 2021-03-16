from asyncio.events import AbstractEventLoop
from kafka.structs import ConsumerRecord
from kafka.structs import TopicPartition
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple


class AIOKafkaProducer:
    _sender: Any

    def __init__(
        self,
        bootstrap_servers: List[str],
        loop: AbstractEventLoop,
        enable_auto_commit: Optional[bool] = True,
        group_id: Optional[str] = None,
        api_version: str = "auto",
    ):
        ...

    async def send(
        self,
        topic_id: str,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[List[Tuple[str, bytes]]] = None,
    ) -> Awaitable[ConsumerRecord]:
        ...

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def flush(self) -> None:
        ...


class AIOKafkaClient:
    async def ready(self, node_id: str, *, group: Optional[str] = None) -> bool:
        ...


class GroupCoordinator:
    coordinator_id: str

    def request_rejoin(self) -> None:
        ...

    def need_rejoin(self, subscription: "Subscription") -> bool:
        ...

    async def ensure_coordinator_known(self) -> None:
        ...

    async def ensure_active_group(self, subscription: Any, assignment: Any) -> None:
        ...


class Subscription:
    subscription: Any

    async def wait_for_subscription(self) -> None:
        ...

    async def partitions_auto_assigned(self) -> bool:
        ...


class AIOKafkaConsumer:
    _client: AIOKafkaClient
    _coordinator: GroupCoordinator
    _subscription: Subscription
    _group_id: Optional[str]

    def __init__(
        self,
        bootstrap_servers: List[str],
        loop: AbstractEventLoop,
        group_id: Optional[str],
        api_version: str = "auto",
        **kwargs: Any,
    ):
        ...

    async def getone(self, *partitions: Optional[List[TopicPartition]]) -> ConsumerRecord:
        ...

    async def subscribe(
        self, pattern: Optional[str] = None, listener: Optional["ConsumerRebalanceListener"] = None
    ) -> None:
        ...

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def commit(self, to_commit: Optional[Dict[TopicPartition, int]] = None) -> None:
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

    def assignment(self) -> Set[TopicPartition]:
        ...

    async def getmany(
        self, *partitions: TopicPartition, timeout_ms: int = 0, max_records: int = None
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        ...


class ConsumerRebalanceListener:
    async def on_partitions_revoked(self, revoked: List[TopicPartition]) -> None:
        ...

    async def on_partitions_assigned(self, assigned: List[TopicPartition]) -> None:
        ...
