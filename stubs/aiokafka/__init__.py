from asyncio.events import AbstractEventLoop
from typing import Awaitable, List, Optional


class AIOKafkaProducer:
    def __init__(
        self,
        bootstrap_servers: List[str],
        loop: AbstractEventLoop,
        enable_auto_commit: Optional[bool] = True,
        group_id: Optional[str] = None,
    ):
        ...

    async def send(self, topic_id: str, value: bytes, key: bytes) -> Awaitable:
        ...


class AIOKafkaConsumer:
    def __init__(
        self, *topics: str, group_id: Optional[str] = None, iter_forever: bool = False
    ):
        ...
