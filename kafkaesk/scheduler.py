from aiokafka import ConsumerRecord
from aiokafka import TopicPartition
from collections import defaultdict
from typing import Awaitable
from typing import Dict
from typing import List
from typing import Optional

import asyncio
import queue


class Job:
    def __init__(self, coro: Awaitable, *, record: ConsumerRecord, tp: TopicPartition):
        self.coro = coro
        self.record = record
        self.tp = tp


class Scheduler:
    def __init__(self, workers: int = 10):
        self._running: Dict = defaultdict(queue.PriorityQueue)
        self._workers = workers
        self._semaphore: asyncio.Semaphore = None  # type: ignore
        self._offsets = dict()

    def on_partitions_revoked(self, revoked: List[TopicPartition]) -> None:
        for tp in revoked:
            del self._running[tp]

    async def initialize(self) -> None:
        self._semaphore = asyncio.Semaphore(self._workers)

    async def spawn(
        self,
        coro: Awaitable,
        record: ConsumerRecord,
        tp: TopicPartition,
        timeout: Optional[int] = None,
    ) -> None:
        await self._semaphore.acquire()
        job = Job(coro=coro, record=record, tp=tp)
        fut = asyncio.create_task(job.coro)
        self._running[tp].put((job.record.offset, fut))
        fut.add_done_callback(self.callback)

    def callback(self, future: asyncio.Future) -> None:
        self._semaphore.release()

    def get_offsets(self) -> Dict:
        new_offsets = dict()
        for tp, running_queue in self._running.items():
            try:
                while x := running_queue.get_nowait():
                    offset, fut = x
                    if fut.done():
                        new_offsets[tp] = offset + 1
                    else:
                        running_queue.put(x)
                        break
            except queue.Empty:
                continue
        self._offsets.update(new_offsets)
        return self._offsets

    def raise_if_errors(self) -> None:
        for tp, running_queue in self._running.items():
            for _, fut in running_queue.queue:
                if fut.done():
                    if exception := fut.exception():
                        raise exception

    async def graceful_shutdown(self) -> None:
        futures = []
        for running_queue in self._running.values():
            for _, fut in running_queue.queue:
                futures.append(fut)
        await asyncio.gather(*futures)
        await self.force_shutdown()

    async def force_shutdown(self) -> None:
        for running_queue in self._running.values():
            for _, fut in running_queue.queue:
                fut.cancel()
                try:
                    await fut
                except asyncio.CancelledError:
                    pass
