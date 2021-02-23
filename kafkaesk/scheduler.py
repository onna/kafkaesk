import asyncio
import queue
from collections import defaultdict
from typing import Optional, List
from aiokafka import ConsumerRecord
from aiokafka import TopicPartition


class Job:
    def __init__(self, coro, *, record: ConsumerRecord, tp: TopicPartition):
        self.coro = coro
        self.record = record
        self.tp = tp


class Scheduler:
    def __init__(self, workers: int = 10):
        self._running = defaultdict(queue.PriorityQueue)
        self._workers = workers
        self._semaphore: asyncio.Semaphore = None

    # aenter / aexit

    def on_partitions_revoked(self, revoked: List[TopicPartition]) -> None:
        for tp in revoked:
            del self._running[tp]

    async def initialize(self):
        self._semaphore = asyncio.Semaphore(self._workers)

    async def spawn(
        self, coro, record: ConsumerRecord, tp: TopicPartition, timeout=None
    ):
        await self._semaphore.acquire()
        job = Job(coro=coro, record=record, tp=tp)
        fut = asyncio.create_task(job.coro)
        self._running[tp].put((job.record.offset, fut))
        fut.add_done_callback(self.callback)

    def callback(self, future):
        self._semaphore.release()

    def get_offsets(self) -> Optional[int]:
        new_offsets = dict()
        for tp, running_queue in self._running.items():
            try:
                while x := running_queue.get_nowait():
                    offset, fut = x
                    if fut.done():
                        new_offsets[tp] = offset
                    else:
                        running_queue.put(x)
                        break
            except queue.Empty:
                continue
        return new_offsets

    def raise_if_errors(self):
        for tp, running_queue in self._running.items():
            for _, fut in running_queue.queue:
                if fut.done():
                    if exception := fut.exception():
                        raise exception

    async def graceful_shutdown(self):
        futures = []
        for running_queue in self._running.values():
            for _, fut in running_queue.queue:
                futures.append(fut)
        await asyncio.gather(*futures)
        await self.force_shutdown()

    async def force_shutdown(self):
        for running_queue in self._running.values():
            for _, fut in running_queue.queue:
                fut.cancel()
                try:
                    await fut
                except asyncio.CancelledError:
                    pass
