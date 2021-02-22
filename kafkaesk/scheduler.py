import asyncio
import functools
import queue
from collections import defaultdict
from typing import Optional


class Record:
    def __init__(self, offset, partition=None):
        self.offset = offset
        self.partition = partition


class Job:
    def __init__(self, coro, *, record: Record):
        self.coro = coro
        self.record = record


class Scheduler:
    def __init__(self, workers: int = 10):
        self._running = defaultdict(queue.PriorityQueue)
        self._semaphore = asyncio.Semaphore(workers)

    # aenter / aexit

    async def start(self):
        ...

    async def spawn(self, coro, record: Record, timeout=None):
        await self._semaphore.acquire()
        job = Job(coro=coro, record=record)
        fut = asyncio.create_task(job.coro)
        self._running[job.record.partition].put((job.record.offset, job, fut))
        fut.add_done_callback(self.callback)

    async def callback(self, future):
        self._semaphore.release()

    def get_offset(self) -> Optional[int]:
        new_offset = dict()
        for partition, running_queue in self._running.items():
            while x := running_queue.get():
                offset, job, fut = xn
                if fut.done():
                    new_offset[partition] = offset
                else:
                    running_queue.put(x)
                    break
        return new_offset

    async def graceful_shutdown(self):
        futures = []
        for running_queue in self._running.values():
            for _, _, fut in running_queue.queue:
                futures.append(fut)
        await asyncio.gather(*futures)
        await self.force_shutdown()

    async def force_shutdown(self):
        for running_queue in self._running.values():
            for _, _, fut in running_queue.queue:
                fut.cancel()
                try:
                    await fut
                except asyncio.CancelledError:
                    pass
