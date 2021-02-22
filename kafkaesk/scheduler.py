import asyncio


class Job:
    def __init__(self, coro, record=None):
        self.record = record
        self.coro = coro


class Scheduler:
    def __init__(self, workers=10):
        self._queue = asyncio.Queue(maxsize=workers)
        self._running = set()

    # aenter / aexit

    async def start(self):
        self.bg_task = asyncio.create_task(self.__background_task())

    async def grateful_shutdown(self):
        await asyncio.gather(*self._running)
        await self.force_shutdown()

    async def force_shutdown(self):
        for job in self._running:
            job.cancel()
            try:
                await job
            except asyncio.CancelledError:
                pass

        self.bg_task.cancel()
        try:
            await self.bg_task
        except asyncio.CancelledError:
            pass

    async def spawn(self, coro, record=0, timeout=None):
        job = Job(coro=coro, record=record)
        await self._queue.put(job)
        return job

    #scheduled
    #running
    #done

    async def __background_task(self):
        while True:
            job = await self._queue.get()

            fut = asyncio.create_task(job.coro)
            self._running.add(fut)
