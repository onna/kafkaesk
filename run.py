from kafkaesk.scheduler import Scheduler, Record
import asyncio


async def f1(timeout=0):
    await asyncio.sleep(timeout)
    print(f"hello {timeout}!")


async def main():
    s = Scheduler()
    await s.start()
    await s.spawn(f1(), record=Record(1))
    await s.spawn(f1(3), record=Record(2))
    await s.spawn(f1(2), record=Record(3))
    await asyncio.sleep(1)
    await s.graceful_shutdown()
    print(s.get_offset())


asyncio.run(main())
