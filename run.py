import kafkaesk.scheduler as scheduler
import asyncio


async def f1(timeout=0):
    await asyncio.sleep(timeout)
    print(f"hello {timeout}!")





async def main():
    s = scheduler.Scheduler()
    await s.start()
    await s.spawn(None, f1())
    await s.spawn(None, f1(3))
    await s.spawn(None, f1(2))
    await asyncio.sleep(0)
    await s.grateful_shutdown()


asyncio.run(main())
