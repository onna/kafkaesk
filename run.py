from kafkaesk.scheduler import Scheduler
import asyncio
from aiokafka import TopicPartition, ConsumerRecord


async def f1(timeout=0):
    await asyncio.sleep(timeout)
    print(f"hello {timeout}!")


async def main():
    s = Scheduler()
    args = {
        "topic": "foo",
        "partition": 2,
        "timestamp": 3,
        "timestamp_type": None,
        'key': None, 'value': None, 'checksum': None, 'serialized_key_size': None, 'serialized_value_size': None, 'headers': None,

    }
    await s.start()
    await s.spawn(f1(), record=ConsumerRecord(offset=1, **args), tp=TopicPartition(topic="foo", partition=2))
    await s.spawn(f1(3), record=ConsumerRecord(offset=2, **args), tp=TopicPartition(topic="foo", partition=2))
    await s.spawn(f1(2), record=ConsumerRecord(offset=3, **args), tp=TopicPartition(topic="foo", partition=2))
    await asyncio.sleep(1)
    print(s.get_offsets())
    await s.graceful_shutdown()
    print(s.get_offsets())


asyncio.run(main())
