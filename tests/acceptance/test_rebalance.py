from .produce import Foo
from .produce import producer
from kafkaesk.consumer import BatchConsumer

import asyncio
import kafkaesk
import pytest

pytestmark = pytest.mark.asyncio

GROUP = TOPIC = "test-rebalance"


async def test_cancel_getone(app):

    app.schema(streams=[TOPIC])(Foo)

    async def handler(*args, **kwargs):
        pass

    async with app:
        consumer = BatchConsumer(
            stream_id=TOPIC,
            group_id=GROUP,
            coro=handler,
            app=app,
            concurrency=1,
            timeout_seconds=1,
        )
        await consumer.initialize()
        raw_consumer = consumer._consumer
        with raw_consumer._subscription.fetch_context():
            try:
                await asyncio.wait_for(raw_consumer._fetcher.next_record([]), timeout=0.1)
            except asyncio.TimeoutError:
                assert len(raw_consumer._fetcher._fetch_waiters) == 0
        await raw_consumer.stop()


async def test_many_consumers_rebalancing(kafka, topic_prefix):
    apps = []
    for idx in range(5):
        app = kafkaesk.Application(
            [f"{kafka[0]}:{kafka[1]}"],
            topic_prefix=topic_prefix,
        )
        app.schema(streams=[TOPIC])(Foo)
        app.id = idx

        @app.subscribe(TOPIC, group=GROUP)
        async def consumer(ob: Foo, record, app):
            ...

        await app.initialize()
        apps.append(app)

    produce = asyncio.create_task(producer(apps[0], TOPIC))

    consumer_tasks = []
    for app in apps:
        consumer_tasks.append(asyncio.create_task(app.consume_forever()))

    await asyncio.sleep(5)

    # cycle through each, destroying...
    for idx in range(5):
        await apps[idx].stop()
        await asyncio.sleep(1)
        assert consumer_tasks[idx].done()

        # start again
        consumer_tasks[idx] = asyncio.create_task(apps[idx].consume_forever())

    produce.cancel()

    for idx in range(5):
        await apps[idx].stop()


async def test_consume_every_message_once_during_rebalance(kafka, topic_prefix):
    """
    No matter what, even without reassignment, some messages
    seem to be relayed. You can see if when a single consumer and no rebalance
    sometimes.
    """
    consumed = {}

    def record_msg(record):
        key = f"{record.partition}-{record.offset}"
        if key not in consumed:
            consumed[key] = 0
        consumed[key] += 1

    apps = []
    for idx in range(5):
        app = kafkaesk.Application(
            [f"{kafka[0]}:{kafka[1]}"],
            topic_prefix=topic_prefix,
        )
        app.schema(streams=[TOPIC])(Foo)
        app.id = idx

        @app.subscribe(TOPIC, group=GROUP)
        async def consumer(ob: Foo, record, app):
            record_msg(record)

        await app.initialize()
        apps.append(app)

    consumer_tasks = []
    for app in apps:
        consumer_tasks.append(asyncio.create_task(app.consume_forever()))

    await asyncio.sleep(1)
    produce = asyncio.create_task(producer(apps[0], TOPIC))
    await asyncio.sleep(5)

    # cycle through each, destroying...
    for idx in range(5):
        await apps[idx].stop()
        await asyncio.sleep(1)
        assert consumer_tasks[idx].done()

        # start again
        consumer_tasks[idx] = asyncio.create_task(apps[idx].consume_forever())

    produce.cancel()

    for idx in range(5):
        await apps[idx].stop()

    assert len(consumed) > 100
    # now check that we always consumed a message only once

    for v in consumed.values():
        assert v == 1
