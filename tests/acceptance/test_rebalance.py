from kafkaesk.subscription import SubscriptionConsumer

import asyncio
import kafkaesk
import pydantic
import pytest

pytestmark = pytest.mark.asyncio

TOPIC = "foo.bar"


class Foo(pydantic.BaseModel):
    foo: str


async def producer(app):
    while True:
        try:
            await app.publish(TOPIC, Foo(foo="bar"))
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            return


async def test_cancel_getone(app):

    app.schema(streams=[TOPIC])(Foo)

    @app.subscribe(TOPIC, group="group")
    async def consumer(ob: Foo, record, app):
        ...

    async with app:
        sub_consumer = SubscriptionConsumer(app, app.subscriptions[0])
        await sub_consumer.initialize()
        raw_consumer = sub_consumer.consumer
        with raw_consumer._subscription.fetch_context():
            try:
                await asyncio.wait_for(raw_consumer._fetcher.next_record([]), timeout=0.1)
            except asyncio.TimeoutError:
                assert len(raw_consumer._fetcher._fetch_waiters) == 0
        await raw_consumer.stop()


async def test_many_consumers_rebalancing(kafka, topic_prefix):
    apps = []
    for idx in range(5):
        app = kafkaesk.Application([f"{kafka[0]}:{kafka[1]}"], topic_prefix=topic_prefix,)
        app.schema(streams=[TOPIC])(Foo)
        app.id = idx

        @app.subscribe(TOPIC, group="group")
        async def consumer(ob: Foo, record, app):
            ...

        await app.initialize()
        apps.append(app)

    produce = asyncio.create_task(producer(apps[0]))

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


async def _test_consume_every_message_once_during_rebalance(kafka, topic_prefix):
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
        app = kafkaesk.Application([f"{kafka[0]}:{kafka[1]}"], topic_prefix=topic_prefix,)
        app.schema(streams=[TOPIC])(Foo)
        app.id = idx

        @app.subscribe(TOPIC, group="group")
        async def consumer(ob: Foo, record, app):
            record_msg(record)

        await app.initialize()
        apps.append(app)

    consumer_tasks = []
    for app in apps:
        consumer_tasks.append(asyncio.create_task(app.consume_forever()))

    await asyncio.sleep(1)
    produce = asyncio.create_task(producer(apps[0]))
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
