from aiokafka import ConsumerRecord
from kafkaesk.kafka import KafkaTopicManager

import asyncio
import pydantic
import pytest
import uuid

pytestmark = pytest.mark.asyncio


async def test_data_binding(app):
    consumed = []

    @app.schema("Foo", streams=["foo.bar"])
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def consume(data: Foo, schema, record):
        consumed.append((data, schema, record))

    async with app:
        await app.publish("foo.bar", Foo(bar="1"))
        await app.flush()
        await app.consume_for(1, seconds=5)

    assert len(consumed) == 1
    assert len(consumed[0]) == 3


async def test_consume_message(app):
    consumed = []

    @app.schema("Foo", streams=["foo.bar"])
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def consume(data: Foo):
        consumed.append(data)

    async with app:
        await app.publish("foo.bar", Foo(bar="1"))
        await app.flush()
        await app.consume_for(1, seconds=5)

    assert len(consumed) == 1


async def test_consume_many_messages(app):
    consumed = []

    @app.schema("Foo", streams=["foo.bar"])
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def consume(data: Foo):
        consumed.append(data)

    async with app:
        fut = asyncio.create_task(app.consume_for(1000, seconds=5))
        await asyncio.sleep(0.1)
        for idx in range(1000):
            await app.publish("foo.bar", Foo(bar=str(idx)))
        await app.flush()
        await fut

    assert len(consumed) == 1000


async def test_not_consume_message_that_does_not_match(app):
    consumed = []

    @app.schema("Foo", streams=["foo.bar"])
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def consume(data: Foo):
        consumed.append(data)

    async with app:
        await app.publish("foo.bar1", Foo(bar="1"))
        await app.flush()
        await app.consume_for(1, seconds=1)

    assert len(consumed) == 0


async def test_subscribe_without_group(app):
    @app.schema("Foo")
    class Foo(pydantic.BaseModel):
        bar: str

    with pytest.raises(TypeError):

        @app.subscribe("foo.bar")
        async def consume(data: Foo):
            ...


async def test_multiple_subscribers_different_models(app):
    consumed1 = []
    consumed2 = []

    @app.schema("Foo", version=1, streams=["foo.bar"])
    class Foo1(pydantic.BaseModel):
        bar: str

    @app.schema("Foo", version=2)
    class Foo2(pydantic.BaseModel):
        foo: str
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def consume1(data: Foo1):
        consumed1.append(data)

    @app.subscribe("foo.bar", group="test_group_2")
    async def consume2(data: Foo2):
        consumed2.append(data)

    async with app:
        fut = asyncio.create_task(app.consume_for(4, seconds=5))
        await asyncio.sleep(0.2)

        await app.publish("foo.bar", Foo1(bar="1"))
        await app.publish("foo.bar", Foo2(foo="2", bar="3"))
        await app.flush()
        await fut

    assert all([isinstance(v, Foo1) for v in consumed1])
    assert all([isinstance(v, Foo2) for v in consumed2])


async def test_subscribe_diff_data_types(app):
    consumed_records = []
    consumed_bytes = []

    @app.schema("Foo", version=1, streams=["foo.bar"])
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def consume_record(data: ConsumerRecord):
        consumed_records.append(data)

    @app.subscribe("foo.bar", group="test_group_2")
    async def consume_bytes(data: bytes):
        consumed_bytes.append(data)

    async with app:
        await app.publish("foo.bar", Foo(bar="1"))
        await app.flush()
        await app.consume_for(1, seconds=5)

    assert len(consumed_records) == 1
    assert len(consumed_bytes) == 1
    assert isinstance(consumed_records[0], ConsumerRecord)
    assert isinstance(consumed_bytes[0], bytes)


async def test_subscribe_to_topic_that_does_not_exist(app):
    consumed_records = []

    @app.schema("Foo", version=1)
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def consume_record(data: Foo):
        consumed_records.append(data)

    async with app:
        fut = asyncio.create_task(app.consume_for(10, seconds=5))
        await asyncio.sleep(0.5)

        for idx in range(10):
            await app.publish("foo.bar", Foo(bar=str(idx)))

        await app.flush()
        await fut

    assert len(consumed_records) == 10


async def test_subscribe_to_topic_that_already_has_messages_for_group(app):
    consumed_records = []

    @app.schema("Foo", version=1)
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def consume_record(data: Foo):
        consumed_records.append(data)

    async with app:
        for idx in range(10):
            await app.publish("foo.bar", Foo(bar=str(idx)))
        await app.flush()

        fut = asyncio.create_task(app.consume_for(20, seconds=5))

        for idx in range(10):
            await app.publish("foo.bar", Foo(bar=str(idx)))
        await app.flush()

        await fut

    assert len(consumed_records) == 20


async def test_cache_topic_exists_topic_mng(kafka):
    mng = KafkaTopicManager(bootstrap_servers=[f"{kafka[0]}:{kafka[1]}"], prefix=uuid.uuid4().hex)

    topic_id = mng.get_topic_id("foobar")
    assert not await mng.topic_exists(topic_id)
    assert topic_id not in mng._topic_cache

    await mng.create_topic(topic_id)
    assert await mng.topic_exists(topic_id)
