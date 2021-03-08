from aiokafka import ConsumerRecord
from kafkaesk.exceptions import ProducerUnhealthyException
from kafkaesk.kafka import KafkaTopicManager
from kafkaesk.retry import Forward
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import aiokafka.structs
import asyncio
import pydantic
import pytest
import uuid

try:
    from unittest.mock import AsyncMock
except:  # noqa
    AsyncMock = None  # type: ignore

pytestmark = pytest.mark.asyncio


async def test_data_binding(app):
    consumed = []

    @app.schema("Foo", streams=["foo.bar"])
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def consume(data: Foo, schema, record, app):
        consumed.append((data, schema, record, app))

    async with app:
        await app.publish_and_wait("foo.bar", Foo(bar="1"))
        await app.flush()
        await app.consume_for(1, seconds=5)

    assert len(consumed) == 1
    assert len(consumed[0]) == 4


async def test_consume_message(app):
    consumed = []

    @app.schema("Foo", streams=["foo.bar"])
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def consume(data: Foo):
        consumed.append(data)

    async with app:
        await app.publish_and_wait("foo.bar", Foo(bar="1"))
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
        fut = asyncio.create_task(app.consume_for(10, seconds=5))
        await asyncio.sleep(0.1)
        for idx in range(10):
            await app.publish("foo.bar", Foo(bar=str(idx)))
        await app.flush()
        await fut

    assert len(consumed) == 10


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

    @app.subscribe(
        "foo.bar",
        group="test_group",
        retry_handlers={Exception: Forward("test_group__foo.bar__Exception")},
    )
    async def consume1(data: Foo1):
        consumed1.append(data)

    @app.subscribe(
        "foo.bar",
        group="test_group_2",
        retry_handlers={Exception: Forward("test_group__foo.bar__Exception")},
    )
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


async def test_subscription_creates_retry_policy(app):
    @app.schema("Foo", version=1)
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def noop(data: Foo):
        ...

    policy_mock = Mock(return_value=AsyncMock())
    factory_mock = Mock(return_value=policy_mock)
    with patch("kafkaesk.subscription.RetryPolicy", new_callable=factory_mock):
        async with app:
            fut = asyncio.create_task(app.consume_for(1, seconds=5))
            await asyncio.sleep(0.2)

            await app.publish("foo.bar", Foo(bar=1))
            await app.flush()
            await fut

        policy_mock.assert_called_once()
        policy_mock.return_value.initialize.assert_awaited_once()
        policy_mock.return_value.finalize.assert_awaited_once()


async def test_subscription_calls_retry_policy(app):
    handler_mock = AsyncMock()

    @app.schema("Foo", version=1)
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group", retry_handlers={Exception: handler_mock})
    async def noop(data: Foo):
        raise Exception("Unhandled Exception")

    async with app:
        fut = asyncio.create_task(app.consume_for(1, seconds=5))
        await asyncio.sleep(0.2)

        await app.publish("foo.bar", Foo(bar=1))
        await app.flush()
        await fut

    handler_mock.assert_awaited_once()


async def test_subscription_failure(app):
    probe = Mock()
    stream_id = "foo-bar-subfailure"
    group_id = "test_sub_group_failure"
    topic_id = app.topic_mng.get_topic_id(stream_id)

    @app.schema(streams=[stream_id])
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe(stream_id, group=group_id)
    async def noop_ng(data: Foo):
        probe("error", data)
        raise Exception("Unhandled Exception")

    async with app:
        await app.publish(stream_id, Foo(bar=1))
        await app.publish(stream_id, Foo(bar=1))
        await app.flush()

        # it fails
        with pytest.raises(Exception):
            await app.consume_for(2, seconds=5)

        # verify we didn't commit
        offsets = [
            v
            for k, v in (await app.topic_mng.list_consumer_group_offsets(group_id)).items()
            if k.topic == topic_id
        ]
        assert offsets == []

    # remove wrong consumer
    app._subscriptions = []

    @app.subscribe(stream_id, group=group_id)
    async def noop_ok(data: Foo):
        probe("ok", data)

    async with app:
        await app.publish(stream_id, Foo(bar=2))
        await app.flush()

        await app.consume_for(3, seconds=3)

        # make sure we that now committed all messages
        assert (
            sum(
                [
                    om.offset
                    for tp, om in (
                        await app.topic_mng.list_consumer_group_offsets(group_id)
                    ).items()
                    if tp.topic == topic_id
                ]
            )
            == 3
        )

    probe.assert_has_calls(
        [call("error", Foo(bar="1")), call("ok", Foo(bar="1")), call("ok", Foo(bar="2"))],
        any_order=True,
    )


async def test_publish_unregistered_schema(app):
    probe = Mock()
    stream_id = "foo-bar-unregistered"
    group_id = "test-sub-unregistered"

    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe(stream_id, group=group_id)
    async def noop(data: Foo):
        probe(data)

    async with app:
        await app.publish(stream_id, Foo(bar=1))
        await app.publish(stream_id, Foo(bar=2))
        await app.flush()

        await app.consume_for(2, seconds=5)

    probe.assert_has_calls(
        [call(Foo(bar="1")), call(Foo(bar="2"))],
        any_order=True,
    )

    # 1 failed + 3 ok
    assert len(probe.mock_calls) == 2


async def test_raw_publish_data(app):
    probe = Mock()
    stream_id = "foo-bar-raw"
    group_id = "test-sub-raw"

    @app.subscribe(stream_id, group=group_id)
    async def noop(record: aiokafka.structs.ConsumerRecord):
        probe(record.value)

    async with app:
        await app.raw_publish(stream_id, b"1")
        await app.raw_publish(stream_id, b"2")
        await app.flush()

        await app.consume_for(2, seconds=5)

    probe.assert_has_calls(
        [call(b"1"), call(b"2")],
        any_order=True,
    )

    # 1 failed + 3 ok
    assert len(probe.mock_calls) == 2


async def test_publish_unhealthy(app):

    async with app:
        app._producer = AsyncMock()
        app._producer._sender = MagicMock()
        app._producer._sender.sender_task.done.return_value = True
        with pytest.raises(ProducerUnhealthyException):
            await app.raw_publish("foobar", b"foobar")
