from kafkaesk import Application
from functools import partial
from kafkaesk import Subscription
from kafkaesk import SubscriptionConsumer
from kafkaesk.exceptions import ConsumerUnhealthyException
from kafkaesk.exceptions import StopConsumer
from kafkaesk.subscription import MessageHandler
from tests.utils import record_factory
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import aiokafka.errors
import asyncio
import opentracing
import pydantic
import pytest

pytestmark = pytest.mark.asyncio


@pytest.fixture()
def subscription():
    yield SubscriptionConsumer(
        Application(kafka_servers=["foobar"]), Subscription("foo", lambda record: 1, "group")
    )


def test_subscription_repr():
    sub = Subscription("stream_id", lambda x: None, "group")
    assert repr(sub) == "<Subscription stream: stream_id >"


class TestMessageHandler:
    def factory(self, func):
        consumer = MagicMock()
        consumer._subscription.func = func
        return MessageHandler(consumer)

    async def test_message_handler(self):
        async def raw_func(data):
            assert isinstance(data, dict)

        mh = self.factory(raw_func)
        await mh.handle(record_factory(), None)

    async def test_message_handler_map_types(self):
        class Foo(pydantic.BaseModel):
            foo: str

        async def handle_func(ob: Foo, schema, record, app, subscriber, span: opentracing.Span):
            assert ob.foo == "bar"
            assert schema == "Foo:1"
            assert record is not None
            assert app is not None
            assert subscriber is not None
            assert span is not None

        mh = self.factory(handle_func)
        await mh.handle(record_factory(), MagicMock())


class TestSubscriptionConsumer:
    async def test_consumer_property_rasises_exception(self, subscription):
        with pytest.raises(RuntimeError):
            subscription.consumer

    async def test_retry_policy_property_rasises_exception(self, subscription):
        with pytest.raises(RuntimeError):
            subscription.retry_policy

    async def test_maybe_commit(self, subscription):
        subscription._consumer = AsyncMock()
        subscription._needs_commit = True
        subscription._last_commit = -10  # monotonic
        await subscription._maybe_commit()
        subscription.consumer.commit.assert_called_once()

    async def test_maybe_commit_on_message_timeout(self, subscription):
        subscription._consumer = AsyncMock()
        subscription._consumer.getone = partial(asyncio.sleep, 1)
        subscription._running = subscription._needs_commit = True
        subscription._last_commit = -10  # monotonic
        maybe_commit = AsyncMock()
        with patch.object(subscription, "_maybe_commit", maybe_commit):
            asyncio.create_task(subscription.stop())
            await subscription._run()
        maybe_commit.assert_called_once()

    async def test_maybe_commit_handles_commit_failure(self, subscription):
        subscription._consumer = AsyncMock()
        subscription._consumer.commit.side_effect = aiokafka.errors.CommitFailedError
        subscription._last_commit = -10  # monotonic
        subscription._needs_commit = True
        await subscription._maybe_commit()
        subscription.consumer.commit.assert_called_once()

    async def test_healthy_with_no_consumer_set(self, subscription):
        assert await subscription.healthy() is None

    async def test_healthy(self, subscription):
        subscription._consumer = MagicMock()
        subscription._consumer._coordinator.coordinator_id = "coordinator_id"
        subscription._consumer._client.ready = AsyncMock(return_value=True)
        assert await subscription.healthy() is None
        subscription._consumer._client.ready.assert_called_with("coordinator_id")

    async def test_unhealthy(self, subscription):
        subscription._consumer = MagicMock()
        subscription._consumer._client.ready = AsyncMock(return_value=False)
        with pytest.raises(ConsumerUnhealthyException):
            assert await subscription.healthy()

    async def test_emit(self):
        probe = AsyncMock()
        sub = SubscriptionConsumer(
            Application(),
            Subscription("foo", lambda record: 1, "group"),
            event_handlers={"event": [probe]},
        )
        await sub.emit("event", "foo", "bar")
        probe.assert_called_with("foo", "bar")

    async def test_emit_raises_stop(self):
        sub = SubscriptionConsumer(
            Application(),
            Subscription("foo", lambda record: 1, "group"),
            event_handlers={"event": [AsyncMock(side_effect=StopConsumer)]},
        )
        with pytest.raises(StopConsumer):
            await sub.emit("event", "foo", "bar")

    async def test_emit_swallow_ex(self):
        sub = SubscriptionConsumer(
            Application(),
            Subscription("foo", lambda record: 1, "group"),
            event_handlers={"event": [AsyncMock(side_effect=Exception)]},
        )
        await sub.emit("event", "foo", "bar")

    async def test_retries_on_connection_failure(self):
        sub = SubscriptionConsumer(
            Application(),
            Subscription("foo", lambda record: 1, "group"),
        )
        run_mock = AsyncMock()
        sleep = AsyncMock()
        run_mock.side_effect = [aiokafka.errors.KafkaConnectionError, StopConsumer]
        with patch.object(sub, "initialize", AsyncMock()), patch.object(
            sub, "finalize", AsyncMock()
        ), patch.object(sub, "_run", run_mock), patch("kafkaesk.subscription.asyncio.sleep", sleep):
            await sub()
            sleep.assert_called_once()
            assert len(run_mock.mock_calls) == 2

    async def test_finalize_handles_exceptions(self):
        sub = SubscriptionConsumer(
            Application(),
            Subscription("foo", lambda record: 1, "group"),
        )
        consumer = AsyncMock()
        consumer.stop.side_effect = Exception
        consumer.commit.side_effect = Exception
        retry_policy = AsyncMock()
        retry_policy.finalize.side_effect = Exception

        sub._consumer = consumer
        sub._retry_policy = retry_policy
        await sub.finalize()

        consumer.stop.assert_called_once()
        consumer.commit.assert_called_once()
        retry_policy.finalize.assert_called_once()

    async def test_run_exits_when_fut_closed_fut(self):
        sub = SubscriptionConsumer(
            Application(),
            Subscription("foo", lambda record: 1, "group"),
        )
        consumer = AsyncMock()
        consumer.getmany.return_value = {"": [record_factory() for _ in range(10)]}
        sub._consumer = consumer
        sub._running = True

        async def _handle_message(record):
            await asyncio.sleep(0.03)

        with patch.object(sub, "_handle_message", _handle_message):
            task = asyncio.create_task(sub._run())
            await asyncio.sleep(0.01)
            stop_task = asyncio.create_task(sub.stop())
            await asyncio.sleep(0.01)
            sub._close_fut.set_result(None)

            await asyncio.wait([stop_task, task])
