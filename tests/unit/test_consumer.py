from kafkaesk import Application
from kafkaesk import Subscription
from kafkaesk.consumer import build_handler
from kafkaesk.consumer import BatchConsumer, Subscription
from kafkaesk.exceptions import ConsumerUnhealthyException
from kafkaesk.exceptions import StopConsumer
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
def subscription_conf():
    subscription = Subscription(
        "foo",
        lambda record: 1,
        "group",
        topics=["foo"],
        timeout_seconds=1,
    )
    yield subscription


@pytest.fixture()
def subscription(subscription_conf):
    yield BatchConsumer(
        subscription=subscription_conf,
        app=Application(kafka_servers=["foobar"]),
    )


def test_subscription_repr():
    sub = Subscription("stream_id", lambda x: None, "group")
    assert repr(sub) == "<Subscription stream: stream_id >"


class TestMessageHandler:
    def factory(self, func):
        return build_handler(func, app=MagicMock(), consumer=None)

    async def test_message_handler(self):
        side_effect = None

        async def raw_func(data):
            nonlocal side_effect
            assert isinstance(data, dict)
            side_effect = True

        handler = self.factory(raw_func)
        await handler(record_factory(), None)
        assert side_effect is True

    async def test_message_handler_map_types(self):
        class Foo(pydantic.BaseModel):
            foo: str

        async def handle_func(ob: Foo, schema, record, app, span: opentracing.Span):
            assert ob.foo == "bar"
            assert schema == "Foo:1"
            assert record is not None
            assert app is not None
            assert span is not None

        handler = self.factory(handle_func)
        await handler(record_factory(), MagicMock())


class TestSubscriptionConsumer:
    async def test_healthy(self, subscription):
        subscription._consumer = MagicMock()
        subscription._running = True
        subscription._consumer._coordinator.coordinator_id = "coordinator_id"
        subscription._consumer._client.ready = AsyncMock(return_value=True)
        assert await subscription.healthy() is None
        subscription._consumer._client.ready.assert_called_with("coordinator_id")

    async def test_unhealthy(self, subscription):
        subscription._consumer = MagicMock()
        subscription._running = True
        subscription._consumer._client.ready = AsyncMock(return_value=False)
        with pytest.raises(ConsumerUnhealthyException):
            assert await subscription.healthy()

        subscription._consumer = MagicMock()
        subscription._running = False
        with pytest.raises(ConsumerUnhealthyException):
            assert await subscription.healthy()

    async def test_emit(self, subscription_conf):
        probe = AsyncMock()

        sub = BatchConsumer(
            subscription=subscription_conf,
            app=Application(kafka_servers=["foobar"]),
            event_handlers={"event": [probe]},
        )
        await sub.emit("event", "foo", "bar")
        probe.assert_called_with("foo", "bar")

    async def test_emit_raises_stop(self, subscription_conf):
        sub = BatchConsumer(
            subscription=subscription_conf,
            app=Application(kafka_servers=["foobar"]),
            event_handlers={"event": [AsyncMock(side_effect=StopConsumer)]},
        )

        with pytest.raises(StopConsumer):
            await sub.emit("event", "foo", "bar")

    async def test_emit_swallow_ex(self, subscription_conf):
        sub = BatchConsumer(
            subscription=subscription_conf,
            app=Application(kafka_servers=["foobar"]),
            event_handlers={"event": [AsyncMock(side_effect=Exception)]},
        )

        await sub.emit("event", "foo", "bar")

    async def test_retries_on_connection_failure(self, subscription):
        run_mock = AsyncMock()
        sleep = AsyncMock()
        run_mock.side_effect = [aiokafka.errors.KafkaConnectionError, StopConsumer]
        subscription._consumer = MagicMock()
        with patch.object(subscription, "initialize", AsyncMock()), patch.object(
            subscription, "finalize", AsyncMock()
        ), patch.object(subscription, "_consume", run_mock), patch("kafkaesk.consumer.asyncio.sleep", sleep):
            await subscription()
            sleep.assert_called_once()
            assert len(run_mock.mock_calls) == 2

    async def test_finalize_handles_exceptions(self, subscription):
        consumer = AsyncMock()
        consumer.stop.side_effect = Exception
        consumer.commit.side_effect = Exception

        subscription._consumer = consumer
        await subscription.finalize()

        consumer.stop.assert_called_once()

    async def test_run_exits_when_fut_closed_fut(self, subscription):
        sub = subscription
        consumer = AsyncMock()
        consumer.getmany.return_value = {"": [record_factory() for _ in range(10)]}
        sub._consumer = consumer
        sub._running = True

        async def _handle_message(record):
            await asyncio.sleep(0.03)

        with patch.object(sub, "_handler", _handle_message):
            task = asyncio.create_task(sub._consume())
            await asyncio.sleep(0.01)
            stop_task = asyncio.create_task(sub.stop())
            await asyncio.sleep(0.01)
            sub._close.set_result(None)

            await asyncio.wait([stop_task, task])

    async def test_auto_commit_can_be_disabled(self, subscription_conf):
        sub = BatchConsumer(
            subscription=subscription_conf,
            app=Application(kafka_servers=["foobar"]),
            auto_commit=False,
        )
        await sub._maybe_commit()
        assert sub._last_commit == 0
