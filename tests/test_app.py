try:
    from unittest.mock import AsyncMock
except ImportError:
    AsyncMock = None


import kafkaesk
import kafkaesk.exceptions
import pydantic
import pytest

pytestmark = pytest.mark.asyncio


async def test_app_events(app):
    async def on_finalize():
        pass

    app.on("finalize", on_finalize)
    assert len(app._event_handlers["finalize"]) == 1


async def test_app_finalize_event(app):
    class CallTracker:
        def __init__(self):
            self.called = False

        async def on_finalize(self):
            self.called = True

    tracker = CallTracker()
    app.on("finalize", tracker.on_finalize)
    await app.finalize()

    assert tracker.called is True


def test_mount_router(app):
    router = kafkaesk.Router()

    @router.schema("Foo", streams=["foo.bar"])
    class Foo(pydantic.BaseModel):
        bar: str

    @router.subscribe("foo.bar", group="test_group")
    async def consume(data: Foo, schema, record):
        ...

    app.mount(router)

    assert app.subscriptions == router.subscriptions
    assert app.schemas == router.schemas
    assert app.event_handlers == router.event_handlers


def test_app_default_retry_policy(app):
    # Test that app used module defaults
    assert app.get_default_retry_policy() == kafkaesk.get_default_retry_policy()

    # Test that the app's default retry policy can be updated
    app.set_default_retry_policy(kafkaesk.retry.Forward)
    assert app.get_default_retry_policy() == kafkaesk.retry.Forward
    assert kafkaesk.get_default_retry_policy() == kafkaesk.retry.NoRetry

    # Test that setting an app's default policy to none will use module default
    app.set_default_retry_policy(None)
    assert app.get_default_retry_policy() == kafkaesk.get_default_retry_policy()


@pytest.mark.skipif(AsyncMock is None, reason="Only py 3.8")
async def test_consumer_health_check():
    app = kafkaesk.Application()
    subscription_consumer = AsyncMock()
    app._subscription_consumers.append(subscription_consumer)
    subscription_consumer.consumer._client.ready.return_value = True
    await app.health_check()


@pytest.mark.skipif(AsyncMock is None, reason="Only py 3.8")
async def test_consumer_health_check_raises_exception():
    app = kafkaesk.Application()
    subscription_consumer = AsyncMock()
    app._subscription_consumers.append(subscription_consumer)
    subscription_consumer.consumer._client.ready.return_value = False
    with pytest.raises(kafkaesk.exceptions.ConsumerUnhealthyException):
        await app.health_check()
