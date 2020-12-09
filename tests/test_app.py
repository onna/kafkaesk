from unittest.mock import AsyncMock
from unittest.mock import MagicMock

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


async def test_consumer_health_check():
    app = kafkaesk.Application()
    subscription_consumer = AsyncMock()
    app._subscription_consumers.append(subscription_consumer)
    subscription_consumer.consumer._client.ready.return_value = True
    await app.health_check()


async def test_consumer_health_check_raises_exception():
    app = kafkaesk.Application()
    subscription_consumer = kafkaesk.SubscriptionConsumer(
        app, kafkaesk.Subscription("foo", lambda: 1, "group")
    )
    app._subscription_consumers.append(subscription_consumer)
    subscription_consumer._consumer = AsyncMock()
    subscription_consumer._consumer._client.ready.return_value = False
    with pytest.raises(kafkaesk.exceptions.ConsumerUnhealthyException):
        await app.health_check()


async def test_consumer_health_check_raises_exception_if_commit_task_done():
    app = kafkaesk.Application()
    subscription_consumer = kafkaesk.SubscriptionConsumer(
        app, kafkaesk.Subscription("foo", lambda: 1, "group")
    )
    subscription_consumer._consumer = MagicMock()
    subscription_consumer._auto_commit_task = MagicMock()
    subscription_consumer._auto_commit_task.done.return_value = True
    app._subscription_consumers.append(subscription_consumer)
    with pytest.raises(kafkaesk.exceptions.AutoCommitError):
        await app.health_check()
