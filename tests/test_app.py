import kafkaesk
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
