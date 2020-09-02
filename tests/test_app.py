import pytest
import kafkaesk


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
