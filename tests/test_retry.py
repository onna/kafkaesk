from aiokafka.structs import ConsumerRecord
from unittest.mock import Mock
from unittest.mock import patch

import asyncio
import kafkaesk
import pydantic
import pytest

try:
    from unittest.mock import AsyncMock
except ImportError:
    AsyncMock = None

pytestmark = pytest.mark.asyncio


@pytest.fixture
def record():
    return ConsumerRecord(
        topic="foobar",
        partition=0,
        offset=0,
        timestamp=1604951726856,
        timestamp_type=0,
        key=None,
        value=b'{"schema":"Foo:1","data":{"bar":"1"}}',
        checksum=None,
        serialized_key_size=-1,
        serialized_value_size=37,
        headers=(),
    )


class NOOPException(Exception):
    ...


async def noop_callback(*args, **kwargs):
    ...


async def test_default_retry_policy():
    # Check that the initial default is correct
    assert kafkaesk.get_default_retry_policy() == kafkaesk.retry.NoRetry

    # Check that the default policy can be changed
    kafkaesk.set_default_retry_policy(kafkaesk.retry.Forward)
    assert kafkaesk.get_default_retry_policy() == kafkaesk.retry.Forward


@pytest.mark.skipif(AsyncMock is None, reason="Only py 3.8")
async def test_retry_policy(app, record):
    class NOOPRetry(kafkaesk.retry.RetryPolicy):
        def __init__(self):
            self._retry_flag = True

            super().__init__()

        def set_should_retry(self, action):
            self._retry_flag = action

        def should_retry(self, *args, **kwargs):
            return self._retry_flag

        handle_retry = AsyncMock()
        handle_failure = AsyncMock()

    policy = NOOPRetry()
    subscription = kafkaesk.app.Subscription("foobar", noop_callback, "group", policy)

    error = NOOPException()

    # Check for initilization errors
    with pytest.raises(RuntimeError):
        await policy(record, error)

    await policy.initialize(app, subscription)

    # Check that retry logic is called
    with patch("kafkaesk.retry.MESSAGE_REQUEUED") as requeued_metrics:
        await policy(record, error)
        policy.handle_retry.assert_called_once()

        requeued_metrics.labels.assert_called_with(
            stream_id="foobar", partition=0, group_id="group", error=error.__class__.__name__
        )
        requeued_metrics.labels(
            stream_id="foobar", partition=0, group_id="group", error=error.__class__.__name__
        ).inc.assert_called_once()

    # Check that failure logic is called
    with patch("kafkaesk.retry.MESSAGE_FAILED") as failed_metrics:
        policy.set_should_retry(False)

        await policy(record, error)
        policy.handle_failure.assert_awaited_once()

        failed_metrics.labels.assert_called_with(
            stream_id="foobar", partition=0, group_id="group", error=error.__class__.__name__
        )
        failed_metrics.labels(
            stream_id="foobar", partition=0, group_id="group", error=error.__class__.__name__
        ).inc.assert_called_once()

    # Check that UnhandledMessage errors are handled properly
    with patch("kafkaesk.retry.MESSAGE_FAILED") as failed_metrics:
        policy.set_should_retry(True)
        message_error = kafkaesk.exceptions.UnhandledMessage()

        policy.handle_failure.reset_mock(side_effect=message_error)

        await policy(record, message_error)
        policy.handle_failure.assert_awaited_once()

        failed_metrics.labels.assert_called_with(
            stream_id="foobar",
            partition=0,
            group_id="group",
            error=message_error.__class__.__name__,
        )
        failed_metrics.labels(
            stream_id="foobar",
            partition=0,
            group_id="group",
            error=message_error.__class__.__name__,
        ).inc.assert_called_once()


async def test_no_retry_policy(app, record):
    policy = kafkaesk.retry.NoRetry()
    subscription = kafkaesk.app.Subscription("foobar", noop_callback, "group", policy)

    await policy.initialize(app, subscription)
    error = NOOPException()

    assert policy.should_retry(record, error) is False
    with pytest.raises(NOOPException):
        await policy.handle_failure(record, error)


@pytest.mark.skipif(AsyncMock is None, reason="Only py 3.8")
async def test_forward_retry_policy(record):
    app_mock = AsyncMock()

    policy = kafkaesk.retry.Forward()
    subscription = kafkaesk.app.Subscription("foobar", noop_callback, "group", policy)

    await policy.initialize(app_mock, subscription)
    error = NOOPException()

    # Make sure we never retry
    assert policy.should_retry(record, error) is False

    # Check that failed messages are forwarded to the correct queue
    await policy(record, error)

    app_mock.publish.assert_awaited_once()
    assert app_mock.publish.mock_calls[0].args[0] == "group__foobar"
    assert isinstance(app_mock.publish.mock_calls[0].args[1], kafkaesk.retry.FailureMessage)


async def test_subscribe_sets_default_retry_policy(app):
    @app.schema("Foo", version=1)
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def noop(data: Foo):
        ...

    assert app._subscriptions[0].retry_policy is None


async def test_subscribe_sets_retry_policy(app):
    @app.schema("Foo", version=1)
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group", retry=kafkaesk.retry.NoRetry())
    async def noop(data: Foo):
        ...

    assert isinstance(app._subscriptions[0].retry_policy, kafkaesk.retry.NoRetry)


@pytest.mark.skipif(AsyncMock is None, reason="Only py 3.8")
async def test_subscription_creates_default_retry_policy(app):
    @app.schema("Foo", version=1)
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group")
    async def noop(data: Foo):
        ...

    factory_mock = Mock(return_value=AsyncMock())
    app.set_default_retry_policy(factory_mock)

    async with app:
        fut = asyncio.create_task(app.consume_for(1, seconds=5))
        await asyncio.sleep(0.2)

        await app.publish("foo.bar", Foo(bar="1"))
        await app.flush()
        await fut

    factory_mock.assert_called_once()
    factory_mock.return_value.initialize.assert_awaited_once()
    factory_mock.return_value.finalize.assert_awaited_once()


@pytest.mark.skipif(AsyncMock is None, reason="Only py 3.8")
async def test_subscription_calls_retry_policy(app):
    policy_mock = AsyncMock()

    @app.schema("Foo", version=1)
    class Foo(pydantic.BaseModel):
        bar: str

    @app.subscribe("foo.bar", group="test_group", retry=policy_mock)
    async def noop(data: Foo):
        raise kafkaesk.exceptions.UnhandledMessage()

    async with app:
        fut = asyncio.create_task(app.consume_for(1, seconds=5))
        await asyncio.sleep(0.2)

        await app.publish("foo.bar", Foo(bar="1"))
        await app.flush()
        await fut

    policy_mock.assert_awaited_once()
