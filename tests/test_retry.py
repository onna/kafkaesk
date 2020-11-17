from aiokafka.structs import ConsumerRecord
from datetime import datetime
from kafkaesk import retry

import kafkaesk
import pytest

try:
    from unittest.mock import AsyncMock
except:  # noqa
    AsyncMock = None  # type: ignore

pytestmark = pytest.mark.asyncio


class NOOPException(Exception):
    ...


class NOOPCallback:
    ...


@pytest.fixture  # type: ignore
def record() -> ConsumerRecord:
    return ConsumerRecord(  # type: ignore
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


async def test_retry_message_is_serializable(record: ConsumerRecord) -> None:
    retry_history = retry.RetryHistory()
    retry_history.failures.append(
        retry.FailureInfo(
            exception="UnhandledMessage", handler_key="Exception", timestamp=datetime.now()
        )
    )
    retry_message = retry.RetryMessage(
        original_record=retry.Record.from_consumer_record(record), retry_history=retry_history
    )

    json_serialized = retry_message.json()

    # Ensure we can re-create a message from the json
    retry.RetryMessage.parse_raw(json_serialized)


@pytest.mark.skipif(AsyncMock is None, reason="Only py 3.8")  # type: ignore
async def test_retry_policy(app: kafkaesk.Application, record: ConsumerRecord) -> None:
    policy = retry.RetryPolicy(app, kafkaesk.app.Subscription("foobar", NOOPCallback, "group"))
    exception = NOOPException()

    # Check that policy errors if not initailized
    with pytest.raises(RuntimeError):
        await policy(record, exception)

    await policy.initialize()

    # Check that un-configured exceptions are re-raised
    with pytest.raises(NOOPException):
        await policy(record, exception)

    # Check that configured exceptions are handled
    handler_mock = AsyncMock()
    await policy.add_retry_handler(NOOPException, handler_mock)
    await policy(record, exception)
    handler_mock.assert_awaited_once()

    # Check that configured exceptions can be removed
    await policy.remove_retry_handler(NOOPException)
    with pytest.raises(NOOPException):
        await policy(record, exception)

    # Check that a configured exception handles inherited exceptions
    handler_mock = AsyncMock()
    await policy.add_retry_handler(Exception, handler_mock)
    await policy(record, exception)
    handler_mock.assert_awaited_once()

    # Check that the passed handler key matches the configured handler
    # rather than the exception that was caught
    assert handler_mock.await_args[0][1] == "Exception"


async def test_noretry_handler(record: ConsumerRecord) -> None:
    exception = NOOPException()
    retry_history = retry.RetryHistory()

    noretry = retry.NoRetry("Dummy")
    await noretry(None, "NOOPException", retry_history, record, exception)


@pytest.mark.skipif(AsyncMock is None, reason="Only py 3.8")  # type: ignore
async def test_forward_handler(record: ConsumerRecord) -> None:
    policy = AsyncMock()
    exception = NOOPException()
    retry_history = retry.RetryHistory()

    forward = retry.Forward("test_stream")
    await forward(policy, "NOOPException", retry_history, record, exception)

    policy.app.publish.assert_awaited_once()
    assert policy.app.publish.await_args[0][0] == "test_stream"
    assert isinstance(policy.app.publish.await_args[0][1], retry.RetryMessage)
