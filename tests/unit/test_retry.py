from aiokafka.structs import ConsumerRecord
from datetime import datetime
from kafkaesk import retry
from unittest.mock import patch

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
        headers=[("test", b"foo")],
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
    deserialized_message = retry.RetryMessage.parse_raw(json_serialized)

    # Ensure we can re-create a ConsumerRecord from our Record object
    deserialized_message.original_record.to_consumer_record()


async def test_retry_policy(app: kafkaesk.Application, record: ConsumerRecord) -> None:
    policy = retry.RetryPolicy(app, kafkaesk.app.Subscription("foobar", NOOPCallback, "group"))
    exception = NOOPException()

    # Check that policy errors if not initailized
    with pytest.raises(RuntimeError):
        await policy(record, exception)

    await policy.initialize()

    # Check that un-configured exceptions are re-raised
    with pytest.raises(NOOPException), patch("kafkaesk.retry.RETRY_POLICY",) as metric_mock, patch(
        "kafkaesk.retry.RETRY_POLICY_TIME",
    ) as metric_time_mock:
        await policy(record, exception)

        # Time Metric
        metric_time_mock.labels.assert_called_with(
            stream_id=record.topic, partition=record.partition, group_id="group"
        )
        metric_time_mock.labels.return_value.time.return_value.__enter__.assert_called_once()
        # Count Metric
        metric_mock.labels.assert_called_with(
            stream_id=record.topic,
            partition=record.partition,
            group_id="group",
            handler=None,
            exception="NOOPException",
        )
        metric_mock.labels.return_value.inc.assert_called_once()

    # Check that configured exceptions are handled
    handler_mock = AsyncMock()
    policy = retry.RetryPolicy(
        app,
        kafkaesk.app.Subscription(
            "foobar", NOOPCallback, "group", retry_handlers={NOOPException: handler_mock}
        ),
    )
    await policy.initialize()
    with patch("kafkaesk.retry.RETRY_POLICY",) as metric_mock, patch(
        "kafkaesk.retry.RETRY_POLICY_TIME",
    ) as metric_time_mock:
        await policy(record, exception)
        handler_mock.assert_awaited_once()

        # Time Metric
        metric_time_mock.labels.assert_called_with(
            stream_id=record.topic, partition=record.partition, group_id="group"
        )
        metric_time_mock.labels.return_value.time.return_value.__enter__.assert_called_once()
        # Count Metric
        metric_mock.labels.assert_called_with(
            stream_id=record.topic,
            partition=record.partition,
            group_id="group",
            handler="AsyncMock",
            exception="NOOPException",
        )
        metric_mock.labels.return_value.inc.assert_called_once()

    # Check that a configured exception handles inherited exceptions
    handler_mock = AsyncMock()
    policy = retry.RetryPolicy(
        app,
        kafkaesk.app.Subscription(
            "foobar", NOOPCallback, "group", retry_handlers={Exception: handler_mock}
        ),
    )
    await policy.initialize()
    await policy(record, exception)
    handler_mock.assert_awaited_once()

    # Check that the passed handler key matches the configured handler
    # rather than the exception that was caught
    assert handler_mock.await_args[0][1] == "Exception"


async def test_retry_policy_truncates_retry_history_failures(
    app: kafkaesk.Application, record: ConsumerRecord
) -> None:
    policy = retry.RetryPolicy(app, kafkaesk.app.Subscription("foobar", NOOPCallback, "group"))
    await policy.initialize()

    exception = NOOPException()
    retry_history = retry.RetryHistory(
        failures=[
            retry.FailureInfo(exception=str(x), handler_key="Exception", timestamp=datetime.now())
            for x in range(10)
        ]
    )

    with pytest.raises(NOOPException):
        await policy(record, exception, retry_history=retry_history)

    assert len(retry_history.failures) == 10
    assert retry_history.failures[0].exception == "1"
    assert retry_history.failures[-1].exception == "NOOPException"


async def test_retry_policy_default_handler(app: kafkaesk.Application) -> None:
    policy = retry.RetryPolicy(app, kafkaesk.app.Subscription("foobar", NOOPCallback, "group"))

    handler_key, handler = policy._get_handler(NOOPException())

    assert handler_key == "Exception"
    assert isinstance(handler, retry.Raise)


async def test_retry_handler(record: ConsumerRecord) -> None:
    class NOOPHandler(retry.RetryHandler):
        ...

    policy = AsyncMock()
    policy.subscription.group = "test_group"
    handler = NOOPHandler()
    retry_history = retry.RetryHistory()
    exception = NOOPException()

    # Test Raise Message
    with patch("kafkaesk.retry.RETRY_HANDLER_RAISE") as metric_mock:
        with pytest.raises(NOOPException):
            await handler._raise_message(policy, retry_history, record, exception)

        metric_mock.labels.assert_called_with(
            stream_id="foobar",
            partition=0,
            group_id="test_group",
            handler="NOOPHandler",
            exception="NOOPException",
        )
        metric_mock.labels.return_value.inc.assert_called_once()

    # Test Drop Message
    with patch("kafkaesk.retry.RETRY_HANDLER_DROP") as metric_mock:
        await handler._drop_message(policy, retry_history, record, exception)

        metric_mock.labels.assert_called_with(
            stream_id="foobar",
            partition=0,
            group_id="test_group",
            handler="NOOPHandler",
            exception="NOOPException",
        )
        metric_mock.labels.return_value.inc.assert_called_once()

    # Test Forward Message
    with patch("kafkaesk.retry.RETRY_HANDLER_FORWARD") as metric_mock:
        await handler._forward_message(
            policy, retry_history, record, exception, "forward_stream_id"
        )

        metric_mock.labels.assert_called_with(
            stream_id="foobar",
            partition=0,
            group_id="test_group",
            handler="NOOPHandler",
            exception="NOOPException",
            forward_stream_id="forward_stream_id",
        )
        metric_mock.labels.return_value.inc.assert_called_once()

        # Make sure publish was called
        policy.app.publish.assert_awaited_once()


async def test_raise_handler(record: ConsumerRecord) -> None:
    policy = AsyncMock()
    exception = NOOPException()
    retry_history = retry.RetryHistory()

    raise_handler = retry.Raise()
    with pytest.raises(NOOPException):
        await raise_handler(policy, "Exception", retry_history, record, exception)


async def test_drop_handler(record: ConsumerRecord) -> None:
    policy = AsyncMock()
    exception = NOOPException()
    retry_history = retry.RetryHistory()

    noretry = retry.Drop()
    await noretry(policy, "NOOPException", retry_history, record, exception)


async def test_forward_handler(record: ConsumerRecord) -> None:
    policy = AsyncMock()
    exception = NOOPException()
    retry_history = retry.RetryHistory()

    forward = retry.Forward("test_stream")
    await forward(policy, "NOOPException", retry_history, record, exception)

    policy.app.publish.assert_awaited_once()
    assert policy.app.publish.await_args[0][0] == "test_stream"
    assert isinstance(policy.app.publish.await_args[0][1], retry.RetryMessage)


def test_format_record(record):
    assert retry.format_record(record)

    # truncates value
    record.value = b"X" * 1024
    assert len(retry.format_record(record)) == 515
