from prometheus_client.utils import INF
from typing import Dict
from typing import Optional
from typing import Type

import prometheus_client as client
import time
import traceback

NOERROR = "none"
ERROR_GENERAL_EXCEPTION = "exception"

KAFKA_ACTION = client.Counter(
    "kafkaesk_kafka_action",
    "Perform action on kafka",
    ["type", "error"],
)

KAFKA_ACTION_TIME = client.Histogram(
    "kafkaesk_kafka_action_time",
    "Time taken to perform kafka action",
    ["type"],
)

PUBLISH_MESSAGES = client.Counter(
    "kafkaesk_publish_messages",
    "Number of messages attempted to be published",
    ["stream_id", "error"],
)

PUBLISH_MESSAGES_TIME = client.Histogram(
    "kafkaesk_publish_messages_time",
    "Time taken for a message to be queued for publishing (in seconds)",
    ["stream_id"],
)

PUBLISHED_MESSAGES = client.Counter(
    "kafkaesk_published_messages",
    "Number of published messages",
    ["stream_id", "partition", "error"],
)

PUBLISHED_MESSAGES_TIME = client.Histogram(
    "kafkaesk_published_messages_time",
    "Time taken for a message to be published (in seconds)",
    ["stream_id"],
)


CONSUMED_MESSAGES = client.Counter(
    "kafkaesk_consumed_messages",
    "Number of consumed messages",
    ["stream_id", "partition", "error", "group_id"],
)

CONSUMED_MESSAGE_TIME = client.Histogram(
    "kafkaesk_consumed_message_elapsed_time",
    "Processing time for consumed message (in seconds)",
    ["stream_id", "group_id", "partition"],
)

PRODUCER_TOPIC_OFFSET = client.Gauge(
    "kafkaesk_produced_topic_offset",
    "Offset for produced messages a the topic",
    ["stream_id", "partition"],
)

CONSUMER_TOPIC_OFFSET = client.Gauge(
    "kafkaesk_consumed_topic_offset",
    "Offset for consumed messages in a topic",
    ["group_id", "partition", "stream_id"],
)

MESSAGE_LEAD_TIME = client.Histogram(
    "kafkaesk_message_lead_time",
    "Time that the message has been waiting to be handled by a consumer (in seconds)",
    ["stream_id", "group_id", "partition"],
    buckets=(0.1, 0.5, 1, 3, 5, 10, 30, 60, 60, 120, 300, INF),
)

CONSUMER_REBALANCED = client.Counter(
    "kafkaesk_consumer_rebalanced",
    "Consumer rebalances",
    ["stream_id", "group_id", "partition"],
)

RETRY_POLICY = client.Counter(
    "kafkaesk_retry_policy",
    "Number of times the retry policy has been run",
    ["stream_id", "group_id", "partition", "handler", "exception"],
)


RETRY_POLICY_TIME = client.Histogram(
    "kafkaesk_retry_policy_time",
    "Time taken for a RetryPolicy to process a message (in seconds)",
    ["stream_id", "group_id", "partition"],
)

RETRY_HANDLER_FORWARD = client.Counter(
    "kafkaesk_retry_handler_forward",
    "Number of times a retry handler has forwarded a message to a new stream",
    ["stream_id", "group_id", "partition", "handler", "exception", "forward_stream_id"],
)

RETRY_HANDLER_DROP = client.Counter(
    "kafkaesk_retry_handler_drop",
    "Number of times the retry handler dropped a message without forwarding it",
    ["stream_id", "group_id", "partition", "handler", "exception"],
)

RETRY_HANDLER_RAISE = client.Counter(
    "kafkaesk_retry_handler_raise",
    "Number of times the retry handler raised a message exception",
    ["stream_id", "group_id", "partition", "handler", "exception"],
)

RETRY_CONSUMER_TOPIC_OFFSET = client.Gauge(
    "kafkaesk_retry_consumed_topic_offset",
    "Offset for consumed messages in a retry topic",
    ["group_id", "partition", "stream_id", "handler", "retry_stream_id", "retry_stream_partition"],
)

RETRY_CONSUMER_MESSAGE_LEAD_TIME = client.Histogram(
    "kafkaesk_retry_message_lead_time",
    "Time that the message has been waiting to be handled by a retry consumer (in seconds)",
    ["stream_id", "group_id", "partition", "handler", "retry_stream_id", "retry_stream_partition"],
)

RETRY_CONSUMER_CONSUMED_MESSAGE_TIME = client.Histogram(
    "kafkaesk_retry_consumed_message_elapsed_time",
    "Processing time for consumed retry message (in seconds)",
    ["stream_id", "group_id", "partition", "handler", "retry_stream_id", "retry_stream_partition"],
)

RETRY_CONSUMER_CONSUMED_MESSAGES = client.Counter(
    "kafkaesk_retry_consumed_messages",
    "Number of consumed retry messages",
    [
        "stream_id",
        "partition",
        "error",
        "group_id",
        "handler",
        "retry_stream_id",
        "retry_stream_partition",
    ],
)


class watch:
    start: float

    def __init__(
        self,
        *,
        counter: Optional[client.Counter] = None,
        histogram: Optional[client.Histogram] = None,
        labels: Optional[Dict[str, str]] = None,
    ):
        self.counter = counter
        self.histogram = histogram
        self.labels = labels or {}

    def __enter__(self) -> None:
        self.start = time.time()

    def __exit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Optional[Exception],
        exc_traceback: Optional[traceback.StackSummary],
    ) -> None:
        error = NOERROR
        if self.histogram is not None:
            finished = time.time()
            self.histogram.labels(**self.labels).observe(finished - self.start)

        if self.counter is not None:
            if exc_value is None:
                error = NOERROR
            else:
                error = ERROR_GENERAL_EXCEPTION
            self.counter.labels(error=error, **self.labels).inc()


class watch_kafka(watch):
    def __init__(self, type: str):
        super().__init__(counter=KAFKA_ACTION, histogram=KAFKA_ACTION_TIME, labels={"type": type})


class watch_publish(watch):
    def __init__(self, stream_id: str):
        super().__init__(
            counter=PUBLISH_MESSAGES,
            histogram=PUBLISH_MESSAGES_TIME,
            labels={"stream_id": stream_id},
        )
