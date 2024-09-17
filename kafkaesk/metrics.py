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

CONSUMED_MESSAGES_BATCH_SIZE = client.Histogram(
    "kafkaesk_consumed_messages_batch_size",
    "Size of message batches consumed",
    ["stream_id", "group_id", "partition"],
    buckets=[1, 5, 10, 20, 50, 100, 200, 500, 1000],
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
    ["group_id", "partition", "event"],
)

CONSUMER_HEALTH = client.Gauge(
    "kafkaesk_consumer_health", "Liveness probe for the consumer", ["group_id"]
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
        exc_type: Optional[Type[Exception]] = None,
        exc_value: Optional[Exception] = None,
        exc_traceback: Optional[traceback.StackSummary] = None,
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
