import prometheus_client as client

NOERROR = "none"

PUBLISHED_MESSAGES = client.Counter(
    "kafkaesk_published_messages",
    "Number of published messages",
    ["stream_id", "partition", "error"],
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
)

CONSUMER_REBALANCED = client.Counter(
    "kafkaesk_consumer_rebalanced", "Consumer rebalances", ["stream_id", "group_id", "partition"],
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
