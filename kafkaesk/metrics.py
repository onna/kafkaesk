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
