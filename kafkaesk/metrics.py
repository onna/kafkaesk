import prometheus_client as client

NOERROR = "none"

PUBLISHED_MESSAGES = client.Counter(
    "published_messages", "Number of published messages", ["stream_id", "partition", "error"]
)


CONSUMED_MESSAGES = client.Counter(
    "consumed_messages",
    "Number of consumed messages",
    ["stream_id", "partition", "error", "group_id"],
)

CONSUMED_MESSAGE_TIME = client.Histogram(
    "consumed_message_elapsed_time",
    "Processing time for consumed message (in seconds)",
    ["stream_id", "group_id", "partition"],
)

PRODUCER_TOPIC_OFFSET = client.Gauge(
    "produced_topic_offset", "Offset for produced messages a the topic", ["stream_id", "partition"],
)

CONSUMER_TOPIC_OFFSET = client.Gauge(
    "consumed_topic_offset",
    "Offset for consumed messages in a topic",
    ["group_id", "partition", "stream_id"],
)

MESSAGE_LEAD_TIME = client.Histogram(
    "message_lead_time",
    "Time that the message has been waiting to be handled by a consumer (in seconds)",
    ["stream_id", "group_id", "partition"],
)
