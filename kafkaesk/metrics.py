try:
    import prometheus_client as client
except ImportError:
    from .utils import NoopClient as client


PUBLISHED_MESSAGES = client.Counter("published_messages", "Number of published messages")
CONSUMED_MESSAGE_TIME = client.Histogram(
    "consumed_message_elapsed_time",
    "Processing time for consumed message (in seconds)",
    ["stream_id"],
)
