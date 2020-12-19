import aiokafka.structs
import json
import time


def record_factory():
    return aiokafka.structs.ConsumerRecord(
        topic="topic",
        partition=0,
        offset=0,
        timestamp=time.time() * 1000,
        timestamp_type=1,
        key="key",
        value=json.dumps({"schema": "Foo:1", "data": {"foo": "bar"}}).encode(),
        checksum="1",
        serialized_key_size=10,
        serialized_value_size=10,
        headers=[],
    )
