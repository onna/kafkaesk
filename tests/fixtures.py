from unittest.mock import DEFAULT
from unittest.mock import patch

import kafkaesk
import os
import pytest_asyncio
import uuid


@pytest_asyncio.fixture()
async def kafka():
    yield os.environ.get("KAFKA", "localhost:9092").split(":")


@pytest_asyncio.fixture()
def topic_prefix():
    return uuid.uuid4().hex


@pytest_asyncio.fixture()
async def app(kafka, topic_prefix):
    yield kafkaesk.Application(
        [f"{kafka[0]}:{kafka[1]}"],
        topic_prefix=topic_prefix,
        kafka_settings={
            "metadata_max_age_ms": 500,
        },
    )


@pytest_asyncio.fixture()
def metrics():
    with patch.multiple(
        "kafkaesk.app",
        PUBLISHED_MESSAGES=DEFAULT,
        PRODUCER_TOPIC_OFFSET=DEFAULT,
        PUBLISHED_MESSAGES_TIME=DEFAULT,
    ) as mock:
        yield mock
