from aiokafka import ConsumerRecord
from kafkaesk import Application
from kafkaesk.exceptions import ConsumerUnhealthyException
from .produce import producer
from kafkaesk.exceptions import ProducerUnhealthyException
from kafkaesk.kafka import KafkaTopicManager
from kafkaesk.retry import Forward
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import aiokafka.structs
import asyncio
import pydantic
import pytest
import uuid

try:
    from unittest.mock import AsyncMock
except:  # noqa
    AsyncMock = None  # type: ignore

pytestmark = pytest.mark.asyncio

TOPIC = "test-hc"


async def test_health_check_should_fail_with_unhandled(app: Application):
    @app.subscribe(TOPIC, group=TOPIC)
    async def consume(data):
        raise Exception("failure!")

    async with app:
        produce = asyncio.create_task(producer(app, TOPIC))
        asyncio.create_task(app.consume_forever())
        await asyncio.sleep(2)  # wait for some to produce and then be consumed to cause failure

        with pytest.raises(ConsumerUnhealthyException):
            await app.health_check()

        produce.cancel()


async def test_health_check_should_succeed(app):
    @app.subscribe(TOPIC, group=TOPIC)
    async def consume(data):
        ...

    async with app:
        produce = asyncio.create_task(producer(app, TOPIC))
        asyncio.create_task(app.consume_forever())
        await asyncio.sleep(2)  # wait for some to produce and then be consumed to cause failure
        await app.health_check()
        produce.cancel()
