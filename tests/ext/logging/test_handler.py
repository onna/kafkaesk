from kafkaesk.ext.logging.formatter import PydanticFormatter
from kafkaesk.ext.logging.handler import PydanticKafkaeskHandler
from kafkaesk.ext.logging.handler import PydanticStreamHandler
from typing import Optional
from unittest import mock

import asyncio
import io
import json
import kafkaesk
import logging
import pydantic
import pytest

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="function")
def logger():
    l = logging.getLogger("test")
    l.propagate = False
    l.setLevel(logging.DEBUG)

    return l


@pytest.fixture(scope="function")
def stream_handler(logger):

    stream = io.StringIO()
    handler = PydanticStreamHandler(stream=stream)
    formatter = PydanticFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return stream


@pytest.fixture(scope="function")
def app():
    with mock.patch("kafkaesk.app.Application", autospec=True) as kafka_mock:
        yield kafkaesk.app.Application()


@pytest.fixture(scope="function")
def kafakesk_handler(app, logger):
    handler = PydanticKafkaeskHandler(app, "log.test")
    handler.setFormatter(PydanticFormatter())
    logger.addHandler(handler)

    return handler


class TestPydanticStreamHandler:
    async def test_stream_handler(self, stream_handler, logger):
        logger.info("Test Message %s", "extra")

        message = stream_handler.getvalue()
        data = json.loads(message)

        assert data["message"] == "Test Message extra"
        assert data["level"] == "INFO"

    async def test_stream_handler_with_log_model(self, stream_handler, logger):
        class LogModel(pydantic.BaseModel):
            _is_log_model = True
            foo: Optional[str]

        logger.info("Test Message %s", "extra", LogModel(foo="bar"))

        message = stream_handler.getvalue()
        data = json.loads(message)

        assert data["message"] == "Test Message extra"
        assert data["foo"] == "bar"


class TestPydanticKafkaeskHandler:
    async def test_kafak_handler(self, app, kafakesk_handler, logger):
        logger.info("Test Message %s", "extra")

        await asyncio.sleep(1)

        assert app.publish.await_count == 1
        assert app.publish.await_args[0][0] == "log.test"
        assert app.publish.await_args[0][1].message == "Test Message extra"

    async def test_kafka_handler_with_log_model(self, app, kafakesk_handler, logger):
        class LogModel(pydantic.BaseModel):
            _is_log_model = True
            foo: Optional[str]

        logger.info("Test Message %s", "extra", LogModel(foo="bar"))

        await asyncio.sleep(1)

        assert app.publish.await_count == 1
        assert app.publish.await_args[0][1].message == "Test Message extra"
        assert app.publish.await_args[0][1].foo == "bar"
