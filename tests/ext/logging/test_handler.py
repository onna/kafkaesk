from kafkaesk.ext.logging.formatter import PydanticFormatter
from kafkaesk.ext.logging.formatter import PydanticLogModel
from kafkaesk.ext.logging.handler import KafkaeskQueue
from kafkaesk.ext.logging.handler import PydanticKafkaeskHandler
from kafkaesk.ext.logging.handler import PydanticStreamHandler
from typing import Optional

import asyncio
import io
import json
import kafkaesk
import logging
import pydantic
import pytest
import uuid

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="function")
def logger():
    ll = logging.getLogger("test")
    ll.propagate = False
    ll.setLevel(logging.DEBUG)

    return ll


@pytest.fixture(scope="function")
def stream_handler(logger):

    stream = io.StringIO()
    handler = PydanticStreamHandler(stream=stream)
    formatter = PydanticFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return stream


@pytest.fixture(scope="function")
def kafakesk_handler(app, logger):
    handler = PydanticKafkaeskHandler(app, "log.test")
    handler.setFormatter(PydanticFormatter())
    logger.addHandler(handler)

    return handler


async def test_handler_initializes_applogger(kafka, logger):
    app = kafkaesk.Application(
        [f"{kafka[0]}:{kafka[1]}"],
        topic_prefix=uuid.uuid4().hex,
        kafka_settings={"metadata_max_age_ms": 500},
    )

    handler = PydanticKafkaeskHandler(app, "log.test")
    handler.setFormatter(PydanticFormatter())
    logger.addHandler(handler)

    logger.error("Hi!")

    await asyncio.sleep(0.1)
    assert app._initialized


@pytest.fixture(scope="function")
def log_consumer(app):
    consumed = []

    @app.subscribe("log.test", group="test_group")
    async def consume(data: PydanticLogModel):
        consumed.append(data)

    yield consumed


class TestPydanticStreamHandler:
    async def test_stream_handler(self, stream_handler, logger):

        logger.info("Test Message %s", "extra")

        message = stream_handler.getvalue()
        data = json.loads(message)

        assert data["message"] == "Test Message extra"
        assert data["level"] == 20
        assert data["severity"] == "INFO"

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
    async def test_kafak_handler(self, app, kafakesk_handler, logger, log_consumer):

        async with app:
            logger.info("Test Message %s", "extra")
            await app.flush()
            await app.consume_for(1, seconds=5)

        assert len(log_consumer) == 1
        assert log_consumer[0].message == "Test Message extra"

    async def test_kafka_handler_with_log_model(self, app, kafakesk_handler, logger, log_consumer):
        class LogModel(pydantic.BaseModel):
            _is_log_model = True
            foo: Optional[str]

        async with app:
            logger.info("Test Message %s", "extra", LogModel(foo="bar"))
            await app.flush()
            await app.consume_for(1, seconds=5)

        assert len(log_consumer) == 1
        assert log_consumer[0].message == "Test Message extra"
        assert log_consumer[0].foo == "bar"

    async def test_kafak_handler_without_initializing_app(
        self, app, kafakesk_handler, logger, capsys
    ):

        logger.info("Test Message %s", "extra")
        await asyncio.sleep(0.01)

        _, err = capsys.readouterr()
        assert "Kafkaesk application is not initialized" in err


class TestKafkaeskQueue:
    @pytest.fixture(scope="function")
    async def queue(self, request, app):

        max_queue = 10000
        for marker in request.node.iter_markers("with_max_queue"):
            max_queue = marker.args[0]

        app.schema("PydanticLogModel")(PydanticLogModel)
        q = KafkaeskQueue(app, max_queue=max_queue)

        return q

    async def test_queue(self, app, queue):
        consumed = []

        @app.subscribe("log.test", group="test_group")
        async def consume(data: PydanticLogModel):
            consumed.append(data)

        async with app:
            queue.start()
            queue.put_nowait("log.test", PydanticLogModel(foo="bar"))

            await app.flush()
            await app.consume_for(1, seconds=5)

        queue.close()
        await queue._task

        assert len(consumed) == 1

    async def test_queue_flush(self, app, queue, log_consumer):

        async with app:
            queue.start()
            for i in range(10):
                queue.put_nowait("log.test", PydanticLogModel(count=i))

            await queue.flush()

            await app.flush()
            await app.consume_for(10, seconds=5)

        assert len(log_consumer) == 10

    async def test_queue_flush_on_close(self, app, queue, log_consumer):

        async with app:
            queue.start()
            await asyncio.sleep(0.1)
            queue.close()

            for i in range(10):
                queue.put_nowait("log.test", PydanticLogModel(count=i))

            await app.flush()
            await app.consume_for(10, seconds=5)

        assert len(log_consumer) == 10
        assert queue._task.done()

    @pytest.mark.with_max_queue(1)
    async def test_queue_max_size(self, app, queue):

        queue.start()
        queue.put_nowait("log.test", PydanticLogModel())

        with pytest.raises(asyncio.QueueFull):
            queue.put_nowait("log.test", PydanticLogModel())
