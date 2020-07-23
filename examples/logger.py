from kafkaesk import Application
from kafkaesk.ext.logging import PydanticFormatter
from kafkaesk.ext.logging import PydanticKafkaeskHandler
from kafkaesk.ext.logging import PydanticLogModel
from kafkaesk.ext.logging import PydanticStreamHandler
from pydantic import BaseModel
from typing import Optional

import asyncio
import logging


class UserLog(BaseModel):
    _is_log_model = True
    user: Optional[str]


async def test_log() -> None:
    app = Application(kafka_servers=["localhost:9092"])

    logger = logging.getLogger("kafkaesk.ext.logging.kafka")
    handler = PydanticKafkaeskHandler(app, "logging.test")
    formatter = PydanticFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    stream_logger = logging.getLogger("kafakesk.ext.logging.stream")
    stream_handler = PydanticStreamHandler()
    stream_handler.setFormatter(formatter)
    stream_logger.addHandler(stream_handler)
    stream_logger.setLevel(logging.DEBUG)

    @app.subscribe("logging.test", group="example.logging.consumer")
    async def consume(data: PydanticLogModel) -> None:
        stream_logger.info(data.json())

    async with app:
        logger.debug("Log Message", UserLog(user="kafkaesk"))
        await app.flush()
        await app.consume_for(1, seconds=5)


if __name__ == "__main__":
    asyncio.run(test_log())
