import pytest
import uuid
import kafkaesk


@pytest.fixture()
async def app(kafka):
    yield kafkaesk.Application(
        [f"{kafka[0]}:{kafka[1]}"],
        topic_prefix=uuid.uuid4().hex,
        kafka_settings={"metadata_max_age_ms": 500},
    )
