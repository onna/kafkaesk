from kafkaesk.ext.logging.record import factory
from kafkaesk.ext.logging.record import PydanticLogRecord
from typing import Optional

import logging
import pydantic
import pytest

pytestmark = pytest.mark.asyncio


async def test_factory_return_type() -> None:
    record = factory(
        name="logger.test",
        level=logging.INFO,
        fn="test_factory_retrun_type",
        lno=4,
        msg="Test Log",
        args=(),
        exc_info=None,
        func=None,
        sinfo=None,
    )

    assert isinstance(record, PydanticLogRecord)
    assert issubclass(PydanticLogRecord, logging.LogRecord)


async def test_factory_adds_pydantic_models() -> None:
    class LogModel(pydantic.BaseModel):
        _is_log_model = True
        foo: Optional[str] = None

    record = factory(
        name="logger.test",
        level=logging.INFO,
        fn="test_factory_retrun_type",
        lno=4,
        msg="Test Log",
        args=(LogModel(foo="bar"),),
        exc_info=None,
        func=None,
        sinfo=None,
    )

    assert len(record.pydantic_data) == 1
    assert len(record.args) == 0


async def test_factory_formats_msg() -> None:
    record = factory(
        name="logger.test",
        level=logging.INFO,
        fn="test_factory_retrun_type",
        lno=4,
        msg="Test Log %s",
        args=("extra",),
        exc_info=None,
        func=None,
        sinfo=None,
    )

    assert record.getMessage() == "Test Log extra"


async def test_factory_formats_msg_and_adds_pydantic_model() -> None:
    class LogModel(pydantic.BaseModel):
        _is_log_model = True
        foo: Optional[str] = None

    record = factory(
        name="logger.test",
        level=logging.INFO,
        fn="test_factory_retrun_type",
        lno=4,
        msg="Test Log %s",
        args=("extra", LogModel(foo="bar")),
        exc_info=None,
        func=None,
        sinfo=None,
    )

    assert record.getMessage() == "Test Log extra"
    assert len(record.pydantic_data) == 1
