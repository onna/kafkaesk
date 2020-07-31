from kafkaesk.ext.logging.formatter import PydanticFormatter
from kafkaesk.ext.logging.formatter import PydanticLogModel
from kafkaesk.ext.logging.record import factory
from types import TracebackType
from typing import Optional

import logging
import pydantic
import pytest
import sys

pytestmark = pytest.mark.asyncio


def make_traceback():
    tb = None
    depth = 0
    while True:
        try:
            frame = sys._getframe(depth)
            depth += 1
        except ValueError:
            break

        tb = TracebackType(tb, frame, frame.f_lasti, frame.f_lineno)

    return tb


def make_stackinfo():
    logger = logging.getLogger()
    _, _, _, sinfo = logger.findCaller(stack_info=True)
    return sinfo


@pytest.fixture(scope="function")
def formatter():
    return PydanticFormatter()


@pytest.fixture(scope="function")
def basic_record():
    return factory(
        name="logger.testing",
        level=logging.INFO,
        fn="test_formatter",
        lno=4,
        msg="Test Message",
        args=(),
        exc_info=None,
        func=None,
        sinfo=None,
    )


@pytest.fixture(scope="function")
def exception_record():
    exc_info = Exception("Unhandled Exception").with_traceback(make_traceback())
    return factory(
        name="logger.testing",
        level=logging.ERROR,
        fn="test_formatter",
        lno=4,
        msg="Test Exception Message",
        args=(),
        exc_info=(type(exc_info), exc_info, exc_info.__traceback__),
        func=None,
        sinfo=None,
    )


@pytest.fixture(scope="function")
def stack_record():
    return factory(
        name="logger.testing",
        level=logging.INFO,
        fn="test_formatter",
        lno=4,
        msg="Test Message",
        args=(),
        exc_info=None,
        func=None,
        sinfo=make_stackinfo(),
    )


@pytest.fixture(scope="function")
def extended_record():
    class FooLogModel(pydantic.BaseModel):
        _is_log_model = True
        foo: Optional[str]

    class FizLogModel(pydantic.BaseModel):
        _is_log_model = True
        fiz: Optional[str]

    return factory(
        name="logger.testing",
        level=logging.INFO,
        fn="test_formatter",
        lno=4,
        msg="Test Message",
        args=(FooLogModel(foo="bar"), FizLogModel(fiz="biz")),
        exc_info=None,
        func=None,
        sinfo=None,
    )


async def test_formatter_return_type(formatter, basic_record):
    message = formatter.format(basic_record)
    assert isinstance(message, PydanticLogModel)


async def test_formatter_populates_timestamp(formatter, basic_record):
    message = formatter.format(basic_record)
    assert message.timestamp


async def test_formatter_populates_exception(formatter, exception_record):
    message = formatter.format(exception_record)
    assert message.exception == "Exception"
    assert message.trace is not None
    assert isinstance(message.trace, str)


async def test_formatter_populates_stack(formatter, stack_record):
    message = formatter.format(stack_record)
    assert message.stack is not None
    assert isinstance(message.stack, str)


async def test_formatter_populates_custom_base_log(basic_record):
    class CustomBase(pydantic.BaseModel):
        when: str = pydantic.Field(alias="asctime")
        level: str = pydantic.Field(alias="levelname")
        message: str

    formatter = PydanticFormatter(model=CustomBase)
    message = formatter.format(basic_record)

    assert message.when is not None
    assert message.level is not None
    assert message.message is not None
    assert hasattr(message, "logger") is False


async def test_formatter_populates_extended_logs(formatter, extended_record):
    message = formatter.format(extended_record)

    assert message.foo == "bar"
    assert message.fiz == "biz"
