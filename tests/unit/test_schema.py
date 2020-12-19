from kafkaesk import Application
from kafkaesk.exceptions import SchemaConflictException

import pydantic
import pytest

pytestmark = pytest.mark.asyncio


async def test_not_allowed_to_register_same_schema_twice():
    app = Application()

    @app.schema("Foo", version=1)
    class Foo1(pydantic.BaseModel):
        bar: str

    with pytest.raises(SchemaConflictException):

        @app.schema("Foo", version=1)
        class Foo2(pydantic.BaseModel):
            foo: str


async def test_do_not_require_schema_name():
    app = Application()

    @app.schema()
    class Foo(pydantic.BaseModel):
        bar: str

    assert "Foo:1" in app._schemas


async def test_get_registered_schema():
    app = Application()

    @app.schema()
    class Foo(pydantic.BaseModel):
        bar: str

    assert app.get_schema_reg(Foo) is not None


async def test_get_registered_schema_missing():
    app = Application()

    class Foo(pydantic.BaseModel):
        bar: str

    assert app.get_schema_reg(Foo) is None
