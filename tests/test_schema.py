import pydantic
import pytest
from kafkaesk.exceptions import (
    SchemaConflictException
)

pytestmark = pytest.mark.asyncio


async def test_not_allowed_to_register_same_schema_twice(app):
    @app.schema("Foo", version=1)
    class Foo1(pydantic.BaseModel):
        bar: str

    with pytest.raises(SchemaConflictException):

        @app.schema("Foo", version=1)
        class Foo2(pydantic.BaseModel):
            foo: str
