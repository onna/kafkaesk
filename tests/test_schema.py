import pydantic
import kafkaesk
import pytest
import uuid
from kafkaesk.exceptions import (
    SchemaConflictException,
    SchemaRegistrationConflictException,
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


async def test_not_allowed_to_overwrite_ummatched_schema(kafka):
    prefix = uuid.uuid4().hex
    app1 = kafkaesk.Application([f"{kafka[0]}:{kafka[1]}"], topic_prefix=prefix,)
    app2 = kafkaesk.Application([f"{kafka[0]}:{kafka[1]}"], topic_prefix=prefix,)

    @app1.schema("Foo", version=1)
    class Foo1(pydantic.BaseModel):
        bar: str

    @app2.schema("Foo", version=1)
    class Foo2(pydantic.BaseModel):
        bar: str
        foo: str  # extra property should cause error

    with pytest.raises(SchemaRegistrationConflictException):
        async with app1, app2:
            ...


async def test_allowed_redef_existing_schema_matches(kafka):
    prefix = uuid.uuid4().hex
    app1 = kafkaesk.Application([f"{kafka[0]}:{kafka[1]}"], topic_prefix=prefix,)
    app2 = kafkaesk.Application([f"{kafka[0]}:{kafka[1]}"], topic_prefix=prefix,)

    @app1.schema("Foo", version=1)
    class Foo1(pydantic.BaseModel):
        bar: str

    @app2.schema("Foo", version=1)
    class Foo2(pydantic.BaseModel):
        # exact same
        bar: str

    async with app1, app2:
        ...
