from kafkaesk.app import SchemaRegistration
from kafkaesk.exceptions import SchemaConflictException


def test_repr_conflict():
    ex = SchemaConflictException(
        SchemaRegistration("id", 1, None), SchemaRegistration("id", 1, None)
    )
    assert "Schema Conflict" in str(ex)
