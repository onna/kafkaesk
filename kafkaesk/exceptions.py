from pydantic import BaseModel
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .app import SchemaRegistration
    from .app import SubscriptionConsumer
else:
    SchemaRegistration = SubscriptionConsumer = None


class JsonSchemaRequiredException(Exception):
    ...


class SchemaConflictException(Exception):
    def __init__(self, existing: SchemaRegistration, new: SchemaRegistration):
        self.existing = existing
        self.new = new

    def __str__(self) -> str:
        return f"""<Schema Conflict:
Existing: {self.existing}
New: {self.new}
/>"""


class UnregisteredSchemaException(Exception):
    def __init__(self, model: BaseModel = None):
        self.model = model

    def __str__(self) -> str:
        return f"""<UnregisteredSchemaException
(Attempted to send message for unregistered schema. Did you initialize the application?)
Model: {self.model}
/>"""


class UnhandledMessage(Exception):
    ...


class StopConsumer(Exception):
    ...


class ConsumerUnhealthyException(Exception):
    def __init__(self, subscriber_consumer: SubscriptionConsumer, reason: str):
        self.subscriber_consumer = subscriber_consumer
        self.reason = reason
