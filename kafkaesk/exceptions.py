from typing import TYPE_CHECKING

import aiokafka

if TYPE_CHECKING:  # pragma: no cover
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


class UnhandledMessage(Exception):
    ...


class StopConsumer(Exception):
    ...


class ConsumerUnhealthyException(Exception):
    def __init__(self, subscriber_consumer: SubscriptionConsumer, reason: str):
        self.subscriber_consumer = subscriber_consumer
        self.reason = reason


class AutoCommitError(ConsumerUnhealthyException):
    ...


class ProducerUnhealthyException(Exception):
    def __init__(self, producer: aiokafka.AIOKafkaProducer):
        self.producer = producer


class AppNotConfiguredException(Exception):
    ...
