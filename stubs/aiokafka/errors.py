class KafkaError(Exception):
    ...


class NodeNotReadyError(KafkaError):
    ...


class RequestTimedOutError(KafkaError):
    ...


class ConsumerStoppedError(KafkaError):
    ...


class IllegalStateError(KafkaError):
    ...


class UnrecognizedBrokerVersion(KafkaError):
    ...


class KafkaConnectionError(KafkaError):
    ...


class CommitFailedError(KafkaError):
    ...
