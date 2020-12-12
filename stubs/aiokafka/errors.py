class NodeNotReadyError(Exception):
    ...


class RequestTimedOutError(Exception):
    ...


class ConsumerStoppedError(Exception):
    ...


class IllegalStateError(Exception):
    ...


class UnrecognizedBrokerVersion(Exception):
    ...


class KafkaConnectionError(Exception):
    ...


class CommitFailedError(Exception):
    ...
