class NodeNotReadyError(Exception):
    ...


class RequestTimedOutError(Exception):
    ...


class IllegalStateError(Exception):
    ...


class UnrecognizedBrokerVersion(Exception):
    ...


class KafkaConnectionError(Exception):
    ...
