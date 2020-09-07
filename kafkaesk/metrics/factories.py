from .dummy import InMemoryCounter, InMemoryGauge, InMemoryHistogram
from . import Counter, Gauge, Histogram


implementations = {
    "dummy": (InMemoryCounter, InMemoryGauge, InMemoryHistogram),
    "prometheus": (None, None, None),
    "noop": (Counter, Gauge, Histogram)
}

class Factory:
    def __init__(self, factory):
        self.name = factory

    def counter(self, name, labels=None, description=None):
        return implementations.get(self.factory)[0](name, labels, description)


    def gauge(self, name, labels=None, description=None):
        return implementations.get(self.factory)[1](name, labels, description)


    def histogram(self, name, labels=None, description=None):
        return implementations.get(self.factory)[2](name, labels, description)
