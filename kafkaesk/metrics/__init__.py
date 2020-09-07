import abc
from functools import wraps
import time


class Metric(abc.ABC):
    def __init__(self, name, labels=None, description=None):
        self.name = name
        self.labels = labels or {}
        self.description = description or ""


# Abstract interfaces
class Counter(Metric):
    @abc.abstractclassmethod
    def inc(self, value):
        ...


class Gauge(Metric):
    @abc.abstractclassmethod
    def inc(self, value):
        ...

    @abc.abstractclassmethod
    def dec(self, value):
        ...


class Histogram(Metric):
    @abc.abstractclassmethod
    def observe(self, value):
        ...

    def watch(self, func):
        @wraps(func)
        async def inner(*args, **kwargs):
            with self:
                return await func(*args, **kwargs)

        return inner

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        elapsed = time.time() - self.start
        self.observe(elapsed)
