from . import Counter, Gauge, Histogram
import threading
import collections

# This implementation for local development and for testing purposes

# Global registry is just a circular buffer with 1000 elements max
metrics = collections.deque(maxlen=1000)

# Avoid conflicts
lock = threading.Lock()


class InMemoryCounter(Counter):
    def inc(self, value=1):
        with lock:
            if len(metrics) > 1000:
                metrics.clear()
            labels = [f"{k}={v}" for k,v in self.labels.items()]
            metrics.append(f"counter({self.name}), {labels}, +{value}")


class InMemoryGauge(Gauge):
    def inc(self, value=1):
        with lock:
            if len(metrics) > 1000:
                metrics.clear()
            labels = [f"{k}={v}" for k,v in self.labels.items()]
            metrics.append(f"gauge({self.name}), {labels}, +{value}")

    def dec(self, value=1):
        with lock:
            if len(metrics) > 1000:
                metrics.clear()
            labels = [f"{k}={v}" for k,v in self.labels.items()]
            metrics.append(f"gauge({self.name}), {labels}, -{value}")


class InMemoryHistogram(Histogram):
    def observe(self, value):
        with lock:
            if len(metrics) > 1000:
                metrics.clear()
            labels = [f"{k}={v}" for k,v in self.labels.items()]
            metrics.append(f"histogram({self.name}), {labels}, {value}")
