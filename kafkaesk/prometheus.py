import prometheus_client as client
from typing import Dict, Optional, Type
import traceback
import time


class Watch:
    def __call__(self):
        pass
    def __aenter__(self):
        self.start = time.time()

    def __exit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Optional[Exception],
        exc_traceback: Optional[traceback.StackSummary],
    ):
        elapsed = time.time() - self.start
