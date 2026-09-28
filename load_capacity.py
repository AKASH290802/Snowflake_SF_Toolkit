import threading
from collections import deque
from contextlib import contextmanager


class SharedLoadCapacity:
    """FIFO admission across dataset workers with one shared active-upload limit."""

    def __init__(self, limit, should_stop=None):
        if limit < 1:
            raise ValueError('Upload capacity must be positive')
        self.limit = limit
        self._should_stop = should_stop
        self._condition = threading.Condition()
        self._waiting = deque()
        self._active = 0

    @contextmanager
    def slot(self):
        ticket = object()
        admitted = False
        with self._condition:
            self._waiting.append(ticket)
            try:
                while True:
                    if self._should_stop and self._should_stop():
                        raise InterruptedError('Upload cancelled before submission')
                    if self._active < self.limit and self._waiting[0] is ticket:
                        break
                    self._condition.wait(timeout=0.2)
                self._waiting.popleft()
                self._active += 1
                admitted = True
                self._condition.notify_all()
            finally:
                if not admitted:
                    self._waiting.remove(ticket)
                    self._condition.notify_all()
        try:
            yield
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def run(self, function, *args, **kwargs):
        with self.slot():
            if self._should_stop and self._should_stop():
                raise InterruptedError('Upload cancelled before submission')
            return function(*args, **kwargs)