import threading


class CallerThreadStatus:
    """Deliver worker status on the creating thread without retaining every poll."""

    def __init__(self, callback):
        self._callback = callback
        self._owner = threading.get_ident()
        self._lock = threading.Lock()
        self._pending = None

    def __call__(self, message, level='info'):
        if threading.get_ident() == self._owner:
            if self._callback:
                self._callback(message, level=level)
        else:
            with self._lock:
                self._pending = (message, level)

    def flush(self):
        with self._lock:
            pending = self._pending
            self._pending = None
        if pending and self._callback:
            self._callback(pending[0], level=pending[1])


class LoadEventBus:
    """Coalesce dashboard events per dataset; API outcomes remain in loader results."""

    def __init__(self):
        self._lock = threading.Lock()
        self._pending = {}

    def callback(self, dataset, kind):
        def publish(value, level='info'):
            with self._lock:
                self._pending[(dataset, kind)] = (value, level)
        return publish

    def drain(self, dashboards):
        with self._lock:
            pending = self._pending
            self._pending = {}
        for (dataset, kind), (value, level) in pending.items():
            dashboard = dashboards[dataset]
            if kind == 'status':
                dashboard.on_status(value, level=level)
            elif kind == 'error':
                dashboard.on_error(value)
            elif kind == 'progress':
                dashboard.on_progress(value)