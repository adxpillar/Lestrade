from __future__ import annotations

import threading
import time


class MinIntervalLimiter:
    """Enforces a minimum elapsed time between successive operations (per instance)."""

    __slots__ = ("_min_interval_s", "_lock", "_last_end")

    def __init__(self, min_interval_s: float) -> None:
        if min_interval_s < 0:
            raise ValueError("min_interval_s must be >= 0")
        self._min_interval_s = min_interval_s
        self._lock = threading.Lock()
        self._last_end = 0.0

    def wait_turn(self) -> None:
        """Block until at least min_interval_s since the previous wait_turn completed."""
        if self._min_interval_s == 0:
            return
        with self._lock:
            now = time.monotonic()
            wait = self._min_interval_s - (now - self._last_end)
            if wait > 0:
                time.sleep(wait)
            self._last_end = time.monotonic()
