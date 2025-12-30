from __future__ import annotations

from threading import Lock

_GIGACHAT_LOCK = Lock()


def gigachat_lock() -> Lock:
    return _GIGACHAT_LOCK
