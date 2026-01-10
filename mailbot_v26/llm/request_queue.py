from __future__ import annotations

import queue
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional


@dataclass(frozen=True, slots=True)
class LLMRequest:
    """EN: LLM request payload. RU: Запрос на LLM."""

    account_email: str
    email_id: int
    subject: str
    from_email: str
    body_text: str
    attachments: list[dict]
    received_at: datetime
    input_chars: int


class LLMRequestQueue:
    """EN: Thread-safe FIFO queue. RU: Потокобезопасная очередь FIFO."""

    def __init__(self, *, max_size: int) -> None:
        self._queue: queue.Queue[LLMRequest] = queue.Queue(maxsize=max_size)

    def enqueue(self, request: LLMRequest, *, timeout_sec: float) -> bool:
        """EN: Synchronous enqueue with timeout. RU: Синхронная постановка в очередь."""

        try:
            self._queue.put(request, timeout=timeout_sec)
            return True
        except queue.Full:
            return False

    def dequeue(self, *, timeout_sec: float) -> Optional[LLMRequest]:
        """EN: Dequeue with timeout. RU: Извлечь из очереди с таймаутом."""

        try:
            return self._queue.get(timeout=timeout_sec)
        except queue.Empty:
            return None

    def task_done(self) -> None:
        self._queue.task_done()


class BackgroundLLMWorker:
    """EN: Background LLM worker. RU: Фоновый обработчик LLM."""

    def __init__(
        self,
        request_queue: LLMRequestQueue,
        handler: Callable[[LLMRequest], None],
        *,
        poll_timeout_sec: float,
    ) -> None:
        self._queue = request_queue
        self._handler = handler
        self._poll_timeout_sec = poll_timeout_sec
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1.0)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            request = self._queue.dequeue(timeout_sec=self._poll_timeout_sec)
            if request is None:
                continue
            try:
                self._handler(request)
            finally:
                self._queue.task_done()


__all__ = ["LLMRequest", "LLMRequestQueue", "BackgroundLLMWorker"]
