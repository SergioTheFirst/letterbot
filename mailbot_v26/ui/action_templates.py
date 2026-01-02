from __future__ import annotations


_DEADLOCK_TEMPLATE = (
    "Предлагаю созвониться на 15 минут сегодня или завтра — так быстрее решим вопрос."
)
_SILENCE_TEMPLATE = "Напомню про наш вопрос. Удобно вернуться к нему сегодня?"


def template_for_deadlock(*, from_email: str | None, subject: str | None) -> str:
    return _DEADLOCK_TEMPLATE


def template_for_silence(*, contact: str) -> str:
    return _SILENCE_TEMPLATE


__all__ = [
    "template_for_deadlock",
    "template_for_silence",
]
