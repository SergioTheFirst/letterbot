from __future__ import annotations

import pytest

from mailbot_v26.pipeline import processor

_PLAIN = "Счёт на оплату"
_SAMPLES = [
    _PLAIN.encode("utf-8").decode("cp1251"),
    _PLAIN.encode("utf-8").decode("latin-1"),
    "рџ”ґ Срочно",
    "Оплатить СЧЁТ №5",
    "Hello World",
    "",
    None,
    123,
]


def _clear_caches() -> None:
    for name in ("_normalized_lower_short", "_normalized_lower_long"):
        cached = getattr(processor, name, None)
        if cached is not None:
            cached.cache_clear()


@pytest.fixture(autouse=True)
def _fresh_caches():
    _clear_caches()
    yield
    _clear_caches()


@pytest.mark.parametrize("sample", _SAMPLES)
def test_cached_result_equals_uncached(sample) -> None:
    expected = processor._normalized_lower_uncached(str(sample or ""))

    assert processor._normalized_lower(sample) == expected
    assert processor._normalized_lower(sample) == expected


def test_mojibake_sample_is_repaired() -> None:
    assert processor._normalized_lower(_SAMPLES[0]) == "счёт на оплату"
    assert processor._normalized_lower(_SAMPLES[1]) == "счёт на оплату"


def test_repeated_text_is_normalized_once(monkeypatch) -> None:
    calls: list[str] = []
    real_impl = processor._normalize_mojibake_text_impl

    def _counting(text: str) -> str:
        calls.append(text)
        return real_impl(text)

    monkeypatch.setattr(processor, "_normalize_mojibake_text_impl", _counting)
    text = "Просьба оплатить счёт до пятницы"

    first = processor._normalized_lower(text)
    calls_after_first = len(calls)
    second = processor._normalized_lower(text)

    assert first == second
    assert calls_after_first >= 1
    assert len(calls) == calls_after_first


def test_short_cache_is_bounded() -> None:
    limit = processor._normalized_lower_short.cache_info().maxsize

    for idx in range(limit + 50):
        processor._normalized_lower(f"Письмо номер {idx}")

    assert limit is not None and limit <= 256
    assert processor._normalized_lower_short.cache_info().currsize == limit


def test_long_texts_use_small_separate_cache() -> None:
    long_limit = processor._normalized_lower_long.cache_info().maxsize
    threshold = processor._NORMALIZED_LOWER_SHORT_MAX_CHARS

    for idx in range(long_limit + 3):
        text = f"{idx} " + ("Длинный текст письма. " * (threshold // 10))
        assert len(text) > threshold
        assert processor._normalized_lower(text) == processor._normalized_lower_uncached(
            text
        )

    assert long_limit is not None and long_limit <= 8
    assert processor._normalized_lower_long.cache_info().currsize == long_limit
    assert processor._normalized_lower_short.cache_info().currsize == 0


def test_oversized_text_is_not_cached() -> None:
    text = "я" * (processor._NORMALIZED_LOWER_CACHE_MAX_CHARS + 1)

    assert processor._normalized_lower(text) == text

    assert processor._normalized_lower_long.cache_info().currsize == 0
    assert processor._normalized_lower_short.cache_info().currsize == 0
