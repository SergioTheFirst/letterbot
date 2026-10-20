"""Equivalence and performance guards for ``mailbot_v26.text.mojibake``.

The placeholder-based ``_repair_with_encoding`` restored each protected
character with one ``str.replace`` over the whole text, so long Cyrillic
bodies (thousands of placeholders under latin-1/cp1252) cost O(n^2). The
linear rewrite must return exactly what the old implementation returned, so
the old implementation is kept below verbatim (sharing the module constants,
which the rewrite did not touch) and compared against the new one on the
golden corpus, existing test inputs, source literals and generated strings.
"""

from __future__ import annotations

import ast
import itertools
import json
import random
import re
import time
from pathlib import Path

import pytest

from mailbot_v26.text import mojibake
from mailbot_v26.text.mojibake import normalize_mojibake_text

_TESTS_DIR = Path(__file__).resolve().parent
_PACKAGE_DIR = _TESTS_DIR.parent
_FIXTURES_DIR = _TESTS_DIR / "fixtures"

_ENCODING_PAIRS = (
    ("cp1251", "utf-8"),
    ("latin-1", "utf-8"),
    ("cp1252", "utf-8"),
    ("latin-1", "cp1251"),
    ("cp1252", "cp1251"),
)

# The P2a slow case (test_telegram_text_cap): one Russian sentence repeated
# to a 6120-char body.
_SENTENCE = "Отгрузка партии номер сорок два задерживается на складе поставщика. "
_P2A_BODY = _SENTENCE * 90


# --- verbatim copy of the pre-rewrite implementation -------------------------


def _legacy_contains_mojibake(text: str) -> bool:
    if any(marker in text for marker in mojibake._MOJIBAKE_MARKERS):
        return True
    if mojibake._LATIN1_GARBAGE_RE.search(text):
        return True
    return mojibake._MOJIBAKE_PAIR_RE.search(text) is not None


def _legacy_repair_with_encoding(
    source: str, encode_encoding: str, decode_encoding: str
) -> str:
    protected_parts: list[str] = []
    placeholders: dict[str, str] = {}
    protected_index = 0
    for char in source:
        try:
            char.encode(encode_encoding)
            protected_parts.append(char)
        except UnicodeEncodeError:
            token = f"__mb_{protected_index}__"
            placeholders[token] = char
            protected_parts.append(token)
            protected_index += 1
    protected = "".join(protected_parts)

    repaired = protected
    try:
        repaired = protected.encode(encode_encoding).decode(decode_encoding)
    except (UnicodeEncodeError, UnicodeDecodeError):
        repaired = protected

    for token, char in placeholders.items():
        repaired = repaired.replace(token, char)
    return repaired


def _legacy_mojibake_score(text: str) -> int:
    score = 0
    for marker in mojibake._MOJIBAKE_MARKERS:
        score += text.count(marker) * 4
    for bad in mojibake._PUNCTUATION_REPLACEMENTS:
        score += text.count(bad) * 2
    for match in mojibake._MOJIBAKE_PAIR_RE.finditer(text):
        score += len(match.group(0))
    for match in mojibake._LATIN1_GARBAGE_RE.finditer(text):
        score += len(match.group(0)) * 2
    return score


def _legacy_normalize_chunk(source: str) -> str:
    repaired = source
    for _ in range(3):
        if not _legacy_contains_mojibake(repaired):
            break
        candidates = [
            repaired,
            _legacy_repair_with_encoding(repaired, "cp1251", "utf-8"),
            _legacy_repair_with_encoding(repaired, "latin-1", "utf-8"),
            _legacy_repair_with_encoding(repaired, "cp1252", "utf-8"),
            _legacy_repair_with_encoding(repaired, "latin-1", "cp1251"),
            _legacy_repair_with_encoding(repaired, "cp1252", "cp1251"),
        ]
        best = min(candidates, key=_legacy_mojibake_score)
        if best == repaired:
            break
        repaired = best
    return repaired


def _legacy_normalize_mojibake_text(text: str) -> str:
    source = str(text or "")
    if not source:
        return ""

    repaired = _legacy_normalize_chunk(source)
    if _legacy_contains_mojibake(repaired) and ("\n" in repaired or "\r" in repaired):
        parts = re.split(r"(\r\n|\n|\r)", repaired)
        repaired = "".join(
            part if part in {"\r\n", "\n", "\r"} else _legacy_normalize_chunk(part)
            for part in parts
        )

    for bad, good in mojibake._PUNCTUATION_REPLACEMENTS.items():
        repaired = repaired.replace(bad, good)
    return repaired


# --- input sets --------------------------------------------------------------


def _mangle_variants(text: str) -> list[str]:
    """Typical mis-decodings of ``text`` (skipped when it cannot be encoded)."""
    try:
        raw = text.encode("utf-8")
    except UnicodeEncodeError:
        return []
    variants = [
        raw.decode("cp1251", errors="replace"),
        raw.decode("latin-1"),
        raw.decode("cp1252", errors="replace"),
    ]
    cp1251_raw = text.encode("cp1251", errors="replace")
    variants.append(cp1251_raw.decode("latin-1"))
    return variants


def _golden_corpus_texts() -> list[str]:
    texts: list[str] = []
    for name in ("cases.json", "bilingual_cases.json"):
        path = _FIXTURES_DIR / "golden_corpus" / name
        data = json.loads(path.read_text(encoding="utf-8"))
        cases = data if isinstance(data, list) else data.get("cases", [])
        for case in cases:
            texts.append(str(case.get("subject") or ""))
            texts.append(str(case.get("body_text") or ""))
            texts.append(str(case.get("sender_email") or ""))
            for attachment in case.get("attachments") or []:
                texts.append(str(attachment.get("filename") or ""))
                texts.append(str(attachment.get("text") or ""))
    return texts


_LITERAL_SOURCE_FILES = (
    "pipeline/processor.py",
    "pipeline/tg_renderer.py",
    "telegram/inbound.py",
    "ui/i18n.py",
    "worker/telegram_sender.py",
    "text/mojibake.py",
    "tests/test_telegram_render.py",
    "tests/test_telegram_sender.py",
    "tests/test_normalized_lower_cache.py",
    "tests/test_source_mojibake_scan.py",
)


def _source_literal_texts() -> list[str]:
    literals: set[str] = set()
    for relative in _LITERAL_SOURCE_FILES:
        tree = ast.parse((_PACKAGE_DIR / relative).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                literals.add(node.value)
    return sorted(literals)


def _existing_test_inputs() -> list[str]:
    plain = "Счёт на оплату"
    sender_texts = (
        "\U0001f535 from sender@example.com:\nCheck payment by 15.04 — urgent…\n• Action",
        "\U0001f7e1 from alerts@example.com:\nTotal payable 1200 USD",
    )
    inputs = [
        plain.encode("utf-8").decode("cp1251"),
        plain.encode("utf-8").decode("latin-1"),
        "рџ”ґ Срочно",
        "Оплатить СЧЁТ №5",
        "Hello World",
        "",
        "Счёт №42 & €",
        "Сумма 1 234,56 € и аванс 50 ₽.",
        "Р Р†Р вЂљ",
        "Р В РЎвЂўР РЋРІР‚С™",
        "РЎР‚РЎСџ",
        "вЂ",
        _P2A_BODY,
    ]
    for text in sender_texts:
        inputs.append(text)
        inputs.append(text.encode("utf-8").decode("cp1251"))
    for path in sorted((_FIXTURES_DIR / "telegram_snapshots").glob("*.txt")):
        inputs.append(path.read_text(encoding="utf-8"))
    inputs.extend(mojibake._MOJIBAKE_MARKERS)
    for bad, good in mojibake._PUNCTUATION_REPLACEMENTS.items():
        inputs.extend((bad, good, f"Срок {bad} пятница", f"Срок {good} пятница"))
    return inputs


_RU_WORDS = (
    "Счёт", "оплата", "договор", "поставки", "срочно", "Просим", "подписать",
    "акт", "сверки", "до", "пятницы", "ИНН", "сумма", "руб.", "Шереметьево",
    "ёлка", "Щука", "объём", "юридический", "ЭДО", "Иванов", "сердце",
)
_EN_WORDS = (
    "invoice", "payment", "due", "number", "December", "member", "thumbnail",
    "Total", "USD", "Please", "review",
)
_PUNCT = (
    "—", "–", "…", "•", "№", "«", "»", "€", "₽", "·", " ", "“", "”", "’",
    ",", ".", "!", "?", ":", ";", "(", ")", "%", "&", "<", ">", "_", "-",
)
_EMOJI = ("🔴", "🟡", "🔵", "📎", "✅", "⚠️")
_PLACEHOLDER_LOOKALIKES = (
    "__mb_0__", "__mb_1__", "__mb_12__", "mb_1__", "mb_0__", "thumb_1.jpg",
    "__mb_", "_mb_01__", "__mb_99999999999999999999__", "mb_2", "__mb_0____mb_1__",
)
_NEWLINES = ("\n", "\r\n", "\r")
_RANDOM_CHAR_POOL = (
    [chr(code) for code in range(0x20, 0x7F)]
    + [chr(code) for code in range(0xA0, 0x100)]
    + [chr(code) for code in range(0x400, 0x460)]
    + list("€‚ƒ„…†‡ˆ‰Š‹ŒŽ‘’“”•–—˜™š›œžŸЂЃѓЉЊЌЋЏђљњќћџЎўЈҐЁЄЇІіґёєјЅѕї")
    + ["�", "中", "🔴", "\n", "\r", "\ud800"]
)


def _sentence(rng: random.Random, words: tuple[str, ...], low: int, high: int) -> str:
    pieces = []
    for _ in range(rng.randint(low, high)):
        pieces.append(rng.choice(words))
        if rng.random() < 0.3:
            pieces.append(rng.choice(_PUNCT))
    return " ".join(pieces)


def _maybe_mangle(text: str, rng: random.Random) -> str:
    variants = _mangle_variants(text)
    if not variants or rng.random() < 0.25:
        return text
    mangled = rng.choice(variants)
    if rng.random() < 0.2:
        again = _mangle_variants(mangled)
        if again:
            mangled = rng.choice(again)
    return mangled


def _generated_inputs(count: int = 420, seed: int = 20261020) -> list[str]:
    rng = random.Random(seed)
    inputs: list[str] = []
    for index in range(count):
        kind = index % 7
        if kind == 0:
            text = _sentence(rng, _RU_WORDS, 3, 25)
        elif kind == 1:
            text = rng.choice(_mangle_variants(_sentence(rng, _RU_WORDS, 3, 25)))
        elif kind == 2:
            text = rng.choice(_NEWLINES).join(
                (
                    _sentence(rng, _RU_WORDS, 2, 8),
                    _maybe_mangle(_sentence(rng, _RU_WORDS, 2, 8), rng),
                    _sentence(rng, _EN_WORDS, 2, 8),
                )
            )
        elif kind == 3:
            lines = [
                _maybe_mangle(_sentence(rng, _RU_WORDS + _EN_WORDS, 0, 10), rng)
                for _ in range(rng.randint(2, 8))
            ]
            text = "".join(line + rng.choice(_NEWLINES) for line in lines)
        elif kind == 4:
            text = _maybe_mangle(
                "".join(rng.choice(_PUNCT + _EMOJI) for _ in range(rng.randint(1, 30))),
                rng,
            )
        elif kind == 5:
            pieces = [
                _maybe_mangle(_sentence(rng, _RU_WORDS, 1, 6), rng)
                for _ in range(rng.randint(1, 4))
            ]
            for _ in range(rng.randint(1, 3)):
                position = rng.randint(0, len(pieces))
                pieces.insert(position, rng.choice(_PLACEHOLDER_LOOKALIKES))
            text = rng.choice(("", " ", "\n")).join(pieces)
        else:
            text = "".join(rng.choice(_RANDOM_CHAR_POOL) for _ in range(rng.randint(1, 80)))
        if index % 60 == 59:
            text = (text + " ") * 25
        inputs.append(text)
    return inputs


# Hand-made strings where literal placeholder look-alikes overlap the real
# placeholders, so the old sequential str.replace order decides the output.
_PLACEHOLDER_OVERLAP_CASES = (
    "ЖЖЖmb_1__",
    "Жmb_0__Ж",
    "__mb_0__Ж",
    "ЖЖ__mb_1__mb_0__",
    "Жmb_1__mb_0__ЖЖ",
    "Ж" * 12 + "mb_1__mb_11__",
    "Ж" * 12 + "mb_11__mb_1__",
    "é€Жmb_2__mb_1__mb_0__",
    "Счёт thumb_1.jpg и thumb_2.png",
    "__mb_0____mb_1__ЖЖ",
)


# --- equivalence -------------------------------------------------------------


def _assert_same_normalization(texts: list[str]) -> None:
    mismatches = []
    for text in texts:
        expected = _legacy_normalize_mojibake_text(text)
        actual = normalize_mojibake_text(text)
        if actual != expected:
            mismatches.append(ascii(text[:120]))
    assert mismatches == []


def test_golden_corpus_normalization_unchanged() -> None:
    texts = _golden_corpus_texts()
    assert len(texts) > 500
    _assert_same_normalization(texts)


def test_existing_test_inputs_normalization_unchanged() -> None:
    _assert_same_normalization(_existing_test_inputs())


def test_source_literals_and_their_mojibake_unchanged() -> None:
    literals = _source_literal_texts()
    assert len(literals) > 1000
    texts = list(literals)
    for literal in literals:
        if not literal.isascii():
            texts.extend(_mangle_variants(literal))
    _assert_same_normalization(texts)


def test_generated_inputs_normalization_unchanged() -> None:
    texts = _generated_inputs()
    _assert_same_normalization(texts + list(_PLACEHOLDER_OVERLAP_CASES))


def test_repair_with_encoding_matches_legacy_for_every_encoding_pair() -> None:
    texts = (
        _generated_inputs(seed=7)
        + list(_PLACEHOLDER_OVERLAP_CASES)
        + _existing_test_inputs()
    )
    mismatches = []
    for text in texts:
        for encode_encoding, decode_encoding in _ENCODING_PAIRS:
            expected = _legacy_repair_with_encoding(text, encode_encoding, decode_encoding)
            actual = mojibake._repair_with_encoding(text, encode_encoding, decode_encoding)
            if actual != expected:
                mismatches.append((encode_encoding, decode_encoding, ascii(text[:80])))
    assert mismatches == []


def test_repair_with_encoding_matches_legacy_on_exhaustive_placeholder_lookalikes() -> None:
    alphabet = ("Ж", "é", "€", "mb_", "__mb_", "0", "1", "__")
    mismatches = []
    for length in range(1, 6):
        for combo in itertools.product(alphabet, repeat=length):
            text = "".join(combo)
            for encode_encoding, decode_encoding in _ENCODING_PAIRS:
                expected = _legacy_repair_with_encoding(text, encode_encoding, decode_encoding)
                actual = mojibake._repair_with_encoding(text, encode_encoding, decode_encoding)
                if actual != expected:
                    mismatches.append((encode_encoding, decode_encoding, ascii(text)))
    assert mismatches == []


@pytest.mark.parametrize("encoding", ["cp1251", "latin-1", "cp1252"])
def test_non_encodable_classification_matches_codec(encoding: str) -> None:
    every_code_point = "".join(map(chr, range(0x110000)))
    pattern = mojibake._NON_ENCODABLE_RUN_RES[encoding]
    expected = every_code_point.encode(encoding, errors="ignore").decode(encoding)
    assert pattern.sub("", every_code_point) == expected


# --- performance -------------------------------------------------------------

# Generous wall-clock budgets. Measured on the dev box: 24480-char body
# 0.02s now vs 5.6s quadratic; 48976 chars with a look-alike (placeholder
# path) 0.4s now vs 26s quadratic.
_LONG_BODY = _SENTENCE * 360  # 24480 chars


def test_p2a_slow_case_output_unchanged() -> None:
    assert normalize_mojibake_text(_P2A_BODY) == _legacy_normalize_mojibake_text(_P2A_BODY)


def test_long_cyrillic_body_normalizes_in_linear_time() -> None:
    started = time.perf_counter()
    result = normalize_mojibake_text(_LONG_BODY)
    elapsed = time.perf_counter() - started

    assert result == _LONG_BODY
    assert elapsed < 1.0, f"normalize took {elapsed:.2f}s for {len(_LONG_BODY)} chars"


def test_long_body_with_placeholder_lookalike_normalizes_in_linear_time() -> None:
    body = _LONG_BODY + "см. thumb_1.jpg " + _LONG_BODY
    started = time.perf_counter()
    result = normalize_mojibake_text(body)
    elapsed = time.perf_counter() - started

    assert result == body
    assert elapsed < 5.0, f"normalize took {elapsed:.2f}s for {len(body)} chars"
