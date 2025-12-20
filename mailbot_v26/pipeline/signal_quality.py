from __future__ import annotations

from dataclasses import dataclass
import math

MIN_LENGTH = 40
MIN_ENTROPY = 2.0
MIN_PRINTABLE_RATIO = 0.7


@dataclass
class SignalQuality:
    length: int
    entropy: float
    printable_ratio: float
    quality_score: float
    is_usable: bool
    reason: str


def _shannon_entropy(text: str) -> float:
    if not text:
        return 0.0
    length = len(text)
    counts: dict[str, int] = {}
    for char in text:
        counts[char] = counts.get(char, 0) + 1
    entropy = 0.0
    for count in counts.values():
        probability = count / length
        entropy -= probability * math.log2(probability)
    return entropy


def _printable_ratio(text: str) -> float:
    if not text:
        return 0.0
    printable = 0
    for char in text:
        if char.isprintable() or char in "\n\r\t":
            printable += 1
    return printable / len(text)


def evaluate_signal_quality(text: str) -> SignalQuality:
    length = len(text)
    entropy = _shannon_entropy(text)
    printable_ratio = _printable_ratio(text)
    length_score = min(length / MIN_LENGTH, 1.0) if MIN_LENGTH > 0 else 1.0
    entropy_score = min(entropy / MIN_ENTROPY, 1.0) if MIN_ENTROPY > 0 else 1.0
    printable_score = (
        min(printable_ratio / MIN_PRINTABLE_RATIO, 1.0)
        if MIN_PRINTABLE_RATIO > 0
        else 1.0
    )
    quality_score = max(
        0.0, min((length_score + entropy_score + printable_score) / 3.0, 1.0)
    )

    is_length_ok = length >= MIN_LENGTH
    is_entropy_ok = entropy >= MIN_ENTROPY
    is_printable_ok = printable_ratio >= MIN_PRINTABLE_RATIO
    is_usable = is_length_ok and is_entropy_ok and is_printable_ok

    if not is_length_ok:
        reason = "length_below_threshold"
    elif not is_entropy_ok:
        reason = "entropy_below_threshold"
    elif not is_printable_ok:
        reason = "printable_ratio_below_threshold"
    else:
        reason = "ok"

    return SignalQuality(
        length=length,
        entropy=entropy,
        printable_ratio=printable_ratio,
        quality_score=quality_score,
        is_usable=is_usable,
        reason=reason,
    )
