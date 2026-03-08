from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DomainClassifier:
    """Lightweight classifier for basic domain intents."""

    def classify(self, sender: str, subject: str, body: str) -> str:
        sender_lower = (sender or "").lower()
        text = f"{subject} {body}".lower()

        marketing_markers = {
            "promotion",
            "discount",
            "sale",
            "offer",
            "акция",
            "скидка",
        }
        invoice_markers = {"invoice", "bill", "billing", "счет", "счёт", "оплат"}
        contract_markers = {"contract", "agreement", "договор"}
        bank_markers = {"bank", "transfer", "payment", "card", "платеж", "перевод"}

        def count_hits(markers: set[str]) -> int:
            return sum(
                1 for marker in markers if marker in text or marker in sender_lower
            )

        if count_hits(bank_markers) >= 2:
            return "BANK"

        if any(marker in text for marker in marketing_markers):
            return "MARKETING"

        if any(marker in text for marker in invoice_markers):
            return "INVOICE"

        if any(marker in text for marker in contract_markers):
            return "CONTRACT"

        return "UNKNOWN"


__all__ = ["DomainClassifier"]
