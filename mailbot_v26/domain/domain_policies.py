from __future__ import annotations

DOMAIN_POLICIES = {
    "BANK": {
        "default_priority": "RED",
        "allowed_types": ["PAYMENT_REQUEST", "PAYMENT_REMINDER", "INVOICE", "SECURITY_ALERT"],
        "default_verb": "Оплатить",
    },
    "TAX": {
        "default_priority": "RED",
        "default_verb": "Оплатить",
    },
    "COURT": {
        "default_priority": "RED",
        "default_verb": "Требуется",
    },
    "GOVERNMENT": {
        "default_priority": "YELLOW",
        "default_verb": "Ознакомиться",
    },
    "CLIENT": {
        "default_priority": "YELLOW",
        "allowed_types": ["CONTRACT_APPROVAL", "CONTRACT_UPDATE", "PRICE_LIST", "INVOICE", "MEETING_CHANGE"],
        "default_verb": "Подписать",
    },
    "SUPPLIER": {
        "default_priority": "YELLOW",
        "default_verb": "Проверить",
    },
    "HR": {
        "default_priority": "BLUE",
        "allowed_types": ["POLICY_UPDATE", "MEETING_CHANGE"],
        "default_verb": "Ознакомиться",
    },
    "IT": {
        "default_priority": "YELLOW",
        "default_verb": "Проверить",
    },
    "DOMAIN_REGISTRAR": {
        "default_priority": "YELLOW",
        "allowed_types": [],
        "default_verb": "Продлить",
    },
    "LOGISTICS": {
        "default_priority": "YELLOW",
        "default_verb": "Проверить",
    },
    "FAMILY": {
        "default_priority": "BLUE",
        "default_verb": "Ответить",
    },
    "INTERNAL": {
        "default_priority": "BLUE",
        "default_verb": "Ознакомиться",
    },
    "UNKNOWN": {
        "default_priority": "BLUE",
        "default_verb": "Ознакомиться",
    },
}
