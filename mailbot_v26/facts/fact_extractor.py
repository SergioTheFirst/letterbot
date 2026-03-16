from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Sequence, Set


@dataclass
class FactBundle:
    amounts: List[str] = field(default_factory=list)
    dates: List[str] = field(default_factory=list)
    actions: List[str] = field(default_factory=list)
    entities: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    doc_numbers: List[str] = field(default_factory=list)


class FactExtractor:
    _ACTION_KEYWORDS: Sequence[str] = (
        "прошу",
        "нужно",
        "требуется",
        "оплатить",
        "подписать",
        "согласовать",
        "отправить",
        "выполнить",
        "принять",
        "выслал",
        "вышлю",
        "направляю",
        "please",
        "kindly",
        "required",
        "action needed",
        "action required",
        "please confirm",
        "please review",
        "please sign",
        "please pay",
        "attached",
        "forwarding",
        "let us know",
        "respond by",
        "confirm receipt",
        "complete payment",
    )
    _TEMPLATE_PHRASES: Sequence[str] = (
        "касается темы",
        "без подробностей",
        "можно просмотреть",
    )
    _STOPWORDS: Set[str] = {
        "это",
        "если",
        "также",
        "когда",
        "где",
        "что",
        "кто",
        "или",
        "как",
        "для",
        "после",
        "такой",
        "теперь",
        "который",
        "которые",
        "которые",
        "которые",
        "этого",
        "этой",
        "этим",
        "этом",
        "было",
        "были",
        "будет",
        "при",
        "они",
        "она",
        "оно",
        "он",
        "ваши",
        "ваш",
        "ваша",
        "ваше",
        "наш",
        "наша",
        "наше",
        "наши",
        "это",
        "эта",
        "эти",
        "эту",
        "есть",
    }
    _MONTH_PATTERN = re.compile(
        r"\b(январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|"
        r"август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\s+\d{4}\b",
        re.IGNORECASE,
    )
    _DATE_PATTERN = re.compile(r"\b\d{1,2}\.\d{1,2}\.(?:\d{2}|\d{4})\b")
    _AMOUNT_PATTERN = re.compile(
        r"(?:[€$₽]\s*)?\b\d{1,3}(?:[ \u00A0]?\d{3})*(?:[.,]\d+)?\s*(?:₽|руб\.?|eur|euro|€|\$)?",
        re.IGNORECASE,
    )
    _DOC_NUMBER_PATTERN = re.compile(r"(?:^|\s)(?:№|No|N)\s*[-\w/]+", re.IGNORECASE)
    _ENTITY_PATTERN = re.compile(
        r'\b(?:ООО|ОАО|ИП)\s+[A-ZА-ЯЁ][\w"«»\- ]+|\b(?:Договор|Соглашение|Счет)\b',
        re.IGNORECASE,
    )

    def extract_facts(self, text: str) -> FactBundle:
        if not isinstance(text, str):
            return FactBundle()

        amounts = self._find_unique_matches(self._AMOUNT_PATTERN, text)
        dates = self._extract_dates(text)
        actions = self._extract_actions(text)
        doc_numbers = self._find_unique_matches(self._DOC_NUMBER_PATTERN, text)
        entities = self._extract_entities(text)
        keywords = self._extract_keywords(text)

        return FactBundle(
            amounts=amounts,
            dates=dates,
            actions=actions,
            entities=entities,
            keywords=keywords,
            doc_numbers=doc_numbers,
        )

    def validate_summary(self, summary: str, facts: FactBundle) -> bool:
        if not isinstance(summary, str) or not isinstance(facts, FactBundle):
            return False

        lowered_summary = summary.lower()
        for phrase in self._TEMPLATE_PHRASES:
            if phrase in lowered_summary:
                return False

        summary_numbers = self._extract_numbers(summary)
        fact_numbers = self._collect_fact_numbers(facts)
        if not summary_numbers.issubset(fact_numbers):
            return False

        summary_keywords = set(self._extract_keywords(summary))
        overlap_count = len(summary_keywords & set(facts.keywords))
        return overlap_count >= 2

    def _extract_dates(self, text: str) -> List[str]:
        matches = self._find_unique_matches(self._DATE_PATTERN, text)
        for month_match in self._MONTH_PATTERN.findall(text):
            if month_match not in matches:
                full_match = self._find_full_month_match(month_match, text)
                if full_match and full_match not in matches:
                    matches.append(full_match)
        return matches

    def _find_full_month_match(self, partial: str, text: str) -> str | None:
        pattern = re.compile(rf"\b{re.escape(partial)}\s+\d{{4}}\b", re.IGNORECASE)
        match = pattern.search(text)
        return match.group(0) if match else None

    def _extract_actions(self, text: str) -> List[str]:
        found: List[str] = []
        lowered_text = text.lower()
        for keyword in self._ACTION_KEYWORDS:
            if keyword in lowered_text:
                found.append(keyword)
        return found

    def _extract_entities(self, text: str) -> List[str]:
        matches = []
        for match in self._ENTITY_PATTERN.finditer(text):
            entity = match.group(0).strip()
            if entity and entity not in matches:
                matches.append(entity)
        return matches

    def _extract_keywords(self, text: str) -> List[str]:
        tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", text.lower())
        keywords: List[str] = []
        for token in tokens:
            if len(token) < 4:
                continue
            if token in self._STOPWORDS:
                continue
            if token not in keywords:
                keywords.append(token)
            if len(keywords) >= 30:
                break
        return keywords

    def _extract_numbers(self, text: str) -> Set[str]:
        raw_numbers = re.findall(r"\d+(?:[ \u00A0]?\d+)*(?:[.,]\d+)?", text)
        return {self._normalize_number(num) for num in raw_numbers if num}

    def _collect_fact_numbers(self, facts: FactBundle) -> Set[str]:
        numbers: Set[str] = set()
        for source in (facts.amounts, facts.doc_numbers, facts.dates):
            for item in source:
                numbers.update(self._extract_numbers(item))
        return numbers

    def _find_unique_matches(self, pattern: re.Pattern[str], text: str) -> List[str]:
        matches = []
        for match in pattern.finditer(text):
            value = match.group(0).strip()
            if value and value not in matches:
                matches.append(value)
        return matches

    def _normalize_number(self, value: str) -> str:
        cleaned = re.sub(r"[ \u00A0]", "", value)
        if cleaned.count(",") > 1 or cleaned.count(".") > 1:
            return cleaned
        if "," in cleaned and "." in cleaned:
            return cleaned
        if cleaned.count(",") == 1 and cleaned.count(".") == 0:
            return cleaned.replace(",", ".")
        return cleaned
