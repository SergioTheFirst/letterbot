from __future__ import annotations

from mailbot_v26.facts.fact_extractor import FactExtractor


def test_stopwords_cover_relative_pronoun_forms() -> None:
    stopwords = FactExtractor._STOPWORDS

    assert {"который", "которая", "которое", "которые"} <= stopwords


def test_stopwords_cover_demonstrative_pronoun_forms() -> None:
    stopwords = FactExtractor._STOPWORDS

    assert {"этот", "эта", "это", "эти", "эту"} <= stopwords


def test_keywords_skip_relative_and_demonstrative_pronouns() -> None:
    text = "Задача, которая важна; письмо, которое пришло; этот договор."

    keywords = FactExtractor().extract_facts(text).keywords

    assert "которая" not in keywords
    assert "которое" not in keywords
    assert "этот" not in keywords
    assert "договор" in keywords
