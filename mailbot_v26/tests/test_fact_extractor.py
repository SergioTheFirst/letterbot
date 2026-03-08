from mailbot_v26.facts.fact_extractor import FactBundle, FactExtractor


def test_extracts_core_facts():
    extractor = FactExtractor()
    text = (
        "9 159,43 ₽ поступили в декабрь 2025. "
        "Прошу подписать договор № 12-1 на оплату услуг."
    )

    facts = extractor.extract_facts(text)

    assert "9 159,43 ₽" in facts.amounts
    assert any(date.startswith("декабрь") for date in facts.dates)
    assert "прошу" in facts.actions
    assert any(num.endswith("12-1") for num in facts.doc_numbers)


def test_validate_summary_rejects_new_number():
    extractor = FactExtractor()
    facts = FactBundle(amounts=["100"], dates=["01.01.2024"], doc_numbers=["№ 1"])

    summary = "Итог по сумме 200 и счет № 1"

    assert extractor.validate_summary(summary, facts) is False


def test_validate_summary_rejects_template_phrase():
    extractor = FactExtractor()
    facts = FactBundle(keywords=["итоги", "проект"])

    summary = "Кратко: касается темы проекта, без подробностей"

    assert extractor.validate_summary(summary, facts) is False
