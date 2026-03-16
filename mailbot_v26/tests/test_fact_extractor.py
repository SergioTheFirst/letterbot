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


def test_extracts_english_action_markers() -> None:
    extractor = FactExtractor()
    text = (
        "Please review the contract attachment and confirm receipt. "
        "Let us know by Friday whether payment can be completed."
    )

    facts = extractor.extract_facts(text)

    assert "please" in facts.actions
    assert "please review" in facts.actions
    assert "confirm receipt" in facts.actions
    assert "let us know" in facts.actions


def test_extract_actions_ignores_negated_or_partial_english_markers() -> None:
    extractor = FactExtractor()
    text = (
        "We were pleased with the outcome. "
        "The file was reattached after review. "
        "No action needed from your side and approval is not required today."
    )

    facts = extractor.extract_facts(text)

    assert "please" not in facts.actions
    assert "attached" not in facts.actions
    assert "required" not in facts.actions
    assert "action needed" not in facts.actions


def test_validate_summary_rejects_template_phrase():
    extractor = FactExtractor()
    facts = FactBundle(keywords=["итоги", "проект"])

    summary = "Кратко: касается темы проекта, без подробностей"

    assert extractor.validate_summary(summary, facts) is False
