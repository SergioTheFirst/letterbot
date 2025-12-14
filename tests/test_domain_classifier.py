from mailbot_v26.domain.domain_classifier import DomainClassifier


def test_bank_classification_requires_multiple_signals():
    classifier = DomainClassifier()
    sender = "alerts@bigbank.com"
    subject = "Payment transfer notice"
    body = "Your bank card transfer was completed."

    result = classifier.classify(sender, subject, body)

    assert result == "BANK"


def test_invoice_and_contract_are_distinct():
    classifier = DomainClassifier()
    sender = "billing@supplier.com"
    subject = "Invoice and billing details"
    body = "Agreement reference contract A12 attached"

    result = classifier.classify(sender, subject, body)

    assert result == "INVOICE"


def test_unknown_when_score_below_threshold():
    classifier = DomainClassifier()
    sender = "user@example.com"
    subject = "Hello"
    body = "Checking in"

    result = classifier.classify(sender, subject, body)

    assert result == "UNKNOWN"


def test_marketing_over_personal_priority():
    classifier = DomainClassifier()
    sender = "news@store.com"
    subject = "Limited promotion discount offer"
    body = "Hello friend, get your discount today"

    result = classifier.classify(sender, subject, body)

    assert result == "MARKETING"

