import importlib


def test_domain_prompts_importable_without_side_effects():
    module = importlib.import_module("mailbot_v26.domain.domain_prompts")

    assert hasattr(module, "PROMPTS_BY_DOMAIN")
    assert isinstance(module.PROMPTS_BY_DOMAIN, dict)
    assert module.PROMPTS_BY_DOMAIN

    for domain, prompt in module.PROMPTS_BY_DOMAIN.items():
        assert isinstance(domain, str)
        assert isinstance(prompt, str)
        assert prompt.strip()
