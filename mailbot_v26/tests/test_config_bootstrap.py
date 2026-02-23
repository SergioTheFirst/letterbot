def test_init_config_creates_files_with_change_me_placeholder(tmp_path) -> None:
    """
    Regression: init_config must produce accounts.ini containing CHANGE_ME
    so the onboarding gate in run_mailbot.bat can detect unconfigured state.
    If this test fails, the bat-level gate will never trigger.
    """
    from mailbot_v26.tools.config_bootstrap import init_config

    result = init_config(tmp_path)

    accounts_file = tmp_path / "accounts.ini"
    assert accounts_file.exists(), "accounts.ini must be created by init_config"

    content = accounts_file.read_text(encoding="utf-8")
    assert "CHANGE_ME" in content, (
        "accounts.ini must contain CHANGE_ME placeholder "
        "so run_mailbot.bat onboarding gate can detect unconfigured state. "
        "If you removed CHANGE_ME from the template, update the bat gate too."
    )
