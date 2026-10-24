from __future__ import annotations

from pathlib import Path

import pytest

from astrq.core.config import load_settings


def _project(tmp_path: Path, env: str) -> Path:
    (tmp_path / "config").mkdir()
    cfg = tmp_path / "config" / "settings.yaml"
    cfg.write_text("provider:\n  synthetic:\n    speed: 10\nlog_level: WARNING\n")
    (tmp_path / ".env").write_text(env)
    return cfg


def test_dotenv_overrides_yaml_and_env_overrides_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _project(
        tmp_path,
        "ASTRQ_PROVIDER__SYNTHETIC__SPEED=45\n"
        "ASTRQ_LOG_LEVEL=DEBUG\n"
        "ASTRQ_ALERTS__TELEGRAM__ENABLED=true\n"
        "ASTRQ_ALERTS__TELEGRAM__BOT_TOKEN=123:abc\n"
        "ASTRQ_ALERTS__TELEGRAM__CHAT_ID=-10042\n",
    )
    s = load_settings(cfg)
    assert s.provider.synthetic.speed == 45  # .env beats YAML
    assert s.log_level == "DEBUG"
    tg = s.alerts.telegram
    assert tg.enabled and tg.chat_id == "-10042"
    assert tg.bot_token is not None and tg.bot_token.get_secret_value() == "123:abc"
    assert "123:abc" not in repr(s)  # secret never printed

    monkeypatch.setenv("ASTRQ_PROVIDER__SYNTHETIC__SPEED", "99")
    assert load_settings(cfg).provider.synthetic.speed == 99  # shell env beats .env


def test_telegram_enabled_without_token_fails_fast(tmp_path: Path) -> None:
    cfg = _project(tmp_path, "ASTRQ_ALERTS__TELEGRAM__ENABLED=true\n")
    with pytest.raises(ValueError, match="BOT_TOKEN"):
        load_settings(cfg)
