from pathlib import Path

import pytest

from ai_decision_platform.config import load_settings


def test_defaults_are_independent_of_host_environment(monkeypatch):
    monkeypatch.setenv("ADP_ENV", "production")
    settings = load_settings({})
    assert settings.environment == "local"
    assert settings.log_level == "INFO"
    assert settings.data_dir == Path("data")


def test_environment_overrides(monkeypatch, tmp_path):
    monkeypatch.setenv("ADP_ENV", "test")
    monkeypatch.setenv("ADP_LOG_LEVEL", "debug")
    monkeypatch.setenv("ADP_DATA_DIR", str(tmp_path))
    settings = load_settings()
    assert settings.environment == "test"
    assert settings.log_level == "DEBUG"
    assert settings.data_dir == tmp_path


@pytest.mark.parametrize(
    "values",
    [
        {"ADP_ENV": "unknown"},
        {"ADP_LOG_LEVEL": "verbose"},
        {"ADP_DATA_DIR": " "},
    ],
)
def test_invalid_configuration_is_rejected(values):
    with pytest.raises(ValueError):
        load_settings(values)


def test_loading_does_not_create_data_directory(tmp_path):
    target = tmp_path / "missing"
    load_settings({"ADP_DATA_DIR": str(target)})
    assert not target.exists()
