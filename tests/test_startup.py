import logging
import os
import subprocess
import sys

from ai_decision_platform.logging_config import configure_logging


def test_logging_setup_is_idempotent():
    logger = logging.getLogger("ai_decision_platform")
    original_handlers = logger.handlers[:]
    original_level, original_propagate = logger.level, logger.propagate
    logger.handlers = []
    try:
        configure_logging("INFO")
        configure_logging("ERROR")
        assert len(logger.handlers) == 1
        assert logger.level == logging.ERROR
        assert not logger.propagate
    finally:
        for handler in logger.handlers:
            handler.close()
        logger.handlers = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate


def test_module_starts_from_another_directory(tmp_path):
    env = {**os.environ, "ADP_ENV": "test", "ADP_LOG_LEVEL": "INFO", "ADP_DATA_DIR": "data"}
    result = subprocess.run(
        [sys.executable, "-m", "ai_decision_platform"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "environnement=test" in result.stderr
    assert not (tmp_path / "data").exists()
