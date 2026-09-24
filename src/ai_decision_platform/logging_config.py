"""Logs applicatifs sur stderr, sans modifier le logger racine."""

import logging


def configure_logging(level: str) -> logging.Logger:
    logger = logging.getLogger("ai_decision_platform")
    logger.setLevel(level)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
    return logger
