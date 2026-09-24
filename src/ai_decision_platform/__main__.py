"""Point d'entrée de vérification de la phase 0."""

from ai_decision_platform.config import load_settings
from ai_decision_platform.logging_config import configure_logging


def main() -> None:
    settings = load_settings()
    logger = configure_logging(settings.log_level)
    logger.info("Plateforme initialisée : environnement=%s", settings.environment)


if __name__ == "__main__":
    main()
