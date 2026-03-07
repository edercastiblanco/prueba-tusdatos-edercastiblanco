import logging
import logging.config
import os
from pathlib import Path

def setup_logging(default_level="INFO"):
    """
    Configuración centralizada de logging usando dictConfig.
    """
    # 1. Determinar rutas (usando Path es más limpio)
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "app.log"

    # 2. Obtener nivel desde entorno o parámetro
    level = os.getenv("LOG_LEVEL", default_level).upper()

    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False, # Importante para no romper librerías externas
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": level,
                "formatter": "standard",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": level,
                "formatter": "detailed",
                "filename": str(log_file),
                "maxBytes": 10_485_760, # 10MB
                "backupCount": 5,
                "encoding": "utf8",
            },
        },
        "loggers": {
            # El logger raíz (root) captura todo
            "": {
                "handlers": ["console", "file"],
                "level": level,
                "propagate": True,
            },
            # Puedes silenciar o aumentar detalle de librerías específicas aquí
            "urllib3": {"level": "WARNING"}, 
        },
    }

    logging.config.dictConfig(LOGGING_CONFIG)

def get_logger(name):
    """
    Retorna un logger con el nombre del módulo (__name__).
    """
    return logging.getLogger(name)