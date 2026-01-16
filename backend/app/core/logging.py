"""Structured logging configuration."""
import json
import logging
import logging.config
from datetime import datetime, timezone
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    """Format log records as JSON lines."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in ("args", "msg", "levelname", "levelno", "name", "pathname",
                                             "filename", "module", "exc_info", "exc_text", "stack_info",
                                             "lineno", "funcName", "created", "msecs", "relativeCreated",
                                             "thread", "threadName", "processName", "process"):
                continue
            if key not in payload:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging(level: str) -> None:
    """Configure root logging to emit JSON."""
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {"()": JsonFormatter},
        },
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "formatter": "json",
            }
        },
        "root": {
            "level": level,
            "handlers": ["stdout"],
        },
    }
    logging.config.dictConfig(logging_config)
