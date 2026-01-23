"""Audit artifact persistence helper."""
import json
from pathlib import Path


class AuditStore:
    def __init__(self, base_path: Path) -> None:
        self.base_path = base_path

    def write_json(self, path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self._safe_json(payload), encoding="utf-8")

    @staticmethod
    def _safe_json(payload: object) -> str:
        def _default(value):
            try:
                import numpy as np

                if isinstance(value, (np.integer, np.floating)):
                    return value.item()
            except Exception:
                pass
            return str(value)

        return json.dumps(payload, indent=2, default=_default)
