from datetime import datetime, timezone
import json
from pathlib import Path
from uuid import uuid4

from app.schemas.feedback import FeedbackMeta, FeedbackRecord, FeedbackSubmission
from app.storage.audit_store import AuditStore


class FeedbackService:
    def __init__(self, artifacts_path: Path) -> None:
        self.base_path = artifacts_path / "feedback"
        self.audit_store = AuditStore(self.base_path)

    def save_feedback(
        self,
        case_id: str,
        payload: FeedbackSubmission,
        meta: FeedbackMeta,
    ) -> FeedbackRecord:
        feedback_id = uuid4().hex
        submitted_at = datetime.now(timezone.utc)
        record = FeedbackRecord(
            feedback_id=feedback_id,
            case_id=case_id,
            submitted_at=submitted_at,
            payload=payload,
            meta=meta,
        )
        timestamp = submitted_at.strftime("%Y%m%dT%H%M%SZ")
        path = self.base_path / case_id / f"{timestamp}_{feedback_id}.json"
        self.audit_store.write_json(path, record.model_dump())
        return record

    def list_feedback(self, limit: int, offset: int, case_id: str | None = None) -> list[FeedbackRecord]:
        base_path = self.base_path / case_id if case_id else self.base_path
        if not base_path.exists():
            return []
        records: list[FeedbackRecord] = []
        for file_path in sorted(base_path.rglob("*.json"), reverse=True):
            try:
                raw = json.loads(file_path.read_text(encoding="utf-8"))
                records.append(FeedbackRecord.model_validate(raw))
            except Exception:
                continue
        return records[offset: offset + limit]

    def get_feedback(self, feedback_id: str) -> FeedbackRecord | None:
        if not self.base_path.exists():
            return None
        for file_path in self.base_path.rglob("*.json"):
            if feedback_id in file_path.name:
                raw = json.loads(file_path.read_text(encoding="utf-8"))
                return FeedbackRecord.model_validate(raw)
        return None

    def count_feedback(self, case_id: str | None = None) -> int:
        base_path = self.base_path / case_id if case_id else self.base_path
        if not base_path.exists():
            return 0
        return sum(1 for _ in base_path.rglob("*.json"))
