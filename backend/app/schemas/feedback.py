"""Pydantic schemas for investigator feedback."""
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

from app.schemas.case import Pagination

class FeedbackSubmission(BaseModel):
    helpful: bool = Field(..., description="Whether the copilot output was helpful.")
    wrong_parts: Optional[List[str]] = Field(
        default=None, description="Specific parts of the output that were wrong."
    )
    missing_data: Optional[List[str]] = Field(
        default=None, description="Data the investigator expected but did not see."
    )
    decision: Optional[str] = Field(
        default=None, description="Investigator decision after review."
    )
    narrative: Optional[str] = Field(
        default=None, description="Edited narrative draft if provided."
    )
    copilot_run_id: Optional[str] = Field(
        default=None, description="Optional reference to the copilot run id."
    )


class FeedbackMeta(BaseModel):
    client_host: Optional[str] = None
    user_agent: Optional[str] = None


class FeedbackRecord(BaseModel):
    feedback_id: str
    case_id: str
    submitted_at: datetime
    payload: FeedbackSubmission
    meta: FeedbackMeta


class FeedbackList(BaseModel):
    items: List[FeedbackRecord]
    pagination: Pagination
