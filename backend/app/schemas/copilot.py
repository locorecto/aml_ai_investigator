from typing import List, Optional

from pydantic import BaseModel, Field


class EvidencePointer(BaseModel):
    ref: str = Field(..., description="Pointer into case_packet data")


class PolicyCitation(BaseModel):
    citation: str = Field(..., description="Short citation to policy passage")
    chunk_id: Optional[str] = None
    doc_id: Optional[str] = None
    version: Optional[str] = None


class PolicyMapping(BaseModel):
    policy_ref: str
    rationale: str
    citations: List[PolicyCitation]


class CopilotSummary(BaseModel):
    case_summary: str
    key_indicators: List[dict]
    benign_explanations_to_rule_out: List[dict]
    policy_mapping: List[PolicyMapping]
    missing_information: List[dict]
    recommended_disposition: str
    confidence: float
    uncertainty_reasons: List[str]
    investigator_next_steps: List[dict]
    narrative_draft: Optional[str] = None
