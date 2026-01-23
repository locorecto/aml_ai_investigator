"""Copilot service unit tests."""
import json
from pathlib import Path

import pytest

from app.core.config import Settings
from app.llm.provider import LLMMessage, LLMProvider
from app.services.copilot_service import CopilotService


class SequenceProvider(LLMProvider):
    def __init__(self, responses):
        self.responses = responses

    def generate(self, messages: list[LLMMessage], model: str) -> str:
        return self.responses.pop(0)


def _valid_response() -> dict:
    return {
        "case_summary": "Summary text with enough detail to pass rubric.",
        "key_indicators": [
            {"indicator": "alert activity", "evidence_refs": ["alerts"], "policy_citations": ["policy:v1"]}
        ],
        "benign_explanations_to_rule_out": [
            {"explanation": "legitimate business", "evidence_refs": ["industry"], "policy_citations": ["policy:v1"]}
        ],
        "policy_mapping": [
            {
                "policy_ref": "SAR guidance",
                "rationale": "Aligned.",
                "citations": [{"citation": "SAR guidance", "doc_id": "sar", "version": "v1"}],
            }
        ],
        "missing_information": [{"item": "source of funds"}],
        "recommended_disposition": "file",
        "confidence": 0.7,
        "uncertainty_reasons": ["limited data"],
        "investigator_next_steps": [
            {"step": "request docs", "evidence_refs": ["party_id"], "policy_citations": ["policy:v1"]}
        ],
        "narrative_draft": None,
    }


def test_copilot_service_generates_summary(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("app.services.copilot_service.retrieve_policy", lambda *args, **kwargs: [])
    provider = SequenceProvider([json.dumps(_valid_response())])
    monkeypatch.setenv("DATA_BASE_PATH", str(tmp_path))
    monkeypatch.setenv("ARTIFACTS_PATH", str(tmp_path))
    settings = Settings()
    service = CopilotService(settings, provider)
    case_packet = {"case_id": "C1", "alerts": [], "industry": "retail", "party_id": "P1"}
    output = service.generate_summary(case_packet)
    assert output["recommended_disposition"] == "file"


def test_copilot_service_repairs_invalid_json(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("app.services.copilot_service.retrieve_policy", lambda *args, **kwargs: [])
    bad_json = "not json"
    good_json = json.dumps(_valid_response())
    provider = SequenceProvider([bad_json, good_json])
    monkeypatch.setenv("DATA_BASE_PATH", str(tmp_path))
    monkeypatch.setenv("ARTIFACTS_PATH", str(tmp_path))
    settings = Settings()
    service = CopilotService(settings, provider)
    case_packet = {"case_id": "C2", "alerts": [], "industry": "retail", "party_id": "P2"}
    output = service.generate_summary(case_packet)
    assert output["case_summary"]
