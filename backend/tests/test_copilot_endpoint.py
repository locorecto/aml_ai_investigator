import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import config
from app.main import create_app


class DummyCopilotService:
    def generate_summary(self, case_packet):
        return {
            "case_summary": "Summary",
            "key_indicators": [{"text": "indicator", "evidence_refs": ["alerts[0].alert_id"]}],
            "benign_explanations_to_rule_out": [{"text": "benign", "evidence_refs": ["party_id"]}],
            "policy_mapping": [
                {
                    "policy_ref": "SAR guidance",
                    "rationale": "Mapping",
                    "citations": [{"citation": "doc section (v1)"}],
                }
            ],
            "missing_information": [{"text": "missing"}],
            "recommended_disposition": "file",
            "confidence": 0.5,
            "uncertainty_reasons": ["limited data"],
            "investigator_next_steps": [{"text": "next", "evidence_refs": ["case_packet"]}],
            "narrative_draft": None,
        }


@pytest.fixture()
def client():
    repo_root = Path(__file__).resolve().parents[2]
    os.environ["DATA_BASE_PATH"] = str(repo_root / "data")
    config.get_settings.cache_clear()
    with TestClient(create_app()) as test_client:
        test_client.app.state.copilot_service = DummyCopilotService()
        yield test_client


def test_copilot_summary_endpoint(client: TestClient):
    cases = client.get("/api/v1/cases", params={"limit": 1})
    assert cases.status_code == 200
    case_id = cases.json()["items"][0]["case_id"]

    resp = client.post(f"/api/v1/cases/{case_id}/copilot-summary")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["case_summary"] == "Summary"
