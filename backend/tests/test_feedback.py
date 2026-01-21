import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import config
from app.main import create_app


@pytest.fixture()
def client():
    repo_root = Path(__file__).resolve().parents[2]
    data_path = repo_root / "data"
    os.environ["DATA_BASE_PATH"] = str(data_path)
    config.get_settings.cache_clear()
    with TestClient(create_app()) as test_client:
        yield test_client


def test_feedback_submission(client: TestClient):
    resp = client.get("/api/v1/cases", params={"limit": 1})
    assert resp.status_code == 200
    payload = resp.json()
    if not payload["items"]:
        pytest.skip("No cases available for feedback test")

    case_id = payload["items"][0]["case_id"]
    response = client.post(
        f"/api/v1/cases/{case_id}/feedback",
        json={
            "helpful": True,
            "wrong_parts": ["example issue"],
            "missing_data": ["expected field"],
            "decision": "review",
            "narrative": "Draft narrative text.",
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["case_id"] == case_id
    assert body["payload"]["helpful"] is True

    list_resp = client.get("/api/v1/feedback", params={"limit": 5})
    assert list_resp.status_code == 200
    list_body = list_resp.json()
    assert list_body["pagination"]["total"] >= 1
    assert any(item["feedback_id"] == body["feedback_id"] for item in list_body["items"])

    get_resp = client.get(f"/api/v1/feedback/{body['feedback_id']}")
    assert get_resp.status_code == 200
    assert get_resp.json()["feedback_id"] == body["feedback_id"]

    case_list = client.get(f"/api/v1/cases/{case_id}/feedback", params={"limit": 5})
    assert case_list.status_code == 200
    assert case_list.json()["pagination"]["total"] >= 1
