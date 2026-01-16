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


def test_health(client: TestClient):
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_cases_endpoints(client: TestClient):
    resp = client.get("/api/v1/cases", params={"limit": 1})
    assert resp.status_code == 200
    payload = resp.json()
    assert "items" in payload
    assert "pagination" in payload
    if not payload["items"]:
        return
    case_id = payload["items"][0]["case_id"]

    detail = client.get(f"/api/v1/cases/{case_id}")
    assert detail.status_code == 200
    assert detail.json()["case_id"] == case_id

    timeline = client.get(f"/api/v1/cases/{case_id}/timeline")
    assert timeline.status_code == 200
    assert isinstance(timeline.json(), list)
