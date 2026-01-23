"""LLM provider tests."""
import json

import httpx
import pytest

from app.llm.provider import LLMMessage, MockChatProvider, OllamaChatProvider, OpenAIChatProvider


class DummyResponse:
    def __init__(self, status_code: int, payload: dict, headers: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400 and self.status_code != 429:
            raise httpx.HTTPStatusError("error", request=None, response=None)


class DummyClient:
    def __init__(self, responses):
        self.responses = responses

    def post(self, url, json=None, headers=None):
        return self.responses.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_mock_provider_returns_json():
    provider = MockChatProvider()
    payload = json.loads(provider.generate([LLMMessage(role="user", content="test")], "mock"))
    assert "case_summary" in payload


def test_openai_provider_retries_on_rate_limit(monkeypatch):
    responses = [
        DummyResponse(429, {}, headers={"retry-after": "0.1"}),
        DummyResponse(200, {"choices": [{"message": {"content": "ok"}}]}),
    ]
    monkeypatch.setattr(httpx, "Client", lambda timeout: DummyClient(responses))
    provider = OpenAIChatProvider("key", "https://api.example.com")
    result = provider.generate([LLMMessage(role="user", content="hi")], "gpt")
    assert result == "ok"


def test_ollama_provider_returns_message(monkeypatch):
    responses = [DummyResponse(200, {"message": {"content": "done"}})]
    monkeypatch.setattr(httpx, "Client", lambda timeout: DummyClient(responses))
    provider = OllamaChatProvider("http://localhost:11434")
    result = provider.generate([LLMMessage(role="user", content="hi")], "llama")
    assert result == "done"
