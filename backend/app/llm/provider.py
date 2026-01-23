"""LLM provider implementations for external inference services."""
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

import httpx


@dataclass(frozen=True)
class LLMMessage:
    role: str
    content: str


class LLMProvider(ABC):
    """Provider-agnostic LLM interface."""

    @abstractmethod
    def generate(self, messages: List[LLMMessage], model: str) -> str:
        raise NotImplementedError


class RateLimitError(RuntimeError):
    """Raised when the LLM provider rate limits requests."""


class OpenAIChatProvider(LLMProvider):
    """Minimal OpenAI-compatible chat completions provider."""

    def __init__(
        self,
        api_key: str,
        base_url: str,
        timeout_seconds: int = 60,
        max_tokens: int = 512,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.max_retries = 3
        self.timeout_seconds = timeout_seconds
        self.max_tokens = max_tokens

    def generate(self, messages: List[LLMMessage], model: str) -> str:
        url = f"{self.base_url}/chat/completions"
        payload = {
            "model": model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": 0,
            "max_tokens": self.max_tokens,
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}
        with httpx.Client(timeout=self.timeout_seconds) as client:
            for attempt in range(self.max_retries + 1):
                response = client.post(url, json=payload, headers=headers)
                if response.status_code == 429:
                    if attempt >= self.max_retries:
                        raise RateLimitError("LLM rate limit exceeded")
                    delay = self._retry_delay(response, attempt)
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                data = response.json()
                return data["choices"][0]["message"]["content"]
        raise RateLimitError("LLM rate limit exceeded")

    @staticmethod
    def _retry_delay(response: httpx.Response, attempt: int) -> float:
        retry_after = response.headers.get("retry-after")
        if retry_after:
            try:
                return max(0.5, float(retry_after))
            except ValueError:
                pass
        return min(10.0, 1.0 * (2 ** attempt))


class MockChatProvider(LLMProvider):
    """Local mock provider for testing without external API calls."""

    def generate(self, messages: List[LLMMessage], model: str) -> str:
        payload = {
            "case_summary": "Mock copilot summary based on available evidence.",
            "key_indicators": [
                {
                    "indicator": "Alert activity indicates elevated risk.",
                    "evidence_refs": ["alerts[0].alert_id"],
                    "policy_citations": ["sar_guidance:v1"],
                }
            ],
            "benign_explanations_to_rule_out": [
                {
                    "explanation": "Legitimate business activity could explain transaction volume.",
                    "evidence_refs": ["industry", "expected_monthly_volume_usd"],
                    "policy_citations": ["sar_guidance:v1"],
                }
            ],
            "policy_mapping": [
                {
                    "policy_ref": "SAR guidance",
                    "rationale": "Indicators align with suspicious activity guidance.",
                    "citations": [
                        {
                            "citation": "SAR guidance (v1)",
                            "doc_id": "sar_guidance",
                            "version": "v1",
                        }
                    ],
                }
            ],
            "missing_information": [{"item": "Confirm source of funds documentation."}],
            "recommended_disposition": "escalate",
            "confidence": 0.5,
            "uncertainty_reasons": ["Limited corroborating evidence."],
            "investigator_next_steps": [
                {
                    "step": "Request additional documentation from the party.",
                    "evidence_refs": ["party_id"],
                    "policy_citations": ["sar_guidance:v1"],
                }
            ],
            "narrative_draft": None,
        }
        return json.dumps(payload)


class OllamaChatProvider(LLMProvider):
    """Ollama native chat provider."""

    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 60,
        max_tokens: int = 512,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_tokens = max_tokens

    def generate(self, messages: List[LLMMessage], model: str) -> str:
        url = f"{self.base_url}/api/chat"
        payload = {
            "model": model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "stream": False,
            "format": "json",
            "options": {"temperature": 0, "num_predict": self.max_tokens},
        }
        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
        return data["message"]["content"]
