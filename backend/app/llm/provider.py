import json
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


class OpenAIChatProvider(LLMProvider):
    """Minimal OpenAI-compatible chat completions provider."""

    def __init__(self, api_key: str, base_url: str) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def generate(self, messages: List[LLMMessage], model: str) -> str:
        url = f"{self.base_url}/chat/completions"
        payload = {
            "model": model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": 0,
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}
        with httpx.Client(timeout=60.0) as client:
            response = client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
        return data["choices"][0]["message"]["content"]


class MockChatProvider(LLMProvider):
    """Local mock provider for testing without external API calls."""

    def generate(self, messages: List[LLMMessage], model: str) -> str:
        payload = {
            "case_summary": "Mock copilot summary based on available evidence.",
            "key_indicators": [
                {
                    "text": "Alert activity indicates elevated risk.",
                    "evidence_refs": ["alerts[0].alert_id"],
                }
            ],
            "benign_explanations_to_rule_out": [
                {
                    "text": "Legitimate business activity could explain transaction volume.",
                    "evidence_refs": ["industry", "expected_monthly_volume_usd"],
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
            "missing_information": [{"text": "Confirm source of funds documentation."}],
            "recommended_disposition": "escalate",
            "confidence": 0.5,
            "uncertainty_reasons": ["Limited corroborating evidence."],
            "investigator_next_steps": [
                {
                    "text": "Request additional documentation from the party.",
                    "evidence_refs": ["party_id"],
                }
            ],
            "narrative_draft": None,
        }
        return json.dumps(payload)
