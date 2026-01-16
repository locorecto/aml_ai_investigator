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
