import json
import logging
import time
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List

import jsonschema
from pydantic import ValidationError

from app.core.config import Settings
from app.llm.provider import LLMMessage, LLMProvider
from app.rag.embeddings import HashEmbeddingProvider
from app.rag.retriever import retrieve_policy
from app.schemas.copilot import CopilotSummary

logger = logging.getLogger(__name__)

PROMPT_VERSION = "v1"


class CopilotValidationError(ValueError):
    """Raised when LLM output fails validation."""


class CopilotService:
    def __init__(
        self,
        settings: Settings,
        llm_provider: LLMProvider,
    ) -> None:
        self.settings = settings
        self.llm_provider = llm_provider
        self.embedding_provider = HashEmbeddingProvider()

    def generate_summary(self, case_packet: Dict) -> Dict:
        policy_passages = retrieve_policy(
            case_packet,
            self.settings.artifacts_path / "rag_index",
            self.embedding_provider,
            top_k=5,
        )

        messages = self._build_messages(case_packet, policy_passages)
        raw = self.llm_provider.generate(messages, self.settings.llm_model)

        output = self._parse_json(raw)
        self._validate_output(output)
        self._store_run(case_packet, policy_passages, output, raw)
        return output

    def _build_messages(self, case_packet: Dict, policy_passages: List[Dict]) -> List[LLMMessage]:
        system_prompt = (
            "You are an AML investigator copilot. Output MUST be valid JSON matching the schema. "
            "Be evidence-grounded, separate fact vs inference, and cite evidence or policy in each "
            "indicator, benign explanation, policy mapping, and next step. If required fields are missing, "
            "return an empty JSON object."
        )
        user_prompt = {
            "case_packet": case_packet,
            "policy_passages": policy_passages,
            "schema": CopilotSummary.model_json_schema(),
        }
        return [
            LLMMessage(role="system", content=system_prompt),
            LLMMessage(role="user", content=json.dumps(user_prompt)),
        ]

    def _parse_json(self, raw: str) -> Dict:
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CopilotValidationError("LLM output is not valid JSON") from exc

    def _validate_output(self, output: Dict) -> None:
        if not output:
            raise CopilotValidationError("LLM output missing required fields")

        schema = CopilotSummary.model_json_schema()
        try:
            jsonschema.validate(instance=output, schema=schema)
        except jsonschema.ValidationError as exc:
            raise CopilotValidationError("LLM output failed JSON schema validation") from exc

        try:
            CopilotSummary.model_validate(output)
        except ValidationError as exc:
            raise CopilotValidationError("LLM output failed model validation") from exc

        self._validate_citations(output)

    def _validate_citations(self, output: Dict) -> None:
        for item in output.get("key_indicators", []):
            if not item.get("evidence_refs") and not item.get("policy_citations"):
                raise CopilotValidationError("Key indicator missing citations")

        for item in output.get("benign_explanations_to_rule_out", []):
            if not item.get("evidence_refs") and not item.get("policy_citations"):
                raise CopilotValidationError("Benign explanation missing citations")

        for item in output.get("investigator_next_steps", []):
            if not item.get("evidence_refs") and not item.get("policy_citations"):
                raise CopilotValidationError("Next step missing citations")

        for mapping in output.get("policy_mapping", []):
            citations = mapping.get("citations") or []
            if not citations:
                raise CopilotValidationError("Policy mapping missing citations")

    def _store_run(
        self,
        case_packet: Dict,
        policy_passages: List[Dict],
        output: Dict,
        raw: str,
    ) -> None:
        run_id = f"{case_packet.get('case_id', 'case')}_{int(time.time())}"
        run_path = self.settings.artifacts_path / "runs" / run_id
        run_path.mkdir(parents=True, exist_ok=True)

        (run_path / "case_packet.json").write_text(json.dumps(case_packet, indent=2), encoding="utf-8")
        (run_path / "policy_passages.json").write_text(json.dumps(policy_passages, indent=2), encoding="utf-8")
        (run_path / "response.json").write_text(json.dumps(output, indent=2), encoding="utf-8")
        (run_path / "raw_response.txt").write_text(raw, encoding="utf-8")
        (run_path / "metadata.json").write_text(
            json.dumps(
                {
                    "prompt_version": PROMPT_VERSION,
                    "model": self.settings.llm_model,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
