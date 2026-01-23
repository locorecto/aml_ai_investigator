"""Service layer for copilot generation and storage."""
import json
import logging
import time
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List

import jsonschema
from pydantic import ValidationError

from app.core.config import Settings
from app.guardrails.validators import (
    GuardrailError,
    parse_patterns,
    redact_pii,
    require_evidence_fields,
    validate_evidence_refs,
)
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
        self._pii_patterns = parse_patterns(settings.pii_patterns)

    def generate_summary(self, case_packet: Dict) -> Dict:
        required_fields = [field for field in self.settings.required_evidence_fields.split(",") if field.strip()]
        if required_fields:
            require_evidence_fields(case_packet, required_fields)

        policy_passages = retrieve_policy(
            case_packet,
            self.settings.artifacts_path / "rag_index",
            self.embedding_provider,
            top_k=5,
        )

        messages = self._build_messages(case_packet, policy_passages)
        raw = self.llm_provider.generate(messages, self.settings.llm_model)
        try:
            output = self._parse_json(raw)
            self._validate_output(output)
        except CopilotValidationError as exc:
            corrected_raw = self.llm_provider.generate(
                self._build_correction_messages(raw),
                self.settings.llm_model,
            )
            try:
                output = self._parse_json(corrected_raw)
                self._validate_output(output)
                raw = corrected_raw
            except CopilotValidationError as retry_exc:
                self._store_failed_run(case_packet, policy_passages, corrected_raw, str(retry_exc))
                raise
        validate_evidence_refs(output, case_packet)
        if self.settings.pii_redaction_enabled:
            output = redact_pii(output, self._pii_patterns)
        self._store_run(case_packet, policy_passages, output, raw)
        return output

    def _build_messages(self, case_packet: Dict, policy_passages: List[Dict]) -> List[LLMMessage]:
        system_prompt = (
            "You are an AML investigator copilot. Output MUST be valid JSON matching the schema and "
            "contain no extra text or code fences. Be evidence-grounded, separate fact vs inference, "
            "and cite evidence or policy in each indicator, benign explanation, policy mapping, and next step. "
            "If required fields are missing, return an empty JSON object."
        )
        truncated_packet = self._summarize_case_packet(case_packet)
        truncated_passages = self._summarize_policy_passages(policy_passages)
        schema_hint = {
            "case_summary": "string",
            "key_indicators": [
                {"indicator": "string", "evidence_refs": ["string"], "policy_citations": ["string"]}
            ],
            "benign_explanations_to_rule_out": [
                {"explanation": "string", "evidence_refs": ["string"], "policy_citations": ["string"]}
            ],
            "policy_mapping": [
                {
                    "policy_ref": "string",
                    "rationale": "string",
                    "citations": [{"citation": "string", "doc_id": "string", "version": "string"}],
                }
            ],
            "missing_information": ["string"],
            "recommended_disposition": "string",
            "confidence": "number",
            "uncertainty_reasons": ["string"],
            "investigator_next_steps": [
                {"step": "string", "evidence_refs": ["string"], "policy_citations": ["string"]}
            ],
            "narrative_draft": "string or null",
        }
        user_prompt = (
            "Return ONLY JSON matching this schema (do not repeat the inputs):\n"
            f"{self._safe_json(schema_hint)}\n\n"
            "Case packet JSON:\n"
            f"{self._safe_json(truncated_packet)}\n\n"
            "Policy passages JSON:\n"
            f"{self._safe_json(truncated_passages)}"
        )
        return [
            LLMMessage(role="system", content=system_prompt),
            LLMMessage(role="user", content=user_prompt),
        ]

    def _parse_json(self, raw: str) -> Dict:
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            start = raw.find("{")
            end = raw.rfind("}")
            if start != -1 and end != -1 and end > start:
                snippet = raw[start:end + 1]
                try:
                    return json.loads(snippet)
                except json.JSONDecodeError:
                    pass
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

        (run_path / "case_packet.json").write_text(self._safe_json(case_packet), encoding="utf-8")
        (run_path / "policy_passages.json").write_text(self._safe_json(policy_passages), encoding="utf-8")
        (run_path / "response.json").write_text(self._safe_json(output), encoding="utf-8")
        (run_path / "raw_response.txt").write_text(raw, encoding="utf-8")
        (run_path / "metadata.json").write_text(
            self._safe_json(
                {
                    "prompt_version": PROMPT_VERSION,
                    "model": self.settings.llm_model,
                },
            ),
            encoding="utf-8",
        )

    def _store_failed_run(
        self,
        case_packet: Dict,
        policy_passages: List[Dict],
        raw: str,
        error: str,
    ) -> None:
        run_id = f"{case_packet.get('case_id', 'case')}_{int(time.time())}_failed"
        run_path = self.settings.artifacts_path / "runs_failed" / run_id
        run_path.mkdir(parents=True, exist_ok=True)

        (run_path / "case_packet.json").write_text(self._safe_json(case_packet), encoding="utf-8")
        (run_path / "policy_passages.json").write_text(self._safe_json(policy_passages), encoding="utf-8")
        (run_path / "raw_response.txt").write_text(raw, encoding="utf-8")
        (run_path / "error.txt").write_text(error, encoding="utf-8")

    @staticmethod
    def _build_correction_messages(raw: str) -> List[LLMMessage]:
        system_prompt = (
            "Return ONLY valid JSON matching the required schema. "
            "Add missing evidence_refs or policy_citations where required. "
            "Do not include any extra text."
        )
        user_prompt = (
            "Fix the following JSON to satisfy the schema and citation requirements. "
            "Do not remove fields; only correct and add missing citations.\n\n"
            f"{raw}"
        )
        return [
            LLMMessage(role="system", content=system_prompt),
            LLMMessage(role="user", content=user_prompt),
        ]

    @staticmethod
    def _safe_json(payload: object) -> str:
        def _default(value):
            try:
                import numpy as np

                if isinstance(value, (np.integer, np.floating)):
                    return value.item()
            except Exception:
                pass
            return str(value)

        return json.dumps(payload, indent=2, default=_default)

    @staticmethod
    def _truncate_lists(value: object, limit: int) -> object:
        if isinstance(value, list):
            if len(value) > limit:
                return value[:limit]
            return [CopilotService._truncate_lists(item, limit) for item in value]
        if isinstance(value, dict):
            return {key: CopilotService._truncate_lists(val, limit) for key, val in value.items()}
        return value

    @staticmethod
    def _summarize_case_packet(case_packet: Dict) -> Dict:
        keys = [
            "case_id",
            "party_id",
            "party_type",
            "party_name",
            "industry",
            "country",
            "state",
            "risk_rating",
            "expected_monthly_volume_usd",
            "expected_avg_txn_usd",
            "alerts_count",
            "alerts_high",
            "alerts_medium",
            "alerts_low",
            "max_risk_score",
            "median_risk_score",
            "model_types",
            "scenario_codes",
            "severities",
            "txn_count_case",
            "amount_total_usd_case",
            "median_amount_usd_case",
            "max_amount_usd_case",
            "intl_ratio_case",
        ]
        summary = {key: case_packet.get(key) for key in keys if key in case_packet}
        summary["alerts"] = CopilotService._truncate_lists(
            CopilotService._as_list(case_packet.get("alerts")), limit=5
        )
        summary["top_counterparties"] = CopilotService._truncate_lists(
            CopilotService._as_list(case_packet.get("top_counterparties")), limit=5
        )
        summary["top_merchants"] = CopilotService._truncate_lists(
            CopilotService._as_list(case_packet.get("top_merchants")), limit=5
        )
        summary["supporting_transactions"] = CopilotService._truncate_lists(
            CopilotService._as_list(case_packet.get("supporting_transactions")), limit=5
        )
        return summary

    @staticmethod
    def _summarize_policy_passages(passages: List[Dict]) -> List[Dict]:
        summarized = []
        for item in passages[:5]:
            passage_text = item.get("passage") or ""
            summarized.append(
                {
                    "chunk_id": item.get("chunk_id"),
                    "doc_id": item.get("doc_id"),
                    "version": item.get("version"),
                    "section": item.get("section"),
                    "citation": item.get("citation"),
                    "passage": passage_text[:500],
                }
            )
        return summarized

    @staticmethod
    def _as_list(value: object) -> List:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if hasattr(value, "tolist"):
            return value.tolist()
        return [value]
