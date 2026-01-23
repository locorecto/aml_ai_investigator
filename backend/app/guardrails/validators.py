"""Guardrail validators for evidence and PII policies."""
import re
from typing import Dict, Iterable, List


class GuardrailError(ValueError):
    """Raised when a guardrail validation fails."""


def parse_patterns(patterns: str) -> List[re.Pattern]:
    compiled: List[re.Pattern] = []
    for raw in patterns.split(";"):
        raw = raw.strip()
        if not raw:
            continue
        compiled.append(re.compile(raw, re.IGNORECASE))
    return compiled


def redact_pii(value: object, patterns: Iterable[re.Pattern]) -> object:
    if isinstance(value, str):
        redacted = value
        for pattern in patterns:
            redacted = pattern.sub("[REDACTED]", redacted)
        return redacted
    if isinstance(value, list):
        return [redact_pii(item, patterns) for item in value]
    if isinstance(value, dict):
        return {key: redact_pii(val, patterns) for key, val in value.items()}
    return value


def require_evidence_fields(case_packet: Dict, required_fields: Iterable[str]) -> None:
    missing = []
    for field in required_fields:
        field = field.strip()
        if not field:
            continue
        value = case_packet.get(field)
        if value in (None, "", [], {}):
            missing.append(field)
    if missing:
        raise GuardrailError(f"Missing required evidence fields: {', '.join(missing)}")


def validate_evidence_refs(output: Dict, case_packet: Dict) -> None:
    valid_roots = set(case_packet.keys())
    for collection in (
        output.get("key_indicators", []),
        output.get("benign_explanations_to_rule_out", []),
        output.get("investigator_next_steps", []),
    ):
        for item in collection:
            for ref in item.get("evidence_refs", []) or []:
                if not isinstance(ref, str):
                    raise GuardrailError("Evidence reference must be a string")
                root = ref.split(".")[0].split("[")[0]
                if root and root not in valid_roots:
                    raise GuardrailError(f"Evidence reference not found in case packet: {ref}")
