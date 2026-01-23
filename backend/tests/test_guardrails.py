"""Guardrail validator tests."""
import pytest

from app.guardrails.validators import (
    GuardrailError,
    parse_patterns,
    redact_pii,
    require_evidence_fields,
    validate_evidence_refs,
)


def test_validate_evidence_refs_accepts_known_roots():
    case_packet = {"alerts": [], "industry": "retail", "party_id": "P1"}
    output = {
        "key_indicators": [{"indicator": "test", "evidence_refs": ["alerts[0].alert_id"]}],
        "benign_explanations_to_rule_out": [{"explanation": "test", "evidence_refs": ["industry"]}],
        "investigator_next_steps": [{"step": "test", "evidence_refs": ["party_id"]}],
    }
    validate_evidence_refs(output, case_packet)


def test_validate_evidence_refs_rejects_unknown_root():
    case_packet = {"alerts": []}
    output = {"key_indicators": [{"indicator": "test", "evidence_refs": ["missing.field"]}]}
    with pytest.raises(GuardrailError):
        validate_evidence_refs(output, case_packet)


def test_require_evidence_fields_detects_missing():
    with pytest.raises(GuardrailError):
        require_evidence_fields({"party_id": ""}, ["party_id", "alerts"])


def test_redact_pii_applies_patterns():
    patterns = parse_patterns(r"\b\d{3}-\d{2}-\d{4}\b")
    payload = {"ssn": "123-45-6789", "nested": ["ok", "987-65-4321"]}
    redacted = redact_pii(payload, patterns)
    assert redacted["ssn"] == "[REDACTED]"
    assert redacted["nested"][1] == "[REDACTED]"
