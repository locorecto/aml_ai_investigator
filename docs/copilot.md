# Copilot

## Purpose
The copilot generates a structured summary for a case packet, grounded in evidence and policy guidance.

## Output Schema
The JSON output includes:
- `case_summary`
- `key_indicators[]`
- `benign_explanations_to_rule_out[]`
- `policy_mapping[]` (with citations)
- `missing_information[]`
- `recommended_disposition`
- `confidence`
- `uncertainty_reasons[]`
- `investigator_next_steps[]`
- `narrative_draft` (optional)

Schema is defined in `backend/app/schemas/copilot.py`.

## Validation
The service enforces:
- JSON schema validation
- citation requirements
- evidence reference validation
- refusal if required evidence is missing

## Storage
Outputs and inputs are stored under `artifacts/runs/` for auditability.
