"""Evaluation runner for copilot outputs and guardrail metrics."""
import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import jsonschema
from pydantic import ValidationError

from app.core.config import Settings
from app.guardrails.validators import GuardrailError, validate_evidence_refs
from app.schemas.copilot import CopilotSummary


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_disposition(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = value.strip().lower()
    mapping = {
        "file": "file",
        "sar": "file",
        "file sar": "file",
        "no file": "no-file",
        "no-file": "no-file",
        "no_sar": "no-file",
        "no sar": "no-file",
    }
    return mapping.get(normalized, normalized)


def _extract_indicator_texts(output: Dict[str, Any]) -> List[str]:
    texts: List[str] = []
    for item in output.get("key_indicators", []):
        if isinstance(item, dict):
            indicator = item.get("indicator")
            if isinstance(indicator, str):
                texts.append(indicator)
            else:
                texts.append(json.dumps(item))
        else:
            texts.append(str(item))
    return texts


def _compute_citation_coverage(output: Dict[str, Any]) -> Dict[str, Any]:
    items = []
    for key in ("key_indicators", "benign_explanations_to_rule_out", "investigator_next_steps"):
        items.extend(output.get(key, []) or [])

    if not items:
        return {"coverage": 0.0, "with_citations": 0, "total": 0}

    with_citations = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("evidence_refs") or item.get("policy_citations"):
            with_citations += 1
    total = len(items)
    return {
        "coverage": with_citations / total,
        "with_citations": with_citations,
        "total": total,
    }


def _compute_completeness(expected: List[str], output: Dict[str, Any]) -> Dict[str, Any]:
    if not expected:
        return {"coverage": None, "matched": [], "missing": []}
    indicator_texts = [text.lower() for text in _extract_indicator_texts(output)]
    matched = []
    missing = []
    for indicator in expected:
        needle = indicator.strip().lower()
        if not needle:
            continue
        if any(needle in text for text in indicator_texts):
            matched.append(indicator)
        else:
            missing.append(indicator)
    coverage = len(matched) / len(expected) if expected else None
    return {"coverage": coverage, "matched": matched, "missing": missing}


def _compute_usefulness_proxy(output: Dict[str, Any]) -> Dict[str, Any]:
    criteria = [
        bool(output.get("case_summary")) and len(str(output.get("case_summary"))) >= 50,
        len(output.get("key_indicators", []) or []) >= 2,
        len(output.get("investigator_next_steps", []) or []) >= 1,
        len(output.get("policy_mapping", []) or []) >= 1,
        len(output.get("missing_information", []) or []) >= 1,
    ]
    score = max(sum(1 for passed in criteria if passed), 1)
    return {"rubric_score": score, "criteria": criteria}


def _evaluate_case(entry: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {"case_id": entry.get("case_id")}
    case_packet_path = Path(entry["case_packet_path"])
    response_path = Path(entry["copilot_response_path"])

    try:
        jsonschema.validate(instance=entry, schema=schema)
    except jsonschema.ValidationError as exc:
        result["error"] = f"Dataset schema validation failed: {exc.message}"
        return result

    if not case_packet_path.exists() or not response_path.exists():
        result["error"] = "Case packet or response file missing"
        return result

    case_packet = _load_json(case_packet_path)
    response = _load_json(response_path)

    try:
        CopilotSummary.model_validate(response)
    except ValidationError as exc:
        result["error"] = f"Copilot response validation failed: {exc.errors()}"
        return result

    citation_metrics = _compute_citation_coverage(response)
    result["factual_consistency"] = citation_metrics
    try:
        validate_evidence_refs(response, case_packet)
        result["factual_consistency"]["evidence_refs_valid"] = True
    except GuardrailError as exc:
        result["factual_consistency"]["evidence_refs_valid"] = False
        result["factual_consistency"]["evidence_refs_error"] = str(exc)

    expected_indicators = entry.get("expected_indicators") or []
    result["completeness"] = _compute_completeness(expected_indicators, response)

    usefulness = _compute_usefulness_proxy(response)
    usefulness_label = entry.get("usefulness_label")
    usefulness["manual_label"] = usefulness_label
    result["usefulness"] = usefulness

    expected_disposition = _normalize_disposition(entry.get("expected_disposition"))
    actual_disposition = _normalize_disposition(response.get("recommended_disposition"))
    if expected_disposition:
        result["disagreement"] = {
            "expected": expected_disposition,
            "actual": actual_disposition,
            "agree": expected_disposition == actual_disposition,
        }
    else:
        result["disagreement"] = None
    return result


def run_eval(dataset_path: Path, schema_path: Path, artifacts_path: Path) -> Path:
    schema = _load_json(schema_path)
    lines = dataset_path.read_text(encoding="utf-8").splitlines()
    entries = [json.loads(line) for line in lines if line.strip()]

    results = [_evaluate_case(entry, schema) for entry in entries]
    totals = len(results)
    coverage_values = [
        item["factual_consistency"]["coverage"]
        for item in results
        if item.get("factual_consistency")
    ]
    completeness_values = [
        item["completeness"]["coverage"]
        for item in results
        if item.get("completeness", {}).get("coverage") is not None
    ]
    rubric_scores = [
        item["usefulness"]["rubric_score"]
        for item in results
        if item.get("usefulness")
    ]
    agreement_values = [
        item["disagreement"]["agree"]
        for item in results
        if item.get("disagreement") is not None
    ]

    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset_path": str(dataset_path),
        "totals": {
            "cases": totals,
            "cases_with_errors": sum(1 for item in results if item.get("error")),
        },
        "metrics": {
            "factual_consistency_avg": sum(coverage_values) / len(coverage_values) if coverage_values else None,
            "completeness_avg": sum(completeness_values) / len(completeness_values) if completeness_values else None,
            "usefulness_rubric_avg": sum(rubric_scores) / len(rubric_scores) if rubric_scores else None,
            "disagreement_rate": (
                sum(1 for agree in agreement_values if not agree) / len(agreement_values)
                if agreement_values
                else None
            ),
        },
        "cases": results,
    }

    artifacts_path.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    report_path = artifacts_path / f"report_{stamp}.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    (artifacts_path / "latest.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run evaluation metrics on copilot outputs.")
    parser.add_argument("--dataset", default="data/eval/cases.jsonl")
    parser.add_argument("--schema", default="data/eval/schema.json")
    parser.add_argument("--artifacts", default="artifacts/eval")
    args = parser.parse_args()

    settings = Settings()
    dataset_path = Path(args.dataset)
    schema_path = Path(args.schema)
    artifacts_path = Path(args.artifacts)

    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema not found: {schema_path}")

    report_path = run_eval(dataset_path, schema_path, artifacts_path)
    print(f"Wrote eval report to {report_path}")


if __name__ == "__main__":
    main()
