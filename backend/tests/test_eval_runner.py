"""Evaluation runner tests."""
import json
from pathlib import Path

from app.eval.runner import run_eval


def test_run_eval_writes_report(tmp_path: Path):
    case_packet = {"case_id": "C1", "alerts": [], "industry": "retail", "party_id": "P1"}
    response = {
        "case_summary": "Summary text with enough detail to pass rubric.",
        "key_indicators": [
            {"indicator": "alert activity", "evidence_refs": ["alerts"], "policy_citations": ["policy:v1"]}
        ],
        "benign_explanations_to_rule_out": [
            {"explanation": "legitimate business", "evidence_refs": ["industry"], "policy_citations": ["policy:v1"]}
        ],
        "policy_mapping": [
            {
                "policy_ref": "SAR guidance",
                "rationale": "Aligned.",
                "citations": [{"citation": "SAR guidance", "doc_id": "sar", "version": "v1"}],
            }
        ],
        "missing_information": [{"item": "source of funds"}],
        "recommended_disposition": "file",
        "confidence": 0.7,
        "uncertainty_reasons": ["limited data"],
        "investigator_next_steps": [
            {"step": "request docs", "evidence_refs": ["party_id"], "policy_citations": ["policy:v1"]}
        ],
        "narrative_draft": None,
    }

    case_packet_path = tmp_path / "case_packet.json"
    response_path = tmp_path / "response.json"
    case_packet_path.write_text(json.dumps(case_packet), encoding="utf-8")
    response_path.write_text(json.dumps(response), encoding="utf-8")

    dataset_path = tmp_path / "cases.jsonl"
    dataset_path.write_text(
        json.dumps(
            {
                "case_id": "C1",
                "case_packet_path": str(case_packet_path),
                "copilot_response_path": str(response_path),
                "expected_disposition": "file",
                "expected_indicators": ["alert activity"],
                "usefulness_label": 4,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    schema_path = Path(__file__).resolve().parents[2] / "data" / "eval" / "schema.json"
    artifacts_path = tmp_path / "eval"

    report_path = run_eval(dataset_path, schema_path, artifacts_path)
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["totals"]["cases"] == 1
    assert report["metrics"]["factual_consistency_avg"] is not None
