# Audit and Traceability

## Copilot Runs
Each copilot invocation stores a full audit record under `artifacts/runs/<run_id>/`:
- `case_packet.json` ? input evidence packet.
- `policy_passages.json` ? retrieved policy chunks.
- `response.json` ? validated output.
- `raw_response.txt` ? raw LLM output.
- `metadata.json` ? prompt version and model.

Failed runs are stored under `artifacts/runs_failed/` with the error message.

## Feedback Audit Trail
Investigator feedback is stored under `artifacts/feedback/` as JSON files.
Each entry includes case id, response, and feedback fields.

## Why This Matters
- Supports compliance review.
- Enables root-cause analysis for model errors.
- Preserves a full trail for training and evaluation.
