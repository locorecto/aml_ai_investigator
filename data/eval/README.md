# Evaluation Dataset Format

Store evaluation cases as JSON Lines in `data/eval/cases.jsonl`.

Each line is a single JSON object with the following fields:

- `case_id` (string, required): Case identifier.
- `case_packet_path` (string, required): Path to the case packet JSON used for the run.
- `copilot_response_path` (string, required): Path to the copilot response JSON to evaluate.
- `expected_disposition` (string, optional): Ground-truth disposition (e.g., "file", "no-file").
- `expected_indicators` (array of strings, optional): Expected key indicators to check for completeness.
- `usefulness_label` (number, optional): Manual usefulness score (1-5).
- `notes` (string, optional): Free-form notes.

Example line:

```
{"case_id":"C123","case_packet_path":"artifacts/runs/C123/case_packet.json","copilot_response_path":"artifacts/runs/C123/response.json","expected_disposition":"file","expected_indicators":["structuring","rapid movement"],"usefulness_label":4}
```
