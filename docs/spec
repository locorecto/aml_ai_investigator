# AML AI Investigator Specification

## Scope and Constraints
- Do not invent logic beyond this specification.
- Generate code in the current repository directory.
- Provide a production-ready repository with deployment scripts.
- The system must be deployable in the Docker instance on this machine.

## Goal
- Provide investigators with a go/no-go decision and an evidence package for SAR decisions.
- Use an LLM API (GPT-5 style) to analyze evidence and support SAR decisions for parties/businesses.
- Provide a FastAPI service to support the LLM copilot.

## Data Requirements
- Use data already created in `data/`, including party-level case packets and `tx_timeline_daily` when relevant.
- Provide schemas for all relevant datasets.
- Store PySpark schemas under `data/spark-schemas` and JSON schemas under `data/schemas`.
- Derive `case_packet`, `case_packet_json`, and `tx_timeline_daily` via the daily Spark job.

## Phase 1 - Evidence API (FastAPI)
- Endpoints:
  - `GET /health`
  - `GET /cases` (triage list, paginated)
  - `GET /cases/{id}` (full packet)
  - `GET /cases/{id}/timeline`
- Production-ready: structured logging, config, error handling, pagination.
- Minimal tests for endpoints.
- Docker support for API.

## Phase 2 - Policy/Playbook RAG
- Policy corpus folder: `./policy_corpus` (markdown/text input).
- Chunking with stable chunk IDs.
- Embedding generation via provider-agnostic interface.
- Vector store (local, Docker-friendly) with `doc_id`/`version`/`section` metadata.
- Retrieval returns top passages, doc IDs/versions, and short citations.
- Index artifacts stored under `./artifacts/rag_index/`.
- Script: `scripts/index_policies.py` (or `.sh`).
- Deliverable: `retrieve_policy(case_packet) -> passages[]`.

## Phase 3 - LLM Copilot MVP (Structured Output)
- Strict JSON output schema:
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
- Prompting constraints:
  - Evidence grounded
  - Separate fact vs inference
  - Cite policy passages and evidence pointers
- Validators:
  - JSON schema validation
  - Citation requirements (must cite evidence or policy)
  - Refuse if required fields are missing
- Endpoint: `POST /cases/{id}/copilot-summary`
  - Load case packet
  - Retrieve policy passages
  - Call LLM API via provider abstraction
  - Store inputs, retrieval results, response, versions under `./artifacts/runs/`
- Docker env configuration for LLM API:
  - `LLM_API_KEY`, `LLM_BASE_URL`, `LLM_MODEL`, `ARTIFACTS_PATH`
- Tests must use mocks (no real API call).

## Phase 4 - Investigator UX + Human-in-the-Loop
- Evidence packet viewer
- Copilot panel
- Accept/edit decision and narrative
- Capture explicit feedback:
  - helpful/not helpful
  - which parts were wrong
  - missing data suggestions
- Deliverable: closed-loop workflow + audit trail

## Phase 5 - Evaluation + Controls (Out of Scope)
- Create labeled evaluation set:
  - historical cases with final dispositions
  - narratives and QA outcomes
- Measure:
  - factual consistency (cite real evidence)
  - completeness (did it miss key indicators)
  - usefulness (investigator time saved)
  - disagreement analysis (file vs no-file)
- Guardrails:
  - must-cite-evidence validator
  - PII redaction
  - hallucination checks (refuse if required fields missing)

## Deployment and Operations
- Docker Compose must support API service and scheduled data job.
- Scheduled job should regenerate `case_packet`, `case_packet_json`, and `tx_timeline_daily` daily.
- LLM configuration must be supplied via environment variables.
- Persist RAG and copilot artifacts under `artifacts/`.
