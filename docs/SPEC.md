# AML AI Investigator  Project Specification

This document is the authoritative specification to recreate the repository from scratch.
It reflects the current state of the project and all accepted prompts to date.

## 1) Goal
Provide an AML investigator platform that:
- Produces case evidence packets from existing data.
- Serves evidence packages through a FastAPI service.
- Retrieves policy guidance through local RAG.
- Generates structured copilot summaries via an LLM provider.
- Provides a local UI with human-in-the-loop feedback.
- Includes evaluation/controls tooling for output quality and guardrails.

## 2) Scope (Implemented)
### Phase 1  Evidence API
- Load existing datasets in `data/` and expose:
  - `GET /health`
  - `GET /health/ready`
  - `GET /cases` (paginated triage list)
  - `GET /cases/{id}` (full case packet)
  - `GET /cases/{id}/timeline`
- Structured logging (JSON) and request logging middleware.
- Configurable CORS.
- Use parquet outputs from the data pipeline.

### Phase 2  Policy/Playbook RAG
- Local policy corpus in `policy_corpus/`.
- Chunking with stable chunk IDs and metadata (doc_id, version, section).
- Embedding provider abstraction.
- Local vector store (SQLite) under `artifacts/rag_index/`.
- Retrieval function: `retrieve_policy(case_packet) -> passages[]`.
- Script: `scripts/index_policies.py`.

### Phase 3  Copilot MVP (Structured Output)
- Strict JSON output schema:
  - case_summary
  - key_indicators[]
  - benign_explanations_to_rule_out[]
  - policy_mapping[] (with citations)
  - missing_information[]
  - recommended_disposition
  - confidence
  - uncertainty_reasons[]
  - investigator_next_steps[]
  - narrative_draft (optional)
- Evidence-grounded prompting; separate fact vs inference; cite evidence/policy.
- Validation:
  - JSON schema validation
  - citation requirements
  - refusal if required evidence fields are missing
- API endpoint:
  - `POST /cases/{id}/copilot-summary`
- Store run artifacts under `artifacts/runs/` (inputs, retrievals, outputs, raw).
- Provider abstraction for LLM (OpenAI-compatible + Ollama + mock).

### Phase 4  Investigator UX + Feedback
- UI (Docker-friendly, locally served) with:
  - case list (triage)
  - evidence packet viewer
  - copilot panel (calls POST endpoint)
  - accept/edit decision & narrative
  - feedback capture: helpful/not helpful, what was wrong, missing data
- Feedback audit trail stored under `artifacts/feedback/`.
- API support to list and review feedback entries.

### Phase 5  Evaluation + Controls
- Evaluation dataset format under `data/eval/` with JSONL schema.
- Metrics:
  - factual consistency (citation coverage + evidence reference validity)
  - completeness (expected indicator coverage)
  - usefulness proxy (rubric + optional manual labels)
  - disagreement analysis (expected vs recommended disposition)
- Guardrails:
  - evidence citation validator
  - PII redaction hooks (configurable)
  - refusal behavior when required evidence is missing
- Eval runner script: `python -m app.eval.runner` and `scripts/run_eval.sh`.
- Reports written to `artifacts/eval/`.

## 3) Repository Layout
- `backend/`  FastAPI service, RAG pipeline, copilot service, guardrails, eval runner, tests.
- `frontend/`  React UI served via Nginx in Docker.
- `data/`  source CSVs + parquet outputs + schema definitions.
- `policy_corpus/`  policy guidance documents.
- `artifacts/`  outputs (rag_index, runs, feedback, eval reports).
- `infra/`  docker compose and Dockerfiles.
- `scripts/`  indexing and evaluation utilities.

## 4) Data Pipeline
- Spark pipeline builds:
  - `data/case_packet` (parquet)
  - `data/case_packet_json` (parquet)
  - `data/tx_timeline_daily` (parquet)
- Source CSVs include:
  - `transactions.csv`, `parties.csv`, `counterparties.csv`, `merchants.csv`
  - `alerts_*.csv` (cash, wires, credit_cards, loans, ngi)
- PySpark schemas live in `data/spark-schemas/`.
- JSON schemas live in `data/schemas/`.

## 5) FastAPI Service
- Base path: `/api/v1`.
- Health endpoints:
  - `GET /health` (returns dependency checks)
  - `GET /health/ready` (returns 503 if required deps missing)
- Evidence endpoints:
  - `GET /cases` (limit/offset pagination)
  - `GET /cases/{id}`
  - `GET /cases/{id}/timeline`
- Copilot endpoint:
  - `POST /cases/{id}/copilot-summary`
- Feedback endpoints:
  - `POST /cases/{id}/feedback`
  - `GET /feedback`

## 6) Caching (Redis)
- Redis cache for low-latency endpoints (cases list/detail/timeline).
- Configurable via env:
  - `CACHE_ENABLED`
  - `REDIS_URL`
  - `CACHE_TTL_SECONDS`
  - `CACHE_PREFIX`
- Docker Compose includes `redis` service and wires API to it.

## 7) LLM Providers
- Provider abstraction in backend:
  - OpenAI-compatible chat provider
  - Ollama provider (`/api/chat` with JSON format)
  - Mock provider for offline testing
- Configurable via env:
  - `LLM_API_KEY`, `LLM_BASE_URL`, `LLM_MODEL`, `LLM_PROVIDER`
  - `LLM_TIMEOUT_SECONDS`, `LLM_MAX_TOKENS`

## 8) UI
- React app served by Nginx via Docker.
- Features:
  - triage list
  - evidence viewer
  - copilot summary panel
  - feedback submission
- Uses `VITE_API_BASE_URL` for API calls.

## 9) Observability
- Structured JSON logging to stdout.
- Request logging middleware (method, path, status, duration).

## 10) Environment and Configuration
- Primary env file: `C:\Users\<user>\.aml_ai_investigator.env`.
- `env.sample` documents expected variables.
- Key variables:
  - `DATA_BASE_PATH`, `ARTIFACTS_PATH`
  - `LLM_*`
  - `CACHE_*`
  - `CORS_ALLOW_ORIGINS`
  - `ROOT_PATH`, `PROXY_HEADERS_ENABLED`

## 11) Docker
- `infra/compose/docker-compose.yml` starts:
  - `api` (FastAPI)
  - `scheduler` (Spark job via cron)
  - `ui` (Nginx + built frontend)
  - `redis`
- API Dockerfile supports proxy headers and worker count via `UVICORN_WORKERS`.

## 12) Testing
- Tests live in `backend/tests/`.
- Coverage target: >= 80%.
- Default test run skips Spark integration tests.
- Spark tests can be enabled with `RUN_SPARK_TESTS=1`.

## 13) Artifacts
- `artifacts/rag_index/`  vector store and policy index files.
- `artifacts/runs/`  copilot inputs/outputs (JSON + raw response).
- `artifacts/runs_failed/`  failed outputs with error logs.
- `artifacts/feedback/`  feedback entries.
- `artifacts/eval/`  evaluation reports.

## 14) Rebuild Steps (High-Level)
1) Create repository structure as specified above.
2) Implement Spark pipeline + schemas under `data/`.
3) Implement FastAPI service with endpoints, logging, config, caching.
4) Implement RAG indexing and retrieval with local SQLite store.
5) Implement copilot service, validators, and storage.
6) Implement UI and Docker integration.
7) Add evaluation runner, guardrails, and tests.
8) Provide Docker Compose for API, UI, scheduler, Redis.

## 15) Non-Goals
- No external managed vector DB.
- No cloud-managed services required to run locally.
- No model reasoning beyond spec-defined output schema.

