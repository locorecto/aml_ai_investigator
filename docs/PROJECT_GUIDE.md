# AML AI Investigator — Junior Developer Guide

This guide explains what the project is, how it works end-to-end, how an investigator uses it, and how the codebase is organized. It assumes no AML or regulatory background.

## 1) What This Project Is
AML AI Investigator is a local, production-minded application that helps financial investigators review suspicious activity. It:
- Builds “evidence packets” from transaction and alert data.
- Serves those packets via an API.
- Retrieves internal policy guidance for context.
- Uses an LLM to generate a structured summary with citations.
- Provides a simple web UI for investigators to review, edit, and give feedback.
- Includes evaluation tools to measure output quality and apply guardrails.

In plain language: it gathers relevant data, shows it to an investigator, and provides a structured AI-assisted summary that references the evidence.

## 2) AML Concepts (Simple Terms)
You do NOT need AML domain knowledge to work on the code, but here’s a quick mapping:
- **Case packet**: a bundle of data for one investigation (party info, alerts, transactions).
- **SAR**: “Suspicious Activity Report.” Investigators decide “file” or “no-file.”
- **Typologies**: known suspicious patterns (e.g., rapid money movement).
- **Policy guidance**: internal or industry guidelines that inform decisions.

## 3) How Investigators Use It
Typical flow:
1) Open the UI and select a case from the triage list.
2) Review the evidence packet (alerts, transactions, counterparties, merchants).
3) Click “Generate Summary” to run the copilot.
4) Read the structured summary with citations.
5) Accept or edit the decision and narrative.
6) Submit feedback (helpful / not helpful, what was wrong, missing data).

## 4) High-Level Architecture
The system has four major layers:
1) **Data Pipeline** (Spark)
2) **API Service** (FastAPI)
3) **RAG Policy Retrieval** (local vector store)
4) **UI** (React + Nginx)

### Diagram (conceptual)
- Raw CSVs ? Spark pipeline ? Parquet case packets
- API reads Parquet ? serves evidence endpoints
- Policy docs ? chunk/embeddings ? vector store
- Copilot endpoint ? case packet + policy passages ? LLM ? structured JSON
- UI ? uses API to display evidence + copilot output

## 5) Execution Flow (Detailed)
### 5.1 Data Pipeline (Spark)
1) `backend/app/pipelines/data_loading.py` builds a Spark session.
2) It loads CSVs with strict PySpark schemas from `data/spark-schemas/`.
3) It aggregates data into:
   - `case_packet` (summary + lists)
   - `case_packet_json` (same content but JSON per case)
   - `tx_timeline_daily` (daily aggregated timeline)
4) Outputs are written as Parquet in `data/`.

Why Spark?
- The datasets can be large.
- Spark is standard for batch ETL.

### 5.2 API Service (FastAPI)
1) `backend/app/main.py` initializes the app.
2) `backend/app/core/config.py` loads configuration from the home `.env` file.
3) The app wires:
   - `CaseDataAccess` for parquet access.
   - `CopilotService` for LLM summaries.
   - `FeedbackService` for feedback storage.
   - Redis cache for low-latency endpoints.
4) Endpoints are mounted under `/api/v1`.

Health checks:
- `/health` returns dependency checks.
- `/health/ready` returns 503 if required deps are missing.

Why FastAPI?
- Fast, modern Python API framework.
- Strong typing + OpenAPI support.

### 5.3 Policy RAG (Retrieval)
1) Policies are stored as markdown in `policy_corpus/`.
2) `scripts/index_policies.py` calls the indexer.
3) `backend/app/rag/indexer.py`:
   - splits documents into chunks
   - embeds chunks using a provider interface
   - stores them in a local SQLite-backed vector store at `artifacts/rag_index/`
4) `backend/app/rag/retriever.py` converts a case packet into a text query and retrieves top passages.

Why local SQLite?
- Easy to run in Docker.
- No external services needed for local development.

### 5.4 Copilot Summary
1) `POST /cases/{id}/copilot-summary` loads the case packet.
2) Retrieves relevant policy passages (RAG).
3) `backend/app/services/copilot_service.py` builds the prompt:
   - includes schema
   - includes case packet
   - includes policy passages
   - enforces evidence-grounded output
4) The LLM provider returns JSON. The service validates:
   - JSON schema
   - citations present
   - evidence references valid
5) Output is saved to `artifacts/runs/`.

Why strict JSON and validators?
- Prevents hallucinations.
- Makes downstream usage deterministic.

### 5.5 UI
1) `frontend/` is a React app.
2) It calls the API at `VITE_API_BASE_URL`.
3) It renders:
   - triage list
   - evidence tables
   - copilot output
   - feedback form

Why a simple UI?
- It demonstrates human-in-the-loop workflows.
- It’s easy to run locally via Docker.

## 6) Data and Schemas
- CSV inputs: `data/*.csv`.
- Parquet outputs: `data/case_packet`, `data/case_packet_json`, `data/tx_timeline_daily`.
- JSON schemas: `data/schemas/`.
- PySpark schemas: `data/spark-schemas/`.

Strict schemas avoid silent type errors in ML/ETL pipelines.

## 7) Guardrails and Evaluation
Guardrails live in `backend/app/guardrails/`:
- Evidence citation validation.
- PII redaction hooks.
- Refusal behavior when required evidence is missing.

Evaluation in `backend/app/eval/`:
- Uses `data/eval/cases.jsonl` to score outputs.
- Metrics:
  - factual consistency (citations)
  - completeness (expected indicators)
  - usefulness proxy
  - disagreement analysis
- Reports saved under `artifacts/eval/`.

## 8) Redis Cache
Redis is used to speed up frequently accessed endpoints:
- `GET /cases`
- `GET /cases/{id}`
- `GET /cases/{id}/timeline`

It is configured via `CACHE_ENABLED`, `REDIS_URL`, and `CACHE_TTL_SECONDS`.

## 9) Docker and Execution
### Start everything
```
docker compose -f infra/compose/docker-compose.yml up --build -d
```

Services:
- `api` (FastAPI)
- `ui` (Nginx + React build)
- `scheduler` (Spark job via cron)
- `redis`

### Run the daily job manually
```
python backend/scripts/run_case_packet_job.py --base-path data
```

### Index policy corpus
```
python scripts/index_policies.py
```

## 10) Files and What They Do
### Backend
- `backend/app/main.py` — app bootstrap and dependency wiring.
- `backend/app/core/config.py` — env-based settings and defaults.
- `backend/app/core/logging.py` — JSON logging setup.
- `backend/app/api/v1/` — API endpoints.
- `backend/app/services/` — service layer (case, copilot, feedback, policy).
- `backend/app/rag/` — chunking, embeddings, indexing, retrieval.
- `backend/app/guardrails/` — validators and redaction.
- `backend/app/eval/` — evaluation runner and metrics.
- `backend/app/storage/` — vector store + cache + audit storage.
- `backend/app/pipelines/` — Spark ETL pipeline.

### Frontend
- `frontend/src/App.tsx` — main UI logic.
- `frontend/src/main.tsx` — React entry point.
- `frontend/src/styles.css` — UI styles.

### Data
- `data/spark-schemas/` — PySpark schema definitions.
- `data/schemas/` — JSON schema definitions.
- `data/case_packet*` — Parquet outputs.

### Infra
- `infra/compose/docker-compose.yml` — all services.
- `infra/docker/` — Dockerfiles for API, scheduler, UI.

### Artifacts
- `artifacts/runs/` — copilot runs.
- `artifacts/rag_index/` — policy index.
- `artifacts/feedback/` — investigator feedback.
- `artifacts/eval/` — evaluation reports.

## 11) Design Decisions (Reasoning)
- **Strict JSON schema**: reliable machine-readable output.
- **Local vector store**: simple, portable, docker-friendly.
- **Redis cache**: faster triage list and detail views.
- **Structured logging**: easier to integrate with log systems.
- **Config via env**: standard for containerized apps.
- **Feedback loop**: required for audit trails and model improvement.

## 12) Common Pitfalls
- Missing data in `data/` ? endpoints return empty lists.
- Missing policy index ? copilot has no policy citations.
- Missing `.aml_ai_investigator.env` ? config defaults used.
- Spark tests are off by default unless `RUN_SPARK_TESTS=1`.

## 13) How to Extend
- Add new policy docs ? re-run `scripts/index_policies.py`.
- Add new endpoints ? register in `backend/app/api/v1/api.py`.
- Improve copilot validation ? update `backend/app/services/copilot_service.py`.
- Add new UI panels ? update `frontend/src/App.tsx`.

