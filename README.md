# AML AI Investigator

## Overview
AML AI Investigator provides a production-ready pipeline and API for AML case evidence packages, policy retrieval, and an LLM copilot summary. It ingests existing data in `data/`, builds case packets via Spark, exposes evidence endpoints through FastAPI, retrieves policy guidance from a local vector store, and generates structured copilot summaries using a provider-agnostic LLM interface.

## Scope
Implemented phases:
- Evidence API (FastAPI)
- Policy/playbook RAG (local vector store)
- Copilot summary MVP (strict JSON + validators)

Not implemented:
- Investigator UX and human-in-the-loop workflow (TBD)
- Evaluation/controls tooling (TBD)

## Architecture
- Data pipeline (Spark) produces:
  - `data/case_packet` (parquet)
  - `data/case_packet_json` (parquet)
  - `data/tx_timeline_daily` (parquet)
- FastAPI service reads the parquet outputs and serves evidence packages.
- RAG indexes policy docs from `policy_corpus/` into `artifacts/rag_index/`.
- Copilot endpoint combines case packets + policy passages and calls an LLM API.
- All copilot runs are stored in `artifacts/runs/`.

## Data Sources
The system uses existing data under `data/`:
- `transactions.csv`, `parties.csv`, `counterparties.csv`, `merchants.csv`
- `alerts_*.csv` (cash, wires, credit_cards, loans, ngi)
- `case_packet`, `case_packet_json`, `tx_timeline_daily` (parquet)

## Schemas
- JSON schemas: `data/schemas/*.schema.json`
- PySpark schemas: `data/spark-schemas/*.py`

These are derived from the grounded schema definitions and parquet outputs.

## API
Base path: `/api/v1`

Implemented endpoints:
- `GET /health` - service health check
- `GET /cases` - paginated triage list
- `GET /cases/{id}` - full case packet
- `GET /cases/{id}/timeline` - daily transaction timeline
- `POST /cases/{id}/copilot-summary` - structured copilot summary

## Policy RAG
- Policy corpus location: `policy_corpus/`
- Index artifacts: `artifacts/rag_index/`
- Index script: `scripts/index_policies.py`
- Retrieval: `retrieve_policy(case_packet) -> passages[]`

Chunking uses stable chunk IDs and stores doc metadata (`doc_id`, `version`, `section`).

## Copilot Output
The copilot output is a strict JSON schema with the following fields:
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

Validators enforce:
- JSON schema compliance
- Citation requirements (evidence and/or policy)
- Refusal when required fields are missing

## Investigator UX
TBD - evidence viewer, copilot panel, and feedback capture.

## Audit Trail
Copilot runs are stored under `artifacts/runs/` with:
- case packet input
- retrieved policy passages
- raw LLM response
- validated JSON output
- prompt/version metadata

## Deployment
Docker Compose (API + scheduler):
```
docker compose -f infra/compose/docker-compose.yml up --build -d
```

The scheduler container runs the daily Spark job via cron.

## Configuration
Environment variables (Docker or local):
- `DATA_BASE_PATH` (default: `data`)
- `ARTIFACTS_PATH` (default: `artifacts`)
- `LLM_API_KEY` (required for copilot)
- `LLM_BASE_URL` (default: `https://api.openai.com/v1`)
- `LLM_MODEL` (default: `gpt-5`)
- `LOG_LEVEL` (default: `INFO`)

## Development
Create/refresh case packets:
```
python backend/scripts/run_case_packet_job.py --base-path data
```

Index policy corpus:
```
python scripts/index_policies.py
```

Run API locally:
```
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Testing
Install dev dependencies:
```
python -m pip install -r backend/requirements-dev.txt
```

Run tests:
```
pytest backend/tests -q
```
