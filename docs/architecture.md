# Architecture

## Overview
The system is split into four layers:
1) Spark ETL pipeline
2) FastAPI service
3) Local RAG policy retrieval
4) React UI

## Data Flow
1) CSVs in `data/` are read by Spark.
2) Spark builds `case_packet`, `case_packet_json`, `tx_timeline_daily` (parquet).
3) FastAPI reads parquet to serve evidence endpoints.
4) Policy corpus is indexed into a SQLite vector store in `artifacts/rag_index/`.
5) Copilot endpoint retrieves policy passages and calls the LLM provider.
6) UI calls the API to display evidence, copilot output, and feedback forms.

## Key Components
- `backend/app/pipelines/` ? ETL pipeline using PySpark.
- `backend/app/services/` ? service layer (case, copilot, feedback, policy).
- `backend/app/rag/` ? chunking, embeddings, indexer, retriever.
- `backend/app/api/v1/` ? API endpoints.
- `frontend/` ? investigator UI.

## Storage
- `data/` ? input CSVs + parquet outputs.
- `policy_corpus/` ? policy documents.
- `artifacts/` ? RAG index, copilot runs, feedback, eval reports.
