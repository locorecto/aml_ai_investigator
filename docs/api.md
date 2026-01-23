# API Guide

Base path: `/api/v1`

## Endpoints
- `GET /health` ? returns service status with dependency checks (data paths, RAG index, cache availability).
- `GET /health/ready` ? readiness check; returns `503` if required dependencies are missing.
- `GET /cases` ? paginated triage list. Query params: `limit`, `offset`.
- `GET /cases/{id}` ? full case packet.
- `GET /cases/{id}/timeline` ? daily transaction timeline.
- `POST /cases/{id}/copilot-summary` ? runs copilot summary with retrieval + validation.
- `POST /cases/{id}/feedback` ? store investigator feedback.
- `GET /feedback` ? list feedback entries.

## Response Shapes
- `GET /cases` returns `{ items: [...], pagination: { limit, offset, total } }`.
- Copilot returns strict JSON defined in `backend/app/schemas/copilot.py`.

## Caching
The cases list/detail/timeline endpoints are cached in Redis when enabled:
- `CACHE_ENABLED=true`
- `REDIS_URL=redis://redis:6379/0`

## Error Handling
- 404 for missing case IDs.
- 422 when guardrails fail (missing evidence or invalid references).
- 503 on readiness check when required dependencies are missing.
