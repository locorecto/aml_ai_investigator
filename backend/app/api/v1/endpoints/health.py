"""Health endpoints for liveness/readiness checks."""
from fastapi import APIRouter, Request, Response

from app.core.config import build_data_paths, get_settings
from app.storage.cache import NullCache

router = APIRouter()


@router.get("/health")
def health(request: Request) -> dict:
    """Return overall health with dependency checks."""
    settings = get_settings()
    data_paths = build_data_paths(settings.data_base_path)
    checks = {
        "data_paths": {
            name: path.exists() for name, path in data_paths.model_dump().items()
        },
        "rag_index": (settings.artifacts_path / "rag_index").exists(),
        "cache": {
            "enabled": settings.cache_enabled,
            "available": not isinstance(request.app.state.cache, NullCache),
        },
    }
    required_ok = all(checks["data_paths"].values()) and checks["rag_index"]
    if settings.cache_enabled:
        required_ok = required_ok and checks["cache"]["available"]
    status = "ok" if required_ok else "degraded"
    return {"status": status, "checks": checks}


@router.get("/health/ready")
def readiness(request: Request, response: Response) -> dict:
    """Return readiness status and set 503 when dependencies are unavailable."""
    payload = health(request)
    if payload["status"] != "ok":
        response.status_code = 503
    return payload
