"""Case packet endpoints with optional caching."""
from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.deps import get_cache, get_case_data_access
from app.core.config import Settings, get_settings
from app.schemas.case import CasePacket, PaginatedCases, TimelineEntry
from app.services.case_service import CaseDataAccess, CaseNotFoundError
from app.storage.cache import BaseCache

router = APIRouter()


@router.get("/cases", response_model=PaginatedCases)
def list_cases(
    limit: int = Query(50, ge=1),
    offset: int = Query(0, ge=0),
    settings: Settings = Depends(get_settings),
    data_access: CaseDataAccess = Depends(get_case_data_access),
    cache: BaseCache = Depends(get_cache),
) -> PaginatedCases:
    bounded_limit = min(limit, settings.max_limit)
    cache_key = f"cases:list:{bounded_limit}:{offset}"
    cached = cache.get_json(cache_key)
    if cached:
        return PaginatedCases(**cached)
    items, total = data_access.list_cases(bounded_limit, offset)
    payload = PaginatedCases(
        items=items,
        pagination={"limit": bounded_limit, "offset": offset, "total": total},
    )
    cache.set_json(cache_key, payload.model_dump())
    return payload


@router.get("/cases/{case_id}", response_model=CasePacket)
def get_case(
    case_id: str,
    data_access: CaseDataAccess = Depends(get_case_data_access),
    cache: BaseCache = Depends(get_cache),
) -> CasePacket:
    try:
        cache_key = f"cases:detail:{case_id}"
        cached = cache.get_json(cache_key)
        if cached:
            return CasePacket(**cached)
        payload = data_access.get_case_packet(case_id)
    except CaseNotFoundError as exc:
        raise HTTPException(status_code=404, detail="case_id not found") from exc
    cache.set_json(cache_key, payload)
    return payload


@router.get("/cases/{case_id}/timeline", response_model=list[TimelineEntry])
def get_case_timeline(
    case_id: str,
    data_access: CaseDataAccess = Depends(get_case_data_access),
    cache: BaseCache = Depends(get_cache),
) -> list[TimelineEntry]:
    cache_key = f"cases:timeline:{case_id}"
    cached = cache.get_json(cache_key)
    if cached:
        return cached
    payload = data_access.get_timeline(case_id)
    cache.set_json(cache_key, payload)
    return payload
