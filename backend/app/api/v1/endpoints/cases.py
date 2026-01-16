from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.deps import get_case_data_access
from app.core.config import Settings, get_settings
from app.schemas.case import CasePacket, PaginatedCases, TimelineEntry
from app.services.case_service import CaseDataAccess, CaseNotFoundError

router = APIRouter()


@router.get("/cases", response_model=PaginatedCases)
def list_cases(
    limit: int = Query(50, ge=1),
    offset: int = Query(0, ge=0),
    settings: Settings = Depends(get_settings),
    data_access: CaseDataAccess = Depends(get_case_data_access),
) -> PaginatedCases:
    bounded_limit = min(limit, settings.max_limit)
    items, total = data_access.list_cases(bounded_limit, offset)
    return PaginatedCases(
        items=items,
        pagination={"limit": bounded_limit, "offset": offset, "total": total},
    )


@router.get("/cases/{case_id}", response_model=CasePacket)
def get_case(case_id: str, data_access: CaseDataAccess = Depends(get_case_data_access)) -> CasePacket:
    try:
        payload = data_access.get_case_packet(case_id)
    except CaseNotFoundError as exc:
        raise HTTPException(status_code=404, detail="case_id not found") from exc
    return payload


@router.get("/cases/{case_id}/timeline", response_model=list[TimelineEntry])
def get_case_timeline(case_id: str, data_access: CaseDataAccess = Depends(get_case_data_access)) -> list[TimelineEntry]:
    return data_access.get_timeline(case_id)
