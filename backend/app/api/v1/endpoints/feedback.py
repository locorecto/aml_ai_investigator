from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from app.api.deps import get_case_data_access, get_feedback_service
from app.schemas.feedback import FeedbackList, FeedbackMeta, FeedbackRecord, FeedbackSubmission
from app.services.case_service import CaseDataAccess, CaseNotFoundError
from app.services.feedback_service import FeedbackService

router = APIRouter()


@router.post("/cases/{case_id}/feedback", response_model=FeedbackRecord, status_code=status.HTTP_201_CREATED)
def submit_feedback(
    case_id: str,
    payload: FeedbackSubmission,
    request: Request,
    case_access: CaseDataAccess = Depends(get_case_data_access),
    feedback_service: FeedbackService = Depends(get_feedback_service),
) -> FeedbackRecord:
    try:
        case_access.get_case_packet(case_id)
    except CaseNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Case not found") from exc

    meta = FeedbackMeta(
        client_host=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )
    return feedback_service.save_feedback(case_id, payload, meta)


@router.get("/feedback", response_model=FeedbackList)
def list_feedback(
    limit: int = Query(default=50, ge=1),
    offset: int = Query(default=0, ge=0),
    case_id: str | None = None,
    feedback_service: FeedbackService = Depends(get_feedback_service),
) -> FeedbackList:
    total = feedback_service.count_feedback(case_id)
    items = feedback_service.list_feedback(limit, offset, case_id)
    return FeedbackList(items=items, pagination={"limit": limit, "offset": offset, "total": total})


@router.get("/feedback/{feedback_id}", response_model=FeedbackRecord)
def get_feedback(
    feedback_id: str,
    feedback_service: FeedbackService = Depends(get_feedback_service),
) -> FeedbackRecord:
    record = feedback_service.get_feedback(feedback_id)
    if not record:
        raise HTTPException(status_code=404, detail="Feedback not found")
    return record


@router.get("/cases/{case_id}/feedback", response_model=FeedbackList)
def list_case_feedback(
    case_id: str,
    limit: int = Query(default=50, ge=1),
    offset: int = Query(default=0, ge=0),
    feedback_service: FeedbackService = Depends(get_feedback_service),
) -> FeedbackList:
    total = feedback_service.count_feedback(case_id)
    items = feedback_service.list_feedback(limit, offset, case_id)
    return FeedbackList(items=items, pagination={"limit": limit, "offset": offset, "total": total})
