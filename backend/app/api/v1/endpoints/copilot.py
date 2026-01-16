from fastapi import APIRouter, Depends, HTTPException

from app.api.deps import get_case_data_access, get_copilot_service
from app.services.case_service import CaseDataAccess, CaseNotFoundError
from app.services.copilot_service import CopilotService, CopilotValidationError

router = APIRouter()


@router.post("/cases/{case_id}/copilot-summary")
def copilot_summary(
    case_id: str,
    data_access: CaseDataAccess = Depends(get_case_data_access),
    copilot: CopilotService = Depends(get_copilot_service),
):
    try:
        case_packet = data_access.get_case_packet(case_id)
    except CaseNotFoundError as exc:
        raise HTTPException(status_code=404, detail="case_id not found") from exc

    try:
        result = copilot.generate_summary(case_packet)
    except CopilotValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="copilot error") from exc

    return result
