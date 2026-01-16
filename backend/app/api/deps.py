from fastapi import Depends, Request

from app.services.case_service import CaseDataAccess


def get_case_data_access(request: Request) -> CaseDataAccess:
    return request.app.state.case_data_access


def get_copilot_service(request: Request):
    return request.app.state.copilot_service
