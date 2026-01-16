from fastapi import APIRouter

from app.api.v1.endpoints import cases, copilot, health

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(cases.router, tags=["cases"])
api_router.include_router(copilot.router, tags=["copilot"])
