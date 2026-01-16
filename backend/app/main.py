import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.api.v1.api import api_router
from app.core.config import build_data_paths, get_settings
from app.core.logging import configure_logging
from app.llm.provider import OpenAIChatProvider
from app.services.case_service import CaseDataAccess
from app.services.copilot_service import CopilotService

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)

    data_paths = build_data_paths(settings.data_base_path)
    for name, path in data_paths.model_dump().items():
        if not path.exists():
            logger.warning("Dataset path missing", extra={"dataset": name, "path": str(path)})
    app.state.case_data_access = CaseDataAccess(data_paths)
    if settings.llm_api_key:
        llm_provider = OpenAIChatProvider(settings.llm_api_key, settings.llm_base_url)
    else:
        llm_provider = OpenAIChatProvider("", settings.llm_base_url)
        logger.warning("LLM_API_KEY is not set; copilot calls will fail")
    app.state.copilot_service = CopilotService(settings, llm_provider)
    logger.info("Case data access initialized")
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=lifespan)

    @app.middleware("http")
    async def request_logger(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "request",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": round(duration_ms, 2),
            },
        )
        return response

    app.include_router(api_router, prefix=settings.api_prefix)
    return app


app = create_app()
