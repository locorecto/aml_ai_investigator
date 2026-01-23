"""Application configuration."""
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables."""

    app_name: str = Field(default="aml-ai-investigator", validation_alias="APP_NAME")
    environment: str = Field(default="dev", validation_alias="ENVIRONMENT")
    api_prefix: str = Field(default="/api/v1", validation_alias="API_PREFIX")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    data_base_path: Path = Field(default=Path("data"), validation_alias="DATA_BASE_PATH")
    artifacts_path: Path = Field(default=Path("artifacts"), validation_alias="ARTIFACTS_PATH")

    llm_api_key: str | None = Field(default=None, validation_alias="LLM_API_KEY")
    llm_base_url: str = Field(default="https://api.openai.com/v1", validation_alias="LLM_BASE_URL")
    llm_model: str = Field(default="gpt-5", validation_alias="LLM_MODEL")
    llm_timeout_seconds: int = Field(default=180, ge=1, validation_alias="LLM_TIMEOUT_SECONDS")
    llm_max_tokens: int = Field(default=512, ge=1, validation_alias="LLM_MAX_TOKENS")
    llm_provider: str = Field(default="openai", validation_alias="LLM_PROVIDER")
    required_evidence_fields: str = Field(
        default="",
        validation_alias="REQUIRED_EVIDENCE_FIELDS",
    )
    pii_redaction_enabled: bool = Field(default=False, validation_alias="PII_REDACTION_ENABLED")
    pii_patterns: str = Field(
        default="\\b\\d{3}-\\d{2}-\\d{4}\\b;[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,};\\b\\+?\\d{1,3}[-. (]*\\d{3}[-. )]*\\d{3}[-. ]*\\d{4}\\b",
        validation_alias="PII_PATTERNS",
    )
    cache_enabled: bool = Field(default=False, validation_alias="CACHE_ENABLED")
    redis_url: str = Field(default="", validation_alias="REDIS_URL")
    cache_ttl_seconds: int = Field(default=60, ge=1, validation_alias="CACHE_TTL_SECONDS")
    cache_prefix: str = Field(default="aml-ai:", validation_alias="CACHE_PREFIX")
    root_path: str = Field(default="", validation_alias="ROOT_PATH")
    proxy_headers_enabled: bool = Field(default=True, validation_alias="PROXY_HEADERS_ENABLED")

    default_limit: int = Field(default=50, ge=1)
    max_limit: int = Field(default=500, ge=1)
    cors_allow_origins: str = Field(
        default="http://localhost:5173,http://127.0.0.1:5173",
        validation_alias="CORS_ALLOW_ORIGINS",
    )

    class Config:
        env_prefix = ""
        env_file = str(Path.home() / ".aml_ai_investigator.env")
        env_file_encoding = "utf-8"


class DataPaths(BaseModel):
    """Resolved data paths for parquet datasets."""

    case_packet: Path
    case_packet_json: Path
    tx_timeline_daily: Path


def build_data_paths(base_path: Path) -> DataPaths:
    """Resolve dataset locations under the base path."""
    return DataPaths(
        case_packet=base_path / "case_packet",
        case_packet_json=base_path / "case_packet_json",
        tx_timeline_daily=base_path / "tx_timeline_daily",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings."""
    return Settings()
