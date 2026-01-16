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

    default_limit: int = Field(default=50, ge=1)
    max_limit: int = Field(default=500, ge=1)

    class Config:
        env_prefix = ""
        env_file = ".env"
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
