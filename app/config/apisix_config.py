"""APISIX Gateway Configuration."""

from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


# Determine project root (where .env file should be located)
# This file is at: app/config/apisix_config.py
# Project root is 2 levels up
_project_root = Path(__file__).parent.parent.parent
_env_file = _project_root / ".env"


class APISIXConfig(BaseSettings):
    """Configuration for APISIX Gateway integration."""

    model_config = SettingsConfigDict(
        env_file=str(_env_file) if _env_file.exists() else ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # APISIX Configuration
    APISIX_ADMIN_URL: str = Field(
        default="http://localhost:9180", env="APISIX_ADMIN_URL"
    )
    APISIX_ADMIN_API_KEY: str = Field(default="", env="APISIX_ADMIN_API_KEY")
    APISIX_JWT_SECRET: str = Field(default="", env="APISIX_JWT_SECRET")
    APISIX_ROUTE_NAME: str = Field(
        default="demo-management-route", env="APISIX_ROUTE_NAME"
    )
    IS_APISIX_ENABLED: bool = Field(default=True, env="IS_APISIX_ENABLED")


@lru_cache
def get_apisix_config() -> APISIXConfig:
    """Get cached APISIX configuration instance."""
    return APISIXConfig()
