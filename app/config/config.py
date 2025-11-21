from __future__ import annotations

from functools import lru_cache
from pydantic_settings import SettingsConfigDict
from app.config.baseapp_config import BaseAppConfig


class Config(BaseAppConfig):
    """Application-specific settings extending BaseAppConfig."""

    # Note: env_file is inherited from BaseAppConfig which uses absolute path
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )


@lru_cache
def get_config() -> Config:
    """Get cached configuration instance."""
    return Config()


# Create config instance - will be cached until cache_clear() is called
config = get_config()
