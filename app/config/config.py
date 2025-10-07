from __future__ import annotations
from uuid import UUID
from functools import lru_cache
from pydantic import Field
from pydantic_settings import SettingsConfigDict
from app.config.baseapp_config import BaseAppConfig



class Config(BaseAppConfig):
    """Application-specific settings extending BaseAppConfig."""
    
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8", 
        case_sensitive=False, 
        extra="ignore"
    )
    SYSTEM_USER_ID: UUID = Field(default="00000000-0000-0000-0000-000000000000", env="SYSTEM_USER_ID")
    
    
    # Additional fields, if any, can be added similarly
    STATUS: str = Field(default="active", env="STATUS")  # Example additional field

@lru_cache
def get_config() -> Config:
    """Get the configuration instance."""
    return Config()

config = get_config()
    