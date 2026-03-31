from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


# Determine project root (where .env file should be located)
# This file is at: app/config/baseapp_config.py
# Project root is 2 levels up
_project_root = Path(__file__).parent.parent.parent
_env_file = _project_root / ".env.dev"


class BaseAppConfig(BaseSettings):
    """Base configuration class with common settings that can be reused by all config classes."""

    model_config = SettingsConfigDict(
        env_file=str(_env_file) if _env_file.exists() else ".env.dev",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Common application settings
    ENV: str = Field(default="development", env="ENV")
    APP_NAME: str = Field(default="DemoManagementService", env="APP_NAME")
    APP_VERSION: str = Field(default="2.0.1", env="APP_VERSION")
    APP_PORT: int = Field(default=8801, env="APP_PORT")
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")

    SERVICE_NAME: str = Field(default="demo-management-service", env="SERVICE_NAME")

    # [x]: We also need to add the Dtabase url for Read Replica.
    # Database settings
    ASYNC_DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/demo_management_db",
        env="ASYNC_DATABASE_URL",
    )

    # Read Replica Database settings
    ASYNC_READ_DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/demo_management_db",
        env="ASYNC_READ_DATABASE_URL",
    )

    IS_QUEUE_LOG: bool = Field(default=False, env="IS_QUEUE_LOG")

    # CORS settings
    CORS_ORIGINS: str = Field(default="*", env="CORS_ORIGINS")

    # Media settings
    MEDIA_PATH: str = Field(default="app/media", env="MEDIA_PATH")

    # Cache and messaging settings
    REDIS_URL: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")
    IS_REDIS_CACHE_ENABLED: bool = Field(
        default=True,
        env="IS_REDIS_CACHE_ENABLED",
        description="Enable/disable Redis caching for this service",
    )
    REDIS_CACHE_TTL: int = Field(
        default=300,
        env="REDIS_CACHE_TTL",
        description="Default TTL for Redis cache in seconds (default: 300 = 5 minutes)",
    )
    RABBITMQ_URL: str = Field(
        default="amqp://guest:guest@localhost:5672/", env="RABBITMQ_URL"
    )
    RABBITMQ_MANAGEMENT_URL: str = Field(
        default="http://guest:guest@localhost:15672", env="RABBITMQ_MANAGEMENT_URL"
    )

    # RabbitMQ Queue Names (comma-separated list or individual queue names)
    # Example: "demo_creation_topic,demo_deletion_topic,log_queue"
    RABBITMQ_QUEUE_NAMES: str = Field(
        default="log_queue",
        env="RABBITMQ_QUEUE_NAMES",
        description="Comma-separated list of queue names to ensure exist on startup",
    )

    # Service-level enable/disable flags (for individual service opt-in/opt-out)
    # Note: Project always has access, but individual services can choose
    # to use or not use these services
    IS_RABBITMQ_ENABLED: bool = Field(
        default=True,
        env="IS_RABBITMQ_ENABLED",
        description=(
            "Enable/disable RabbitMQ usage for this service "
            "(service-level opt-in/opt-out)"
        ),
    )
    IS_POSTGRES_ENABLED: bool = Field(
        default=True,
        env="IS_POSTGRES_ENABLED",
        description=(
            "Enable/disable PostgreSQL usage for this service "
            "(service-level opt-in/opt-out)"
        ),
    )

    # Database routing settings
    IS_USE_READ_REPLICA: bool = Field(default=False, env="IS_USE_READ_REPLICA")

    # Endpoint visibility settings
    IS_DEMO_ENDPOINTS_ENABLED: bool = Field(
        default=True,
        env="IS_DEMO_ENDPOINTS_ENABLED",
        description="Enable/disable demo-related endpoints",
    )
    IS_BACKUP_ENABLED: bool = Field(
        default=True,
        env="IS_BACKUP_ENABLED",
        description="Enable/disable backup endpoints",
    )

    # Wasabi/S3 settings for migration storage
    WASABI_ENDPOINT_URL: str = Field(
        default="https://s3.wasabisys.com", env="WASABI_ENDPOINT_URL"
    )
    WASABI_ACCESS_KEY_ID: str = Field(default="", env="WASABI_ACCESS_KEY_ID")
    WASABI_SECRET_ACCESS_KEY: str = Field(default="", env="WASABI_SECRET_ACCESS_KEY")
    WASABI_REGION: str = Field(default="us-east-1", env="WASABI_REGION")
    MIGRATION_BUCKET_NAME: str = Field(default="", env="MIGRATION_BUCKET_NAME")
    BACKUP_BUCKET_NAME: str = Field(default="", env="BACKUP_BUCKET_NAME")

    # SERP Service settings
    SERP_LAMBDA_SERVICE_URL: str = Field(
        default="http://localhost:8000",
        env="SERP_LAMBDA_SERVICE_URL",
        description="URL of the SERP Lambda Service for search operations",
    )

    def is_production(self) -> bool:
        """Check if running in production environment."""
        return str(self.ENV).lower() in ["production", "prod"]

    def is_development(self) -> bool:
        """Check if running in development environment."""
        return str(self.ENV).lower() in ["development", "dev"]


@lru_cache
def get_base_config() -> BaseAppConfig:
    """Get cached base configuration instance."""
    return BaseAppConfig()
