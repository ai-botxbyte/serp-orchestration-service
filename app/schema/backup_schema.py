"""Schemas for database backup operations."""

from __future__ import annotations
from typing import Optional, List
from pydantic import Field

from app.schema.baseapp_schema import BaseAppSchema


class BackupCreateRequestSchema(BaseAppSchema):
    """Schema for backup creation request."""

    backup_name: Optional[str] = Field(
        default=None,
        description="Custom backup name (optional). If not provided, uses timestamp",
        example="pre_deployment_backup",
    )
    description: Optional[str] = Field(
        default=None,
        description="Optional description for the backup",
        example="Backup before major feature deployment",
    )


class BackupRestoreRequestSchema(BaseAppSchema):
    """Schema for backup restore request."""

    backup_filename: str = Field(
        ...,
        description="Name of the backup ZIP file to restore",
        example="demo-management-service_20240114_120000.sql.zip",
    )


class BackupInfoSchema(BaseAppSchema):
    """Schema for backup information."""

    filename: str = Field(
        ...,
        description="Backup filename",
        example="demo-management-service_20240114_120000.sql.zip",
    )
    key: Optional[str] = Field(
        default=None,
        description="S3 key of the backup",
        example="backups/demo-management-service/demo-management-service_20240114_120000.sql.zip",
    )
    size: Optional[int] = Field(
        default=None, description="Size of backup file in bytes", example=1048576
    )
    last_modified: Optional[str] = Field(
        default=None,
        description="Last modified timestamp",
        example="2024-01-14T12:00:00",
    )


class BackupCreateResponseSchema(BaseAppSchema):
    """Schema for backup creation response."""

    backup_name: str = Field(
        ..., description="Name of the backup", example="pre_deployment_backup"
    )
    description: Optional[str] = Field(
        default=None,
        description="Backup description",
        example="Backup before deployment",
    )
    timestamp: str = Field(
        ..., description="Backup timestamp", example="20240114_120000"
    )
    database: str = Field(
        ..., description="Database name", example="demo_management_db"
    )
    zip_filename: str = Field(
        ...,
        description="ZIP filename",
        example="demo-management-service_20240114_120000.sql.zip",
    )
    zip_size: int = Field(..., description="ZIP file size in bytes", example=1048576)
    s3_key: str = Field(
        ...,
        description="S3 storage key",
        example="backups/demo-management-service/demo-management-service_20240114_120000.sql.zip",
    )


class BackupListResponseSchema(BaseAppSchema):
    """Schema for backup list response."""

    backups: List[BackupInfoSchema] = Field(
        default_factory=list, description="List of available backups"
    )
    count: int = Field(..., description="Total number of backups", example=5)


class BackupRestoreResponseSchema(BaseAppSchema):
    """Schema for backup restore response."""

    backup_filename: str = Field(
        ...,
        description="Restored backup filename",
        example="demo-management-service_20240114_120000.sql.zip",
    )
    database: str = Field(
        ..., description="Database name", example="demo_management_db"
    )
    restored_at: str = Field(
        ..., description="Restore timestamp", example="2024-01-14T12:30:00"
    )
    success: bool = Field(
        ..., description="Whether restore was successful", example=True
    )
