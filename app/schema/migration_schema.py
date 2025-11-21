from __future__ import annotations
from typing import Optional
from pydantic import Field

from app.schema.baseapp_schema import BaseAppSchema


class RevisionRequestSchema(BaseAppSchema):
    """Schema for revision creation requests."""

    message: str = Field(
        ..., description="Migration message for the revision", example="First Migration"
    )


class MigrationResponseSchema(BaseAppSchema):
    """Schema for migration responses."""

    operation: str = Field(
        ..., description="Operation that was performed", example="upgrade"
    )
    success: bool = Field(
        default=False, description="Whether the operation was successful", example=True
    )
    message: str = Field(
        ..., description="Result message", example="Migration completed successfully"
    )
    revision_id: Optional[str] = Field(
        default=None,
        description="Created revision ID (for revision operation)",
        example="1234567890",
    )
    current_revision: Optional[str] = Field(
        default=None,
        description="Current database revision after operation",
        example="1234567890",
    )
    file_count: Optional[int] = Field(
        default=None, description="Number of migration files processed", example=10
    )
    location: Optional[str] = Field(
        default=None,
        description="Location/path of the migration files",
        example="/app/alembic/versions",
    )
