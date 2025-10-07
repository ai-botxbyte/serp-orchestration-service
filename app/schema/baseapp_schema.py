from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import UUID
from pydantic import BaseModel, model_validator

class BaseAppSchema(BaseModel):
    """Base schema with common functionality that can be reused by all entities."""

    model_config = dict(populate_by_name=True, extra="forbid")

    @model_validator(mode='before')
    @classmethod
    def strip_strings(cls, values: Any) -> Any:
        """Strip whitespace from all string fields."""
        # If values is not a dict (e.g., SQLAlchemy object), return as-is
        if not isinstance(values, dict):
            return values
            
        for field, value in values.items():
            if isinstance(value, str):
                values[field] = value.strip()
        return values

class TimestampSchema(BaseModel):
    """Tracks when a record was created and last updated."""

    created_at: datetime
    updated_at: Optional[datetime] = None


class SoftDeleteSchema(BaseModel):
    """Represents soft deletion metadata."""

    deleted_at: Optional[datetime] = None
    deleted_by: Optional[UUID] = None
