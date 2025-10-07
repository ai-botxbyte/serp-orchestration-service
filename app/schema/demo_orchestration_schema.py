from __future__ import annotations
from typing import  Dict
from pydantic import ConfigDict, Field


from app.schema.baseapp_schema import (
    BaseAppSchema
)



class DemoOrchestrationMessageSchema(BaseAppSchema):
    """Schema for demo orchestration message."""
    data: Dict = Field(..., description="Data")

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)