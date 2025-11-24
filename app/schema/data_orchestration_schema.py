"""Schema for data orchestration queue messages"""

from __future__ import annotations
from typing import Dict, Any, Optional
from pydantic import Field

from app.schema.baseapp_schema import BaseAppSchema


class DataOrchestrationMessageSchema(BaseAppSchema):
    """Schema for job status messages from orchestration consumers"""
    
    source_queue: str = Field(..., description="Source queue name (e.g., demo_A_queue, demo_B_queue)")
    source_job: str = Field(..., description="Source job name (e.g., DemoA1Job, DemoB1Job)")
    trace_id: Optional[str] = Field(default=None, description="Trace ID for request tracking")
    # pylint: disable-next=boolean-field-rule
    job_completed: bool = Field(default=True, description="Whether the job completed successfully")
    error_message: Optional[str] = Field(default=None, description="Error message if job failed")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Optional metadata")    
