from __future__ import annotations
from typing import  Dict, Optional
from pydantic import ConfigDict, Field, field_validator


from app.schema.baseapp_schema import (
    BaseAppSchema
)



class DemoOrchestrationMessageSchema(BaseAppSchema):
    """
    Schema for demo orchestration message with job routing support.
    
    The job_type field determines which job handler processes the message:
    - For demo_A_consumer: 'job1' routes to DemoA1Job, 'job2' routes to DemoA2Job
    - For demo_B_consumer: 'job1' routes to DemoB1Job, 'job2' routes to DemoB2Job
    """
    job_type: str = Field(..., description="Job type for routing (e.g., 'job1', 'job2')")
    data: Dict = Field(..., description="Job-specific data payload")
    trace_id: Optional[str] = Field(default=None, description="Trace ID for request tracking")
    
    @field_validator('job_type')
    @classmethod
    def validate_job_type(cls, v: str) -> str:
        """Validate that job_type is one of the supported types."""
        allowed_types = ['job1', 'job2']
        if v not in allowed_types:
            raise ValueError(f"job_type must be one of {allowed_types}, got: {v}")
        return v

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)