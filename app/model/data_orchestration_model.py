"""Data Orchestration Model"""

import uuid
from sqlalchemy import Column, String, Boolean, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.model.baseapp_model import BaseAppModel


class DataOrchestrationModel(BaseAppModel):
    """Model for storing data orchestration job status."""
    
    __tablename__ = "data_orchestration"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_queue = Column(String(255), nullable=False)
    source_job = Column(String(255), nullable=False)
    trace_id = Column(String(255), nullable=True)
    job_completed = Column(Boolean, default=False, nullable=False)
    error_message = Column(Text, nullable=True)
    metadata_info = Column(JSONB, nullable=True)
