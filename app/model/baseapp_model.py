from datetime import datetime
from sqlalchemy import Column, String, DateTime, Boolean, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BaseAppModel(Base):
    """Base model for all models."""

    __abstract__ = True

    workspace_id = Column(UUID(as_uuid=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    created_by = Column(UUID(as_uuid=True), nullable=False)
    updated_by = Column(UUID(as_uuid=True), nullable=True)
    deleted_by = Column(UUID(as_uuid=True), nullable=True)
    status = Column(String(20), nullable=False, default="created")
    is_active = Column(Boolean, nullable=False, default=True)
    error_message = Column(Text, nullable=True)
    error_user_message = Column(Text, nullable=True)
