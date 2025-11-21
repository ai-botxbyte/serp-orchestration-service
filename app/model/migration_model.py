# pylint: disable= model-must-inherit-baseappmodel, primary-key-uuid-required
from sqlalchemy import Column, String
from app.model.baseapp_model import Base


class MigrationModel(Base):
    """Migration model."""

    __tablename__ = "alembic_version"
    version_num = Column(String(32), primary_key=True)
