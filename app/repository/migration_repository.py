from __future__ import annotations
from sqlalchemy import MetaData, Table, inspect, text, Column, String
from sqlalchemy.exc import SQLAlchemyError

from app.exception.baseapp_exception import InternalServerErrorException
from app.repository.baseapp_repository import BaseAppRepository
from app.model.migration_model import MigrationModel


class MigrationRepository(BaseAppRepository[MigrationModel]):
    """Repository for migration-related database operations."""

    def __init__(self, db):
        """
        Initialize migration repository.

        Args:
            db: The asynchronous database session.
        """
        super().__init__(db, MigrationModel)
        self.db = db

        # Define alembic_version table structure
        self.metadata = MetaData()
        self.alembic_version_table = Table(
            "alembic_version",
            self.metadata,
            Column("version_num", String, primary_key=True),
            schema="public",
        )

    async def check_alembic_version_table_exists(self) -> bool:
        """Check if the alembic_version table exists."""
        try:
            async with self.db.begin():
                conn = await self.db.connection()
                inspector = inspect(conn.sync_connection)
                return inspector.has_table(self.alembic_version_table.name)
        except SQLAlchemyError:
            return False

    async def get_current_revision(self) -> str:
        """Get the current revision of the database."""
        try:
            if not await self.check_alembic_version_table_exists():
                return ""

            async with self.db.begin():
                query = text(
                    f"SELECT version_num FROM {self.alembic_version_table.name}"
                )
                result = await self.db.execute(query)
                revision = result.scalar_one_or_none()
                return revision or ""
        except SQLAlchemyError as e:
            raise InternalServerErrorException(
                f"Error getting current revision: {e}"
            ) from e
