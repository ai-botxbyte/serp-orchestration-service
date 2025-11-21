from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path

from fastapi import HTTPException, status

from alembic.config import Config
from alembic.script import ScriptDirectory

from app.config.baseapp_config import get_base_config
from app.config.database import async_session_local
from app.config.logger_config import logger
from app.helper.migration_helper import MigrationHelper
from app.helper.path_helper import PathHelper
from app.repository.migration_repository import MigrationRepository
from app.schema.migration_schema import MigrationResponseSchema


class MigrationService:
    """Service for handling database migrations."""

    def __init__(self):
        self.base_config = get_base_config()
        self.alembic_cfg = self._get_alembic_config()
        self.migration_helper = MigrationHelper()

    def _get_alembic_config(self) -> Config:
        """Get Alembic configuration."""
        try:
            project_root = PathHelper.find_project_root(Path(__file__))
        except FileNotFoundError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Project root not found: {e}",
            ) from e

        alembic_ini_path = project_root / "alembic.ini"
        if not alembic_ini_path.exists():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Alembic configuration file not found: {alembic_ini_path}",
            )

        alembic_dir = project_root / "alembic"
        if not alembic_dir.exists():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Alembic directory not found: {alembic_dir}",
            )

        (alembic_dir / "versions").mkdir(exist_ok=True)

        alembic_cfg = Config(str(alembic_ini_path))
        database_url = get_base_config().ASYNC_DATABASE_URL
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        script_location = alembic_cfg.get_main_option("script_location")
        if not script_location:
            alembic_cfg.set_main_option("script_location", "alembic")

        return alembic_cfg

    async def _run_alembic_command(self, command_name: str, **kwargs):
        """Run an Alembic command as a subprocess to avoid event loop conflicts."""
        project_root = PathHelper.find_project_root(Path(__file__))

        cmd = ["alembic", command_name]
        if command_name == "revision":
            if kwargs.get("autogenerate"):
                cmd.append("--autogenerate")
            if "message" in kwargs:
                cmd.extend(["-m", kwargs["message"]])
        elif command_name == "upgrade":
            cmd.append(kwargs.get("revision", "head"))

        def run_command_in_subprocess():
            result = subprocess.run(
                cmd,
                cwd=str(project_root),
                capture_output=True,
                text=True,
                check=False,
                env=os.environ,
            )
            if result.returncode != 0:
                logger.error(f"Alembic command failed: {cmd}")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Alembic command failed: {result.stderr}",
                )
            logger.info(f"Alembic command successful: {cmd}")
            logger.info(f"STDOUT: {result.stdout}")
            return result

        await asyncio.to_thread(run_command_in_subprocess)

    async def create_revision(self, message: str) -> MigrationResponseSchema:
        """Create a new Alembic revision."""
        if not self.base_config.IS_POSTGRES_ENABLED:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="PostgreSQL is disabled. Set IS_POSTGRES_ENABLED=True.",
            )

        # Check current revision in its own session scope
        current_db_rev = None
        async with async_session_local() as check_db_session:
            check_migration_repo = MigrationRepository(check_db_session)
            current_db_rev = await check_migration_repo.get_current_revision()

        # Validate the revision exists in local files (outside session scope)
        if current_db_rev:
            script = ScriptDirectory.from_config(self.alembic_cfg)
            try:
                script.get_revision(current_db_rev)
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"DB rev '{current_db_rev}' not in local files.",
                ) from e

        # Run alembic command (no database session needed here)
        await self._run_alembic_command("revision", autogenerate=True, message=message)

        # Get the new revision in a fresh session
        async with async_session_local() as new_db_session:
            new_migration_repo = MigrationRepository(new_db_session)
            script = ScriptDirectory.from_config(self.alembic_cfg)
            head_revision = script.get_current_head()
            current_rev = await new_migration_repo.get_current_revision()

            return MigrationResponseSchema(
                operation="revision",
                success=True,
                message=f"Revision created successfully with message: '{message}'",
                revision_id=head_revision,
                current_revision=current_rev,
            )

    async def upgrade_database(self) -> MigrationResponseSchema:
        """Upgrade database to the latest revision."""
        if not self.base_config.IS_POSTGRES_ENABLED:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="PostgreSQL is disabled. Set IS_POSTGRES_ENABLED=True.",
            )

        project_root = PathHelper.find_project_root(Path(__file__))
        versions_dir = project_root / "alembic" / "versions"
        if not any(versions_dir.glob("*.py")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No migration files found. Please create a migration first.",
            )

        await self._run_alembic_command("upgrade", revision="head")

        async with async_session_local() as new_db_session:
            new_migration_repo = MigrationRepository(new_db_session)
            current_rev = await new_migration_repo.get_current_revision()
            return MigrationResponseSchema(
                operation="upgrade",
                success=True,
                message="Database upgraded successfully",
                current_revision=current_rev,
            )

    async def upload_migrations(self) -> MigrationResponseSchema:
        """Upload migration files to Wasabi/S3."""
        if not self.migration_helper.wasabi_helper.s3_client:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Wasabi/S3 not configured.",
            )

        project_root = PathHelper.find_project_root(Path(__file__))
        migrations_path = project_root / "alembic" / "versions"
        if not migrations_path.exists() or not any(migrations_path.glob("*.py")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No migration files found in {migrations_path}",
            )

        success = await asyncio.get_event_loop().run_in_executor(
            None, self.migration_helper.upload_migrations, migrations_path, None
        )
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to upload migrations to Wasabi",
            )

        zip_filename = f"{self.base_config.SERVICE_NAME}-migrations.zip"
        return MigrationResponseSchema(
            operation="upload",
            success=True,
            message="Migration files uploaded successfully",
            file_count=len(list(migrations_path.glob("*.py"))),
            location=f"migrations/latest/{zip_filename}",
        )

    async def download_migrations(self) -> MigrationResponseSchema:
        """Download migration files from Wasabi/S3."""
        if not self.migration_helper.wasabi_helper.s3_client:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Wasabi/S3 not configured.",
            )

        migrations_path = (
            PathHelper.find_project_root(Path(__file__)) / "alembic" / "versions"
        )
        success = await asyncio.get_event_loop().run_in_executor(
            None, self.migration_helper.download_migrations, migrations_path, None
        )
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to download migrations from Wasabi",
            )

        return MigrationResponseSchema(
            operation="download",
            success=True,
            message="Migration files downloaded successfully",
            file_count=len(list(migrations_path.glob("*.py"))),
            location=str(migrations_path),
        )
