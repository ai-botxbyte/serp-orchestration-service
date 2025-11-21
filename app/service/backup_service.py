"""Service for handling database backup and restore operations."""

from __future__ import annotations
import asyncio
from typing import Optional

from fastapi import HTTPException, status

from app.config.baseapp_config import get_base_config
from app.config.logger_config import logger
from app.helper.backup_helper import BackupHelper
from app.schema.backup_schema import (
    BackupCreateResponseSchema,
    BackupListResponseSchema,
    BackupRestoreResponseSchema,
    BackupInfoSchema,
)


class BackupService:
    """Service for handling database backup and restore operations."""

    def __init__(self):
        self.base_config = get_base_config()
        self.backup_helper = BackupHelper()

    async def create_backup(
        self, backup_name: Optional[str] = None, description: Optional[str] = None
    ) -> BackupCreateResponseSchema:
        """
        Create a database backup and upload to Wasabi.

        Args:
            backup_name: Optional custom backup name
            description: Optional backup description

        Returns:
            BackupCreateResponseSchema with backup details
        """
        if not self.base_config.IS_POSTGRES_ENABLED:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="PostgreSQL is disabled. Set IS_POSTGRES_ENABLED=True.",
            )

        if not self.backup_helper.wasabi_helper.s3_client:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Wasabi/S3 not configured for backups. Set BACKUP_BUCKET_NAME.",
            )

        try:
            # Run backup in executor to avoid blocking
            backup_metadata = await asyncio.get_event_loop().run_in_executor(
                None, self.backup_helper.create_backup, backup_name, description
            )

            return BackupCreateResponseSchema(**backup_metadata)

        except Exception as e:
            logger.error(f"Backup creation failed: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create backup: {str(e)}",
            ) from e

    async def list_backups(self) -> BackupListResponseSchema:
        """
        List all available backups from Wasabi.

        Returns:
            BackupListResponseSchema with list of backups
        """
        if not self.backup_helper.wasabi_helper.s3_client:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Wasabi/S3 not configured for backups. Set BACKUP_BUCKET_NAME.",
            )

        try:
            # Run list operation in executor
            backups_list = await asyncio.get_event_loop().run_in_executor(
                None, self.backup_helper.list_backups
            )

            # Convert to schema objects
            backup_schemas = [BackupInfoSchema(**backup) for backup in backups_list]

            return BackupListResponseSchema(
                backups=backup_schemas, count=len(backup_schemas)
            )

        except Exception as e:
            logger.error(f"Failed to list backups: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list backups: {str(e)}",
            ) from e

    async def restore_backup(self, backup_filename: str) -> BackupRestoreResponseSchema:
        """
        Restore database from a backup file.

        Args:
            backup_filename: Name of the backup ZIP file to restore

        Returns:
            BackupRestoreResponseSchema with restore details
        """
        if not self.base_config.IS_POSTGRES_ENABLED:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="PostgreSQL is disabled. Set IS_POSTGRES_ENABLED=True.",
            )

        if not self.backup_helper.wasabi_helper.s3_client:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Wasabi/S3 not configured for backups. Set BACKUP_BUCKET_NAME.",
            )

        try:
            # Run restore in executor to avoid blocking
            restore_metadata = await asyncio.get_event_loop().run_in_executor(
                None, self.backup_helper.restore_backup, backup_filename
            )

            return BackupRestoreResponseSchema(**restore_metadata)

        except Exception as e:
            logger.error(f"Backup restore failed: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to restore backup: {str(e)}",
            ) from e
