"""Migration-specific helper using WasabiHelper for migration file operations."""

from __future__ import annotations
from pathlib import Path
from typing import Optional, List
from botocore.exceptions import ClientError, NoCredentialsError

from app.config.config import config
from app.config.logger_config import logger
from app.helper.wasabi_helper import WasabiHelper


class MigrationHelper:
    """Helper for migration file operations using Wasabi/S3 storage."""

    def __init__(self):
        """Initialize migration helper with Wasabi configuration."""
        self.service_name = config.SERVICE_NAME
        self.migration_zip_name = f"{self.service_name}-migrations.zip"

        # Initialize Wasabi helper with migration-specific config
        self.wasabi_helper = WasabiHelper(
            bucket_name=config.MIGRATION_BUCKET_NAME,
            endpoint_url=config.WASABI_ENDPOINT_URL,
            aws_access_key_id=config.WASABI_ACCESS_KEY_ID,
            aws_secret_access_key=config.WASABI_SECRET_ACCESS_KEY,
            region=config.WASABI_REGION,
        )

        if self.wasabi_helper.s3_client:
            logger.info("Migration helper initialized with Wasabi/S3")
        else:
            logger.warning(
                "Migration helper not configured. "
                "Set WASABI_ACCESS_KEY_ID, WASABI_SECRET_ACCESS_KEY, and MIGRATION_BUCKET_NAME "
                "to enable migration syncing."
            )

    def _is_configured(self) -> bool:
        """Check if Wasabi/S3 is properly configured."""
        return bool(self.wasabi_helper.s3_client)

    def upload_migrations(
        self, local_path: Path, version: Optional[str] = None
    ) -> bool:
        """
        Upload migration files to Wasabi/S3.

        Args:
            local_path: Local directory path containing migration files
            version: Optional version tag. If None, uses 'latest'

        Returns:
            True if upload was successful, False otherwise
        """
        if not local_path.exists():
            logger.error(f"Migration directory does not exist: {local_path}")
            return False

        # Get all migration files
        migration_files = list(local_path.glob("*.py"))
        if not migration_files:
            logger.warning(f"No migration files found in {local_path}")
            return False

        try:
            # Upload to both latest and versioned locations if version provided
            keys_to_upload = [f"migrations/latest/{self.migration_zip_name}"]
            if version:
                keys_to_upload.append(f"migrations/{version}/{self.migration_zip_name}")

            success_count = 0
            for key in keys_to_upload:
                upload_success = self.wasabi_helper.upload_folder_as_zip(
                    local_folder_path=local_path,
                    s3_key=key,
                    zip_filename=self.migration_zip_name,
                    file_pattern="*.py",
                )
                if upload_success:
                    success_count += 1

            if success_count > 0:
                logger.info(
                    f"Successfully uploaded {len(migration_files)} migration files to Wasabi "
                    f"(version: {version or 'latest'})"
                )
                return True

            logger.error("Failed to upload migrations to any location")
            return False

        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Error uploading migrations: {str(e)}", exc_info=True)
            return False

    def download_migrations(
        self, local_path: Path, version: Optional[str] = None
    ) -> bool:
        """
        Download migration files from Wasabi/S3.

        Args:
            local_path: Local directory path to store migrations
            version: Optional version tag to download specific version.
                    If None, downloads 'latest'

        Returns:
            True if download was successful, False otherwise
        """
        try:
            # Determine S3 key based on version
            if version:
                key = f"migrations/{version}/{self.migration_zip_name}"
            else:
                key = f"migrations/latest/{self.migration_zip_name}"

            logger.info(f"Downloading migrations from Wasabi: {key}")

            # Download and extract zip file
            success = self.wasabi_helper.download_and_extract_zip(
                s3_key=key,
                local_folder_path=local_path,
                zip_filename=self.migration_zip_name,
            )

            if success:
                # Count extracted files
                migration_files = list(local_path.glob("*.py"))
                logger.info(
                    f"Successfully downloaded {len(migration_files)} migration files "
                    f"from Wasabi to {local_path}"
                )

            return success

        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Error downloading migrations: {str(e)}", exc_info=True)
            return False

    def ensure_migrations_exist(self, local_path: Path) -> bool:
        """
        Ensure migration files exist locally, download from Wasabi if missing.

        Args:
            local_path: Local directory path to check/store migrations

        Returns:
            True if migrations exist locally or were successfully downloaded, False otherwise
        """
        # Check if migrations directory exists and has Python files
        if local_path.exists():
            migration_files = list(local_path.glob("*.py"))
            if migration_files:
                logger.info(
                    f"Migrations already exist locally at {local_path} "
                    f"({len(migration_files)} files)"
                )
                return True

        # Migrations don't exist locally, try to download from Wasabi
        logger.info(
            f"Migrations not found locally at {local_path}, "
            "attempting to download from Wasabi..."
        )

        if self.download_migrations(local_path):
            return True

        # Download failed
        logger.error(
            f"Failed to download migrations from Wasabi. Please ensure migrations "
            f"are uploaded to Wasabi bucket: {self.wasabi_helper.bucket_name}"
        )
        return False

    def list_available_versions(self) -> List[str]:
        """
        List all available migration versions in Wasabi.

        Returns:
            List of version strings available in Wasabi
        """
        try:
            # Get all prefixes under migrations/
            prefixes = self.wasabi_helper.list_prefixes(
                prefix="migrations/", delimiter="/"
            )

            # Filter to only include actual version directories (not 'latest')
            versions = [p for p in prefixes if p and p != "latest"]

            return sorted(versions)
        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Error listing migration versions: {str(e)}")
            return []
