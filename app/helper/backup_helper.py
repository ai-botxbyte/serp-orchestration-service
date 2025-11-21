"""Backup helper for database backup and restore operations using Wasabi/S3 storage."""

from __future__ import annotations
import os
import subprocess
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urlparse
from botocore.exceptions import ClientError, NoCredentialsError

from app.config.config import config
from app.config.logger_config import logger
from app.helper.wasabi_helper import WasabiHelper


class BackupHelper:
    """Helper for database backup and restore operations using Wasabi/S3 storage."""

    def __init__(self):
        """Initialize backup helper with Wasabi configuration."""
        self.service_name = config.SERVICE_NAME

        # Initialize Wasabi helper with backup-specific config
        self.wasabi_helper = WasabiHelper(
            bucket_name=config.BACKUP_BUCKET_NAME,
            endpoint_url=config.WASABI_ENDPOINT_URL,
            aws_access_key_id=config.WASABI_ACCESS_KEY_ID,
            aws_secret_access_key=config.WASABI_SECRET_ACCESS_KEY,
            region=config.WASABI_REGION,
        )

        if self.wasabi_helper.s3_client:
            logger.info("Backup helper initialized with Wasabi/S3")
        else:
            logger.warning(
                "Backup helper not configured. "
                "Set WASABI_ACCESS_KEY_ID, WASABI_SECRET_ACCESS_KEY, and BACKUP_BUCKET_NAME "
                "to enable backup operations."
            )

    def _is_configured(self) -> bool:
        """Check if Wasabi/S3 is properly configured."""
        return bool(self.wasabi_helper.s3_client)

    def _parse_database_url(self, db_url: str) -> Dict[str, str]:
        """
        Parse database URL into components.

        Args:
            db_url: Database URL (e.g., postgresql+asyncpg://user:pass@host:port/dbname)

        Returns:
            Dictionary with host, port, user, password, and database
        """
        # Remove asyncpg or other async drivers from the scheme
        db_url = db_url.replace("+asyncpg", "").replace("+psycopg", "")

        parsed = urlparse(db_url)

        return {
            "host": parsed.hostname or "localhost",
            "port": str(parsed.port) if parsed.port else "5432",
            "user": parsed.username or "postgres",
            "password": parsed.password or "",
            "database": parsed.path.lstrip("/") if parsed.path else "postgres",
        }

    def create_backup(  # pylint: disable=too-many-locals
        self, backup_name: Optional[str] = None, description: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Create a database backup, compress it to .sql.zip, and upload to Wasabi.

        Args:
            backup_name: Optional custom backup name. If None, uses timestamp
            description: Optional description for the backup

        Returns:
            Dictionary with backup details including filename, size, and location
        """
        if not self._is_configured():
            raise Exception("Wasabi/S3 not configured for backups")  # pylint: disable=broad-exception-raised

        # Parse database connection info
        db_info = self._parse_database_url(config.ASYNC_DATABASE_URL)

        # Generate backup filename
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        if backup_name:
            # Sanitize backup name (remove special characters)
            safe_name = "".join(
                c for c in backup_name if c.isalnum() or c in ("-", "_")
            )
            sql_filename = f"{self.service_name}_{safe_name}_{timestamp}.sql"
        else:
            sql_filename = f"{self.service_name}_{timestamp}.sql"

        zip_filename = f"{sql_filename}.zip"

        # Create temporary directory for backup files
        temp_dir = Path("/tmp/db_backups")
        temp_dir.mkdir(parents=True, exist_ok=True)

        sql_path = temp_dir / sql_filename
        zip_path = temp_dir / zip_filename

        try:
            # Create PostgreSQL backup using pg_dump
            logger.info(f"Creating database backup: {db_info['database']}")

            # Set password via environment variable for pg_dump
            env = os.environ.copy()
            env["PGPASSWORD"] = db_info["password"]

            # Run pg_dump command
            # Note: Using --no-sync for better compatibility and performance
            dump_cmd = [
                "pg_dump",
                "-h",
                db_info["host"],
                "-p",
                db_info["port"],
                "-U",
                db_info["user"],
                "-d",
                db_info["database"],
                "-F",
                "p",  # Plain text SQL format
                "--no-sync",  # Don't wait for output to be written to disk
                "-f",
                str(sql_path),
            ]

            result = subprocess.run(
                dump_cmd, env=env, capture_output=True, text=True, check=False
            )

            if result.returncode != 0:
                logger.error(f"pg_dump failed: {result.stderr}")

                # Check for version mismatch and provide helpful message
                if (
                    "server version" in result.stderr
                    and "pg_dump version" in result.stderr
                ):
                    raise Exception(  # pylint: disable=broad-exception-raised
                        f"PostgreSQL version mismatch. Server and pg_dump versions "
                        f"must be compatible.\n"
                        f"Error: {result.stderr}\n\n"
                        f"To fix this on macOS, upgrade PostgreSQL client:\n"
                        f"  brew upgrade postgresql@17\n"
                        f"  brew link postgresql@17 --force"
                    )

                raise Exception(f"Database backup failed: {result.stderr}")  # pylint: disable=broad-exception-raised

            if not sql_path.exists():
                raise Exception("Backup file was not created")  # pylint: disable=broad-exception-raised

            # Get SQL file size
            sql_size = sql_path.stat().st_size
            logger.info(
                f"Backup created successfully: {sql_filename} ({sql_size} bytes)"
            )

            # Compress to ZIP
            logger.info(f"Compressing backup to {zip_filename}...")
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(sql_path, sql_filename)

            # Get ZIP file size
            zip_size = zip_path.stat().st_size
            logger.info(f"Backup compressed: {zip_filename} ({zip_size} bytes)")

            # Upload to Wasabi
            s3_key = f"backups/{self.service_name}/{zip_filename}"
            logger.info(f"Uploading backup to Wasabi: {s3_key}")

            upload_success = self.wasabi_helper.upload_file(zip_path, s3_key)

            if not upload_success:
                raise Exception("Failed to upload backup to Wasabi")  # pylint: disable=broad-exception-raised

            # Return metadata for API response
            metadata = {
                "backup_name": backup_name or f"auto_backup_{timestamp}",
                "description": description,
                "timestamp": timestamp,
                "database": db_info["database"],
                "zip_filename": zip_filename,
                "zip_size": zip_size,
                "s3_key": s3_key,
            }

            logger.info(f"Backup uploaded successfully: {s3_key}")

            return metadata

        except (ClientError, NoCredentialsError, subprocess.CalledProcessError) as e:
            logger.error(f"Error creating backup: {str(e)}", exc_info=True)
            raise
        finally:
            # Cleanup temporary files
            if sql_path.exists():
                sql_path.unlink()
            if zip_path.exists():
                zip_path.unlink()

    def list_backups(self) -> List[Dict[str, any]]:
        """
        List all available backups from Wasabi.

        Returns:
            List of backup metadata dictionaries
        """
        if not self._is_configured():
            raise Exception("Wasabi/S3 not configured for backups")  # pylint: disable=broad-exception-raised

        try:
            prefix = f"backups/{self.service_name}/"
            logger.info(f"Listing backups from Wasabi: {prefix}")

            # List all backup files
            backup_keys = self.wasabi_helper.list_objects(prefix=prefix)

            backups = []
            for key in backup_keys:
                if key.endswith(".zip"):
                    # Extract filename
                    filename = os.path.basename(key)

                    # Get file metadata from S3
                    try:
                        response = self.wasabi_helper.s3_client.head_object(
                            Bucket=self.wasabi_helper.bucket_name, Key=key
                        )

                        backups.append(
                            {
                                "filename": filename,
                                "key": key,
                                "size": response.get("ContentLength", 0),
                                "last_modified": (
                                    response.get("LastModified").isoformat()
                                    if response.get("LastModified")
                                    else None
                                ),
                            }
                        )
                    except ClientError as e:
                        logger.warning(f"Could not get metadata for {key}: {e}")
                        continue

            # Sort by last modified date (newest first)
            backups.sort(key=lambda x: x["last_modified"] or "", reverse=True)

            logger.info(f"Found {len(backups)} backup(s)")
            return backups

        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Error listing backups: {str(e)}", exc_info=True)
            raise

    def restore_backup(self, backup_filename: str) -> Dict[str, any]:
        """
        Restore a database from a backup file.

        Args:
            backup_filename: Name of the backup ZIP file to restore

        Returns:
            Dictionary with restore operation details
        """
        if not self._is_configured():
            raise Exception("Wasabi/S3 not configured for backups")  # pylint: disable=broad-exception-raised

        # Parse database connection info
        db_info = self._parse_database_url(config.ASYNC_DATABASE_URL)

        # Construct S3 key
        s3_key = f"backups/{self.service_name}/{backup_filename}"

        # Create temporary directory
        temp_dir = Path("/tmp/db_backups")
        temp_dir.mkdir(parents=True, exist_ok=True)

        zip_path = temp_dir / backup_filename

        try:
            # Download backup from Wasabi
            logger.info(f"Downloading backup from Wasabi: {s3_key}")
            download_success = self.wasabi_helper.download_file(s3_key, zip_path)

            if not download_success:
                raise Exception(f"Failed to download backup: {backup_filename}")  # pylint: disable=broad-exception-raised

            # Extract SQL file from ZIP
            logger.info(f"Extracting backup: {backup_filename}")
            with zipfile.ZipFile(zip_path, "r") as zipf:
                # Get the first SQL file from the archive
                sql_files = [f for f in zipf.namelist() if f.endswith(".sql")]
                if not sql_files:
                    raise Exception("No SQL file found in backup archive")  # pylint: disable=broad-exception-raised

                sql_filename = sql_files[0]
                zipf.extract(sql_filename, temp_dir)

            sql_path = temp_dir / sql_filename

            # Restore database using psql
            logger.info(f"Restoring database: {db_info['database']}")

            # Set password via environment variable
            env = os.environ.copy()
            env["PGPASSWORD"] = db_info["password"]

            # Run psql command to restore
            restore_cmd = [
                "psql",
                "-h",
                db_info["host"],
                "-p",
                db_info["port"],
                "-U",
                db_info["user"],
                "-d",
                db_info["database"],
                "-f",
                str(sql_path),
            ]

            result = subprocess.run(
                restore_cmd, env=env, capture_output=True, text=True, check=False
            )

            if result.returncode != 0:
                logger.error(f"Database restore failed: {result.stderr}")
                raise Exception(f"Database restore failed: {result.stderr}")  # pylint: disable=broad-exception-raised

            logger.info(f"Database restored successfully from {backup_filename}")

            return {
                "backup_filename": backup_filename,
                "database": db_info["database"],
                "restored_at": datetime.utcnow().isoformat(),
                "success": True,
            }

        except (
            ClientError,
            NoCredentialsError,
            subprocess.CalledProcessError,
            zipfile.BadZipFile,
        ) as e:
            logger.error(f"Error restoring backup: {str(e)}", exc_info=True)
            raise
        finally:
            # Cleanup temporary files
            if zip_path.exists():
                zip_path.unlink()
            if "sql_path" in locals() and sql_path.exists():
                sql_path.unlink()
