"""Reusable Wasabi/S3 storage helper for file and folder operations."""

from __future__ import annotations
import os
import zipfile
from pathlib import Path
from typing import Optional, List
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from app.config.logger_config import logger


class WasabiHelper:
    """Reusable helper for Wasabi/S3 file and folder operations."""

    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region: str = "us-east-1",
    ):
        """
        Initialize Wasabi/S3 helper.

        Args:
            bucket_name: S3 bucket name
            endpoint_url: Wasabi/S3 endpoint URL
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            region: AWS region (default: us-east-1)
        """
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region
        self.s3_client = None

        # Initialize S3 client if credentials are provided
        if self._is_configured():
            try:
                self.s3_client = boto3.client(
                    "s3",
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region,
                )
                logger.info("Wasabi/S3 helper initialized successfully")
            except (ClientError, NoCredentialsError) as e:
                logger.warning(f"Failed to initialize S3 client: {str(e)}")
        else:
            logger.warning(
                "Wasabi/S3 helper not configured. "
                "Provide bucket_name, endpoint_url, aws_access_key_id, and aws_secret_access_key."
            )

    def _is_configured(self) -> bool:
        """Check if Wasabi/S3 is properly configured."""
        return bool(
            self.bucket_name and self.aws_access_key_id and self.aws_secret_access_key
        )

    def upload_file(self, local_file_path: Path, s3_key: str) -> bool:
        """
        Upload a single file to Wasabi/S3.

        Args:
            local_file_path: Path to local file to upload
            s3_key: S3 key (path) where file should be stored

        Returns:
            True if upload was successful, False otherwise
        """
        if not self.s3_client:
            logger.warning("S3 client not configured, skipping file upload")
            return False

        if not local_file_path.exists():
            logger.error(f"File does not exist: {local_file_path}")
            return False

        try:
            logger.info(f"Uploading file to Wasabi: {self.bucket_name}/{s3_key}")
            self.s3_client.upload_file(str(local_file_path), self.bucket_name, s3_key)
            logger.info(f"Successfully uploaded file: {s3_key}")
            return True
        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Error uploading file: {str(e)}", exc_info=True)
            return False

    def download_file(self, s3_key: str, local_file_path: Path) -> bool:
        """
        Download a single file from Wasabi/S3.

        Args:
            s3_key: S3 key (path) of file to download
            local_file_path: Local path where file should be saved

        Returns:
            True if download was successful, False otherwise
        """
        if not self.s3_client:
            logger.warning("S3 client not configured, skipping file download")
            return False

        try:
            # Ensure parent directory exists
            local_file_path.parent.mkdir(parents=True, exist_ok=True)

            logger.info(f"Downloading file from Wasabi: {self.bucket_name}/{s3_key}")
            self.s3_client.download_file(self.bucket_name, s3_key, str(local_file_path))
            logger.info(f"Successfully downloaded file: {local_file_path}")
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "NoSuchKey":
                logger.warning(f"File not found in Wasabi: {s3_key}")
            else:
                logger.error(f"Failed to download file from Wasabi: {str(e)}")
            return False
        except NoCredentialsError:
            logger.error("Wasabi credentials not found or invalid")
            return False

    def upload_folder_as_zip(
        self,
        local_folder_path: Path,
        s3_key: str,
        zip_filename: Optional[str] = None,
        file_pattern: str = "*",
    ) -> bool:
        """
        Upload a folder as a zip file to Wasabi/S3.

        Args:
            local_folder_path: Path to local folder to zip and upload
            s3_key: S3 key (path) where zip file should be stored
            zip_filename: Optional zip filename (default: folder name + .zip)
            file_pattern: Pattern to match files (default: "*" for all files)

        Returns:
            True if upload was successful, False otherwise
        """
        if not self.s3_client:
            logger.warning("S3 client not configured, skipping folder upload")
            return False

        if not local_folder_path.exists():
            logger.error(f"Folder does not exist: {local_folder_path}")
            return False

        # Get all files matching pattern
        files_to_zip = list(local_folder_path.glob(file_pattern))
        if not files_to_zip:
            logger.warning(
                f"No files found in {local_folder_path} matching pattern: {file_pattern}"
            )
            return False

        # Determine zip filename
        if not zip_filename:
            zip_filename = f"{local_folder_path.name}.zip"

        # Create zip file in parent directory
        zip_path = local_folder_path.parent / zip_filename

        try:
            logger.info(
                f"Creating zip archive '{zip_filename}' with {len(files_to_zip)} files..."
            )
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for file_path in files_to_zip:
                    if file_path.is_file():
                        # Preserve directory structure relative to local_folder_path
                        arcname = file_path.relative_to(local_folder_path)
                        zipf.write(file_path, arcname)

            # Upload zip file
            upload_success = self.upload_file(zip_path, s3_key)

            # Clean up zip file
            if zip_path.exists():
                zip_path.unlink()

            if upload_success:
                logger.info(
                    f"Successfully uploaded {len(files_to_zip)} files as zip to Wasabi: {s3_key}"
                )

            return upload_success

        except (ClientError, NoCredentialsError, zipfile.BadZipFile) as e:
            logger.error(f"Error creating/uploading zip: {str(e)}", exc_info=True)
            # Clean up zip file if it exists
            if zip_path.exists():
                zip_path.unlink()
            return False

    def download_and_extract_zip(
        self, s3_key: str, local_folder_path: Path, zip_filename: Optional[str] = None
    ) -> bool:
        """
        Download a zip file from Wasabi/S3 and extract it to a local folder.

        Args:
            s3_key: S3 key (path) of zip file to download
            local_folder_path: Local folder path where files should be extracted
            zip_filename: Optional zip filename (default: extracted from s3_key)

        Returns:
            True if download and extraction was successful, False otherwise
        """
        if not self.s3_client:
            logger.warning("S3 client not configured, skipping zip download")
            return False

        # Determine zip filename
        if not zip_filename:
            zip_filename = os.path.basename(s3_key) or "downloaded.zip"

        # Create local directory
        local_folder_path.mkdir(parents=True, exist_ok=True)
        zip_path = local_folder_path.parent / zip_filename

        try:
            # Download zip file
            download_success = self.download_file(s3_key, zip_path)
            if not download_success:
                return False

            # Extract zip file
            logger.info(f"Extracting zip file to {local_folder_path}...")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(local_folder_path)

            # Clean up zip file
            if zip_path.exists():
                zip_path.unlink()

            # Count extracted files
            extracted_files = list(local_folder_path.glob("*"))
            logger.info(
                f"Successfully extracted {len(extracted_files)} files "
                f"from zip to {local_folder_path}"
            )
            return True

        except (ClientError, NoCredentialsError, zipfile.BadZipFile) as e:
            logger.error(f"Error downloading/extracting zip: {str(e)}", exc_info=True)
            # Clean up zip file if it exists
            if zip_path.exists():
                zip_path.unlink()
            return False

    def list_objects(
        self, prefix: str = "", delimiter: Optional[str] = None
    ) -> List[str]:
        """
        List objects in Wasabi/S3 bucket.

        Args:
            prefix: Prefix to filter objects (default: "")
            delimiter: Delimiter for grouping (e.g., "/" for folders)

        Returns:
            List of object keys
        """
        if not self.s3_client:
            return []

        try:
            objects = []
            paginator = self.s3_client.get_paginator("list_objects_v2")

            # Build pagination params - only include Delimiter if it's not None
            pagination_params = {"Bucket": self.bucket_name, "Prefix": prefix}
            if delimiter is not None:
                pagination_params["Delimiter"] = delimiter

            pages = paginator.paginate(**pagination_params)

            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects.append(obj["Key"])

            return objects
        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Error listing objects: {str(e)}")
            return []

    def list_prefixes(self, prefix: str = "", delimiter: str = "/") -> List[str]:
        """
        List common prefixes (folders) in Wasabi/S3 bucket.

        Args:
            prefix: Prefix to filter prefixes (default: "")
            delimiter: Delimiter for grouping (default: "/")

        Returns:
            List of prefix strings
        """
        if not self.s3_client:
            return []

        try:
            prefixes = []
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter
            )

            for page in pages:
                if "CommonPrefixes" in page:
                    for prefix_info in page["CommonPrefixes"]:
                        prefix_path = prefix_info["Prefix"]
                        # Remove the base prefix and trailing delimiter
                        clean_prefix = prefix_path.replace(prefix, "").rstrip(delimiter)
                        if clean_prefix:
                            prefixes.append(clean_prefix)

            return sorted(prefixes)
        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Error listing prefixes: {str(e)}")
            return []

    def file_exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in Wasabi/S3.

        Args:
            s3_key: S3 key (path) of file to check

        Returns:
            True if file exists, False otherwise
        """
        if not self.s3_client:
            return False

        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Error checking file existence: {str(e)}")
            return False
