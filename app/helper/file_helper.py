from __future__ import annotations
import io
import uuid
from typing import Optional, List
from pathlib import Path
from fastapi import status
from fastapi import UploadFile, HTTPException
from PIL import Image
from app.config.logger_config import logger
from app.helper.baseapp_helper import BaseAppHelper


class FileHelper(BaseAppHelper):
    """Helper for handling file operations."""

    def __init__(self, base_media_path: str = "app/media", logo_subdir: str = "logo"):
        self.base_media_path = Path(base_media_path)
        self.logo_path = self.base_media_path / logo_subdir
        self.max_file_size = 5 * 1024 * 1024  # 5MB
        self.allowed_extensions = {
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".webp",
            ".bmp",
            ".tiff",
            ".tif",
        }
        self.max_image_size = (1024, 1024)  # Max width/height
        self.webp_quality = 85  # WebP quality (0-100)

        # Ensure directories exist
        self._ensure_directories()

    def _ensure_directories(self):
        """Create necessary directories if they don't exist."""
        self.logo_path.mkdir(parents=True, exist_ok=True)
        # logger.info(f" Media directories ensured: {self.logo_path}")
        # Disabled to reduce log noise

    def _validate_image_file(self, file: UploadFile) -> None:
        """Validate uploaded image file."""
        if not file.filename:
            raise HTTPException(status_code=400, detail="No filename provided")

        # Check file extension
        file_ext = Path(file.filename).suffix.lower()
        if file_ext not in self.allowed_extensions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file type. Allowed types: {', '.join(self.allowed_extensions)}",
            )

        # Check file size (if available)
        if hasattr(file, "size") and file.size and file.size > self.max_file_size:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File too large. Maximum size: {self.max_file_size // (1024 * 1024)}MB",
            )

    def _generate_filename(self, original_filename: str, demo_id: str) -> str:
        """Generate unique filename for logo (always WebP format)."""
        unique_id = str(uuid.uuid4())[:8]
        return f"demo_{demo_id}_{unique_id}.webp"

    def _get_logo_url(self, filename: str) -> str:
        """Generate URL for logo file."""
        return f"/media/logo/{filename}"

    async def upload_logo(self, file: UploadFile, demo_id: str) -> str:
        """
        Upload and process demo logo.
        All images are automatically converted to WebP format for optimal storage.

        Args:
            file: Uploaded file (supports JPG, PNG, GIF, BMP, TIFF, WebP)
            demo_id: Demo ID for filename generation

        Returns:
            str: URL of the uploaded logo (always .webp format)

        Raises:
            HTTPException: If upload fails
        """
        try:
            # Validate file
            self._validate_image_file(file)

            # Generate filename
            filename = self._generate_filename(file.filename, demo_id)
            file_path = self.logo_path / filename

            # Read file content
            content = await file.read()

            # Check file size from content
            if len(content) > self.max_file_size:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File too large. Maximum size: {self.max_file_size // (1024 * 1024)}MB",
                )

            # Process and save image
            await self._process_and_save_image(content, file_path)

            # Generate URL
            logo_url = self._get_logo_url(filename)

            logger.info(f" Logo uploaded successfully: {filename} for demo {demo_id}")
            return logo_url

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f" Logo upload failed: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Logo upload failed: {str(e)}",
            ) from e

    async def _process_and_save_image(self, content: bytes, file_path: Path) -> None:
        """Process and save image as WebP with optimization."""
        try:
            # Open image with PIL
            image = Image.open(io.BytesIO(content))

            # Convert to RGB if necessary (for WebP compatibility)
            if image.mode in ("RGBA", "P", "LA"):
                # For images with transparency, convert to RGBA first
                if image.mode == "P" and "transparency" in image.info:
                    image = image.convert("RGBA")
                else:
                    image = image.convert("RGB")

            # Resize if too large
            if (
                image.size[0] > self.max_image_size[0]
                or image.size[1] > self.max_image_size[1]
            ):
                image.thumbnail(self.max_image_size, Image.Resampling.LANCZOS)
                logger.info(f" Image resized to: {image.size}")

            # Save as WebP with optimization
            image.save(file_path, "WEBP", quality=self.webp_quality, optimize=True)
            logger.info(f" Image converted to WebP: {file_path.name}")

        except Exception as e:
            logger.error(f" Image processing failed: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process image",
            ) from e

    async def delete_logo(self, logo_url: str) -> bool:
        """
        Delete logo file.

        Args:
            logo_url: URL of the logo to delete

        Returns:
            bool: True if deleted successfully
        """
        try:
            if not logo_url:
                return False

            # Extract filename from URL
            filename = Path(logo_url).name
            file_path = self.logo_path / filename

            # Check if file exists and delete
            if file_path.exists():
                file_path.unlink()
                logger.info(f" Logo deleted: {filename}")
                return True

            logger.warning(f" Logo file not found: {filename}")
            return False

        except (OSError, IOError) as e:
            logger.error(f" Logo deletion failed: {str(e)}")
            return False

    def get_logo_path(self, logo_url: str) -> Optional[Path]:
        """
        Get file system path for logo URL.

        Args:
            logo_url: URL of the logo

        Returns:
            Path: File system path if exists, None otherwise
        """
        try:
            if not logo_url:
                return None

            filename = Path(logo_url).name
            file_path = self.logo_path / filename

            return file_path if file_path.exists() else None

        except (OSError, IOError):
            return None

    async def cleanup_orphaned_logos(self, active_logo_urls: List[str]) -> int:
        """
        Clean up orphaned logo files.

        Args:
            active_logo_urls: List of currently used logo URLs

        Returns:
            int: Number of files deleted
        """
        try:
            active_filenames = {Path(url).name for url in active_logo_urls if url}
            deleted_count = 0

            for file_path in self.logo_path.iterdir():
                if file_path.is_file() and file_path.name not in active_filenames:
                    file_path.unlink()
                    deleted_count += 1
                    logger.info(f" Orphaned logo deleted: {file_path.name}")

            if deleted_count > 0:
                logger.info(
                    f" Cleanup completed: {deleted_count} orphaned logos deleted"
                )

            return deleted_count

        except (OSError, IOError) as e:
            logger.error(f" Logo cleanup failed: {str(e)}")
            return 0

    def get_conversion_info(self) -> dict:
        """
        Get information about image conversion settings.

        Returns:
            dict: Conversion configuration details
        """
        return {
            "output_format": "WebP",
            "quality": self.webp_quality,
            "max_size": f"{self.max_image_size[0]}x{self.max_image_size[1]}",
            "max_file_size_mb": self.max_file_size // (1024 * 1024),
            "supported_input_formats": list(self.allowed_extensions),
            "benefits": [
                "Smaller file sizes (typically 25-50% smaller than JPEG)",
                "Better compression with same quality",
                "Supports transparency",
                "Modern web standard",
            ],
        }
