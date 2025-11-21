from __future__ import annotations
from pathlib import Path


class PathHelper:
    """Helper for file system path operations."""

    @staticmethod
    def find_project_root(start_path: Path, marker: str = "alembic.ini") -> Path:
        """
        Find the project root by searching upwards from a starting path for a marker file.

        Args:
            start_path: The path to start searching from.
            marker: The marker file to look for (e.g., 'alembic.ini', 'pyproject.toml').

        Returns:
            The path to the project root.

        Raises:
            FileNotFoundError: If the project root cannot be found.
        """
        current_path = start_path.resolve()
        while current_path != current_path.parent:
            if (current_path / marker).exists():
                return current_path
            current_path = current_path.parent
        raise FileNotFoundError(
            f"Project root with marker '{marker}' not found from '{start_path}'"
        )
