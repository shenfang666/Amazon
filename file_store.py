from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path


def list_files_by_suffix(root: Path, allowed_suffixes: set[str], limit: int | None = None) -> list[dict]:
    """List files in root directory with given suffixes, sorted by modification time."""
    files: list[dict] = []
    for path in sorted(root.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not path.is_file() or path.suffix.lower() not in allowed_suffixes:
            continue
        stat = path.stat()
        files.append(
            {
                "filename": path.name,
                "size": stat.st_size,
                "updated_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            }
        )
        if limit is not None and len(files) >= limit:
            break
    return files


def list_globbed_files(root: Path, pattern: str) -> list[dict]:
    """List files matching glob pattern, sorted by modification time."""
    files: list[dict] = []
    for path in sorted(root.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True):
        stat = path.stat()
        files.append(
            {
                "filename": path.name,
                "size": stat.st_size,
                "updated_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            }
        )
    return files
