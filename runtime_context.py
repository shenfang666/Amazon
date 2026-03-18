from __future__ import annotations

from pathlib import Path


# Runtime configuration - must be set before use
ROOT: Path | None = None
WEB_DIR: Path | None = None
DB_PATH: Path | None = None
ETL_RUNNER: Path | None = None
MANUAL_DIR: Path | None = None
ATTACHMENT_DIR: Path | None = None

TEXT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
}


def get_root() -> Path:
    if ROOT is None:
        raise RuntimeError("ROOT not initialized. Call configure() first.")
    return ROOT


def get_web_dir() -> Path:
    if WEB_DIR is None:
        raise RuntimeError("WEB_DIR not initialized. Call configure() first.")
    return WEB_DIR


def get_db_path() -> Path:
    if DB_PATH is None:
        raise RuntimeError("DB_PATH not initialized. Call configure() first.")
    return DB_PATH


def get_attachment_dir() -> Path:
    if ATTACHMENT_DIR is None:
        raise RuntimeError("ATTACHMENT_DIR not initialized. Call configure() first.")
    return ATTACHMENT_DIR


def configure(root: Path, web_dir: Path, db_path: Path, etl_runner: Path, manual_dir: Path, attachment_dir: Path) -> None:
    """Configure runtime context with all necessary paths."""
    global ROOT, WEB_DIR, DB_PATH, ETL_RUNNER, MANUAL_DIR, ATTACHMENT_DIR
    ROOT = root
    WEB_DIR = web_dir
    DB_PATH = db_path
    ETL_RUNNER = etl_runner
    MANUAL_DIR = manual_dir
    ATTACHMENT_DIR = attachment_dir
