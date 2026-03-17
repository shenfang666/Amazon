from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_DB_NAME = "amazon_finance.db"


@dataclass(frozen=True)
class AppConfig:
    base_dir: Path
    db_path: Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_config() -> AppConfig:
    base_dir = Path(__file__).resolve().parent.parent
    db_env = os.environ.get("AMAZON_FINANCE_DB", "").strip()
    db_path = Path(db_env) if db_env else base_dir / DEFAULT_DB_NAME
    return AppConfig(base_dir=base_dir, db_path=db_path)


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def execute_script(conn: sqlite3.Connection, sql: str) -> None:
    conn.executescript(sql)


def execute_many(
    conn: sqlite3.Connection,
    sql: str,
    rows: Sequence[Sequence[object]],
) -> None:
    conn.executemany(sql, rows)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def stable_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def register_etl_run(
    conn: sqlite3.Connection,
    script_name: str,
    run_type: str,
    target_month: str | None = None,
    status: str = "started",
    notes: str | None = None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO etl_run_log (
            run_started_at,
            script_name,
            run_type,
            target_month,
            status,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (utc_now_iso(), script_name, run_type, target_month, status, notes),
    )
    return int(cursor.lastrowid)


def finish_etl_run(
    conn: sqlite3.Connection,
    run_id: int,
    status: str,
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE etl_run_log
        SET run_finished_at = ?,
            status = ?,
            notes = COALESCE(?, notes)
        WHERE run_id = ?
        """,
        (utc_now_iso(), status, notes, run_id),
    )


def record_file_import(
    conn: sqlite3.Connection,
    run_id: int,
    source_file: Path,
    file_role: str,
    import_status: str = "registered",
    row_count: int | None = None,
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO file_import_log (
            run_id,
            source_file,
            source_file_hash,
            file_role,
            imported_at,
            import_status,
            row_count,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            str(source_file),
            sha256_file(source_file),
            file_role,
            utc_now_iso(),
            import_status,
            row_count,
            notes,
        ),
    )


def print_banner(title: str) -> None:
    print(f"[{utc_now_iso()}] {title}")


def rows_to_tuples(rows: Iterable[sqlite3.Row]) -> list[tuple[object, ...]]:
    return [tuple(row) for row in rows]
