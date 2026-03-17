from __future__ import annotations

from common import (
    connect,
    ensure_parent_dir,
    execute_script,
    finish_etl_run,
    get_config,
    print_banner,
    register_etl_run,
    utc_now_iso,
)
from schema import SCHEMA_SQL


SCHEMA_NAME = "amazon_finance"
SCHEMA_VERSION = "v2_finance_closure_refactor"


def main() -> int:
    config = get_config()
    ensure_parent_dir(config.db_path)

    print_banner(f"Initializing database at {config.db_path}")
    conn = connect(config.db_path)

    try:
        execute_script(conn, SCHEMA_SQL)
        run_id = register_etl_run(
            conn=conn,
            script_name="00_init_db.py",
            run_type="init_db",
            status="started",
            notes=f"schema_version={SCHEMA_VERSION}",
        )
        conn.execute(
            """
            INSERT INTO schema_version (
                schema_name,
                schema_version,
                applied_at,
                notes
            ) VALUES (?, ?, ?, ?)
            """,
            (SCHEMA_NAME, SCHEMA_VERSION, utc_now_iso(), "Finance closure refactor schema with receivables, exceptions, and month-close state logs."),
        )
        finish_etl_run(conn, run_id, "success", "Database schema initialized.")
        conn.commit()
        print_banner("Database initialization completed")
        return 0
    except Exception as exc:  # pragma: no cover
        finish_etl_run(conn, run_id, "failed", str(exc))
        conn.commit()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
