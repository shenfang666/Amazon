from __future__ import annotations

import argparse
import csv
from pathlib import Path

from common import connect, finish_etl_run, get_config, print_banner, record_file_import, register_etl_run, utc_now_iso


MONTH_FILE_PATTERN = '8_Removal Fees_Amazon{yyyymm}.csv'
YEAR_FILE_PATTERN = '8_Removal Fees_Amazon{yyyy}.csv'
ENCODINGS = ['utf-8-sig', 'utf-8', 'cp1252', 'latin-1']


def normalize_month(value: str) -> tuple[str, str, str]:
    text = value.strip()
    if len(text) == 7 and '-' in text:
        return text, text.replace('-', ''), text[:4]
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}", text, text[:4]
    raise ValueError('target_month must be YYYY-MM or YYYYMM')


def normalize_text(value: object) -> str:
    return str(value).strip() if value is not None else ''


def to_float(value: object) -> float | None:
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value).strip().replace(',', ''))


def resolve_source_path(base_dir: Path, month_compact: str, year_text: str) -> tuple[Path, bool]:
    month_path = base_dir / MONTH_FILE_PATTERN.format(yyyymm=month_compact)
    if month_path.exists():
        return month_path, False
    year_path = base_dir / YEAR_FILE_PATTERN.format(yyyy=year_text)
    if year_path.exists():
        return year_path, True
    raise FileNotFoundError(month_path)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    last_error: Exception | None = None
    for encoding in ENCODINGS:
        try:
            with path.open('r', encoding=encoding, newline='') as handle:
                return list(csv.DictReader(handle))
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error
    return []


def filter_rows(rows: list[dict[str, str]], month_text: str, use_year_file: bool) -> list[dict[str, str]]:
    if not use_year_file:
        return rows
    filtered = []
    for row in rows:
        request_date = normalize_text(row.get('request-date'))
        if request_date[:7] == month_text:
            filtered.append(row)
    return filtered


def main() -> int:
    parser = argparse.ArgumentParser(description='Load monthly removal fees by SKU.')
    parser.add_argument('target_month')
    args = parser.parse_args()
    month_text, month_compact, year_text = normalize_month(args.target_month)
    config = get_config()
    source_path, use_year_file = resolve_source_path(config.base_dir, month_compact, year_text)

    print_banner(f'Loading removal fees from {source_path.name}')
    conn = connect(config.db_path)
    run_id = register_etl_run(conn, '10_load_removal_fees.py', 'load_removal_fees', target_month=month_text, status='started')
    try:
        all_rows = read_csv_rows(source_path)
        rows = filter_rows(all_rows, month_text, use_year_file)
        record_file_import(conn, run_id, source_path, 'removal_fees', import_status='loaded', row_count=len(rows), notes='year_file_fallback' if use_year_file else None)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS manual_removal_fee_controls (
            control_id INTEGER PRIMARY KEY AUTOINCREMENT,
            period_month TEXT NOT NULL,
            order_id TEXT NOT NULL,
            sku TEXT,
            removal_category TEXT NOT NULL,
            accounting_treatment TEXT NOT NULL,
            source_note TEXT,
            created_at TEXT,
            UNIQUE(period_month, order_id)
        )
        """)
        existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(fact_removal_monthly_sku)").fetchall()}
        for column_name, column_sql in [
            ('order_source', 'TEXT'),
            ('removal_order_type', 'TEXT'),
            ('order_status', 'TEXT'),
            ('fnsku', 'TEXT'),
            ('disposition', 'TEXT'),
            ('requested_quantity', 'REAL'),
            ('cancelled_quantity', 'REAL'),
            ('disposed_quantity', 'REAL'),
            ('in_process_quantity', 'REAL'),
        ]:
            if column_name not in existing_columns:
                conn.execute(f"ALTER TABLE fact_removal_monthly_sku ADD COLUMN {column_name} {column_sql}")
        conn.execute("DELETE FROM fact_removal_monthly_sku WHERE source_file = ? AND period_month = ?", (str(source_path), month_text))
        insert_rows = [
            (
                str(source_path),
                month_text,
                normalize_text(row.get('order-id')) or None,
                normalize_text(row.get('order-source')) or None,
                normalize_text(row.get('order-type')) or None,
                normalize_text(row.get('order-status')) or None,
                normalize_text(row.get('sku')) or None,
                normalize_text(row.get('fnsku')) or None,
                normalize_text(row.get('disposition')) or None,
                to_float(row.get('requested-quantity')),
                to_float(row.get('cancelled-quantity')),
                to_float(row.get('disposed-quantity')),
                to_float(row.get('shipped-quantity')),
                to_float(row.get('in-process-quantity')),
                to_float(row.get('removal-fee')),
                utc_now_iso(),
            )
            for row in rows
        ]
        conn.executemany(
            """
            INSERT INTO fact_removal_monthly_sku (
                source_file, period_month, order_id, order_source, removal_order_type, order_status,
                sku, fnsku, disposition, requested_quantity, cancelled_quantity, disposed_quantity,
                shipped_quantity, in_process_quantity, removal_fee, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
        conn.commit()
        note = f'Loaded {len(insert_rows)} removal fee rows for {month_text}'
        if use_year_file:
            note += ' from year file fallback'
        finish_etl_run(conn, run_id, 'success', note)
        conn.commit()
        print_banner(note)
        return 0
    except Exception as exc:
        conn.rollback()
        finish_etl_run(conn, run_id, 'failed', str(exc))
        conn.commit()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
