from __future__ import annotations

import argparse
import csv
import html
from pathlib import Path

from common import connect, finish_etl_run, get_config, print_banner, record_file_import, register_etl_run, utc_now_iso


MONTH_FILE_PATTERN = '9_Reimbursements_Amazon{yyyymm}.csv'
YEAR_FILE_PATTERN = '9_Reimbursements_Amazon{yyyy}.csv'
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


def to_float(value: object) -> float:
    if value is None or value == '':
        return 0.0
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
        approval_date = normalize_text(row.get('approval-date'))
        if approval_date[:7] == month_text:
            filtered.append(row)
    return filtered


def resolve_sku(conn, raw_sku: str, asin: str) -> str | None:
    sku = html.unescape(raw_sku)
    if sku:
        row = conn.execute('select sku from dim_sku where sku = ?', (sku,)).fetchone()
        if row:
            return row[0]
    if asin:
        rows = conn.execute("select distinct sku from fact_order_lines where asin = ? and sku is not null", (asin,)).fetchall()
        sku_values = sorted({r[0] for r in rows if r[0]})
        if len(sku_values) == 1:
            return sku_values[0]
    return sku or None


def main() -> int:
    parser = argparse.ArgumentParser(description='Load monthly compensation rows by SKU.')
    parser.add_argument('target_month')
    args = parser.parse_args()
    month_text, month_compact, year_text = normalize_month(args.target_month)
    config = get_config()
    source_path, use_year_file = resolve_source_path(config.base_dir, month_compact, year_text)

    print_banner(f'Loading compensations from {source_path.name}')
    conn = connect(config.db_path)
    run_id = register_etl_run(conn, '11_load_compensations.py', 'load_compensations', target_month=month_text, status='started')
    try:
        all_rows = read_csv_rows(source_path)
        rows = filter_rows(all_rows, month_text, use_year_file)
        record_file_import(conn, run_id, source_path, 'compensations', import_status='loaded', row_count=len(rows), notes='year_file_fallback' if use_year_file else None)
        conn.execute("DELETE FROM fact_compensation_monthly_sku WHERE source_file = ? AND period_month = ?", (str(source_path), month_text))
        insert_rows = []
        for row in rows:
            asin = normalize_text(row.get('asin'))
            insert_rows.append((
                str(source_path),
                normalize_text(row.get('reimbursement-id')),
                month_text,
                normalize_text(row.get('amazon-order-id')) or None,
                resolve_sku(conn, normalize_text(row.get('sku')), asin),
                normalize_text(row.get('reason')) or None,
                to_float(row.get('amount-total')),
                to_float(row.get('quantity-reimbursed-cash')),
                to_float(row.get('quantity-reimbursed-inventory')),
                utc_now_iso(),
            ))
        conn.executemany(
            """
            INSERT INTO fact_compensation_monthly_sku (
                source_file, reimbursement_id, period_month, amazon_order_id, sku, reason,
                amount_total, quantity_reimbursed_cash, quantity_reimbursed_inventory, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(reimbursement_id) DO UPDATE SET
                period_month = excluded.period_month,
                amazon_order_id = excluded.amazon_order_id,
                sku = excluded.sku,
                reason = excluded.reason,
                amount_total = excluded.amount_total,
                quantity_reimbursed_cash = excluded.quantity_reimbursed_cash,
                quantity_reimbursed_inventory = excluded.quantity_reimbursed_inventory
            """,
            insert_rows,
        )
        conn.commit()
        note = f'Loaded {len(insert_rows)} compensation rows for {month_text}'
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
