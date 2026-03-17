from __future__ import annotations

import argparse
import csv
from pathlib import Path

from common import connect, finish_etl_run, get_config, print_banner, record_file_import, register_etl_run, utc_now_iso


FILE_PATTERN = '6_FBA Storage Fees_Amazon{yyyymm}.csv'


def normalize_month(value: str) -> tuple[str, str]:
    text = value.strip()
    if len(text) == 7 and '-' in text:
        return text, text.replace('-', '')
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}", text
    raise ValueError('target_month must be YYYY-MM or YYYYMM')


def normalize_text(value: object) -> str:
    return str(value).strip() if value is not None else ''


def to_float(value: object) -> float:
    if value is None or value == '':
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value).strip().replace(',', ''))


def resolve_sku(conn, asin: str, fnsku: str) -> str | None:
    if fnsku:
        row = conn.execute("select sku from dim_sku where sku = ?", (fnsku,)).fetchone()
        if row:
            return row[0]
    if fnsku:
        rows = conn.execute("select distinct sku from fact_order_lines where sku = ?", (fnsku,)).fetchall()
        sku_values = sorted({r[0] for r in rows if r[0]})
        if len(sku_values) == 1:
            return sku_values[0]
    if asin:
        rows = conn.execute("select distinct sku from fact_order_lines where asin = ? and sku is not null", (asin,)).fetchall()
        sku_values = sorted({r[0] for r in rows if r[0]})
        if len(sku_values) == 1:
            return sku_values[0]
        rows = conn.execute("select distinct sku from fact_settlement_lines where transaction_type='Order' and sku is not null and order_id is not null and sku in (select sku from fact_order_lines where asin = ? and sku is not null)", (asin,)).fetchall()
        sku_values = sorted({r[0] for r in rows if r[0]})
        if len(sku_values) == 1:
            return sku_values[0]
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description='Load monthly storage fees by SKU.')
    parser.add_argument('target_month')
    args = parser.parse_args()
    month_text, month_compact = normalize_month(args.target_month)
    config = get_config()
    source_path = config.base_dir / FILE_PATTERN.format(yyyymm=month_compact)
    if not source_path.exists():
        raise FileNotFoundError(source_path)

    print_banner(f'Loading storage fees from {source_path.name}')
    conn = connect(config.db_path)
    run_id = register_etl_run(conn, '09_load_storage_fees.py', 'load_storage_fees', target_month=month_text, status='started')
    try:
        with source_path.open('r', encoding='utf-8-sig', newline='') as handle:
            rows = list(csv.DictReader(handle))
        record_file_import(conn, run_id, source_path, 'storage_fees', import_status='loaded', row_count=len(rows))
        conn.execute("DELETE FROM fact_storage_monthly_sku WHERE source_file = ? AND period_month = ?", (str(source_path), month_text))
        insert_rows = []
        for row in rows:
            asin = normalize_text(row.get('asin'))
            fnsku = normalize_text(row.get('fnsku'))
            sku = resolve_sku(conn, asin, fnsku)
            insert_rows.append((
                str(source_path),
                month_text,
                fnsku or None,
                asin or None,
                sku,
                to_float(row.get('average_quantity_on_hand')),
                to_float(row.get('estimated_monthly_storage_fee')),
                to_float(row.get('total_incentive_fee_amount')),
                utc_now_iso(),
            ))
        conn.executemany(
            """
            INSERT INTO fact_storage_monthly_sku (
                source_file, period_month, fnsku, asin, sku, average_quantity_on_hand,
                estimated_monthly_storage_fee, incentive_fee_amount, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
        conn.commit()
        note = f'Loaded {len(insert_rows)} storage fee rows for {month_text}'
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
