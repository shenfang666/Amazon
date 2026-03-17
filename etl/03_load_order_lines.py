from __future__ import annotations

import argparse
import csv
from pathlib import Path

from common import (
    connect,
    finish_etl_run,
    get_config,
    print_banner,
    record_file_import,
    register_etl_run,
    sha256_text,
    stable_json,
    utc_now_iso,
)


FILE_PATTERN = '2_Order report_Amazon{yyyymm}.txt'


def normalize_month(value: str) -> tuple[str, str]:
    text = value.strip()
    if len(text) == 7 and '-' in text:
        return text, text.replace('-', '')
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}", text
    raise ValueError('target_month must be YYYY-MM or YYYYMM')


def normalize_text(value: object) -> str:
    return str(value).strip() if value is not None else ''


def parse_float(value: object) -> float | None:
    text = normalize_text(value)
    if text == '':
        return None
    return float(text)


def parse_int(value: object) -> float | None:
    text = normalize_text(value)
    if text == '':
        return None
    return float(text)


def build_row_hash(source_file: str, row: dict[str, object]) -> str:
    payload = {'source_file': source_file, 'row': row}
    return sha256_text(stable_json(payload))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Load Amazon order report into fact_order_lines.')
    parser.add_argument('target_month', help='YYYY-MM or YYYYMM')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    month_text, month_compact = normalize_month(args.target_month)
    config = get_config()
    source_path = config.base_dir / FILE_PATTERN.format(yyyymm=month_compact)
    if not source_path.exists():
        raise FileNotFoundError(f'Source file not found: {source_path}')

    print_banner(f'Loading order lines from {source_path.name}')
    conn = connect(config.db_path)
    run_id = register_etl_run(
        conn,
        script_name='03_load_order_lines.py',
        run_type='load_order_lines',
        target_month=month_text,
        status='started',
    )

    try:
        with source_path.open('r', encoding='utf-8-sig', newline='') as handle:
            reader = csv.DictReader(handle, delimiter='\t')
            raw_rows = list(reader)

        record_file_import(
            conn,
            run_id=run_id,
            source_file=source_path,
            file_role='order_report',
            import_status='loaded',
            row_count=len(raw_rows),
        )

        conn.execute(
            """
            DELETE FROM bridge_orderline_settlement
            WHERE order_line_id IN (
                SELECT order_line_id
                FROM fact_order_lines
                WHERE source_file = ?
            )
            """,
            (str(source_path),),
        )
        conn.execute('DELETE FROM fact_order_lines WHERE source_file = ?', (str(source_path),))

        insert_rows = []
        seen_hashes: set[str] = set()
        duplicate_rows_skipped = 0
        for row in raw_rows:
            normalized = {k.strip(): v for k, v in row.items()}
            row_hash = build_row_hash(str(source_path), normalized)
            if row_hash in seen_hashes:
                duplicate_rows_skipped += 1
                continue
            seen_hashes.add(row_hash)
            order_line_id = sha256_text(f"{source_path}|{row_hash}")
            sales_channel = normalize_text(normalized.get('sales-channel'))
            insert_rows.append(
                (
                    order_line_id,
                    str(source_path),
                    row_hash,
                    normalize_text(normalized.get('amazon-order-id')),
                    normalize_text(normalized.get('purchase-date')),
                    normalize_text(normalized.get('last-updated-date')) or None,
                    month_text,
                    normalize_text(normalized.get('order-status')),
                    normalize_text(normalized.get('fulfillment-channel')) or None,
                    sales_channel or None,
                    normalize_text(normalized.get('sku')) or None,
                    normalize_text(normalized.get('asin')) or None,
                    parse_int(normalized.get('quantity')),
                    normalize_text(normalized.get('currency')) or None,
                    parse_float(normalized.get('item-price')),
                    parse_float(normalized.get('item-tax')),
                    parse_float(normalized.get('shipping-price')),
                    parse_float(normalized.get('shipping-tax')),
                    parse_float(normalized.get('item-promotion-discount')),
                    parse_float(normalized.get('ship-promotion-discount')),
                    normalize_text(normalized.get('promotion-ids')) or None,
                    1 if sales_channel == 'Amazon.com' else 0,
                    None,
                    utc_now_iso(),
                )
            )

        conn.executemany(
            """
            INSERT INTO fact_order_lines (
                order_line_id,
                source_file,
                source_row_hash,
                amazon_order_id,
                purchase_date,
                last_updated_date,
                order_month,
                order_status,
                fulfillment_channel,
                sales_channel,
                sku,
                asin,
                quantity,
                currency,
                item_price,
                item_tax,
                shipping_price,
                shipping_tax,
                item_promotion_discount,
                ship_promotion_discount,
                promotion_ids,
                is_amazon_channel,
                settlement_state,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )

        conn.commit()
        note = f'Loaded {len(insert_rows)} order lines for {month_text}; duplicate_rows_skipped={duplicate_rows_skipped}'
        finish_etl_run(conn, run_id, 'success', note)
        conn.commit()
        print_banner(note)
        return 0
    except Exception as exc:  # pragma: no cover
        finish_etl_run(conn, run_id, 'failed', str(exc))
        conn.commit()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
