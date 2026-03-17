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


FILE_PATTERN = '3_Settlement Details_Amazon{yyyymm}.csv'
HEADER_MARKER = '"date/time","settlement id","type","order id"'


def normalize_month(value: str) -> tuple[str, str]:
    text = value.strip()
    if len(text) == 7 and '-' in text:
        return text, text.replace('-', '')
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}", text
    raise ValueError('target_month must be YYYY-MM or YYYYMM')


def normalize_text(value: object) -> str:
    return str(value).strip() if value is not None else ''


def parse_float(value: object) -> float:
    text = normalize_text(value)
    if text == '':
        return 0.0
    return float(text.replace(',', ''))


def detect_header_row(source_path: Path) -> int:
    with source_path.open('r', encoding='utf-8-sig', newline='') as handle:
        for index, line in enumerate(handle):
            if HEADER_MARKER in line.lower():
                return index
    raise ValueError(f'Unable to detect settlement header row in {source_path.name}')


def build_row_hash(source_file: str, row: dict[str, object]) -> str:
    payload = {'source_file': source_file, 'row': row}
    return sha256_text(stable_json(payload))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Load Amazon settlement lines into fact_settlement_lines.')
    parser.add_argument('target_month', help='YYYY-MM or YYYYMM')
    return parser.parse_args()


def classify_order_types(conn, month_text: str) -> str:
    conn.execute(
        "UPDATE fact_settlement_lines SET order_type = NULL WHERE transaction_month = ?",
        (month_text,),
    )
    conn.execute(
        """
        UPDATE fact_settlement_lines
        SET order_type = 'vine_sale'
        WHERE transaction_month = ?
          AND transaction_type = 'Order'
          AND is_amazon_channel = 1
          AND order_id IS NOT NULL
          AND product_sales > 0
          AND ABS(product_sales - ABS(promotional_rebates)) < 0.01
        """,
        (month_text,),
    )
    conn.execute(
        """
        UPDATE fact_settlement_lines
        SET order_type = 'vine_refund'
        WHERE transaction_month = ?
          AND transaction_type = 'Refund'
          AND order_id IN (
              SELECT DISTINCT order_id
              FROM fact_settlement_lines
              WHERE order_type = 'vine_sale'
          )
        """,
        (month_text,),
    )
    conn.execute(
        """
        UPDATE fact_settlement_lines
        SET order_type = 'review_sale'
        WHERE transaction_month = ?
          AND transaction_type = 'Order'
          AND order_type IS NULL
          AND order_id IN (
              SELECT amazon_order_id FROM fact_review_orders
          )
        """,
        (month_text,),
    )
    conn.execute(
        """
        UPDATE fact_settlement_lines
        SET order_type = 'review_refund'
        WHERE transaction_month = ?
          AND transaction_type = 'Refund'
          AND order_type IS NULL
          AND order_id IN (
              SELECT amazon_order_id FROM fact_review_orders
          )
        """,
        (month_text,),
    )
    conn.execute(
        """
        UPDATE fact_settlement_lines
        SET order_type = 'normal_refund'
        WHERE transaction_month = ?
          AND transaction_type = 'Refund'
          AND order_type IS NULL
        """,
        (month_text,),
    )
    conn.execute(
        """
        UPDATE fact_settlement_lines
        SET order_type = 'normal_sale'
        WHERE transaction_month = ?
          AND transaction_type = 'Order'
          AND order_type IS NULL
        """,
        (month_text,),
    )
    summary = conn.execute(
        """
        SELECT order_type, COUNT(*)
        FROM fact_settlement_lines
        WHERE transaction_month = ?
          AND order_type IS NOT NULL
        GROUP BY order_type
        ORDER BY COUNT(*) DESC
        """,
        (month_text,),
    ).fetchall()
    return '; '.join([f"{order_type}={count}" for order_type, count in summary]) or 'no classified rows'


def main() -> int:
    args = parse_args()
    month_text, month_compact = normalize_month(args.target_month)
    config = get_config()
    source_path = config.base_dir / FILE_PATTERN.format(yyyymm=month_compact)
    if not source_path.exists():
        raise FileNotFoundError(f'Source file not found: {source_path}')

    print_banner(f'Loading settlement lines from {source_path.name}')
    conn = connect(config.db_path)
    run_id = register_etl_run(
        conn,
        script_name='04_load_settlement_lines.py',
        run_type='load_settlement_lines',
        target_month=month_text,
        status='started',
    )

    try:
        header_row = detect_header_row(source_path)
        with source_path.open('r', encoding='utf-8-sig', newline='') as handle:
            for _ in range(header_row):
                next(handle)
            reader = csv.DictReader(handle)
            raw_rows = list(reader)

        record_file_import(
            conn,
            run_id=run_id,
            source_file=source_path,
            file_role='settlement_report',
            import_status='loaded',
            row_count=len(raw_rows),
            notes=f'header_row={header_row}',
        )

        conn.execute(
            """
            DELETE FROM fact_platform_fee_lines
            WHERE source_settlement_line_id IN (
                SELECT settlement_line_id
                FROM fact_settlement_lines
                WHERE source_file = ?
            )
            """,
            (str(source_path),),
        )
        conn.execute(
            """
            DELETE FROM bridge_orderline_settlement
            WHERE settlement_line_id IN (
                SELECT settlement_line_id
                FROM fact_settlement_lines
                WHERE source_file = ?
            )
            """,
            (str(source_path),),
        )
        conn.execute('DELETE FROM fact_settlement_lines WHERE source_file = ?', (str(source_path),))

        insert_rows = []
        duplicate_count = 0

        for row_index, row in enumerate(raw_rows, start=1):
            normalized = {k.strip(): v for k, v in row.items()}
            row_hash = sha256_text(stable_json({'source_file': str(source_path), 'row_index': row_index, 'row': normalized}))
            # Keep legitimate repeated settlement rows; source row position is part of the line identity.
            settlement_line_id = sha256_text(f"{source_path}|{row_index}|{row_hash}")
            marketplace = normalize_text(normalized.get('marketplace'))
            transaction_type = normalize_text(normalized.get('type'))
            insert_rows.append(
                (
                    settlement_line_id,
                    str(source_path),
                    row_hash,
                    normalize_text(normalized.get('date/time')),
                    month_text,
                    normalize_text(normalized.get('settlement id')),
                    transaction_type,
                    normalize_text(normalized.get('description')) or None,
                    normalize_text(normalized.get('order id')) or None,
                    normalize_text(normalized.get('sku')) or None,
                    parse_float(normalized.get('quantity')) if normalize_text(normalized.get('quantity')) else None,
                    marketplace or None,
                    normalize_text(normalized.get('fulfillment')) or None,
                    parse_float(normalized.get('product sales')),
                    parse_float(normalized.get('product sales tax')),
                    parse_float(normalized.get('shipping credits')),
                    parse_float(normalized.get('shipping credits tax')),
                    parse_float(normalized.get('gift wrap credits')),
                    parse_float(normalized.get('giftwrap credits tax')),
                    parse_float(normalized.get('Regulatory Fee')),
                    parse_float(normalized.get('Tax On Regulatory Fee')),
                    parse_float(normalized.get('promotional rebates')),
                    parse_float(normalized.get('promotional rebates tax')),
                    parse_float(normalized.get('marketplace withheld tax')),
                    parse_float(normalized.get('selling fees')),
                    parse_float(normalized.get('fba fees')),
                    parse_float(normalized.get('other transaction fees')),
                    parse_float(normalized.get('other')),
                    parse_float(normalized.get('total')),
                    normalize_text(normalized.get('Transaction Status')) or None,
                    normalize_text(normalized.get('Transaction Release Date')) or None,
                    1 if marketplace in ('amazon.com', 'Amazon.com') else 0,
                    None,
                    utc_now_iso(),
                )
            )

        conn.executemany(
            """
            INSERT INTO fact_settlement_lines (
                settlement_line_id,
                source_file,
                source_row_hash,
                transaction_datetime,
                transaction_month,
                settlement_id,
                transaction_type,
                transaction_subtype,
                order_id,
                sku,
                quantity,
                marketplace,
                fulfillment,
                product_sales,
                product_sales_tax,
                shipping_credits,
                shipping_credits_tax,
                gift_wrap_credits,
                gift_wrap_credits_tax,
                regulatory_fee,
                regulatory_fee_tax,
                promotional_rebates,
                promotional_rebates_tax,
                marketplace_withheld_tax,
                selling_fees,
                fba_fees,
                other_transaction_fees,
                other_amount,
                total,
                transaction_status,
                transaction_release_date,
                is_amazon_channel,
                order_type,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )

        classification_note = classify_order_types(conn, month_text)
        conn.commit()
        note = f'Loaded {len(insert_rows)} settlement lines for {month_text}; duplicate_rows_skipped={duplicate_count}; {classification_note}'
        finish_etl_run(conn, run_id, 'success', note)
        conn.commit()
        print_banner(note)
        return 0
    except Exception as exc:  # pragma: no cover
        conn.rollback()
        finish_etl_run(conn, run_id, 'failed', str(exc))
        conn.commit()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
