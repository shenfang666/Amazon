from __future__ import annotations

import argparse

from common import connect, finish_etl_run, get_config, print_banner, register_etl_run, utc_now_iso


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS fact_platform_fee_lines (
    platform_fee_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source_settlement_line_id  TEXT NOT NULL UNIQUE,
    source_file                TEXT NOT NULL,
    period_month               TEXT NOT NULL,
    settlement_id              TEXT,
    source_order_id            TEXT,
    fee_type                   TEXT NOT NULL,
    fee_subtype                TEXT,
    amount_total               REAL NOT NULL DEFAULT 0,
    created_at                 TEXT,
    FOREIGN KEY (source_settlement_line_id) REFERENCES fact_settlement_lines(settlement_line_id)
);
"""


def normalize_month(value: str) -> str:
    text = value.strip()
    if len(text) == 7 and '-' in text:
        return text
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}"
    raise ValueError('target_month must be YYYY-MM or YYYYMM')


def derive_fee_type(transaction_type: str, transaction_subtype: str) -> str | None:
    if transaction_type == 'Service Fee' and transaction_subtype == 'Subscription':
        return 'subscription_fee'
    if transaction_type == 'Amazon Fees' and transaction_subtype == 'Coupon Participation Fee':
        return 'coupon_participation_fee'
    if transaction_type == 'Amazon Fees' and transaction_subtype == 'Coupon Performance Based Fee':
        return 'coupon_performance_fee'
    if transaction_type == 'Amazon Fees' and transaction_subtype == 'Vine Enrollment Fee':
        return 'vine_enrollment_fee_source'
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Extract platform fee lines from settlement data.')
    parser.add_argument('target_month', help='YYYY-MM or YYYYMM')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    month_text = normalize_month(args.target_month)
    config = get_config()
    print_banner(f'Loading platform fees for {month_text}')
    conn = connect(config.db_path)
    run_id = register_etl_run(
        conn,
        script_name='12_load_platform_fees.py',
        run_type='load_platform_fees',
        target_month=month_text,
        status='started',
    )
    try:
        conn.executescript(CREATE_SQL)
        conn.execute("DELETE FROM fact_platform_fee_lines WHERE period_month = ?", (month_text,))

        source_rows = conn.execute(
            """
            SELECT
                settlement_line_id,
                source_file,
                transaction_month,
                settlement_id,
                order_id,
                transaction_type,
                COALESCE(transaction_subtype, ''),
                total
            FROM fact_settlement_lines
            WHERE transaction_month = ?
              AND (
                (transaction_type = 'Service Fee' AND transaction_subtype = 'Subscription')
                OR (transaction_type = 'Amazon Fees' AND transaction_subtype IN (
                    'Coupon Participation Fee',
                    'Coupon Performance Based Fee',
                    'Vine Enrollment Fee'
                ))
              )
            """,
            (month_text,),
        ).fetchall()

        insert_rows = []
        for row in source_rows:
            fee_type = derive_fee_type(row[5], row[6])
            if not fee_type:
                continue
            insert_rows.append((
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                fee_type,
                row[6] or None,
                float(row[7] or 0),
                utc_now_iso(),
            ))

        if insert_rows:
            conn.executemany(
                """
                INSERT INTO fact_platform_fee_lines (
                    source_settlement_line_id,
                    source_file,
                    period_month,
                    settlement_id,
                    source_order_id,
                    fee_type,
                    fee_subtype,
                    amount_total,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                insert_rows,
            )

        conn.commit()
        note = f'Loaded {len(insert_rows)} platform fee lines for {month_text}'
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
