from __future__ import annotations

import argparse

from common import connect, finish_etl_run, get_config, print_banner, register_etl_run


def normalize_month(value: str) -> str:
    text = value.strip()
    if len(text) == 7 and '-' in text:
        return text
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}"
    raise ValueError('target_month must be YYYY-MM or YYYYMM')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Classify settlement lines into order types.')
    parser.add_argument('target_month', help='YYYY-MM or YYYYMM')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    month_text = normalize_month(args.target_month)
    config = get_config()

    print_banner(f'Classifying order types for {month_text}')
    conn = connect(config.db_path)
    run_id = register_etl_run(
        conn,
        script_name='07_classify_order_types.py',
        run_type='classify_order_types',
        target_month=month_text,
        status='started',
    )

    try:
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

        conn.commit()
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
        note = '; '.join([f"{order_type}={count}" for order_type, count in summary]) or 'no classified rows'
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
