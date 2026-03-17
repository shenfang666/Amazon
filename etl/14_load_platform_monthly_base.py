from __future__ import annotations

import argparse

from common import connect, finish_etl_run, get_config, print_banner, register_etl_run, utc_now_iso


def normalize_month(value: str) -> str:
    text = value.strip()
    if len(text) == 7 and '-' in text:
        return text
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}"
    raise ValueError('target_month must be YYYY-MM or YYYYMM')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Load platform monthly base metrics for Amazon.')
    parser.add_argument('target_month', help='YYYY-MM or YYYYMM')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    month_text = normalize_month(args.target_month)
    config = get_config()
    print_banner(f'Loading platform monthly base for {month_text}')
    conn = connect(config.db_path)
    run_id = register_etl_run(
        conn,
        script_name='14_load_platform_monthly_base.py',
        run_type='load_platform_monthly_base',
        target_month=month_text,
        status='started',
    )
    try:
        net_sales = conn.execute(
            """
            select coalesce(sum(net_sales), 0)
            from (
                select distinct period_month, sku, order_type, net_sales
                from v_monthly_sku_order_type_summary
                where period_month = ?
            )
            """,
            (month_text,),
        ).fetchone()[0]

        shipped_qty = conn.execute(
            """
            select coalesce(sum(quantity), 0)
            from fact_order_lines
            where order_month = ?
              and is_amazon_channel = 1
              and order_status = 'Shipped'
            """,
            (month_text,),
        ).fetchone()[0]

        order_line_count = conn.execute(
            """
            select count(*)
            from fact_order_lines
            where order_month = ?
              and is_amazon_channel = 1
            """,
            (month_text,),
        ).fetchone()[0]

        conn.execute(
            """
            INSERT INTO dim_platform_monthly_base (
                period_month,
                platform,
                net_sales,
                shipped_qty,
                order_line_count,
                source_type,
                source_note,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(period_month, platform) DO UPDATE SET
                net_sales = excluded.net_sales,
                shipped_qty = excluded.shipped_qty,
                order_line_count = excluded.order_line_count,
                source_type = excluded.source_type,
                source_note = excluded.source_note,
                created_at = excluded.created_at
            """,
            (
                month_text,
                'Amazon',
                float(net_sales or 0),
                float(shipped_qty or 0),
                float(order_line_count or 0),
                'system',
                'Derived from fact_order_lines and v_monthly_sku_order_type_summary',
                utc_now_iso(),
            ),
        )

        conn.commit()
        note = f'Loaded platform base for Amazon {month_text}: net_sales={net_sales:.2f}, shipped_qty={shipped_qty}, order_lines={order_line_count}'
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
