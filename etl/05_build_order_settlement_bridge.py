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
    parser = argparse.ArgumentParser(description='Build order-settlement bridge for a target month.')
    parser.add_argument('target_month', help='YYYY-MM or YYYYMM')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    month_text = normalize_month(args.target_month)
    config = get_config()

    print_banner(f'Building order-settlement bridge for {month_text}')
    conn = connect(config.db_path)
    run_id = register_etl_run(
        conn,
        script_name='05_build_order_settlement_bridge.py',
        run_type='build_bridge',
        target_month=month_text,
        status='started',
    )

    try:
        conn.execute(
            """
            DELETE FROM bridge_orderline_settlement
            WHERE order_line_id IN (
                SELECT order_line_id
                FROM fact_order_lines
                WHERE order_month = ?
            )
            """,
            (month_text,),
        )

        order_rows = conn.execute(
            """
            SELECT order_line_id, amazon_order_id, sku
            FROM fact_order_lines
            WHERE order_month = ?
              AND is_amazon_channel = 1
            """,
            (month_text,),
        ).fetchall()

        bridge_rows = []
        for order_line_id, amazon_order_id, sku in order_rows:
            if not amazon_order_id or not sku:
                continue
            settlement_rows = conn.execute(
                """
                SELECT settlement_line_id
                FROM fact_settlement_lines
                WHERE transaction_month = ?
                  AND is_amazon_channel = 1
                  AND order_id = ?
                  AND sku = ?
                """,
                (month_text, amazon_order_id, sku),
            ).fetchall()
            for (settlement_line_id,) in settlement_rows:
                bridge_rows.append((order_line_id, settlement_line_id, 'exact_order_sku', utc_now_iso()))

        if bridge_rows:
            conn.executemany(
                """
                INSERT INTO bridge_orderline_settlement (
                    order_line_id,
                    settlement_line_id,
                    match_method,
                    matched_at
                ) VALUES (?, ?, ?, ?)
                """,
                bridge_rows,
            )

        conn.execute(
            "UPDATE fact_order_lines SET settlement_state = NULL WHERE order_month = ?",
            (month_text,),
        )

        state_rows = conn.execute(
            """
            SELECT
                o.order_line_id,
                o.order_status,
                COUNT(b.settlement_line_id) AS linked_count,
                SUM(CASE WHEN s.transaction_type = 'Order' THEN 1 ELSE 0 END) AS order_count,
                SUM(CASE WHEN s.transaction_type = 'Refund' THEN 1 ELSE 0 END) AS refund_count,
                SUM(CASE WHEN COALESCE(s.transaction_status, '') = 'Released' THEN 1 ELSE 0 END) AS released_count
            FROM fact_order_lines o
            LEFT JOIN bridge_orderline_settlement b
                ON o.order_line_id = b.order_line_id
            LEFT JOIN fact_settlement_lines s
                ON b.settlement_line_id = s.settlement_line_id
            WHERE o.order_month = ?
              AND o.is_amazon_channel = 1
            GROUP BY o.order_line_id, o.order_status
            """,
            (month_text,),
        ).fetchall()

        updates = []
        for order_line_id, order_status, linked_count, order_count, refund_count, released_count in state_rows:
            linked_count = linked_count or 0
            order_count = order_count or 0
            refund_count = refund_count or 0
            released_count = released_count or 0

            if order_status == 'Cancelled' and linked_count == 0:
                state = 'cancelled_before_settlement'
            elif order_status in ('Pending', 'Shipping') and linked_count == 0:
                state = 'pending_not_shipped'
            elif linked_count == 0:
                state = 'shipped_waiting_settlement'
            elif order_count > 0 and refund_count > 0:
                state = 'refunded_after_settlement'
            elif linked_count > 0 and released_count == linked_count:
                state = 'fully_settled_released'
            elif linked_count > 0:
                state = 'fully_settled_unreleased'
            else:
                state = 'exception_needs_review'

            updates.append((state, order_line_id))

        conn.executemany(
            "UPDATE fact_order_lines SET settlement_state = ? WHERE order_line_id = ?",
            updates,
        )

        conn.commit()
        note = f'Built {len(bridge_rows)} bridge rows and updated {len(updates)} order line states for {month_text}'
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
