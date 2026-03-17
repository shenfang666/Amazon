from __future__ import annotations

import argparse
import json

from common import connect, finish_etl_run, get_config, print_banner, register_etl_run, utc_now_iso


DETAIL_SUM_METRICS = [
    'qty_sold',
    'product_sales',
    'shipping_credits',
    'gift_wrap_credits',
    'promotional_rebates',
    'net_sales',
    'selling_fees',
    'fba_fees',
    'other_transaction_fees',
    'marketplace_withheld_tax',
    'storage_fees',
    'removal_fees',
    'removal_fee_capitalized',
    'removal_fee_unclassified',
    'ad_spend',
    'compensation_income',
    'review_cost',
    'vine_fee',
    'subscription_fee',
    'coupon_participation_fee',
    'coupon_performance_fee',
    'product_cost',
    'inbound_cost',
    'receivable_ad_spend',
    'receivable_storage_fees',
    'receivable_removal_fees',
    'receivable_compensation_income',
    'receivable_subscription_fee',
    'receivable_coupon_participation_fee',
    'receivable_coupon_performance_fee',
    'receivable_vine_fee',
    'inventory_capitalized_cost',
    'settlement_net_total',
]


def normalize_month(value: str) -> str:
    text = value.strip()
    if len(text) == 7 and '-' in text:
        return text
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}"
    raise ValueError('target_month must be YYYY-MM or YYYYMM')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run month close checks.')
    parser.add_argument('target_month', help='YYYY-MM or YYYYMM')
    return parser.parse_args()


def insert_issue(
    conn,
    month_text: str,
    severity: str,
    issue_code: str,
    issue_key: str | None,
    issue_value: str | None,
    metric_value: float | None,
    source_table: str | None,
    source_ref: str | None,
    note: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO monthly_close_issue_detail (
            period_month,
            severity,
            issue_code,
            issue_key,
            issue_value,
            metric_value,
            source_table,
            source_ref,
            note,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (month_text, severity, issue_code, issue_key, issue_value, metric_value, source_table, source_ref, note, utc_now_iso()),
    )


def collect_detail_rollup_mismatches(conn, month_text: str) -> list[tuple[str, float, float, float]]:
    summary_select = ',\n                '.join([f"COALESCE(SUM({metric}), 0) AS {metric}" for metric in DETAIL_SUM_METRICS])
    comparison_select = ',\n            '.join(
        [
            f"summary.{metric} AS summary_{metric}, detail.{metric} AS detail_{metric}"
            for metric in DETAIL_SUM_METRICS
        ]
    )
    row = conn.execute(
        f"""
        WITH summary AS (
            SELECT
                {summary_select}
            FROM v_monthly_sku_order_type_summary
            WHERE period_month = ?
        ),
        detail AS (
            SELECT
                {summary_select}
            FROM v_finance_detail_lines
            WHERE period_month = ?
        )
        SELECT
            {comparison_select}
        FROM summary
        CROSS JOIN detail
        """,
        (month_text, month_text),
    ).fetchone()
    mismatches: list[tuple[str, float, float, float]] = []
    for metric in DETAIL_SUM_METRICS:
        summary_value = float(row[f'summary_{metric}'] or 0)
        detail_value = float(row[f'detail_{metric}'] or 0)
        diff_value = detail_value - summary_value
        if abs(diff_value) > 0.01:
            mismatches.append((metric, summary_value, detail_value, diff_value))
    return mismatches



def refresh_receivable_snapshot(conn, month_text: str) -> dict:
    previous = conn.execute(
        """
        SELECT closing_receivable
        FROM fact_platform_receivable_snapshot
        WHERE period_month < ?
          AND platform_code = 'amazon'
        ORDER BY period_month DESC
        LIMIT 1
        """,
        (month_text,),
    ).fetchone()
    opening_receivable = float(previous['closing_receivable'] or 0) if previous else 0.0

    current_row = conn.execute(
        """
        SELECT
            COALESCE(SUM(net_sales), 0)
            - COALESCE(SUM(selling_fees), 0)
            - COALESCE(SUM(fba_fees), 0)
            - COALESCE(SUM(other_transaction_fees), 0)
            - COALESCE(SUM(marketplace_withheld_tax), 0)
            - COALESCE(SUM(receivable_storage_fees), 0)
            - COALESCE(SUM(receivable_removal_fees), 0)
            - COALESCE(SUM(receivable_ad_spend), 0)
            + COALESCE(SUM(receivable_compensation_income), 0)
            - COALESCE(SUM(receivable_subscription_fee), 0)
            - COALESCE(SUM(receivable_coupon_participation_fee), 0)
            - COALESCE(SUM(receivable_coupon_performance_fee), 0)
            - COALESCE(SUM(receivable_vine_fee), 0) AS receivable_amount
        FROM v_monthly_sku_order_type_summary
        WHERE period_month = ?
        """,
        (month_text,),
    ).fetchone()
    current_receivable = float(current_row['receivable_amount'] or 0) if current_row else 0.0

    current_receipts = float(
        conn.execute(
            """
            SELECT COALESCE(SUM(receipt_amount), 0)
            FROM fact_platform_receipts
            WHERE period_month = ?
              AND platform_code = 'amazon'
            """,
            (month_text,),
        ).fetchone()[0]
        or 0
    )
    expected_total = opening_receivable + current_receivable
    closing_receivable = expected_total - current_receipts
    unmatched_receipts = max(0.0, current_receipts - expected_total)
    receivable_gap = closing_receivable if abs(closing_receivable) > 0.01 else 0.0
    reconciliation_status = 'balanced' if abs(receivable_gap) <= 0.01 else 'pending'
    notes = json.dumps(
        {
            'opening_receivable': round(opening_receivable, 2),
            'current_receivable': round(current_receivable, 2),
            'current_receipts': round(current_receipts, 2),
        },
        ensure_ascii=False,
    )

    conn.execute(
        """
        INSERT INTO fact_platform_receivable_snapshot (
            period_month,
            platform_code,
            store_code,
            opening_receivable,
            current_receivable,
            current_receipts,
            closing_receivable,
            unmatched_receipts,
            receivable_gap,
            reconciliation_status,
            generated_at,
            notes
        ) VALUES (?, 'amazon', '', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(period_month, platform_code, store_code) DO UPDATE SET
            opening_receivable = excluded.opening_receivable,
            current_receivable = excluded.current_receivable,
            current_receipts = excluded.current_receipts,
            closing_receivable = excluded.closing_receivable,
            unmatched_receipts = excluded.unmatched_receipts,
            receivable_gap = excluded.receivable_gap,
            reconciliation_status = excluded.reconciliation_status,
            generated_at = excluded.generated_at,
            notes = excluded.notes
        """,
        (
            month_text,
            opening_receivable,
            current_receivable,
            current_receipts,
            closing_receivable,
            unmatched_receipts,
            receivable_gap,
            reconciliation_status,
            utc_now_iso(),
            notes,
        ),
    )
    return {
        'opening_receivable': opening_receivable,
        'current_receivable': current_receivable,
        'current_receipts': current_receipts,
        'closing_receivable': closing_receivable,
        'unmatched_receipts': unmatched_receipts,
        'receivable_gap': receivable_gap,
        'reconciliation_status': reconciliation_status,
    }


def sync_month_close_state(conn, month_text: str, blockers: list[str], warnings: list[str], receivable_snapshot: dict) -> str:
    if blockers:
        state_code = 'exception_pending'
    elif warnings:
        state_code = 'exception_pending'
    elif abs(float(receivable_snapshot.get('receivable_gap') or 0)) > 0.01:
        state_code = 'receivable_pending'
    else:
        state_code = 'inventory_pending'

    latest = conn.execute(
        """
        SELECT state_code
        FROM month_close_state_log
        WHERE period_month = ?
        ORDER BY state_log_id DESC
        LIMIT 1
        """,
        (month_text,),
    ).fetchone()
    latest_state = latest['state_code'] if latest else None
    if latest_state != state_code:
        conn.execute(
            """
            INSERT INTO month_close_state_log (
                period_month,
                state_code,
                state_source,
                state_note,
                created_by,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (month_text, state_code, 'system_check', 'updated by month close checks', 'system', utc_now_iso()),
        )
    return state_code

def main() -> int:
    args = parse_args()
    month_text = normalize_month(args.target_month)
    config = get_config()
    print_banner(f'Running month-close checks for {month_text}')
    conn = connect(config.db_path)
    run_id = register_etl_run(conn, '17_run_month_close_checks.py', 'month_close_checks', target_month=month_text, status='started')

    try:
        blockers: list[str] = []
        warnings: list[str] = []
        conn.execute('DELETE FROM monthly_close_issue_detail WHERE period_month = ?', (month_text,))

        pending_rows = conn.execute(
            """
            select pending_id, source_table, source_file, ambiguous_value, mapping_type, notes
            from pending_mapping_queue
            where status='pending'
            order by source_table, pending_id
            """
        ).fetchall()
        if pending_rows:
            blockers.append(f'pending_mappings={len(pending_rows)}')
            for row in pending_rows:
                insert_issue(
                    conn,
                    month_text,
                    'blocker',
                    'pending_mapping',
                    f"{row['source_table']}:{row['pending_id']}",
                    row['ambiguous_value'],
                    None,
                    row['source_table'],
                    row['source_file'],
                    f"{row['mapping_type']} | {row['notes']}",
                )

        missing_cost_rows = conn.execute(
            """
            select sku, sum(qty_sold) as qty_sold, sum(net_sales) as net_sales
            from v_monthly_sku_order_type_summary
            where period_month = ?
              and qty_sold > 0
              and product_unit_cost = 0
            group by sku
            order by qty_sold desc, sku
            """,
            (month_text,),
        ).fetchall()
        if missing_cost_rows:
            blockers.append(f'missing_product_cost_skus={len(missing_cost_rows)}')
            for row in missing_cost_rows:
                insert_issue(
                    conn,
                    month_text,
                    'blocker',
                    'missing_product_cost',
                    row['sku'],
                    row['sku'],
                    float(row['qty_sold'] or 0),
                    'v_monthly_sku_order_type_summary',
                    row['sku'],
                    f"net_sales={float(row['net_sales'] or 0):.2f}",
                )

        waiting_rows = conn.execute(
            """
            select order_line_id, amazon_order_id, purchase_date, sku, quantity
            from fact_order_lines
            where order_month = ?
              and is_amazon_channel = 1
              and settlement_state = 'shipped_waiting_settlement'
            order by purchase_date, amazon_order_id, order_line_id
            """,
            (month_text,),
        ).fetchall()
        if waiting_rows:
            warnings.append(f'shipped_waiting_settlement={len(waiting_rows)}')
            for row in waiting_rows:
                insert_issue(
                    conn,
                    month_text,
                    'warning',
                    'shipped_waiting_settlement',
                    row['order_line_id'],
                    row['amazon_order_id'],
                    float(row['quantity'] or 0),
                    'fact_order_lines',
                    row['order_line_id'],
                    f"purchase_date={row['purchase_date']}; sku={row['sku']}",
                )

        storage_rows = conn.execute(
            """
            select storage_id, asin, fnsku, estimated_monthly_storage_fee - incentive_fee_amount as net_storage_fee
            from fact_storage_monthly_sku
            where period_month = ?
              and sku is null
            order by storage_id
            """,
            (month_text,),
        ).fetchall()
        if storage_rows:
            warnings.append(f'storage_unmapped_rows={len(storage_rows)}')
            for row in storage_rows:
                insert_issue(
                    conn,
                    month_text,
                    'warning',
                    'storage_unmapped',
                    str(row['storage_id']),
                    row['asin'] or row['fnsku'],
                    float(row['net_storage_fee'] or 0),
                    'fact_storage_monthly_sku',
                    str(row['storage_id']),
                    f"asin={row['asin']}; fnsku={row['fnsku']}",
                )

        removal_control_rows = conn.execute(
            """
            select r.order_id, r.sku, r.removal_order_type, r.disposition, r.removal_fee
            from fact_removal_monthly_sku r
            left join manual_removal_fee_controls c
              on r.period_month = c.period_month
             and r.order_id = c.order_id
            where r.period_month = ?
              and r.sku is not null
              and abs(coalesce(r.removal_fee, 0)) > 0.000001
              and lower(coalesce(r.removal_order_type, '')) <> 'disposal'
              and coalesce(r.disposed_quantity, 0) = 0
              and c.order_id is null
            order by r.order_id, r.sku
            """,
            (month_text,),
        ).fetchall()
        if removal_control_rows:
            blockers.append(f'removal_fee_control_missing={len(removal_control_rows)}')
            for row in removal_control_rows:
                insert_issue(
                    conn,
                    month_text,
                    'blocker',
                    'removal_fee_control_missing',
                    row['order_id'],
                    row['sku'],
                    float(row['removal_fee'] or 0),
                    'fact_removal_monthly_sku',
                    row['order_id'],
                    f"removal_order_type={row['removal_order_type']}; disposition={row['disposition']}",
                )

        unknown_order_type_rows = conn.execute(
            """
            select settlement_line_id, order_id, sku, transaction_type
            from fact_settlement_lines
            where transaction_month = ?
              and transaction_type in ('Order', 'Refund')
              and sku is not null
              and order_id is not null
              and (
                    order_type is null
                    or trim(order_type) = ''
                    or lower(order_type) = 'unknown'
              )
            order by order_id, sku, settlement_line_id
            """,
            (month_text,),
        ).fetchall()
        if unknown_order_type_rows:
            blockers.append(f'unknown_order_type_in_settlement_detail={len(unknown_order_type_rows)}')
            for row in unknown_order_type_rows:
                insert_issue(
                    conn,
                    month_text,
                    'blocker',
                    'unknown_order_type_in_settlement_detail',
                    row['order_id'],
                    row['sku'],
                    None,
                    'fact_settlement_lines',
                    row['settlement_line_id'],
                    f"transaction_type={row['transaction_type']}",
                )

        raw_refund_qty = conn.execute(
            """
            select coalesce(sum(coalesce(quantity, 0)), 0)
            from fact_settlement_lines
            where transaction_month = ?
              and order_type like '%refund'
              and transaction_type in ('Order', 'Refund')
            """,
            (month_text,),
        ).fetchone()[0]
        summary_refund_qty = conn.execute(
            """
            select coalesce(sum(qty_sold), 0)
            from v_monthly_sku_order_type_summary
            where period_month = ?
              and order_type like '%refund'
            """,
            (month_text,),
        ).fetchone()[0]
        if abs(float(raw_refund_qty or 0) - float(summary_refund_qty or 0)) > 0.01:
            blockers.append(f'refund_qty_mismatch=raw:{raw_refund_qty:.2f},summary:{summary_refund_qty:.2f}')
            insert_issue(
                conn,
                month_text,
                'blocker',
                'refund_qty_mismatch',
                month_text,
                month_text,
                float(summary_refund_qty or 0) - float(raw_refund_qty or 0),
                'v_monthly_sku_order_type_summary',
                month_text,
                f'raw={raw_refund_qty:.2f}; summary={summary_refund_qty:.2f}',
            )

        raw_ad_report = conn.execute(
            '''
            select coalesce(sum(coalesce(spend, 0)), 0)
            from fact_advertising_monthly_sku
            where period_month = ?
            ''',
            (month_text,),
        ).fetchone()[0]
        settlement_ad = conn.execute(
            '''
            select coalesce(sum(-coalesce(total, 0)), 0)
            from fact_settlement_lines
            where transaction_month = ?
              and lower(coalesce(transaction_subtype, '')) = 'cost of advertising'
            ''',
            (month_text,),
        ).fetchone()[0]
        if abs(float(raw_ad_report or 0) - float(settlement_ad or 0)) > 0.01:
            blockers.append(f'ad_report_settlement_mismatch=report:{raw_ad_report:.2f},settlement:{settlement_ad:.2f}')
            insert_issue(
                conn,
                month_text,
                'blocker',
                'ad_report_settlement_mismatch',
                month_text,
                month_text,
                float(settlement_ad or 0) - float(raw_ad_report or 0),
                'fact_advertising_monthly_sku',
                month_text,
                f'report={raw_ad_report:.2f}; settlement={settlement_ad:.2f}',
            )

        report_storage = conn.execute(
            '''
            select coalesce(sum(coalesce(estimated_monthly_storage_fee, 0) - coalesce(incentive_fee_amount, 0)), 0)
            from fact_storage_monthly_sku
            where period_month = ?
            ''',
            (month_text,),
        ).fetchone()[0]
        settlement_storage = conn.execute(
            '''
            select coalesce(sum(
                case
                    when lower(coalesce(transaction_subtype, '')) in ('fba storage fee', 'fba long-term storage fee')
                        then -coalesce(total, 0)
                    else 0
                end
            ), 0)
            from fact_settlement_lines
            where transaction_month = ?
            ''',
            (month_text,),
        ).fetchone()[0]
        if abs(float(report_storage or 0) - float(settlement_storage or 0)) > 0.01:
            blockers.append(f'storage_report_settlement_mismatch=report:{report_storage:.2f},settlement:{settlement_storage:.2f}')
            insert_issue(
                conn,
                month_text,
                'blocker',
                'storage_report_settlement_mismatch',
                month_text,
                month_text,
                float(settlement_storage or 0) - float(report_storage or 0),
                'fact_storage_monthly_sku',
                month_text,
                f'report={report_storage:.2f}; settlement={settlement_storage:.2f}',
            )

        raw_storage = conn.execute(
            """
            select coalesce(sum(coalesce(estimated_monthly_storage_fee, 0) - coalesce(incentive_fee_amount, 0)), 0)
            from fact_storage_monthly_sku
            where period_month = ?
            """,
            (month_text,),
        ).fetchone()[0]
        summary_storage = conn.execute(
            """
            select coalesce(sum(storage_fees), 0)
            from v_monthly_sku_order_type_summary
            where period_month = ?
            """,
            (month_text,),
        ).fetchone()[0]
        if abs(float(raw_storage or 0) - float(summary_storage or 0)) > 0.01:
            blockers.append(f'storage_fee_mismatch=raw:{raw_storage:.2f},summary:{summary_storage:.2f}')
            insert_issue(
                conn,
                month_text,
                'blocker',
                'storage_fee_mismatch',
                month_text,
                month_text,
                float(summary_storage or 0) - float(raw_storage or 0),
                'v_monthly_sku_order_type_summary',
                month_text,
                f'raw={raw_storage:.2f}; summary={summary_storage:.2f}',
            )

        raw_compensation = conn.execute(
            """
            select coalesce(sum(coalesce(amount_total, 0)), 0)
            from fact_compensation_monthly_sku
            where period_month = ?
            """,
            (month_text,),
        ).fetchone()[0]
        summary_compensation = conn.execute(
            """
            select coalesce(sum(compensation_income), 0)
            from v_monthly_sku_order_type_summary
            where period_month = ?
            """,
            (month_text,),
        ).fetchone()[0]
        if abs(float(raw_compensation or 0) - float(summary_compensation or 0)) > 0.01:
            blockers.append(f'compensation_mismatch=raw:{raw_compensation:.2f},summary:{summary_compensation:.2f}')
            insert_issue(
                conn,
                month_text,
                'blocker',
                'compensation_mismatch',
                month_text,
                month_text,
                float(summary_compensation or 0) - float(raw_compensation or 0),
                'v_monthly_sku_order_type_summary',
                month_text,
                f'raw={raw_compensation:.2f}; summary={summary_compensation:.2f}',
            )

        raw_shipping_credits = conn.execute(
            """
            select coalesce(sum(coalesce(shipping_credits, 0)), 0)
            from fact_settlement_lines
            where transaction_month = ?
            """,
            (month_text,),
        ).fetchone()[0]
        summary_shipping_credits = conn.execute(
            """
            select coalesce(sum(shipping_credits), 0)
            from v_monthly_sku_order_type_summary
            where period_month = ?
            """,
            (month_text,),
        ).fetchone()[0]
        if abs(float(raw_shipping_credits or 0) - float(summary_shipping_credits or 0)) > 0.01:
            blockers.append(f'shipping_credits_mismatch=raw:{raw_shipping_credits:.2f},summary:{summary_shipping_credits:.2f}')
            insert_issue(
                conn,
                month_text,
                'blocker',
                'shipping_credits_mismatch',
                month_text,
                month_text,
                float(summary_shipping_credits or 0) - float(raw_shipping_credits or 0),
                'v_monthly_sku_order_type_summary',
                month_text,
                f'raw={raw_shipping_credits:.2f}; summary={summary_shipping_credits:.2f}',
            )

        raw_gift_wrap_credits = conn.execute(
            """
            select coalesce(sum(coalesce(gift_wrap_credits, 0)), 0)
            from fact_settlement_lines
            where transaction_month = ?
            """,
            (month_text,),
        ).fetchone()[0]
        summary_gift_wrap_credits = conn.execute(
            """
            select coalesce(sum(gift_wrap_credits), 0)
            from v_monthly_sku_order_type_summary
            where period_month = ?
            """,
            (month_text,),
        ).fetchone()[0]
        if abs(float(raw_gift_wrap_credits or 0) - float(summary_gift_wrap_credits or 0)) > 0.01:
            blockers.append(f'gift_wrap_credits_mismatch=raw:{raw_gift_wrap_credits:.2f},summary:{summary_gift_wrap_credits:.2f}')
            insert_issue(
                conn,
                month_text,
                'blocker',
                'gift_wrap_credits_mismatch',
                month_text,
                month_text,
                float(summary_gift_wrap_credits or 0) - float(raw_gift_wrap_credits or 0),
                'v_monthly_sku_order_type_summary',
                month_text,
                f'raw={raw_gift_wrap_credits:.2f}; summary={summary_gift_wrap_credits:.2f}',
            )

        detail_rollup_mismatches = collect_detail_rollup_mismatches(conn, month_text)
        if detail_rollup_mismatches:
            blockers.append(f'detail_rollup_mismatch={len(detail_rollup_mismatches)}')
            for metric, summary_value, detail_value, diff_value in detail_rollup_mismatches:
                insert_issue(
                    conn,
                    month_text,
                    'blocker',
                    'detail_rollup_mismatch',
                    metric,
                    metric,
                    diff_value,
                    'v_finance_detail_lines',
                    month_text,
                    f'summary={summary_value:.6f}; detail={detail_value:.6f}',
                )

        review_unmapped = conn.execute(
            """
            select count(*)
            from pending_mapping_queue
            where status='pending'
              and source_table='fact_review_orders'
            """
        ).fetchone()[0]
        if review_unmapped > 0:
            blockers.append(f'review_mapping_pending={review_unmapped}')

        vine_source = conn.execute(
            """
            select coalesce(sum(abs(amount_total)), 0)
            from fact_platform_fee_lines
            where period_month = ?
              and fee_type = 'vine_enrollment_fee_source'
            """,
            (month_text,),
        ).fetchone()[0]
        vine_allocated = conn.execute(
            """
            select coalesce(sum(fee_amount), 0)
            from manual_vine_fee_allocations
            where period_month = ?
            """,
            (month_text,),
        ).fetchone()[0]
        if vine_source > 0 and abs(vine_source - vine_allocated) > 0.01:
            blockers.append(f'vine_fee_unallocated=source:{vine_source:.2f},allocated:{vine_allocated:.2f}')
            insert_issue(
                conn,
                month_text,
                'blocker',
                'vine_fee_unallocated',
                month_text,
                month_text,
                float(vine_source - vine_allocated),
                'fact_platform_fee_lines',
                month_text,
                f'source={vine_source:.2f}; allocated={vine_allocated:.2f}',
            )

        subscription_source = conn.execute(
            """
            select coalesce(sum(abs(amount_total)), 0)
            from fact_platform_fee_lines
            where period_month = ?
              and fee_type = 'subscription_fee'
            """,
            (month_text,),
        ).fetchone()[0]
        if subscription_source == 0:
            warnings.append('subscription_fee_source_missing_or_zero')
            insert_issue(
                conn,
                month_text,
                'warning',
                'subscription_fee_source_missing_or_zero',
                month_text,
                month_text,
                0,
                'fact_platform_fee_lines',
                month_text,
                'No subscription fee source found in settlement fee extraction',
            )

        receivable_snapshot = refresh_receivable_snapshot(conn, month_text)
        if abs(float(receivable_snapshot.get('receivable_gap') or 0)) > 0.01:
            warnings.append(f"receivable_gap={receivable_snapshot['receivable_gap']:.2f}")
            insert_issue(
                conn,
                month_text,
                'warning',
                'receivable_reconciliation_pending',
                month_text,
                month_text,
                float(receivable_snapshot['receivable_gap']),
                'fact_platform_receivable_snapshot',
                month_text,
                f"opening={receivable_snapshot['opening_receivable']:.2f}; current={receivable_snapshot['current_receivable']:.2f}; receipts={receivable_snapshot['current_receipts']:.2f}",
            )

        close_status = 'blocked' if blockers else 'warning' if warnings else 'ready'
        business_state = sync_month_close_state(conn, month_text, blockers, warnings, receivable_snapshot)
        notes_payload = json.dumps(
            {
                'blockers': blockers,
                'warnings': warnings,
                'business_state': business_state,
            },
            ensure_ascii=False,
        )

        conn.execute(
            """
            INSERT INTO monthly_close_log (
                period_month,
                close_status,
                blocker_count,
                warning_count,
                receivable_gap,
                closed_at,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(period_month) DO UPDATE SET
                close_status = excluded.close_status,
                blocker_count = excluded.blocker_count,
                warning_count = excluded.warning_count,
                receivable_gap = excluded.receivable_gap,
                closed_at = excluded.closed_at,
                notes = excluded.notes
            """,
            (
                month_text,
                close_status,
                len(blockers),
                len(warnings),
                round(float(receivable_snapshot.get('receivable_gap') or 0), 2),
                utc_now_iso(),
                notes_payload,
            ),
        )

        conn.commit()
        note = f'status={close_status}; business_state={business_state}; blockers={len(blockers)}; warnings={len(warnings)}'
        finish_etl_run(conn, run_id, 'success', note)
        conn.commit()
        print_banner(note)
        print('BLOCKERS')
        for item in blockers:
            print(item)
        print('WARNINGS')
        for item in warnings:
            print(item)
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
