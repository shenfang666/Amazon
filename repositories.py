from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path


def query_all(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def query_one(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> dict:
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else {}


def get_months(conn: sqlite3.Connection) -> list[str]:
    rows = query_all(
        conn,
        """
        SELECT DISTINCT month
        FROM (
            SELECT order_month AS month
            FROM fact_order_lines
            WHERE COALESCE(trim(order_month), '') <> ''
            UNION
            SELECT transaction_month AS month
            FROM fact_settlement_lines
            WHERE COALESCE(trim(transaction_month), '') <> ''
            UNION
            SELECT period_month AS month
            FROM monthly_close_log
            WHERE COALESCE(trim(period_month), '') <> ''
            UNION
            SELECT period_month AS month
            FROM fact_platform_receivable_snapshot
            WHERE COALESCE(trim(period_month), '') <> ''
            UNION
            SELECT period_month AS month
            FROM fact_inventory_movements
            WHERE COALESCE(trim(period_month), '') <> ''
            UNION
            SELECT period_month AS month
            FROM fact_inventory_snapshot
            WHERE COALESCE(trim(period_month), '') <> ''
        )
        ORDER BY month DESC
        """,
    )
    return [row["month"] for row in rows]


def fetch_dashboard_close_timeline(conn: sqlite3.Connection) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT
            period_month,
            close_status,
            blocker_count,
            warning_count,
            pdf_amount,
            receivable_gap,
            notes,
            closed_at
        FROM monthly_close_log
        ORDER BY period_month DESC
        """,
    )


def fetch_dashboard_top_skus(conn: sqlite3.Connection, month: str, gross_profit_expr: str) -> list[dict]:
    return query_all(
        conn,
        f"""
        SELECT
            sku,
            ROUND(SUM(qty_sold), 2) AS qty_sold,
            ROUND(SUM(net_sales), 2) AS net_sales,
            ROUND(SUM(ad_spend), 2) AS ad_spend,
            ROUND(SUM({gross_profit_expr}), 2) AS gross_profit,
            ROUND(CASE WHEN SUM(net_sales) = 0 THEN 0 ELSE (SUM({gross_profit_expr}) / SUM(net_sales)) * 100 END, 2) AS margin_pct
        FROM v_monthly_sku_order_type_summary
        WHERE period_month = ?
        GROUP BY sku
        ORDER BY gross_profit DESC, net_sales DESC
        LIMIT 8
        """,
        (month,),
    )


def fetch_dashboard_alerts(conn: sqlite3.Connection, month: str) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT
            severity,
            issue_code,
            issue_value,
            metric_value,
            note,
            created_at
        FROM monthly_close_issue_detail
        WHERE period_month = ?
        ORDER BY CASE severity WHEN 'blocker' THEN 0 ELSE 1 END, created_at DESC
        LIMIT 20
        """,
        (month,),
    )


def fetch_receivable_balances(conn: sqlite3.Connection) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT
            period_month,
            platform_code,
            opening_receivable,
            current_receivable,
            current_receipts,
            closing_receivable,
            unmatched_receipts,
            receivable_gap,
            reconciliation_status,
            generated_at
        FROM fact_platform_receivable_snapshot
        WHERE platform_code = 'amazon'
          AND store_code = ''
        ORDER BY period_month DESC
        """,
    )


def fetch_unmatched_receipt_rows(conn: sqlite3.Connection, month: str) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT
            receipt_date,
            receipt_reference,
            settlement_id,
            currency,
            ROUND(receipt_amount, 2) AS receipt_amount,
            receipt_type,
            memo
        FROM fact_platform_receipts
        WHERE period_month = ?
          AND (settlement_id IS NULL OR trim(settlement_id) = '')
        ORDER BY receipt_date DESC, receipt_id DESC
        """,
        (month,),
    )


def fetch_profit_sku_details(conn: sqlite3.Connection, month: str, gross_profit_expr: str) -> list[dict]:
    return query_all(
        conn,
        f"""
        SELECT
            sku,
            ROUND(SUM(qty_sold), 2) AS qty_sold,
            ROUND(SUM(net_sales), 2) AS net_sales,
            ROUND(SUM(ad_spend), 2) AS ad_spend,
            ROUND(SUM({gross_profit_expr}), 2) AS gross_profit,
            ROUND(CASE WHEN SUM(net_sales) = 0 THEN 0 ELSE (SUM({gross_profit_expr}) / SUM(net_sales)) * 100 END, 2) AS margin_pct,
            ROUND(CASE WHEN SUM(net_sales) = 0 THEN 0 ELSE (SUM(ad_spend) / SUM(net_sales)) * 100 END, 2) AS acos_pct
        FROM v_monthly_sku_order_type_summary
        WHERE period_month = ?
        GROUP BY sku
        ORDER BY gross_profit DESC, net_sales DESC
        """,
        (month,),
    )


def fetch_profit_order_details(conn: sqlite3.Connection, month: str) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT
            amazon_order_id,
            purchase_date,
            sku,
            order_status,
            settlement_state,
            ROUND(COALESCE(item_price, 0), 2) AS item_price,
            ROUND(COALESCE(item_promotion_discount, 0), 2) AS item_promotion_discount,
            ROUND(COALESCE(settled_product_sales, 0), 2) AS settled_product_sales,
            ROUND(COALESCE(settled_order_net, 0), 2) AS settled_order_net,
            released_line_count
        FROM v_order_settlement_tracking
        WHERE order_month = ?
        ORDER BY purchase_date DESC
        """,
        (month,),
    )


def fetch_inventory_periods(conn: sqlite3.Connection) -> list[str]:
    movement_months = {
        row["period_month"]
        for row in query_all(
            conn,
            """
            SELECT DISTINCT period_month
            FROM fact_inventory_movements
            WHERE COALESCE(trim(period_month), '') <> ''
            """,
        )
    }
    snapshot_months = {
        row["period_month"]
        for row in query_all(
            conn,
            """
            SELECT DISTINCT period_month
            FROM fact_inventory_snapshot
            WHERE COALESCE(trim(period_month), '') <> ''
            """,
        )
    }
    close_months = {
        row["period_month"]
        for row in query_all(
            conn,
            """
            SELECT DISTINCT period_month
            FROM monthly_close_log
            WHERE COALESCE(trim(period_month), '') <> ''
            """,
        )
    }
    return sorted(set(get_months(conn)) | movement_months | snapshot_months | close_months)


def fetch_inventory_snapshot_exists(conn: sqlite3.Connection, month: str) -> int:
    return int(
        query_one(
            conn,
            """
            SELECT COUNT(*) AS total
            FROM fact_inventory_snapshot
            WHERE period_month = ?
            """,
            (month,),
        ).get("total", 0)
        or 0
    )


def fetch_inventory_summary(conn: sqlite3.Connection, month: str) -> dict:
    return query_one(
        conn,
        """
        SELECT
            COUNT(*) AS snapshot_count,
            COALESCE(SUM(opening_qty), 0) AS opening_qty,
            COALESCE(SUM(inbound_qty), 0) AS inbound_qty,
            COALESCE(SUM(outbound_qty), 0) AS outbound_qty,
            COALESCE(SUM(transfer_qty), 0) AS transfer_qty,
            COALESCE(SUM(return_qty), 0) AS return_qty,
            COALESCE(SUM(adjust_qty), 0) AS adjust_qty,
            COALESCE(SUM(closing_qty), 0) AS closing_qty,
            COALESCE(SUM(CASE WHEN closing_qty < -0.01 THEN 1 ELSE 0 END), 0) AS negative_sku_count
        FROM fact_inventory_snapshot
        WHERE period_month = ?
        """,
        (month,),
    )


def fetch_inventory_movement_count(conn: sqlite3.Connection, month: str) -> int:
    return int(
        query_one(
            conn,
            """
            SELECT COUNT(*) AS total
            FROM fact_inventory_movements
            WHERE period_month = ?
            """,
            (month,),
        ).get("total", 0)
        or 0
    )


def fetch_inventory_snapshot_rows(conn: sqlite3.Connection, month: str) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT
            sku,
            ROUND(opening_qty, 2) AS opening_qty,
            ROUND(inbound_qty, 2) AS inbound_qty,
            ROUND(outbound_qty, 2) AS outbound_qty,
            ROUND(transfer_qty, 2) AS transfer_qty,
            ROUND(return_qty, 2) AS return_qty,
            ROUND(adjust_qty, 2) AS adjust_qty,
            ROUND(closing_qty, 2) AS closing_qty,
            generated_at
        FROM fact_inventory_snapshot
        WHERE period_month = ?
        ORDER BY ABS(closing_qty) DESC, sku
        """,
        (month,),
    )


def fetch_inventory_movement_rows(conn: sqlite3.Connection, month: str) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT
            movement_id,
            movement_date,
            movement_type,
            sku,
            ROUND(quantity, 2) AS quantity,
            ROUND(unit_cost, 2) AS unit_cost,
            ROUND(amount_total, 2) AS amount_total,
            source_ref,
            created_at
        FROM fact_inventory_movements
        WHERE period_month = ?
        ORDER BY COALESCE(movement_date, created_at) DESC, movement_id DESC
        LIMIT 200
        """,
        (month,),
    )


def fetch_inventory_adjustment_rows(conn: sqlite3.Connection, month: str) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT
            adjustment_id,
            target_table,
            target_key,
            adjustment_type,
            notes,
            adjusted_by,
            adjusted_at
        FROM manual_adjustment_log
        WHERE target_key LIKE ?
        ORDER BY adjustment_id DESC
        LIMIT 20
        """,
        (f"{month}:%",),
    )


def fetch_exception_override_rows(conn: sqlite3.Connection, months: list[str]) -> list[dict]:
    if not months:
        return []
    placeholders = ", ".join("?" for _ in months)
    return query_all(
        conn,
        f"""
        SELECT
            exception_case_id,
            period_month,
            exception_code,
            exception_type,
            source_table,
            source_ref,
            order_id,
            sku,
            user_choice,
            case_status,
            note,
            updated_at
        FROM manual_exception_case
        WHERE period_month IN ({placeholders})
          AND COALESCE(source_table, '') <> ''
        ORDER BY exception_case_id DESC
        """,
        tuple(months),
    )


def fetch_latest_month_close_state_rows(conn: sqlite3.Connection) -> list[dict]:
    return query_all(
        conn,
        """
        WITH latest AS (
            SELECT period_month, MAX(state_log_id) AS state_log_id
            FROM month_close_state_log
            GROUP BY period_month
        )
        SELECT l.period_month, s.state_code
        FROM latest l
        JOIN month_close_state_log s
          ON s.period_month = l.period_month
         AND s.state_log_id = l.state_log_id
        """,
    )


def fetch_issue_month_rows(conn: sqlite3.Connection) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT DISTINCT period_month
        FROM monthly_close_issue_detail
        ORDER BY period_month DESC
        """,
    )


def fetch_month_close_issue_rows(
    conn: sqlite3.Connection,
    months: list[str],
    select_sql: str,
    order_by: str | None = None,
) -> list[dict]:
    if not months:
        return []
    placeholders = ", ".join("?" for _ in months)
    sql = f"""
        SELECT {select_sql}
        FROM monthly_close_issue_detail
        WHERE period_month IN ({placeholders})
    """
    if order_by:
        sql += f"\nORDER BY {order_by}"
    return query_all(conn, sql, tuple(months))


def fetch_manual_exception_cases(conn: sqlite3.Connection, months: list[str]) -> list[dict]:
    if not months:
        return []
    placeholders = ", ".join("?" for _ in months)
    return query_all(
        conn,
        f"""
        SELECT
            exception_case_id,
            period_month,
            exception_code,
            exception_type,
            source_platform,
            source_store,
            source_table,
            source_ref,
            order_id,
            sku,
            amount_value,
            system_suggestion,
            user_choice,
            case_status,
            approval_status,
            note,
            created_at,
            updated_at,
            resolved_at
        FROM manual_exception_case
        WHERE period_month IN ({placeholders})
        ORDER BY CASE case_status WHEN 'open' THEN 0 ELSE 1 END, exception_case_id DESC
        """,
        tuple(months),
    )


def fetch_exception_attachments(conn: sqlite3.Connection, months: list[str]) -> list[dict]:
    if not months:
        return []
    placeholders = ", ".join("?" for _ in months)
    return query_all(
        conn,
        f"""
        SELECT exception_case_id, attachment_id, file_name, file_path, uploaded_at
        FROM exception_attachment
        WHERE exception_case_id IN (
            SELECT exception_case_id
            FROM manual_exception_case
            WHERE period_month IN ({placeholders})
        )
        ORDER BY attachment_id DESC
        """,
        tuple(months),
    )


def fetch_month_close_check_log(conn: sqlite3.Connection, month: str) -> dict:
    return query_one(
        conn,
        """
        SELECT period_month, close_status, blocker_count, warning_count, pdf_amount, receivable_gap, notes, closed_at
        FROM monthly_close_log
        WHERE period_month = ?
        ORDER BY close_id DESC
        LIMIT 1
        """,
        (month,),
    )


def fetch_month_close_state_history(conn: sqlite3.Connection, month: str) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT state_code, state_source, state_note, created_by, created_at
        FROM month_close_state_log
        WHERE period_month = ?
        ORDER BY state_log_id DESC
        """,
        (month,),
    )


def fetch_month_close_action_history(conn: sqlite3.Connection, month: str) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT action_code, from_state, to_state, action_result, action_note, created_by, created_at
        FROM month_close_action_log
        WHERE period_month = ?
        ORDER BY action_log_id DESC
        """,
        (month,),
    )


def fetch_recent_batches(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT
            batch_type,
            target_month,
            source_filename,
            uploaded_by,
            uploaded_at,
            notes
        FROM upload_batch
        ORDER BY batch_id DESC
        LIMIT ?
        """,
        (limit,),
    )


def fetch_rule_versions(conn: sqlite3.Connection, limit: int = 10) -> list[dict]:
    return query_all(
        conn,
        """
        SELECT
            rule_scope,
            version_name,
            applied_at,
            notes
        FROM rule_version
        ORDER BY rule_version_id DESC
        LIMIT ?
        """,
        (limit,),
    )


def check_pending_mapping_queue(conn: sqlite3.Connection) -> bool:
    row = query_one(conn, "SELECT COUNT(*) AS total FROM pending_mapping_queue WHERE status = 'pending'")
    return row.get("total", 0) == 0
