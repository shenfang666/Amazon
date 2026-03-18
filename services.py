from __future__ import annotations

import sqlite3
import json

import app
import repositories
import file_store


def get_dashboard_payload(month: str | None) -> dict:
    app.ensure_runtime_schema()
    conn = sqlite3.connect(app.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        months = repositories.get_months(conn)
        if not months:
            raise RuntimeError("No dashboard months available in the database.")

        selected_month = month if month in months else months[0]
        previous_month = months[months.index(selected_month) + 1] if months.index(selected_month) + 1 < len(months) else None

        overview = app.build_overview(conn, selected_month)
        previous_overview = app.build_overview(conn, previous_month) if previous_month else None
        comparison = app.build_comparison(overview, previous_overview, previous_month)

        close_timeline = repositories.fetch_dashboard_close_timeline(conn)
        for item in close_timeline:
            item["close_notes"] = app.parse_close_notes(item.get("notes"))
            item["pdf_amount"] = app.round_money(item.get("pdf_amount"))
            item["receivable_gap"] = app.round_money(item.get("receivable_gap"))

        top_skus = repositories.fetch_dashboard_top_skus(conn, selected_month, app.GROSS_PROFIT_EXPR)
        alerts = repositories.fetch_dashboard_alerts(conn, selected_month)
        fee_validations = app.build_fee_validation_rows(conn, selected_month)
        receivable_summary = app.get_receivable_snapshot(conn, selected_month, refresh_if_missing=True)
        inventory_summary = app.build_inventory_status(conn, selected_month, refresh=True).get("summary", {})

        return {
            "generated_at": app.now_iso(),
            "selected_month": selected_month,
            "available_months": months,
            "overview": overview,
            "comparison": comparison,
            "close_timeline": close_timeline,
            "top_skus": top_skus,
            "alerts": alerts,
            "fee_validations": fee_validations,
            "receivable_summary": receivable_summary,
            "inventory_summary": inventory_summary,
            "capabilities": [
                "Order settlement tracking",
                "PnL and receivable dual basis",
                "Unified exception workbench",
                "Month-close state actions",
                "Inventory reconciliation workspace",
                "Replayable monthly ETL",
                "Frontend uploads and attachments",
            ],
            "second_phase": [
                "Inventory center and ERP/WMS",
                "Multi-platform onboarding",
                "PDF close reconciliation",
                "Deeper approval governance",
            ],
        }
    finally:
        conn.close()


def get_receivables_payload(month: str | None = None) -> dict:
    app.ensure_runtime_schema()
    conn = sqlite3.connect(app.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        months = repositories.get_months(conn)
        selected_month = month if month in months else (months[0] if months else None)
        if not selected_month:
            raise RuntimeError('No months available for receivables.')

        app.ensure_receivable_snapshots(conn, months)

        balances = repositories.fetch_receivable_balances(conn)
        for row in balances:
            for key in ('opening_receivable', 'current_receivable', 'current_receipts', 'closing_receivable', 'unmatched_receipts', 'receivable_gap'):
                row[key] = app.round_money(row.get(key))

        summary = next((row for row in balances if row['period_month'] == selected_month), None) or app.get_receivable_snapshot(conn, selected_month, refresh_if_missing=True)
        aging_rows = []
        for index, row in enumerate(balances):
            aging_rows.append(
                {
                    'period_month': row['period_month'],
                    'aging_bucket': '0-30d' if index == 0 else '31d+',
                    'closing_receivable': row['closing_receivable'],
                    'receivable_gap': row['receivable_gap'],
                    'reconciliation_status': row['reconciliation_status'],
                }
            )

        unsettled_rows = [row for row in balances if abs(float(row.get('closing_receivable') or 0)) > 0.01]
        unmatched_receipt_rows = repositories.fetch_unmatched_receipt_rows(conn, selected_month)
        return {
            'selected_month': selected_month,
            'available_months': months,
            'summary': summary,
            'balances': balances,
            'aging': aging_rows,
            'unsettled': unsettled_rows,
            'unmatched_receipts': unmatched_receipt_rows,
        }
    finally:
        conn.close()


def get_profit_payload(month: str | None) -> dict:
    app.ensure_runtime_schema()
    conn = sqlite3.connect(app.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        months = repositories.get_months(conn)
        if not months:
            raise RuntimeError("No profit-analysis months available in the database.")
        selected_month = month if month in months else months[0]

        sku_details = repositories.fetch_profit_sku_details(conn, selected_month, app.GROSS_PROFIT_EXPR)
        order_details = repositories.fetch_profit_order_details(conn, selected_month)
        return {
            "generated_at": app.now_iso(),
            "selected_month": selected_month,
            "available_months": months,
            "sku_details": sku_details,
            "order_details": order_details,
        }
    finally:
        conn.close()


def get_uploads_payload() -> dict:
    app.ensure_manual_templates()
    app.ensure_runtime_schema()
    source_files = file_store.list_files_by_suffix(app.ROOT, app.SOURCE_FILE_SUFFIXES, limit=30)

    conn = sqlite3.connect(app.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        recent_batches = repositories.fetch_recent_batches(conn)
        rule_versions = repositories.fetch_rule_versions(conn)
    finally:
        conn.close()

    return {
        "targets": ["source", "attachment"],
        "source_files": source_files,
        "attachment_dir": str(app.ATTACHMENT_DIR),
        "recent_batches": recent_batches,
        "rule_versions": rule_versions,
    }


def get_operations_payload(month: str | None = None) -> dict:
    app.ensure_runtime_schema()
    app.ensure_manual_templates()
    conn = sqlite3.connect(app.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        manual_files = []
        for key, config in app.MANUAL_FILE_CONFIG.items():
            path = app.MANUAL_DIR / config["filename"]
            headers, rows, encoding = app.read_csv_with_headers(path, config["headers"])
            manual_files.append(
                {
                    "key": key,
                    "label": config["label"],
                    "filename": config["filename"],
                    "headers": headers,
                    "rows": rows,
                    "row_count": len(rows),
                    "encoding": encoding,
                    "updated_at": app.datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds") if path.exists() else None,
                }
            )

        worklists = file_store.list_globbed_files(app.MANUAL_DIR, "worklist_*.csv")
        source_files = file_store.list_files_by_suffix(app.ROOT, app.SOURCE_FILE_SUFFIXES, limit=20)
        selected_month, pending_removal_controls = app.get_pending_removal_controls(conn, month)
    finally:
        conn.close()

    with app.JOB_LOCK:
        job_snapshot = json.loads(json.dumps(app.MONTHLY_JOB, ensure_ascii=False))

    return {
        "selected_month": selected_month,
        "manual_files": manual_files,
        "worklists": worklists,
        "source_files": source_files,
        "pending_removal_controls": pending_removal_controls,
        "monthly_job": job_snapshot,
        "upload_targets": ["source", "attachment"],
    }


def get_inventory_payload(month: str | None = None) -> dict:
    app.ensure_runtime_schema()
    conn = sqlite3.connect(app.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        months = sorted(set(repositories.get_months(conn)) | set(repositories.fetch_inventory_periods(conn)), reverse=True)
        selected_month = month if month in months else (months[0] if months else None)
        if not selected_month:
            raise RuntimeError("No months available for inventory reconciliation.")

        payload = app.build_inventory_status(conn, selected_month, refresh=True)
        return {
            "selected_month": selected_month,
            "available_months": months,
            **payload,
        }
    finally:
        conn.close()


def _build_exception_override_map(conn: sqlite3.Connection, months: list[str]) -> dict[str, dict]:
    override_rows = repositories.fetch_exception_override_rows(conn, months)
    override_map: dict[str, dict] = {}
    for row in override_rows:
        key = app.build_exception_case_key(
            row.get("period_month"),
            row.get("exception_code"),
            row.get("source_table"),
            row.get("source_ref"),
            row.get("order_id"),
            row.get("sku"),
        )
        override_map.setdefault(key, row)
    return override_map


def _get_latest_month_close_state_map(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        row["period_month"]: row["state_code"]
        for row in repositories.fetch_latest_month_close_state_rows(conn)
    }


def _get_unclosed_issue_months(conn: sqlite3.Connection, anchor_month: str | None = None) -> list[str]:
    state_map = _get_latest_month_close_state_map(conn)
    months = repositories.get_months(conn)
    month_scope = anchor_month if anchor_month in months else (months[0] if months else None)
    issue_months = {row["period_month"] for row in repositories.fetch_issue_month_rows(conn)}
    if month_scope:
        issue_months.add(month_scope)
    return [
        month
        for month in sorted(issue_months, reverse=True)
        if (not month_scope or month <= month_scope) and state_map.get(month) != "closed"
    ]


def _get_effective_issue_counts(conn: sqlite3.Connection, month: str) -> dict:
    override_map = _build_exception_override_map(conn, [month])
    issue_rows = repositories.fetch_month_close_issue_rows(
        conn,
        [month],
        """
        severity,
        issue_code,
        issue_key,
        issue_value,
        source_table,
        source_ref
        """,
        order_by="issue_id ASC",
    )
    raw_blockers = 0
    raw_warnings = 0
    blocker_count = 0
    warning_count = 0
    overridden_count = 0
    for row in issue_rows:
        severity = str(row.get("severity", "")).strip().lower()
        if severity == "blocker":
            raw_blockers += 1
        elif severity == "warning":
            raw_warnings += 1
        key = app.build_exception_case_key(
            month,
            row.get("issue_code"),
            row.get("source_table"),
            row.get("source_ref"),
            row.get("issue_key"),
            row.get("issue_value"),
        )
        if app.is_normal_override(override_map.get(key)):
            overridden_count += 1
            continue
        if severity == "blocker":
            blocker_count += 1
        elif severity == "warning":
            warning_count += 1
    return {
        "raw_blocker_count": raw_blockers,
        "raw_warning_count": raw_warnings,
        "blocker_count": blocker_count,
        "warning_count": warning_count,
        "overridden_count": overridden_count,
    }


def get_exceptions_payload(month: str | None = None) -> dict:
    app.ensure_runtime_schema()
    conn = sqlite3.connect(app.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        months = repositories.get_months(conn)
        selected_month = month if month in months else (months[0] if months else None)
        if not selected_month:
            raise RuntimeError("No months available for exceptions.")

        open_months = _get_unclosed_issue_months(conn, selected_month)
        manual_months = open_months or [selected_month]
        manual_cases = repositories.fetch_manual_exception_cases(conn, manual_months)
        override_map = _build_exception_override_map(conn, manual_months)

        generated_cases = []
        issue_rows = repositories.fetch_month_close_issue_rows(
            conn,
            manual_months,
            """
            period_month,
            issue_id,
            severity,
            issue_code,
            issue_key,
            issue_value,
            metric_value,
            source_table,
            source_ref,
            note,
            created_at
            """,
            order_by="period_month DESC, CASE severity WHEN 'blocker' THEN 0 ELSE 1 END, issue_id DESC",
        )
        for row in issue_rows:
            if row["issue_code"] == "removal_fee_control_missing":
                continue
            case_key = app.build_exception_case_key(
                row["period_month"],
                row["issue_code"],
                row["source_table"],
                row["source_ref"],
                row["issue_key"],
                row["issue_value"],
            )
            override_case = override_map.get(case_key)
            generated_cases.append(
                {
                    "case_key": case_key,
                    "period_month": row["period_month"],
                    "exception_code": row["issue_code"],
                    "exception_type": "generated_issue",
                    "source_platform": "amazon",
                    "source_store": "",
                    "source_table": row["source_table"],
                    "source_ref": row["source_ref"],
                    "order_id": row["issue_key"],
                    "sku": row["issue_value"],
                    "amount_value": row["metric_value"],
                    "system_suggestion": row["note"],
                    "user_choice": "",
                    "case_status": "open",
                    "approval_status": "not_required",
                    "note": row["note"],
                    "created_at": row["created_at"],
                    "origin": row["severity"],
                    "override_case_id": override_case.get("exception_case_id") if override_case else None,
                    "override_user_choice": override_case.get("user_choice") if override_case else "",
                    "override_case_status": override_case.get("case_status") if override_case else "",
                    "override_is_normal": app.is_normal_override(override_case),
                    "override_note": override_case.get("note") if override_case else "",
                }
            )

        for issue_month in manual_months:
            _, pending_removal = app.get_pending_removal_controls(conn, issue_month)
            for row in pending_removal:
                case_key = app.build_exception_case_key(
                    issue_month,
                    "pending_removal_control",
                    "fact_removal_monthly_sku",
                    row["order_id"],
                    row["order_id"],
                    row["sku"],
                )
                override_case = override_map.get(case_key)
                generated_cases.append(
                    {
                        "case_key": case_key,
                        "period_month": issue_month,
                        "exception_code": "pending_removal_control",
                        "exception_type": "removal_control",
                        "source_platform": "amazon",
                        "source_store": "",
                        "source_table": "fact_removal_monthly_sku",
                        "source_ref": row["order_id"],
                        "order_id": row["order_id"],
                        "sku": row["sku"],
                        "amount_value": row["removal_fee"],
                        "system_suggestion": row["suggested_category"],
                        "user_choice": "",
                        "case_status": "open",
                        "approval_status": "not_required",
                        "note": row["product_name_cn"],
                        "created_at": app.now_iso(),
                        "origin": "blocker",
                        "override_case_id": override_case.get("exception_case_id") if override_case else None,
                        "override_user_choice": override_case.get("user_choice") if override_case else "",
                        "override_case_status": override_case.get("case_status") if override_case else "",
                        "override_is_normal": app.is_normal_override(override_case),
                        "override_note": override_case.get("note") if override_case else "",
                    }
                )

        attachments = repositories.fetch_exception_attachments(conn, manual_months)
        attachment_map: dict[int, list[dict]] = {}
        for item in attachments:
            attachment_map.setdefault(int(item["exception_case_id"]), []).append(item)
        for item in manual_cases:
            item["attachments"] = attachment_map.get(int(item["exception_case_id"]), [])

        manual_by_category: dict[str, dict] = {}
        for case in manual_cases:
            category_key = f"{case.get('exception_type', 'unknown')}|{case.get('exception_code', 'unknown')}"
            category = manual_by_category.setdefault(
                category_key,
                {
                    "exception_type": case.get("exception_type", "unknown"),
                    "exception_code": case.get("exception_code", "unknown"),
                    "count": 0,
                    "total_amount": 0.0,
                    "resolved_count": 0,
                    "open_count": 0,
                    "latest_updated": None,
                },
            )
            category["count"] += 1
            category["total_amount"] += float(case.get("amount_value") or 0)
            if str(case.get("case_status", "")).strip().lower() == "resolved":
                category["resolved_count"] += 1
            else:
                category["open_count"] += 1
            updated_at = case.get("updated_at") or case.get("created_at")
            if updated_at and (category["latest_updated"] is None or updated_at > category["latest_updated"]):
                category["latest_updated"] = updated_at
        manual_summary = sorted(
            manual_by_category.values(),
            key=lambda item: (
                str(item.get("exception_type") or ""),
                str(item.get("exception_code") or ""),
            ),
        )

        removal_by_month: dict[str, dict] = {}
        for case in manual_cases:
            exception_code = str(case.get("exception_code", "")).strip().lower()
            exception_type = str(case.get("exception_type", "")).strip().lower()
            if exception_code != "pending_removal_control" and exception_type != "removal_control_resolved":
                continue
            month_key = case.get("period_month") or "unknown"
            bucket = removal_by_month.setdefault(
                month_key,
                {
                    "period_month": month_key,
                    "transfer_count": 0,
                    "disposal_count": 0,
                    "expense_count": 0,
                    "capitalize_count": 0,
                    "total_amount": 0.0,
                    "latest_updated": None,
                },
            )
            bucket["total_amount"] += float(case.get("amount_value") or 0)
            note_text = str(case.get("note") or "").lower()
            suggestion_text = str(case.get("system_suggestion") or "").lower()
            if "transfer" in note_text or "transfer" in suggestion_text:
                bucket["transfer_count"] += 1
            elif "disposal" in note_text or "disposal" in suggestion_text:
                bucket["disposal_count"] += 1
            if "expense" in note_text:
                bucket["expense_count"] += 1
            elif "capitalize" in note_text:
                bucket["capitalize_count"] += 1
            updated_at = case.get("updated_at") or case.get("created_at")
            if updated_at and (bucket["latest_updated"] is None or updated_at > bucket["latest_updated"]):
                bucket["latest_updated"] = updated_at
        removal_control_summary = sorted(
            removal_by_month.values(),
            key=lambda item: str(item.get("period_month") or ""),
            reverse=True,
        )

        return {
            "selected_month": selected_month,
            "available_months": months,
            "open_months": manual_months,
            "manual_cases": manual_cases,
            "manual_summary": manual_summary,
            "removal_control_summary": removal_control_summary,
            "generated_cases": generated_cases,
        }
    finally:
        conn.close()


def get_month_close_payload(month: str | None = None) -> dict:
    app.ensure_runtime_schema()
    conn = sqlite3.connect(app.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        months = repositories.get_months(conn)
        selected_month = month if month in months else (months[0] if months else None)
        if not selected_month:
            raise RuntimeError("No months available for month close.")

        snapshot = app.get_receivable_snapshot(conn, selected_month, refresh_if_missing=True)
        inventory_status = app.build_inventory_status(conn, selected_month, refresh=True)
        inventory_ready = bool(inventory_status.get("summary", {}).get("ready"))
        recommended_state = app.derive_recommended_close_state(conn, selected_month, inventory_ready=inventory_ready)
        current_state = app.get_latest_month_close_state(conn, selected_month) or recommended_state
        effective_issues = _get_effective_issue_counts(conn, selected_month)
        check_log = repositories.fetch_month_close_check_log(conn, selected_month)
        check_log["raw_blocker_count"] = int(check_log.get("blocker_count") or 0)
        check_log["raw_warning_count"] = int(check_log.get("warning_count") or 0)
        check_log["blocker_count"] = int(effective_issues.get("blocker_count") or 0)
        check_log["warning_count"] = int(effective_issues.get("warning_count") or 0)
        check_log["overridden_issue_count"] = int(effective_issues.get("overridden_count") or 0)
        state_history = repositories.fetch_month_close_state_history(conn, selected_month)
        action_history = repositories.fetch_month_close_action_history(conn, selected_month)
        prerequisites = {
            "mapping_completed": repositories.check_pending_mapping_queue(conn),
            "issues_cleared": int(effective_issues.get("blocker_count") or 0) == 0 and int(effective_issues.get("warning_count") or 0) == 0,
            "receivable_balanced": abs(float(snapshot.get("receivable_gap") or 0)) <= 0.01,
            "inventory_ready": inventory_ready,
        }
        return {
            "selected_month": selected_month,
            "available_months": months,
            "current_state": current_state,
            "recommended_state": recommended_state,
            "check_log": check_log,
            "receivable_snapshot": snapshot,
            "inventory_summary": inventory_status.get("summary", {}),
            "inventory_issues": inventory_status.get("issues", []),
            "prerequisites": prerequisites,
            "state_history": state_history,
            "action_history": action_history,
            "available_actions": ["start_close", "submit_for_approval", "approve_close", "reopen_close"],
            "inventory_note": inventory_status.get("note"),
        }
    finally:
        conn.close()
