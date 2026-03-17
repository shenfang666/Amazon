from __future__ import annotations


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_name           TEXT NOT NULL,
    schema_version        TEXT NOT NULL,
    applied_at            TEXT NOT NULL,
    notes                 TEXT
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_started_at        TEXT NOT NULL,
    run_finished_at       TEXT,
    script_name           TEXT NOT NULL,
    run_type              TEXT NOT NULL,
    target_month          TEXT,
    status                TEXT NOT NULL,
    notes                 TEXT
);

CREATE TABLE IF NOT EXISTS file_import_log (
    file_import_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                INTEGER NOT NULL,
    source_file           TEXT NOT NULL,
    source_file_hash      TEXT NOT NULL,
    file_role             TEXT NOT NULL,
    imported_at           TEXT NOT NULL,
    import_status         TEXT NOT NULL,
    row_count             INTEGER,
    notes                 TEXT,
    FOREIGN KEY (run_id) REFERENCES etl_run_log(run_id)
);

CREATE TABLE IF NOT EXISTS dim_sku (
    sku                   TEXT PRIMARY KEY,
    asin                  TEXT,
    fnsku                 TEXT,
    product_name_cn       TEXT NOT NULL,
    product_name_en       TEXT,
    product_group         TEXT,
    is_active             INTEGER NOT NULL DEFAULT 1,
    created_at            TEXT,
    updated_at            TEXT
);

CREATE TABLE IF NOT EXISTS dim_sku_alias (
    alias_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    alias_type            TEXT NOT NULL,
    alias_value           TEXT NOT NULL,
    sku                   TEXT NOT NULL,
    is_unique_mapping     INTEGER NOT NULL DEFAULT 1,
    created_at            TEXT,
    UNIQUE(alias_type, alias_value, sku),
    FOREIGN KEY (sku) REFERENCES dim_sku(sku)
);

CREATE TABLE IF NOT EXISTS manual_sku_alias (
    alias_type            TEXT NOT NULL,
    alias_value           TEXT NOT NULL,
    sku                   TEXT NOT NULL,
    source_note           TEXT,
    is_active             INTEGER NOT NULL DEFAULT 1,
    created_at            TEXT,
    updated_at            TEXT,
    PRIMARY KEY (alias_type, alias_value),
    FOREIGN KEY (sku) REFERENCES dim_sku(sku)
);

CREATE TABLE IF NOT EXISTS dim_cost_monthly (
    sku                   TEXT NOT NULL,
    cost_month            TEXT NOT NULL,
    product_unit_cost     REAL NOT NULL,
    inbound_unit_cost     REAL NOT NULL,
    total_unit_cost       REAL GENERATED ALWAYS AS (product_unit_cost + inbound_unit_cost) STORED,
    source_file           TEXT NOT NULL,
    source_row_ref        TEXT,
    created_at            TEXT,
    PRIMARY KEY (sku, cost_month),
    FOREIGN KEY (sku) REFERENCES dim_sku(sku)
);

CREATE TABLE IF NOT EXISTS dim_platform_monthly_base (
    period_month          TEXT NOT NULL,
    platform              TEXT NOT NULL,
    net_sales             REAL NOT NULL,
    shipped_qty           REAL,
    order_line_count      REAL,
    source_type           TEXT NOT NULL,
    source_note           TEXT,
    created_at            TEXT,
    PRIMARY KEY (period_month, platform)
);

CREATE TABLE IF NOT EXISTS dim_platform (
    platform_code         TEXT PRIMARY KEY,
    platform_name         TEXT NOT NULL,
    is_active             INTEGER NOT NULL DEFAULT 1,
    created_at            TEXT,
    updated_at            TEXT
);

CREATE TABLE IF NOT EXISTS dim_region (
    region_code           TEXT PRIMARY KEY,
    region_name           TEXT NOT NULL,
    created_at            TEXT,
    updated_at            TEXT
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_code          TEXT PRIMARY KEY,
    country_name          TEXT NOT NULL,
    region_code           TEXT,
    created_at            TEXT,
    updated_at            TEXT,
    FOREIGN KEY (region_code) REFERENCES dim_region(region_code)
);

CREATE TABLE IF NOT EXISTS dim_legal_entity (
    legal_entity_code     TEXT PRIMARY KEY,
    legal_entity_name     TEXT NOT NULL,
    country_code          TEXT,
    created_at            TEXT,
    updated_at            TEXT,
    FOREIGN KEY (country_code) REFERENCES dim_country(country_code)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_code            TEXT PRIMARY KEY,
    platform_code         TEXT NOT NULL,
    store_name            TEXT NOT NULL,
    country_code          TEXT,
    legal_entity_code     TEXT,
    is_active             INTEGER NOT NULL DEFAULT 1,
    created_at            TEXT,
    updated_at            TEXT,
    FOREIGN KEY (platform_code) REFERENCES dim_platform(platform_code),
    FOREIGN KEY (country_code) REFERENCES dim_country(country_code),
    FOREIGN KEY (legal_entity_code) REFERENCES dim_legal_entity(legal_entity_code)
);

CREATE TABLE IF NOT EXISTS dim_currency_rate (
    rate_date             TEXT NOT NULL,
    from_currency         TEXT NOT NULL,
    to_currency           TEXT NOT NULL,
    exchange_rate         REAL NOT NULL,
    source_note           TEXT,
    created_at            TEXT,
    PRIMARY KEY (rate_date, from_currency, to_currency)
);

CREATE TABLE IF NOT EXISTS dim_fee_type (
    fee_type_code         TEXT PRIMARY KEY,
    fee_type_name         TEXT NOT NULL,
    pnl_category          TEXT,
    receivable_category   TEXT,
    created_at            TEXT,
    updated_at            TEXT
);

CREATE TABLE IF NOT EXISTS dim_order_type_rule (
    rule_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    platform_code         TEXT,
    transaction_type      TEXT,
    transaction_subtype   TEXT,
    source_order_type     TEXT,
    normalized_order_type TEXT NOT NULL,
    priority              INTEGER NOT NULL DEFAULT 100,
    is_active             INTEGER NOT NULL DEFAULT 1,
    created_at            TEXT,
    updated_at            TEXT
);

CREATE TABLE IF NOT EXISTS rule_version (
    rule_version_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_scope            TEXT NOT NULL,
    version_name          TEXT NOT NULL,
    applied_at            TEXT NOT NULL,
    notes                 TEXT
);

CREATE TABLE IF NOT EXISTS upload_batch (
    batch_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_key             TEXT NOT NULL UNIQUE,
    batch_type            TEXT NOT NULL,
    target_month          TEXT,
    source_filename       TEXT,
    uploaded_by           TEXT,
    uploaded_at           TEXT NOT NULL,
    notes                 TEXT
);

CREATE TABLE IF NOT EXISTS fact_order_lines (
    order_line_id              TEXT PRIMARY KEY,
    source_file                TEXT NOT NULL,
    source_row_hash            TEXT NOT NULL UNIQUE,
    amazon_order_id            TEXT NOT NULL,
    purchase_date              TEXT NOT NULL,
    last_updated_date          TEXT,
    order_month                TEXT NOT NULL,
    order_status               TEXT NOT NULL,
    fulfillment_channel        TEXT,
    sales_channel              TEXT,
    sku                        TEXT,
    asin                       TEXT,
    quantity                   REAL,
    currency                   TEXT,
    item_price                 REAL,
    item_tax                   REAL,
    shipping_price             REAL,
    shipping_tax               REAL,
    item_promotion_discount    REAL,
    ship_promotion_discount    REAL,
    promotion_ids              TEXT,
    is_amazon_channel          INTEGER NOT NULL,
    settlement_state           TEXT,
    created_at                 TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_order_lines_order_id
    ON fact_order_lines(amazon_order_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_lines_month_sku
    ON fact_order_lines(order_month, sku);

CREATE TABLE IF NOT EXISTS fact_settlement_lines (
    settlement_line_id         TEXT PRIMARY KEY,
    source_file                TEXT NOT NULL,
    source_row_hash            TEXT NOT NULL UNIQUE,
    transaction_datetime       TEXT NOT NULL,
    transaction_month          TEXT NOT NULL,
    settlement_id              TEXT NOT NULL,
    transaction_type           TEXT NOT NULL,
    transaction_subtype        TEXT,
    order_id                   TEXT,
    sku                        TEXT,
    quantity                   REAL,
    marketplace                TEXT,
    fulfillment                TEXT,
    product_sales              REAL DEFAULT 0,
    product_sales_tax          REAL DEFAULT 0,
    shipping_credits           REAL DEFAULT 0,
    shipping_credits_tax       REAL DEFAULT 0,
    gift_wrap_credits          REAL DEFAULT 0,
    gift_wrap_credits_tax      REAL DEFAULT 0,
    regulatory_fee             REAL DEFAULT 0,
    regulatory_fee_tax         REAL DEFAULT 0,
    promotional_rebates        REAL DEFAULT 0,
    promotional_rebates_tax    REAL DEFAULT 0,
    marketplace_withheld_tax   REAL DEFAULT 0,
    selling_fees               REAL DEFAULT 0,
    fba_fees                   REAL DEFAULT 0,
    other_transaction_fees     REAL DEFAULT 0,
    other_amount               REAL DEFAULT 0,
    total                      REAL DEFAULT 0,
    transaction_status         TEXT,
    transaction_release_date   TEXT,
    is_amazon_channel          INTEGER NOT NULL,
    order_type                 TEXT,
    created_at                 TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_settlement_lines_order_id
    ON fact_settlement_lines(order_id);

CREATE INDEX IF NOT EXISTS idx_fact_settlement_lines_settlement_id
    ON fact_settlement_lines(settlement_id);

CREATE INDEX IF NOT EXISTS idx_fact_settlement_lines_month_sku
    ON fact_settlement_lines(transaction_month, sku);

CREATE TABLE IF NOT EXISTS bridge_orderline_settlement (
    bridge_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    order_line_id              TEXT NOT NULL,
    settlement_line_id         TEXT NOT NULL,
    match_method               TEXT NOT NULL,
    matched_at                 TEXT,
    UNIQUE(order_line_id, settlement_line_id),
    FOREIGN KEY (order_line_id) REFERENCES fact_order_lines(order_line_id),
    FOREIGN KEY (settlement_line_id) REFERENCES fact_settlement_lines(settlement_line_id)
);

CREATE TABLE IF NOT EXISTS fact_review_orders (
    review_order_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file                TEXT NOT NULL,
    source_row_hash            TEXT NOT NULL UNIQUE,
    amazon_order_id            TEXT NOT NULL,
    order_date                 TEXT,
    product_name               TEXT,
    sku                        TEXT,
    platform                   TEXT,
    currency                   TEXT,
    sale_amount                REAL,
    review_cost                REAL,
    created_at                 TEXT
);

CREATE TABLE IF NOT EXISTS fact_advertising_monthly_sku (
    advertising_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file                TEXT NOT NULL,
    period_month               TEXT NOT NULL,
    sku                        TEXT NOT NULL,
    spend                      REAL NOT NULL DEFAULT 0,
    impressions                REAL,
    clicks                     REAL,
    sales_7d                   REAL,
    source_note                TEXT,
    created_at                 TEXT,
    UNIQUE(source_file, period_month, sku)
);

CREATE TABLE IF NOT EXISTS fact_storage_monthly_sku (
    storage_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file                TEXT NOT NULL,
    period_month               TEXT NOT NULL,
    fnsku                      TEXT,
    asin                       TEXT,
    sku                        TEXT,
    average_quantity_on_hand   REAL,
    estimated_monthly_storage_fee REAL DEFAULT 0,
    incentive_fee_amount       REAL DEFAULT 0,
    created_at                 TEXT
);

CREATE TABLE IF NOT EXISTS fact_removal_monthly_sku (
    removal_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file                TEXT NOT NULL,
    period_month               TEXT NOT NULL,
    order_id                   TEXT,
    order_source               TEXT,
    removal_order_type         TEXT,
    order_status               TEXT,
    sku                        TEXT,
    fnsku                      TEXT,
    disposition                TEXT,
    requested_quantity         REAL,
    cancelled_quantity         REAL,
    disposed_quantity          REAL,
    shipped_quantity           REAL,
    in_process_quantity        REAL,
    removal_fee                REAL,
    created_at                 TEXT
);

CREATE TABLE IF NOT EXISTS fact_compensation_monthly_sku (
    compensation_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file                TEXT NOT NULL,
    reimbursement_id           TEXT NOT NULL,
    period_month               TEXT NOT NULL,
    amazon_order_id            TEXT,
    sku                        TEXT,
    reason                     TEXT,
    amount_total               REAL DEFAULT 0,
    quantity_reimbursed_cash   REAL,
    quantity_reimbursed_inventory REAL,
    created_at                 TEXT,
    UNIQUE(reimbursement_id)
);


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

CREATE TABLE IF NOT EXISTS fact_platform_receipts (
    receipt_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file                 TEXT NOT NULL,
    source_row_hash             TEXT NOT NULL UNIQUE,
    period_month                TEXT NOT NULL,
    receipt_date                TEXT,
    receipt_reference           TEXT,
    settlement_id               TEXT,
    platform_code               TEXT NOT NULL DEFAULT 'amazon',
    store_code                  TEXT NOT NULL DEFAULT '',
    currency                    TEXT,
    receipt_amount              REAL NOT NULL DEFAULT 0,
    receipt_type                TEXT,
    memo                        TEXT,
    created_at                  TEXT
);

CREATE TABLE IF NOT EXISTS fact_platform_receivable_snapshot (
    snapshot_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month                TEXT NOT NULL,
    platform_code               TEXT NOT NULL DEFAULT 'amazon',
    store_code                  TEXT NOT NULL DEFAULT '',
    opening_receivable          REAL NOT NULL DEFAULT 0,
    current_receivable          REAL NOT NULL DEFAULT 0,
    current_receipts            REAL NOT NULL DEFAULT 0,
    closing_receivable          REAL NOT NULL DEFAULT 0,
    unmatched_receipts          REAL NOT NULL DEFAULT 0,
    receivable_gap              REAL NOT NULL DEFAULT 0,
    reconciliation_status       TEXT NOT NULL DEFAULT 'pending',
    generated_at                TEXT NOT NULL,
    notes                       TEXT,
    UNIQUE (period_month, platform_code, store_code)
);

CREATE TABLE IF NOT EXISTS fact_inventory_movements (
    movement_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month                TEXT NOT NULL,
    source_file                 TEXT,
    movement_date               TEXT,
    movement_type               TEXT NOT NULL,
    sku                         TEXT,
    quantity                    REAL NOT NULL DEFAULT 0,
    unit_cost                   REAL,
    amount_total                REAL,
    source_ref                  TEXT,
    created_at                  TEXT
);

CREATE TABLE IF NOT EXISTS fact_inventory_snapshot (
    inventory_snapshot_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month                TEXT NOT NULL,
    sku                         TEXT NOT NULL,
    opening_qty                 REAL NOT NULL DEFAULT 0,
    inbound_qty                 REAL NOT NULL DEFAULT 0,
    outbound_qty                REAL NOT NULL DEFAULT 0,
    transfer_qty                REAL NOT NULL DEFAULT 0,
    return_qty                  REAL NOT NULL DEFAULT 0,
    adjust_qty                  REAL NOT NULL DEFAULT 0,
    closing_qty                 REAL NOT NULL DEFAULT 0,
    generated_at                TEXT NOT NULL,
    UNIQUE(period_month, sku)
);

CREATE TABLE IF NOT EXISTS manual_vine_fee_allocations (
    allocation_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month               TEXT NOT NULL,
    sku                        TEXT NOT NULL,
    fee_amount                 REAL NOT NULL,
    source_note                TEXT,
    created_at                 TEXT,
    UNIQUE(period_month, sku)
);

CREATE TABLE IF NOT EXISTS manual_shared_costs (
    shared_cost_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month               TEXT NOT NULL,
    cost_type                  TEXT NOT NULL,
    description                TEXT,
    total_amount               REAL NOT NULL,
    currency                   TEXT NOT NULL DEFAULT 'USD',
    platforms                  TEXT NOT NULL DEFAULT 'all',
    allocation_method          TEXT NOT NULL DEFAULT 'revenue_share',
    direct_sku                 TEXT,
    custom_pct_json            TEXT,
    source_note                TEXT,
    created_at                 TEXT
);

CREATE TABLE IF NOT EXISTS manual_removal_fee_controls (
    control_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month               TEXT NOT NULL,
    order_id                   TEXT NOT NULL,
    sku                        TEXT,
    removal_category           TEXT NOT NULL,
    accounting_treatment       TEXT NOT NULL,
    source_note                TEXT,
    created_at                 TEXT,
    UNIQUE(period_month, order_id)
);

CREATE TABLE IF NOT EXISTS pending_mapping_queue (
    pending_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    source_table               TEXT NOT NULL,
    source_file                TEXT,
    source_row_hash            TEXT,
    ambiguous_value            TEXT NOT NULL,
    mapping_type               TEXT NOT NULL,
    status                     TEXT NOT NULL DEFAULT 'pending',
    notes                      TEXT,
    created_at                 TEXT,
    resolved_at                TEXT
);

CREATE TABLE IF NOT EXISTS manual_adjustment_log (
    adjustment_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_table               TEXT NOT NULL,
    target_key                 TEXT NOT NULL,
    adjustment_type            TEXT NOT NULL,
    adjustment_payload         TEXT NOT NULL,
    adjusted_by                TEXT,
    adjusted_at                TEXT NOT NULL,
    notes                      TEXT
);

CREATE TABLE IF NOT EXISTS manual_exception_case (
    exception_case_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month               TEXT NOT NULL,
    exception_code             TEXT NOT NULL,
    exception_type             TEXT NOT NULL,
    source_platform            TEXT NOT NULL DEFAULT 'amazon',
    source_store               TEXT,
    source_table               TEXT,
    source_ref                 TEXT,
    order_id                   TEXT,
    sku                        TEXT,
    amount_value               REAL,
    system_suggestion          TEXT,
    user_choice                TEXT,
    case_status                TEXT NOT NULL DEFAULT 'open',
    approval_status            TEXT NOT NULL DEFAULT 'not_required',
    note                       TEXT,
    created_by                 TEXT,
    created_at                 TEXT NOT NULL,
    updated_at                 TEXT,
    resolved_at                TEXT
);

CREATE TABLE IF NOT EXISTS exception_attachment (
    attachment_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    exception_case_id          INTEGER NOT NULL,
    file_name                  TEXT NOT NULL,
    file_path                  TEXT NOT NULL,
    uploaded_at                TEXT NOT NULL,
    uploaded_by                TEXT,
    FOREIGN KEY (exception_case_id) REFERENCES manual_exception_case(exception_case_id)
);

CREATE TABLE IF NOT EXISTS exception_approval_log (
    approval_log_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    exception_case_id          INTEGER NOT NULL,
    action_type                TEXT NOT NULL,
    action_by                  TEXT,
    action_note                TEXT,
    acted_at                   TEXT NOT NULL,
    FOREIGN KEY (exception_case_id) REFERENCES manual_exception_case(exception_case_id)
);

CREATE TABLE IF NOT EXISTS monthly_close_issue_detail (
    issue_id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month                TEXT NOT NULL,
    severity                    TEXT NOT NULL,
    issue_code                  TEXT NOT NULL,
    issue_key                   TEXT,
    issue_value                 TEXT,
    metric_value                REAL,
    source_table                TEXT,
    source_ref                  TEXT,
    note                        TEXT,
    created_at                  TEXT
);

CREATE TABLE IF NOT EXISTS monthly_close_log (
    close_id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month               TEXT NOT NULL UNIQUE,
    close_status               TEXT NOT NULL,
    blocker_count              INTEGER NOT NULL DEFAULT 0,
    warning_count              INTEGER NOT NULL DEFAULT 0,
    pdf_amount                 REAL,
    receivable_gap             REAL,
    closed_at                  TEXT,
    notes                      TEXT
);

CREATE TABLE IF NOT EXISTS month_close_state_log (
    state_log_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month               TEXT NOT NULL,
    state_code                 TEXT NOT NULL,
    state_source               TEXT NOT NULL,
    state_note                 TEXT,
    created_by                 TEXT,
    created_at                 TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS month_close_action_log (
    action_log_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month               TEXT NOT NULL,
    action_code                TEXT NOT NULL,
    from_state                 TEXT,
    to_state                   TEXT,
    action_result              TEXT NOT NULL,
    action_note                TEXT,
    created_by                 TEXT,
    created_at                 TEXT NOT NULL
);
"""
