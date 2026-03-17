from __future__ import annotations

from common import connect, finish_etl_run, get_config, print_banner, register_etl_run


VIEW_SQL = """
DROP VIEW IF EXISTS v_order_settlement_tracking;
CREATE VIEW v_order_settlement_tracking AS
SELECT
    o.order_line_id,
    o.order_month,
    o.amazon_order_id,
    o.purchase_date,
    o.order_status,
    o.sales_channel,
    o.sku,
    o.asin,
    o.quantity,
    o.item_price,
    o.item_promotion_discount,
    o.settlement_state,
    COUNT(DISTINCT b.settlement_line_id) AS linked_settlement_lines,
    GROUP_CONCAT(DISTINCT s.settlement_id) AS settlement_ids,
    SUM(CASE WHEN s.transaction_type = 'Order' THEN s.product_sales ELSE 0 END) AS settled_product_sales,
    SUM(CASE WHEN s.transaction_type = 'Order' THEN s.total ELSE 0 END) AS settled_order_net,
    SUM(CASE WHEN s.transaction_type = 'Refund' THEN s.total ELSE 0 END) AS settled_refund_net,
    SUM(CASE WHEN COALESCE(s.transaction_status, '') = 'Released' THEN 1 ELSE 0 END) AS released_line_count
FROM fact_order_lines o
LEFT JOIN bridge_orderline_settlement b
    ON o.order_line_id = b.order_line_id
LEFT JOIN fact_settlement_lines s
    ON b.settlement_line_id = s.settlement_line_id
GROUP BY
    o.order_line_id,
    o.order_month,
    o.amazon_order_id,
    o.purchase_date,
    o.order_status,
    o.sales_channel,
    o.sku,
    o.asin,
    o.quantity,
    o.item_price,
    o.item_promotion_discount,
    o.settlement_state;

DROP VIEW IF EXISTS v_monthly_platform_fee_pool;
CREATE VIEW v_monthly_platform_fee_pool AS
SELECT
    period_month,
    SUM(CASE WHEN fee_type = 'subscription_fee' THEN -amount_total ELSE 0 END) AS subscription_fee,
    SUM(CASE WHEN fee_type = 'coupon_participation_fee' THEN -amount_total ELSE 0 END) AS coupon_participation_fee,
    SUM(CASE WHEN fee_type = 'coupon_performance_fee' THEN -amount_total ELSE 0 END) AS coupon_performance_fee,
    SUM(CASE WHEN fee_type = 'vine_enrollment_fee_source' THEN -amount_total ELSE 0 END) AS vine_enrollment_fee_source
FROM fact_platform_fee_lines
GROUP BY period_month;

DROP VIEW IF EXISTS v_monthly_sku_fee_pool;
CREATE VIEW v_monthly_sku_fee_pool AS
WITH advertising AS (
    SELECT period_month, sku, SUM(spend) AS ad_spend
    FROM fact_advertising_monthly_sku
    GROUP BY period_month, sku
),
storage AS (
    SELECT period_month, sku, SUM(estimated_monthly_storage_fee - incentive_fee_amount) AS storage_fees
    FROM fact_storage_monthly_sku
    WHERE sku IS NOT NULL
    GROUP BY period_month, sku
),
removal AS (
    SELECT
        r.period_month,
        r.sku,
        SUM(
            CASE
                WHEN lower(COALESCE(r.removal_order_type, '')) = 'disposal' OR COALESCE(r.disposed_quantity, 0) > 0
                    THEN COALESCE(r.removal_fee, 0)
                WHEN lower(COALESCE(c.accounting_treatment, '')) = 'expense'
                    THEN COALESCE(r.removal_fee, 0)
                ELSE 0
            END
        ) AS removal_fees,
        SUM(
            CASE
                WHEN lower(COALESCE(r.removal_order_type, '')) <> 'disposal'
                     AND COALESCE(r.disposed_quantity, 0) = 0
                     AND lower(COALESCE(c.accounting_treatment, '')) = 'capitalize'
                    THEN COALESCE(r.removal_fee, 0)
                ELSE 0
            END
        ) AS removal_fee_capitalized,
        SUM(
            CASE
                WHEN lower(COALESCE(r.removal_order_type, '')) <> 'disposal'
                     AND COALESCE(r.disposed_quantity, 0) = 0
                     AND c.order_id IS NULL
                    THEN COALESCE(r.removal_fee, 0)
                ELSE 0
            END
        ) AS removal_fee_unclassified
    FROM fact_removal_monthly_sku r
    LEFT JOIN manual_removal_fee_controls c
      ON r.period_month = c.period_month
     AND r.order_id = c.order_id
    WHERE r.sku IS NOT NULL
    GROUP BY r.period_month, r.sku
),
review AS (
    SELECT substr(order_date, 1, 7) AS period_month, sku, SUM(COALESCE(review_cost, 0)) AS review_cost
    FROM fact_review_orders
    WHERE sku IS NOT NULL AND order_date IS NOT NULL
    GROUP BY substr(order_date, 1, 7), sku
),
vine AS (
    SELECT period_month, sku, SUM(fee_amount) AS vine_fee
    FROM manual_vine_fee_allocations
    GROUP BY period_month, sku
),
costs AS (
    SELECT cost_month AS period_month, sku, product_unit_cost, inbound_unit_cost
    FROM dim_cost_monthly
),
sales_base AS (
    SELECT
        transaction_month AS period_month,
        sku,
        ABS(
            COALESCE(product_sales, 0)
            + COALESCE(shipping_credits, 0)
            + COALESCE(gift_wrap_credits, 0)
            + COALESCE(promotional_rebates, 0)
        ) AS abs_net_sales
    FROM fact_settlement_lines
    WHERE transaction_type = 'Order'
      AND sku IS NOT NULL
      AND order_id IS NOT NULL
      AND order_type IS NOT NULL
      AND trim(order_type) <> ''
      AND lower(order_type) <> 'unknown'
),
sales_totals AS (
    SELECT period_month, sku, SUM(abs_net_sales) AS sku_abs_net_sales
    FROM sales_base
    GROUP BY period_month, sku
),
month_totals AS (
    SELECT period_month, SUM(sku_abs_net_sales) AS month_abs_net_sales
    FROM sales_totals
    GROUP BY period_month
),
sku_counts AS (
    SELECT period_month, COUNT(*) AS sku_count
    FROM sales_totals
    GROUP BY period_month
),
shared_cost_direct AS (
    SELECT period_month, direct_sku AS sku, SUM(COALESCE(total_amount, 0)) AS shared_cost_amount
    FROM manual_shared_costs
    WHERE direct_sku IS NOT NULL
      AND trim(direct_sku) <> ''
    GROUP BY period_month, direct_sku
),
shared_cost_equal AS (
    SELECT
        m.period_month,
        s.sku,
        SUM(
            CASE
                WHEN COALESCE(sc.sku_count, 0) = 0 THEN 0
                ELSE COALESCE(m.total_amount, 0) * 1.0 / sc.sku_count
            END
        ) AS shared_cost_amount
    FROM manual_shared_costs m
    JOIN sales_totals s
      ON m.period_month = s.period_month
    JOIN sku_counts sc
      ON m.period_month = sc.period_month
    WHERE lower(COALESCE(m.allocation_method, 'revenue_share')) = 'equal_share'
      AND COALESCE(trim(m.direct_sku), '') = ''
    GROUP BY m.period_month, s.sku
),
shared_cost_revenue AS (
    SELECT
        m.period_month,
        s.sku,
        SUM(
            CASE
                WHEN COALESCE(mt.month_abs_net_sales, 0) = 0 THEN 0
                ELSE COALESCE(m.total_amount, 0) * s.sku_abs_net_sales / mt.month_abs_net_sales
            END
        ) AS shared_cost_amount
    FROM manual_shared_costs m
    JOIN sales_totals s
      ON m.period_month = s.period_month
    JOIN month_totals mt
      ON m.period_month = mt.period_month
    WHERE lower(COALESCE(m.allocation_method, 'revenue_share')) = 'revenue_share'
      AND COALESCE(trim(m.direct_sku), '') = ''
    GROUP BY m.period_month, s.sku
),
shared_cost_custom AS (
    SELECT
        m.period_month,
        trim(j.key) AS sku,
        SUM(
            COALESCE(m.total_amount, 0) *
            CASE
                WHEN ABS(CAST(j.value AS REAL)) > 1 THEN CAST(j.value AS REAL) / 100.0
                ELSE CAST(j.value AS REAL)
            END
        ) AS shared_cost_amount
    FROM manual_shared_costs m
    JOIN json_each(m.custom_pct_json) j
    WHERE lower(COALESCE(m.allocation_method, 'revenue_share')) = 'custom_pct'
      AND COALESCE(trim(m.custom_pct_json), '') <> ''
    GROUP BY m.period_month, trim(j.key)
),
shared_cost_seed AS (
    SELECT * FROM shared_cost_direct
    UNION ALL
    SELECT * FROM shared_cost_equal
    UNION ALL
    SELECT * FROM shared_cost_revenue
    UNION ALL
    SELECT * FROM shared_cost_custom
),
shared_cost_allocated AS (
    SELECT period_month, sku, SUM(shared_cost_amount) AS manual_shared_cost
    FROM shared_cost_seed
    GROUP BY period_month, sku
)
SELECT
    c.period_month,
    c.sku,
    COALESCE(ad.ad_spend, 0) AS ad_spend,
    COALESCE(st.storage_fees, 0) AS storage_fees,
    COALESCE(rm.removal_fees, 0) AS removal_fees,
    COALESCE(rm.removal_fee_capitalized, 0) AS removal_fee_capitalized,
    COALESCE(rm.removal_fee_unclassified, 0) AS removal_fee_unclassified,
    COALESCE(rv.review_cost, 0) AS review_cost,
    COALESCE(vn.vine_fee, 0) AS vine_fee,
    COALESCE(sc.manual_shared_cost, 0) AS manual_shared_cost,
    COALESCE(ct.product_unit_cost, 0) AS product_unit_cost,
    COALESCE(ct.inbound_unit_cost, 0) AS inbound_unit_cost
FROM (
    SELECT period_month, sku FROM advertising
    UNION
    SELECT period_month, sku FROM storage
    UNION
    SELECT period_month, sku FROM removal
    UNION
    SELECT period_month, sku FROM review
    UNION
    SELECT period_month, sku FROM vine
    UNION
    SELECT period_month, sku FROM shared_cost_allocated
    UNION
    SELECT period_month, sku FROM costs
) c
LEFT JOIN advertising ad ON c.period_month = ad.period_month AND c.sku = ad.sku
LEFT JOIN storage st ON c.period_month = st.period_month AND c.sku = st.sku
LEFT JOIN removal rm ON c.period_month = rm.period_month AND c.sku = rm.sku
LEFT JOIN review rv ON c.period_month = rv.period_month AND c.sku = rv.sku
LEFT JOIN vine vn ON c.period_month = vn.period_month AND c.sku = vn.sku
LEFT JOIN shared_cost_allocated sc ON c.period_month = sc.period_month AND c.sku = sc.sku
LEFT JOIN costs ct ON c.period_month = ct.period_month AND c.sku = ct.sku;

DROP VIEW IF EXISTS v_finance_detail_lines;
CREATE VIEW v_finance_detail_lines AS
WITH settlement_base AS (
    SELECT
        s.settlement_line_id AS detail_line_id,
        'settlement' AS detail_source,
        s.settlement_line_id,
        s.source_file,
        s.transaction_datetime,
        s.transaction_month AS period_month,
        s.settlement_id,
        s.transaction_type,
        s.transaction_subtype,
        s.order_id AS amazon_order_id,
        s.sku,
        s.marketplace,
        s.fulfillment,
        CASE
            WHEN lower(COALESCE(s.order_type, '')) = 'review_sale' THEN 'test_order_sale'
            WHEN lower(COALESCE(s.order_type, '')) = 'review_refund' THEN 'test_order_refund'
            ELSE lower(COALESCE(s.order_type, ''))
        END AS order_type,
        COALESCE(s.quantity, 0) AS qty_sold,
        COALESCE(s.product_sales, 0) AS product_sales,
        COALESCE(s.shipping_credits, 0) AS shipping_credits,
        COALESCE(s.gift_wrap_credits, 0) AS gift_wrap_credits,
        COALESCE(s.promotional_rebates, 0) AS promotional_rebates,
        COALESCE(s.product_sales, 0) + COALESCE(s.shipping_credits, 0) + COALESCE(s.gift_wrap_credits, 0) + COALESCE(s.promotional_rebates, 0) AS net_sales,
        ABS(COALESCE(s.product_sales, 0) + COALESCE(s.shipping_credits, 0) + COALESCE(s.gift_wrap_credits, 0) + COALESCE(s.promotional_rebates, 0)) AS abs_net_sales,
        -COALESCE(s.selling_fees, 0) AS selling_fees,
        -COALESCE(s.fba_fees, 0) AS fba_fees,
        -COALESCE(s.other_transaction_fees, 0) AS other_transaction_fees,
        -COALESCE(s.marketplace_withheld_tax, 0) AS marketplace_withheld_tax,
        COALESCE(s.total, 0) AS settlement_net_total,
        CASE WHEN lower(COALESCE(s.order_type, '')) LIKE '%refund' THEN 1 ELSE 0 END AS is_refund,
        CASE
            WHEN lower(COALESCE(s.order_type, '')) IN ('review_sale', 'test_order_sale') THEN 1
            ELSE 0
        END AS is_review_type,
        CASE WHEN lower(COALESCE(s.order_type, '')) LIKE 'vine_%' AND lower(COALESCE(s.order_type, '')) NOT LIKE '%refund' THEN 1 ELSE 0 END AS is_vine_type
    FROM fact_settlement_lines s
    WHERE s.transaction_type IN ('Order', 'Refund')
      AND s.sku IS NOT NULL
      AND s.order_id IS NOT NULL
      AND s.order_type IS NOT NULL
      AND trim(s.order_type) <> ''
      AND lower(s.order_type) <> 'unknown'
),
eligible_sales AS (
    SELECT *
    FROM settlement_base
    WHERE is_refund = 0
),
sku_sale_totals AS (
    SELECT period_month, sku, SUM(abs_net_sales) AS sku_abs_net_sales
    FROM eligible_sales
    GROUP BY period_month, sku
),
month_sale_totals AS (
    SELECT period_month, SUM(abs_net_sales) AS month_abs_net_sales
    FROM eligible_sales
    GROUP BY period_month
),
review_sale_totals AS (
    SELECT period_month, sku, SUM(abs_net_sales) AS review_abs_net_sales, COUNT(*) AS review_row_count
    FROM eligible_sales
    WHERE is_review_type = 1
    GROUP BY period_month, sku
),
vine_sale_totals AS (
    SELECT period_month, sku, SUM(qty_sold) AS vine_qty_sold, COUNT(*) AS vine_row_count
    FROM eligible_sales
    WHERE is_vine_type = 1
    GROUP BY period_month, sku
),
order_presence AS (
    SELECT
        period_month,
        amazon_order_id,
        sku,
        SUM(CASE WHEN is_refund = 1 THEN abs_net_sales ELSE 0 END) AS refund_abs_net_sales,
        SUM(CASE WHEN is_refund = 0 THEN abs_net_sales ELSE 0 END) AS non_refund_abs_net_sales,
        SUM(CASE WHEN is_refund = 1 THEN 1 ELSE 0 END) AS refund_row_count,
        SUM(CASE WHEN is_refund = 0 THEN 1 ELSE 0 END) AS non_refund_row_count
    FROM settlement_base
    GROUP BY period_month, amazon_order_id, sku
),
direct_comp_source AS (
    SELECT compensation_id, period_month, amazon_order_id, sku, amount_total
    FROM fact_compensation_monthly_sku
    WHERE amazon_order_id IS NOT NULL
      AND sku IS NOT NULL
),
direct_comp_alloc AS (
    SELECT
        sb.detail_line_id,
        SUM(
            CASE
                WHEN op.refund_row_count > 0 AND sb.is_refund = 1 THEN
                    CASE
                        WHEN COALESCE(op.refund_abs_net_sales, 0) > 0 THEN dcs.amount_total * sb.abs_net_sales / op.refund_abs_net_sales
                        WHEN COALESCE(op.refund_row_count, 0) > 0 THEN dcs.amount_total * 1.0 / op.refund_row_count
                        ELSE 0
                    END
                WHEN op.refund_row_count = 0 AND op.non_refund_row_count > 0 AND sb.is_refund = 0 THEN
                    CASE
                        WHEN COALESCE(op.non_refund_abs_net_sales, 0) > 0 THEN dcs.amount_total * sb.abs_net_sales / op.non_refund_abs_net_sales
                        WHEN COALESCE(op.non_refund_row_count, 0) > 0 THEN dcs.amount_total * 1.0 / op.non_refund_row_count
                        ELSE 0
                    END
                ELSE 0
            END
        ) AS compensation_income
    FROM direct_comp_source dcs
    JOIN order_presence op
      ON dcs.period_month = op.period_month
     AND dcs.amazon_order_id = op.amazon_order_id
     AND dcs.sku = op.sku
    JOIN settlement_base sb
      ON op.period_month = sb.period_month
     AND op.amazon_order_id = sb.amazon_order_id
     AND op.sku = sb.sku
     AND (
        (op.refund_row_count > 0 AND sb.is_refund = 1)
        OR (op.refund_row_count = 0 AND op.non_refund_row_count > 0 AND sb.is_refund = 0)
     )
    GROUP BY sb.detail_line_id
),
unassigned_compensation AS (
    SELECT
        c.period_month,
        c.amazon_order_id,
        c.sku,
        SUM(c.amount_total) AS compensation_income
    FROM fact_compensation_monthly_sku c
    LEFT JOIN order_presence op
      ON c.period_month = op.period_month
     AND c.amazon_order_id = op.amazon_order_id
     AND c.sku = op.sku
    WHERE c.sku IS NOT NULL
      AND (c.amazon_order_id IS NULL OR op.amazon_order_id IS NULL)
    GROUP BY c.period_month, c.amazon_order_id, c.sku
),
ad_report_sku AS (
    SELECT period_month, sku, SUM(spend) AS report_ad_spend
    FROM fact_advertising_monthly_sku
    GROUP BY period_month, sku
),
ad_report_month AS (
    SELECT period_month, SUM(report_ad_spend) AS report_ad_total
    FROM ad_report_sku
    GROUP BY period_month
),
settlement_ad_pool AS (
    SELECT transaction_month AS period_month, SUM(-COALESCE(total, 0)) AS receivable_ad_spend
    FROM fact_settlement_lines
    WHERE lower(COALESCE(transaction_subtype, '')) = 'cost of advertising'
    GROUP BY transaction_month
),
storage_report_sku AS (
    SELECT period_month, sku, SUM(COALESCE(estimated_monthly_storage_fee, 0) - COALESCE(incentive_fee_amount, 0)) AS report_storage_fees
    FROM fact_storage_monthly_sku
    WHERE sku IS NOT NULL
    GROUP BY period_month, sku
),
storage_report_month AS (
    SELECT period_month, SUM(report_storage_fees) AS report_storage_total
    FROM storage_report_sku
    GROUP BY period_month
),
settlement_storage_pool AS (
    SELECT
        transaction_month AS period_month,
        SUM(
            CASE
                WHEN lower(COALESCE(transaction_subtype, '')) IN ('fba storage fee', 'fba long-term storage fee')
                    THEN -COALESCE(total, 0)
                ELSE 0
            END
        ) AS receivable_storage_fees
    FROM fact_settlement_lines
    GROUP BY transaction_month
),
removal_report_sku AS (
    SELECT
        period_month,
        sku,
        SUM(COALESCE(removal_fee, 0)) AS report_removal_fees,
        SUM(
            CASE
                WHEN lower(COALESCE(removal_order_type, '')) <> 'disposal' AND COALESCE(disposed_quantity, 0) = 0
                    THEN COALESCE(shipped_quantity, 0)
                ELSE 0
            END
        ) AS transfer_quantity,
        SUM(COALESCE(disposed_quantity, 0)) AS disposal_quantity
    FROM fact_removal_monthly_sku
    WHERE sku IS NOT NULL
    GROUP BY period_month, sku
),
removal_report_month AS (
    SELECT period_month, SUM(report_removal_fees) AS report_removal_total
    FROM removal_report_sku
    GROUP BY period_month
),
settlement_removal_pool AS (
    SELECT
        transaction_month AS period_month,
        SUM(
            CASE
                WHEN lower(COALESCE(transaction_subtype, '')) LIKE 'fba removal order:%'
                    THEN -COALESCE(total, 0)
                ELSE 0
            END
        ) AS receivable_removal_fees
    FROM fact_settlement_lines
    GROUP BY transaction_month
),
settlement_removal_rows AS (
    SELECT
        s.settlement_line_id AS detail_line_id,
        'settlement' AS detail_source,
        s.settlement_line_id,
        s.source_file,
        s.transaction_datetime,
        s.transaction_month AS period_month,
        s.settlement_id,
        s.transaction_type,
        s.transaction_subtype,
        s.order_id AS amazon_order_id,
        rm.sku,
        s.marketplace,
        s.fulfillment,
        'non_order_fee' AS order_type,
        0.0 AS qty_sold,
        0.0 AS product_sales,
        0.0 AS shipping_credits,
        0.0 AS gift_wrap_credits,
        0.0 AS promotional_rebates,
        0.0 AS net_sales,
        0.0 AS selling_fees,
        0.0 AS fba_fees,
        0.0 AS other_transaction_fees,
        0.0 AS marketplace_withheld_tax,
        0.0 AS transfer_quantity,
        0.0 AS disposal_quantity,
        0.0 AS storage_fees,
        0.0 AS removal_fees,
        0.0 AS removal_fee_capitalized,
        0.0 AS removal_fee_unclassified,
        0.0 AS ad_spend,
        0.0 AS compensation_income,
        0.0 AS review_cost,
        0.0 AS vine_fee,
        0.0 AS review_quantity,
        0.0 AS vine_quantity,
        0.0 AS subscription_fee,
        0.0 AS coupon_participation_fee,
        0.0 AS coupon_performance_fee,
        0.0 AS product_unit_cost,
        0.0 AS inbound_unit_cost,
        0.0 AS product_cost,
        0.0 AS inbound_cost,
        0.0 AS receivable_ad_spend,
        0.0 AS receivable_storage_fees,
        -COALESCE(s.total, 0) AS receivable_removal_fees,
        0.0 AS receivable_compensation_income,
        0.0 AS receivable_subscription_fee,
        0.0 AS receivable_coupon_participation_fee,
        0.0 AS receivable_coupon_performance_fee,
        0.0 AS receivable_vine_fee,
        0.0 AS inventory_capitalized_cost,
        COALESCE(s.total, 0) AS settlement_net_total,
        0.0 AS alloc_share
    FROM fact_settlement_lines s
    LEFT JOIN (
        SELECT period_month, order_id, MAX(sku) AS sku
        FROM fact_removal_monthly_sku
        WHERE order_id IS NOT NULL
          AND sku IS NOT NULL
        GROUP BY period_month, order_id
    ) rm
      ON s.transaction_month = rm.period_month
     AND COALESCE(s.order_id, '') = COALESCE(rm.order_id, '')
    WHERE lower(COALESCE(s.transaction_subtype, '')) LIKE 'fba removal order:%'
),
settlement_rows AS (
    SELECT
        sb.detail_line_id,
        sb.detail_source,
        sb.settlement_line_id,
        sb.source_file,
        sb.transaction_datetime,
        sb.period_month,
        sb.settlement_id,
        sb.transaction_type,
        sb.transaction_subtype,
        sb.amazon_order_id,
        sb.sku,
        sb.marketplace,
        sb.fulfillment,
        sb.order_type,
        sb.qty_sold,
        sb.product_sales,
        sb.shipping_credits,
        sb.gift_wrap_credits,
        sb.promotional_rebates,
        sb.net_sales,
        sb.selling_fees,
        sb.fba_fees,
        sb.other_transaction_fees
        + CASE
            WHEN sb.is_refund = 1 OR COALESCE(st.sku_abs_net_sales, 0) = 0 THEN 0
            ELSE COALESCE(fp.manual_shared_cost, 0) * sb.abs_net_sales / st.sku_abs_net_sales
        END AS other_transaction_fees,
        sb.marketplace_withheld_tax,
        0.0 AS transfer_quantity,
        0.0 AS disposal_quantity,
        0.0 AS storage_fees,
        CASE
            WHEN sb.is_refund = 1 OR COALESCE(st.sku_abs_net_sales, 0) = 0 THEN 0
            ELSE COALESCE(fp.removal_fees, 0) * sb.abs_net_sales / st.sku_abs_net_sales
        END AS removal_fees,
        0.0 AS removal_fee_capitalized,
        0.0 AS removal_fee_unclassified,
        CASE
            WHEN sb.is_refund = 1 OR COALESCE(st.sku_abs_net_sales, 0) = 0 THEN 0
            ELSE COALESCE(fp.ad_spend, 0) * sb.abs_net_sales / st.sku_abs_net_sales
        END AS ad_spend,
        COALESCE(dca.compensation_income, 0) AS compensation_income,
        CASE
            WHEN sb.is_refund = 1 OR sb.is_review_type = 0 OR COALESCE(rt.review_abs_net_sales, 0) = 0 THEN 0
            ELSE COALESCE(fp.review_cost, 0) * sb.abs_net_sales / rt.review_abs_net_sales
        END AS review_cost,
        CASE
            WHEN sb.is_refund = 1 OR sb.is_vine_type = 0 OR COALESCE(vt.vine_row_count, 0) = 0 THEN 0
            WHEN COALESCE(vt.vine_qty_sold, 0) > 0 THEN COALESCE(fp.vine_fee, 0) * sb.qty_sold / vt.vine_qty_sold
            ELSE COALESCE(fp.vine_fee, 0) * 1.0 / vt.vine_row_count
        END AS vine_fee,
        CASE WHEN lower(COALESCE(sb.order_type, '')) LIKE 'review_%' THEN COALESCE(sb.qty_sold, 0) ELSE 0 END AS review_quantity,
        CASE WHEN lower(COALESCE(sb.order_type, '')) LIKE 'vine_%' THEN COALESCE(sb.qty_sold, 0) ELSE 0 END AS vine_quantity,
        CASE
            WHEN sb.is_refund = 1 OR COALESCE(mt.month_abs_net_sales, 0) = 0 THEN 0
            ELSE COALESCE(pp.subscription_fee, 0) * sb.abs_net_sales / mt.month_abs_net_sales
        END AS subscription_fee,
        CASE
            WHEN sb.is_refund = 1 OR COALESCE(mt.month_abs_net_sales, 0) = 0 THEN 0
            ELSE COALESCE(pp.coupon_participation_fee, 0) * sb.abs_net_sales / mt.month_abs_net_sales
        END AS coupon_participation_fee,
        CASE
            WHEN sb.is_refund = 1 OR COALESCE(mt.month_abs_net_sales, 0) = 0 THEN 0
            ELSE COALESCE(pp.coupon_performance_fee, 0) * sb.abs_net_sales / mt.month_abs_net_sales
        END AS coupon_performance_fee,
        COALESCE(fp.product_unit_cost, 0) AS product_unit_cost,
        COALESCE(fp.inbound_unit_cost, 0) AS inbound_unit_cost,
        CASE
            WHEN sb.is_refund = 1 THEN -COALESCE(fp.product_unit_cost, 0) * sb.qty_sold
            ELSE COALESCE(fp.product_unit_cost, 0) * sb.qty_sold
        END AS product_cost,
        CASE
            WHEN sb.is_refund = 1 THEN -COALESCE(fp.inbound_unit_cost, 0) * sb.qty_sold
            ELSE COALESCE(fp.inbound_unit_cost, 0) * sb.qty_sold
        END AS inbound_cost,
        CASE
            WHEN sb.is_refund = 1 THEN 0
            WHEN COALESCE(arm.report_ad_total, 0) > 0 THEN
                CASE
                    WHEN COALESCE(ars.report_ad_spend, 0) > 0 AND COALESCE(st.sku_abs_net_sales, 0) > 0
                        THEN COALESCE(sap.receivable_ad_spend, 0) * ars.report_ad_spend / arm.report_ad_total * sb.abs_net_sales / st.sku_abs_net_sales
                    ELSE 0
                END
            WHEN COALESCE(mt.month_abs_net_sales, 0) > 0
                THEN COALESCE(sap.receivable_ad_spend, 0) * sb.abs_net_sales / mt.month_abs_net_sales
            ELSE 0
        END AS receivable_ad_spend,
        0.0 AS receivable_storage_fees,
        0.0 AS receivable_removal_fees,
        COALESCE(dca.compensation_income, 0) AS receivable_compensation_income,
        CASE
            WHEN sb.is_refund = 1 OR COALESCE(mt.month_abs_net_sales, 0) = 0 THEN 0
            ELSE COALESCE(pp.subscription_fee, 0) * sb.abs_net_sales / mt.month_abs_net_sales
        END AS receivable_subscription_fee,
        CASE
            WHEN sb.is_refund = 1 OR COALESCE(mt.month_abs_net_sales, 0) = 0 THEN 0
            ELSE COALESCE(pp.coupon_participation_fee, 0) * sb.abs_net_sales / mt.month_abs_net_sales
        END AS receivable_coupon_participation_fee,
        CASE
            WHEN sb.is_refund = 1 OR COALESCE(mt.month_abs_net_sales, 0) = 0 THEN 0
            ELSE COALESCE(pp.coupon_performance_fee, 0) * sb.abs_net_sales / mt.month_abs_net_sales
        END AS receivable_coupon_performance_fee,
        CASE
            WHEN sb.is_refund = 1 OR sb.is_vine_type = 0 OR COALESCE(vt.vine_row_count, 0) = 0 THEN 0
            WHEN COALESCE(vt.vine_qty_sold, 0) > 0 THEN COALESCE(fp.vine_fee, 0) * sb.qty_sold / vt.vine_qty_sold
            ELSE COALESCE(fp.vine_fee, 0) * 1.0 / vt.vine_row_count
        END AS receivable_vine_fee,
        0.0 AS inventory_capitalized_cost,
        sb.settlement_net_total,
        CASE
            WHEN sb.is_refund = 1 OR COALESCE(st.sku_abs_net_sales, 0) = 0 THEN 0
            ELSE sb.abs_net_sales * 1.0 / st.sku_abs_net_sales
        END AS alloc_share
    FROM settlement_base sb
    LEFT JOIN v_monthly_sku_fee_pool fp
      ON sb.period_month = fp.period_month
     AND sb.sku = fp.sku
    LEFT JOIN v_monthly_platform_fee_pool pp
      ON sb.period_month = pp.period_month
    LEFT JOIN sku_sale_totals st
      ON sb.period_month = st.period_month
     AND sb.sku = st.sku
    LEFT JOIN month_sale_totals mt
      ON sb.period_month = mt.period_month
    LEFT JOIN review_sale_totals rt
      ON sb.period_month = rt.period_month
     AND sb.sku = rt.sku
    LEFT JOIN vine_sale_totals vt
      ON sb.period_month = vt.period_month
     AND sb.sku = vt.sku
    LEFT JOIN direct_comp_alloc dca
      ON sb.detail_line_id = dca.detail_line_id
    LEFT JOIN ad_report_sku ars
      ON sb.period_month = ars.period_month
     AND sb.sku = ars.sku
    LEFT JOIN ad_report_month arm
      ON sb.period_month = arm.period_month
    LEFT JOIN settlement_ad_pool sap
      ON sb.period_month = sap.period_month
    LEFT JOIN storage_report_sku srs
      ON sb.period_month = srs.period_month
     AND sb.sku = srs.sku
    LEFT JOIN storage_report_month srm
      ON sb.period_month = srm.period_month
    LEFT JOIN settlement_storage_pool ssp
      ON sb.period_month = ssp.period_month
    LEFT JOIN removal_report_sku rrs
      ON sb.period_month = rrs.period_month
     AND sb.sku = rrs.sku
    LEFT JOIN removal_report_month rrm
      ON sb.period_month = rrm.period_month
    LEFT JOIN settlement_removal_pool srmp
      ON sb.period_month = srmp.period_month
),
synthetic_comp_rows AS (
    SELECT
        'synthetic_comp:' || uc.period_month || ':' || COALESCE(uc.amazon_order_id, 'no_order') || ':' || COALESCE(uc.sku, 'no_sku') AS detail_line_id,
        'synthetic' AS detail_source,
        NULL AS settlement_line_id,
        NULL AS source_file,
        NULL AS transaction_datetime,
        uc.period_month,
        NULL AS settlement_id,
        'Synthetic' AS transaction_type,
        'unassigned_compensation' AS transaction_subtype,
        uc.amazon_order_id,
        uc.sku,
        NULL AS marketplace,
        NULL AS fulfillment,
        'non_order_fee' AS order_type,
        0.0 AS qty_sold,
        0.0 AS product_sales,
        0.0 AS shipping_credits,
        0.0 AS gift_wrap_credits,
        0.0 AS promotional_rebates,
        0.0 AS net_sales,
        0.0 AS selling_fees,
        0.0 AS fba_fees,
        0.0 AS other_transaction_fees,
        0.0 AS marketplace_withheld_tax,
        0.0 AS transfer_quantity,
        0.0 AS disposal_quantity,
        0.0 AS storage_fees,
        0.0 AS removal_fees,
        0.0 AS removal_fee_capitalized,
        0.0 AS removal_fee_unclassified,
        0.0 AS ad_spend,
        uc.compensation_income,
        0.0 AS review_cost,
        0.0 AS vine_fee,
        0.0 AS review_quantity,
        0.0 AS vine_quantity,
        0.0 AS subscription_fee,
        0.0 AS coupon_participation_fee,
        0.0 AS coupon_performance_fee,
        0.0 AS product_unit_cost,
        0.0 AS inbound_unit_cost,
        0.0 AS product_cost,
        0.0 AS inbound_cost,
        0.0 AS receivable_ad_spend,
        0.0 AS receivable_storage_fees,
        0.0 AS receivable_removal_fees,
        uc.compensation_income AS receivable_compensation_income,
        0.0 AS receivable_subscription_fee,
        0.0 AS receivable_coupon_participation_fee,
        0.0 AS receivable_coupon_performance_fee,
        0.0 AS receivable_vine_fee,
        0.0 AS inventory_capitalized_cost,
        0.0 AS settlement_net_total,
        0.0 AS alloc_share
    FROM unassigned_compensation uc
    WHERE ABS(COALESCE(uc.compensation_income, 0)) > 0.000001
),
synthetic_fee_rows AS (
    SELECT
        'synthetic_fee:' || seed.period_month || ':' || COALESCE(seed.sku, 'no_sku') AS detail_line_id,
        'synthetic' AS detail_source,
        NULL AS settlement_line_id,
        NULL AS source_file,
        NULL AS transaction_datetime,
        seed.period_month,
        NULL AS settlement_id,
        'Synthetic' AS transaction_type,
        'residual_fee_pool' AS transaction_subtype,
        NULL AS amazon_order_id,
        seed.sku,
        NULL AS marketplace,
        NULL AS fulfillment,
        'non_order_fee' AS order_type,
        0.0 AS qty_sold,
        0.0 AS product_sales,
        0.0 AS shipping_credits,
        0.0 AS gift_wrap_credits,
        0.0 AS promotional_rebates,
        0.0 AS net_sales,
        0.0 AS selling_fees,
        0.0 AS fba_fees,
        CASE
            WHEN COALESCE(st.sku_abs_net_sales, 0) = 0 THEN COALESCE(fp.manual_shared_cost, 0)
            ELSE 0
        END AS other_transaction_fees,
        0.0 AS marketplace_withheld_tax,
        COALESCE(rrs.transfer_quantity, 0) AS transfer_quantity,
        COALESCE(rrs.disposal_quantity, 0) AS disposal_quantity,
        COALESCE(fp.storage_fees, 0) AS storage_fees,
        CASE WHEN COALESCE(st.sku_abs_net_sales, 0) = 0 THEN COALESCE(fp.removal_fees, 0) ELSE 0 END AS removal_fees,
        COALESCE(fp.removal_fee_capitalized, 0) AS removal_fee_capitalized,
        COALESCE(fp.removal_fee_unclassified, 0) AS removal_fee_unclassified,
        CASE WHEN COALESCE(st.sku_abs_net_sales, 0) = 0 THEN COALESCE(fp.ad_spend, 0) ELSE 0 END AS ad_spend,
        0.0 AS compensation_income,
        CASE WHEN COALESCE(rt.review_row_count, 0) = 0 THEN COALESCE(fp.review_cost, 0) ELSE 0 END AS review_cost,
        CASE WHEN COALESCE(vt.vine_row_count, 0) = 0 THEN COALESCE(fp.vine_fee, 0) ELSE 0 END AS vine_fee,
        0.0 AS review_quantity,
        0.0 AS vine_quantity,
        0.0 AS subscription_fee,
        0.0 AS coupon_participation_fee,
        0.0 AS coupon_performance_fee,
        0.0 AS product_unit_cost,
        0.0 AS inbound_unit_cost,
        0.0 AS product_cost,
        0.0 AS inbound_cost,
        CASE
            WHEN COALESCE(st.sku_abs_net_sales, 0) = 0 AND COALESCE(arm.report_ad_total, 0) > 0 AND COALESCE(ars.report_ad_spend, 0) > 0
                THEN COALESCE(sap.receivable_ad_spend, 0) * ars.report_ad_spend / arm.report_ad_total
            ELSE 0
        END AS receivable_ad_spend,
        CASE
            WHEN COALESCE(srm.report_storage_total, 0) > 0 AND COALESCE(srs.report_storage_fees, 0) > 0
                THEN COALESCE(ssp.receivable_storage_fees, 0) * srs.report_storage_fees / srm.report_storage_total
            ELSE 0
        END AS receivable_storage_fees,
        0.0 AS receivable_removal_fees,
        0.0 AS receivable_compensation_income,
        0.0 AS receivable_subscription_fee,
        0.0 AS receivable_coupon_participation_fee,
        0.0 AS receivable_coupon_performance_fee,
        0.0 AS receivable_vine_fee,
        COALESCE(fp.removal_fee_capitalized, 0) AS inventory_capitalized_cost,
        0.0 AS settlement_net_total,
        0.0 AS alloc_share
    FROM (
        SELECT period_month, sku FROM v_monthly_sku_fee_pool
        UNION
        SELECT period_month, sku FROM unassigned_compensation
    ) seed
    LEFT JOIN v_monthly_sku_fee_pool fp
      ON seed.period_month = fp.period_month
     AND seed.sku = fp.sku
    LEFT JOIN sku_sale_totals st
      ON seed.period_month = st.period_month
     AND seed.sku = st.sku
    LEFT JOIN review_sale_totals rt
      ON seed.period_month = rt.period_month
     AND seed.sku = rt.sku
    LEFT JOIN vine_sale_totals vt
      ON seed.period_month = vt.period_month
     AND seed.sku = vt.sku
    LEFT JOIN ad_report_sku ars
      ON seed.period_month = ars.period_month
     AND seed.sku = ars.sku
    LEFT JOIN ad_report_month arm
      ON seed.period_month = arm.period_month
    LEFT JOIN settlement_ad_pool sap
      ON seed.period_month = sap.period_month
    LEFT JOIN storage_report_sku srs
      ON seed.period_month = srs.period_month
     AND seed.sku = srs.sku
    LEFT JOIN storage_report_month srm
      ON seed.period_month = srm.period_month
    LEFT JOIN settlement_storage_pool ssp
      ON seed.period_month = ssp.period_month
    LEFT JOIN removal_report_sku rrs
      ON seed.period_month = rrs.period_month
     AND seed.sku = rrs.sku
    LEFT JOIN removal_report_month rrm
      ON seed.period_month = rrm.period_month
    LEFT JOIN settlement_removal_pool srmp
      ON seed.period_month = srmp.period_month
    WHERE ABS(COALESCE(fp.removal_fee_capitalized, 0)) > 0.000001
       OR ABS(COALESCE(fp.removal_fee_unclassified, 0)) > 0.000001
       OR ABS(COALESCE(fp.storage_fees, 0)) > 0.000001
       OR (COALESCE(srm.report_storage_total, 0) > 0 AND COALESCE(srs.report_storage_fees, 0) > 0)
       OR (COALESCE(st.sku_abs_net_sales, 0) = 0 AND (
            ABS(COALESCE(fp.removal_fees, 0)) > 0.000001
            OR ABS(COALESCE(fp.ad_spend, 0)) > 0.000001
            OR (COALESCE(arm.report_ad_total, 0) > 0 AND COALESCE(ars.report_ad_spend, 0) > 0)
            OR (COALESCE(rrm.report_removal_total, 0) > 0 AND COALESCE(rrs.report_removal_fees, 0) > 0)
       ))
       OR (COALESCE(rt.review_row_count, 0) = 0 AND ABS(COALESCE(fp.review_cost, 0)) > 0.000001)
       OR (COALESCE(vt.vine_row_count, 0) = 0 AND ABS(COALESCE(fp.vine_fee, 0)) > 0.000001)
       OR ABS(COALESCE(fp.manual_shared_cost, 0)) > 0.000001
)
SELECT * FROM settlement_rows
UNION ALL
SELECT * FROM settlement_removal_rows
UNION ALL
SELECT * FROM synthetic_comp_rows
UNION ALL
SELECT * FROM synthetic_fee_rows;

DROP VIEW IF EXISTS v_monthly_sku_order_type_summary;
CREATE VIEW v_monthly_sku_order_type_summary AS
SELECT
    period_month,
    sku,
    order_type,
    SUM(qty_sold) AS qty_sold,
    SUM(product_sales + shipping_credits + gift_wrap_credits) AS gmv,
    SUM(product_sales) AS product_sales,
    SUM(shipping_credits) AS shipping_credits,
    SUM(gift_wrap_credits) AS gift_wrap_credits,
    SUM(promotional_rebates) AS promotional_rebates,
    SUM(net_sales) AS net_sales,
    SUM(selling_fees) AS selling_fees,
    SUM(fba_fees) AS fba_fees,
    SUM(other_transaction_fees) AS other_transaction_fees,
    SUM(marketplace_withheld_tax) AS marketplace_withheld_tax,
    SUM(transfer_quantity) AS transfer_quantity,
    SUM(disposal_quantity) AS disposal_quantity,
    SUM(storage_fees) AS storage_fees,
    SUM(removal_fees) AS removal_fees,
    SUM(removal_fee_capitalized) AS removal_fee_capitalized,
    SUM(removal_fee_unclassified) AS removal_fee_unclassified,
    SUM(ad_spend) AS ad_spend,
    SUM(compensation_income) AS compensation_income,
    SUM(review_cost) AS review_cost,
    SUM(vine_fee) AS vine_fee,
    SUM(review_quantity) AS review_quantity,
    SUM(vine_quantity) AS vine_quantity,
    SUM(subscription_fee) AS subscription_fee,
    SUM(coupon_participation_fee) AS coupon_participation_fee,
    SUM(coupon_performance_fee) AS coupon_performance_fee,
    MAX(product_unit_cost) AS product_unit_cost,
    MAX(inbound_unit_cost) AS inbound_unit_cost,
    SUM(product_cost) AS product_cost,
    SUM(inbound_cost) AS inbound_cost,
    SUM(receivable_ad_spend) AS receivable_ad_spend,
    SUM(receivable_storage_fees) AS receivable_storage_fees,
    SUM(receivable_removal_fees) AS receivable_removal_fees,
    SUM(receivable_compensation_income) AS receivable_compensation_income,
    SUM(receivable_subscription_fee) AS receivable_subscription_fee,
    SUM(receivable_coupon_participation_fee) AS receivable_coupon_participation_fee,
    SUM(receivable_coupon_performance_fee) AS receivable_coupon_performance_fee,
    SUM(receivable_vine_fee) AS receivable_vine_fee,
    SUM(inventory_capitalized_cost) AS inventory_capitalized_cost,
    SUM(settlement_net_total) AS settlement_net_total,
    SUM(CASE WHEN detail_source = 'settlement' THEN 1 ELSE 0 END) AS settlement_line_count,
    SUM(alloc_share) AS alloc_share
FROM v_finance_detail_lines
GROUP BY period_month, sku, order_type;
"""


def main() -> int:
    config = get_config()
    print_banner('Building reporting views')
    conn = connect(config.db_path)
    run_id = register_etl_run(conn, '16_build_monthly_finance_views.py', 'build_views', status='started')
    try:
        conn.executescript(VIEW_SQL)
        conn.commit()
        note = 'Built settlement tracking, platform fee pool, sku fee pool, finance detail, and allocated summary views'
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
