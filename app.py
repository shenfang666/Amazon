from __future__ import annotations

import argparse
import csv
import io
import json
import mimetypes
import os
import re
import runpy
import sqlite3
import subprocess
import sys
import threading
from datetime import datetime
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
WEB_DIR = ROOT / "web"
DB_PATH = ROOT / "amazon_finance.db"
ETL_RUNNER = ROOT / "etl" / "99_run_monthly.py"
MANUAL_DIR = ROOT / "manual"
ATTACHMENT_DIR = MANUAL_DIR / "attachments"

GROSS_PROFIT_EXPR = """
    net_sales
    - selling_fees
    - fba_fees
    - other_transaction_fees
    - marketplace_withheld_tax
    - storage_fees
    - removal_fees
    - ad_spend
    + compensation_income
    - review_cost
    - subscription_fee
    - coupon_participation_fee
    - coupon_performance_fee
    - vine_fee
    - product_cost
    - inbound_cost
"""

RECEIVABLE_EXPR = """
    net_sales
    - selling_fees
    - fba_fees
    - other_transaction_fees
    - marketplace_withheld_tax
    - receivable_storage_fees
    - receivable_removal_fees
    - receivable_ad_spend
    + receivable_compensation_income
    - receivable_subscription_fee
    - receivable_coupon_participation_fee
    - receivable_coupon_performance_fee
    - receivable_vine_fee
"""

TEXT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
}

CSV_ENCODINGS = ["utf-8-sig", "utf-8", "gb18030", "gbk", "cp1252", "latin-1"]
SOURCE_FILE_SUFFIXES = {".csv", ".txt", ".xlsx", ".pdf"}
MANUAL_FILE_CONFIG = {
    "manual_sku_aliases": {
        "filename": "manual_sku_aliases.csv",
        "label": "SKU 别名表",
        "headers": ["alias_type", "alias_value", "sku", "source_note", "is_active"],
    },
    "manual_vine_fee_allocations": {
        "filename": "manual_vine_fee_allocations.csv",
        "label": "Vine 费用分配",
        "headers": ["period_month", "sku", "fee_amount", "source_note"],
    },
    "manual_shared_costs": {
        "filename": "manual_shared_costs.csv",
        "label": "共摊费用表",
        "headers": [
            "period_month",
            "cost_type",
            "description",
            "total_amount",
            "currency",
            "platforms",
            "allocation_method",
            "direct_sku",
            "custom_pct_json",
            "source_note",
        ],
    },
    "manual_platform_monthly_base": {
        "filename": "manual_platform_monthly_base.csv",
        "label": "平台月度基表",
        "headers": ["period_month", "platform", "net_sales", "shipped_qty", "order_line_count", "source_note"],
    },
    "manual_removal_fee_controls": {
        "filename": "manual_removal_fee_controls.csv",
        "label": "Removal 费用分类",
        "headers": ["period_month", "order_id", "sku", "removal_category", "accounting_treatment", "source_note"],
    },
}

MONTHLY_JOB: dict = {
    "job_id": None,
    "status": "idle",
    "target_month": None,
    "skip_init": True,
    "started_at": None,
    "finished_at": None,
    "exit_code": None,
    "logs": [],
}
JOB_LOCK = threading.Lock()
RUNTIME_SCHEMA_READY = False
RULE_VERSION_SCOPE = "finance_control"
RULE_VERSION_NAME = "2026-03-16-audit-gap-remediation"
RULE_VERSION_NOTES = "Enable inventory reconciliation, upload batch tracking, and auditable month-close actions."
INVENTORY_MOVEMENT_TYPES = {"inbound", "outbound", "transfer", "return", "adjust"}
NORMAL_OVERRIDE_CHOICES = {"normal", "normal_timing_difference", "expected_timing_difference"}
INVENTORY_TAB_HTML = """      <button class="tab-button" data-tab="inventory" type="button">库存核对</button>
"""
INVENTORY_PANEL_HTML = """

      <section class="tab-panel" data-tab-panel="inventory">
        <section class="hero-panel reveal">
          <div class="hero-copy">
            <p class="eyebrow">库存闭环</p>
            <h2 id="inventory-title">库存核对中心</h2>
            <p id="inventory-description" class="hero-description"></p>
          </div>
          <div id="inventory-current" class="close-status-card"></div>
        </section>
        <section class="panel reveal compact-top">
          <div class="panel-head"><div><p class="eyebrow">库存汇总</p><h3>收发存概览</h3></div></div>
          <div id="inventory-summary" class="comparison-strip"></div>
        </section>
        <section class="panel reveal compact-top">
          <div class="panel-head"><div><p class="eyebrow">问题提示</p><h3>库存异常检查</h3></div></div>
          <div id="inventory-issues" class="table-stack"></div>
        </section>
        <section class="panel reveal compact-top">
          <div class="panel-head"><div><p class="eyebrow">手工录入</p><h3>新增库存流水</h3></div></div>
          <div class="download-grid">
            <label class="control"><span>发生日期</span><input id="inventory-movement-date" type="date"></label>
            <label class="control"><span>流水类型</span><select id="inventory-movement-type"><option value="inbound">inbound</option><option value="outbound">outbound</option><option value="transfer">transfer</option><option value="return">return</option><option value="adjust">adjust</option></select></label>
            <label class="control"><span>SKU</span><input id="inventory-sku" type="search" placeholder="例如 NMN-magnesium-90ct"></label>
            <label class="control"><span>数量</span><input id="inventory-quantity" type="number" step="0.01"></label>
            <label class="control"><span>单件成本</span><input id="inventory-unit-cost" type="number" step="0.01"></label>
            <label class="control"><span>金额</span><input id="inventory-amount-total" type="number" step="0.01" placeholder="留空则按数量 x 单件成本"></label>
            <label class="control"><span>来源引用</span><input id="inventory-source-ref" type="search" placeholder="例如 ERP-2026-02-001"></label>
          </div>
          <label class="control"><span>备注</span><textarea id="inventory-note" rows="3" placeholder="补充来源、判断依据、差异说明等"></textarea></label>
          <div class="panel-actions">
            <button id="save-inventory-movement-button" class="ghost-button" type="button">保存库存流水</button>
          </div>
          <div id="inventory-form-status" class="status-panel"></div>
        </section>
        <section class="two-column reveal compact-top">
          <article class="panel"><div class="panel-head"><div><p class="eyebrow">库存快照</p><h3>SKU 收发存</h3></div></div><div id="inventory-snapshot-table" class="table-stack"></div></article>
          <article class="panel"><div class="panel-head"><div><p class="eyebrow">库存流水</p><h3>最近录入记录</h3></div></div><div id="inventory-movement-table" class="table-stack"></div></article>
        </section>
        <section class="panel reveal compact-top">
          <div class="panel-head"><div><p class="eyebrow">审计留痕</p><h3>最近手工调整</h3></div></div>
          <div id="inventory-adjustment-table" class="table-stack"></div>
        </section>
      </section>
"""
UPLOAD_GOVERNANCE_HTML = """
        <section class="two-column reveal compact-top">
          <article class="panel"><div class="panel-head"><div><p class="eyebrow">批次治理</p><h3>最近上传批次</h3></div></div><div id="upload-batches" class="table-stack"></div></article>
          <article class="panel"><div class="panel-head"><div><p class="eyebrow">规则版本</p><h3>当前已启用规则</h3></div></div><div id="upload-rule-versions" class="table-stack"></div></article>
        </section>
"""
RUNTIME_APP_JS = """
(() => {
  state.inventory = state.inventory || null;
  state.profit = state.profit || null;
  const inventoryTypeLabels = {
    inbound: "入库",
    outbound: "出库",
    transfer: "调拨",
    return: "退回",
    adjust: "调整"
  };

  const baseRenderOverview = renderOverview;
  renderOverview = function renderOverviewWithInventory() {
    baseRenderOverview();
    const inventory = state.dashboard?.inventory_summary || {};
    const metricsGrid = $("#metrics-grid");
    if (!metricsGrid || !Object.keys(inventory).length) return;
    $("#overview-inventory-metric")?.remove();
    metricsGrid.insertAdjacentHTML(
      "beforeend",
      `<article id="overview-inventory-metric" class="metric-card"><span class="metric-label">库存结存</span><strong class="metric-value">${dec(inventory.closing_qty || 0)}</strong><small class="metric-meta">${inventory.ready ? "库存已就绪" : `负库存 ${intf(inventory.negative_sku_count || 0)}`}</small></article>`
    );
  };

  renderProfit = function renderProfitWithLazyData() {
    const payload = state.profit;
    if (!payload) {
      $("#sku-summary").innerHTML = '<div class="empty-state">进入“利润分析”后加载利润明细。</div>';
      $("#sku-table").innerHTML = '<div class="empty-state">利润明细尚未加载</div>';
      $("#order-state-summary").innerHTML = '<div class="empty-state">进入“订单追踪”后加载订单状态明细。</div>';
      $("#order-table").innerHTML = '<div class="empty-state">订单追踪尚未加载</div>';
      if (!$("#order-query-result")?.innerHTML.trim()) {
        $("#order-query-result").innerHTML = '<div class="empty-state">输入订单号后可查看该订单的完整财务明细。</div>';
      }
      return;
    }

    const settlementStates = ["all", ...new Set((payload.order_details || []).map((row) => row.settlement_state || "unknown"))];
    $("#state-filter").innerHTML = settlementStates.map((item) => `<option value="${esc(item)}">${item === "all" ? "全部状态" : esc(settlementState(item))}</option>`).join("");

    const skuKeyword = ($("#sku-search")?.value || "").trim().toLowerCase();
    const skuRows = (payload.sku_details || []).filter((row) => !skuKeyword || String(row.sku || "").toLowerCase().includes(skuKeyword));
    $("#sku-summary").innerHTML = [
      ["SKU 数", intf(skuRows.length)],
      ["销量", dec(skuRows.reduce((sum, row) => sum + Number(row.qty_sold || 0), 0))],
      ["净销售", money(skuRows.reduce((sum, row) => sum + Number(row.net_sales || 0), 0))],
      ["毛利", money(skuRows.reduce((sum, row) => sum + Number(row.gross_profit || 0), 0))]
    ].map((item) => `<article class="compare-card"><span>${esc(item[0])}</span><strong>${item[1]}</strong></article>`).join("");
    table("#sku-table", [
      { label: "SKU", key: "sku" },
      { label: "销量", render: (r) => dec(r.qty_sold), cls: "number-cell" },
      { label: "净销售", render: (r) => money(r.net_sales), cls: "number-cell" },
      { label: "广告费", render: (r) => money(r.ad_spend), cls: "number-cell" },
      { label: "毛利", render: (r) => money(r.gross_profit), cls: "number-cell" },
      { label: "毛利率", render: (r) => pct(r.margin_pct), cls: "number-cell" },
      { label: "ACOS", render: (r) => pct(r.acos_pct), cls: "number-cell" }
    ], skuRows, "没有匹配到 SKU 数据");

    const orderKeyword = ($("#order-search")?.value || "").trim().toLowerCase();
    const stateFilter = $("#state-filter")?.value || "all";
    const orderRows = (payload.order_details || []).filter((row) => {
      const matchesKeyword = !orderKeyword || String(row.amazon_order_id || "").toLowerCase().includes(orderKeyword) || String(row.sku || "").toLowerCase().includes(orderKeyword);
      const matchesState = stateFilter === "all" || String(row.settlement_state || "") === stateFilter;
      return matchesKeyword && matchesState;
    });
    const groups = orderRows.reduce((acc, row) => {
      const key = row.settlement_state || "unknown";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    $("#order-state-summary").innerHTML = Object.entries(groups).map(([key, value]) => `<article class="compare-card"><span>${esc(settlementState(key))}</span><strong>${intf(value)}</strong></article>`).join("") || '<div class="empty-state">当前筛选条件下没有订单</div>';
    table("#order-table", [
      { label: "订单号", key: "amazon_order_id" },
      { label: "日期", render: (r) => esc(String(r.purchase_date || "").slice(0, 10)) },
      { label: "SKU", key: "sku" },
      { label: "订单状态", key: "order_status" },
      { label: "结算状态", render: (r) => `<span class="${chipClass(r.settlement_state)}">${esc(settlementState(r.settlement_state))}</span>` },
      { label: "订单金额", render: (r) => money(r.item_price), cls: "number-cell" },
      { label: "促销折扣", render: (r) => money(r.item_promotion_discount), cls: "number-cell" },
      { label: "结算销售额", render: (r) => money(r.settled_product_sales), cls: "number-cell" },
      { label: "订单净额", render: (r) => money(r.settled_order_net), cls: "number-cell" }
    ], orderRows, "没有匹配到订单数据");
    if (!$("#order-query-result")?.innerHTML.trim()) {
      $("#order-query-result").innerHTML = '<div class="empty-state">输入订单号后可查看该订单的完整财务明细。</div>';
    }
  };

  function renderInventory() {
    const payload = state.inventory;
    if (!payload) return;
    const summary = payload.summary || {};
    const issues = payload.issues || [];
    const currentClass = summary.ready ? "status-ok" : (issues.some((item) => item.severity === "blocker") ? "status-blocker" : "status-warning");
    $("#inventory-title").textContent = `${payload.selected_month} 库存核对中心`;
    $("#inventory-description").textContent = payload.note || "通过库存快照与库存流水确认账期是否具备关账条件。";
    $("#inventory-current").innerHTML = `<div class="close-stat ${currentClass}"><p>库存状态</p><strong>${summary.ready ? "已就绪" : "待处理"}</strong><span>SKU ${intf(summary.snapshot_count || 0)}</span><span>流水 ${intf(summary.movement_count || 0)}</span></div>`;
    $("#inventory-summary").innerHTML = [
      ["期初库存", dec(summary.opening_qty || 0)],
      ["本期入库", dec(summary.inbound_qty || 0)],
      ["本期出库", dec(summary.outbound_qty || 0)],
      ["期末库存", dec(summary.closing_qty || 0)],
      ["负库存 SKU", intf(summary.negative_sku_count || 0)]
    ].map((item) => `<article class="compare-card"><span>${esc(item[0])}</span><strong>${item[1]}</strong></article>`).join("");
    $("#inventory-issues").innerHTML = issues.length
      ? issues.map((item) => `<div class="status-panel ${item.severity === "blocker" ? "status-blocker" : "status-warning"}"><strong>${esc(item.issue_code || item.severity)}</strong><div class="table-muted">${esc(item.note || "-")}</div></div>`).join("")
      : '<div class="empty-state">当前账期没有库存异常</div>';
    table("#inventory-snapshot-table", [
      { label: "SKU", key: "sku" },
      { label: "期初", render: (r) => dec(r.opening_qty), cls: "number-cell" },
      { label: "入库", render: (r) => dec(r.inbound_qty), cls: "number-cell" },
      { label: "出库", render: (r) => dec(r.outbound_qty), cls: "number-cell" },
      { label: "调拨", render: (r) => dec(r.transfer_qty), cls: "number-cell" },
      { label: "退回", render: (r) => dec(r.return_qty), cls: "number-cell" },
      { label: "调整", render: (r) => dec(r.adjust_qty), cls: "number-cell" },
      { label: "期末", render: (r) => dec(r.closing_qty), cls: "number-cell" }
    ], payload.snapshots || [], "当前账期还没有库存快照");
    table("#inventory-movement-table", [
      { label: "日期", render: (r) => esc(String(r.movement_date || r.created_at || "").slice(0, 10)) },
      { label: "类型", render: (r) => esc(inventoryTypeLabels[r.movement_type] || r.movement_type || "-") },
      { label: "SKU", key: "sku" },
      { label: "数量", render: (r) => dec(r.quantity), cls: "number-cell" },
      { label: "单件成本", render: (r) => money(r.unit_cost), cls: "number-cell" },
      { label: "金额", render: (r) => money(r.amount_total), cls: "number-cell" },
      { label: "来源引用", key: "source_ref" }
    ], payload.movements || [], "当前账期还没有库存流水");
    table("#inventory-adjustment-table", [
      { label: "调整类型", key: "adjustment_type" },
      { label: "对象", key: "target_key" },
      { label: "备注", key: "notes" },
      { label: "操作人", key: "adjusted_by" },
      { label: "时间", render: (r) => esc(dt(r.adjusted_at)) }
    ], payload.adjustments || [], "当前账期还没有手工调整记录");
    if (!$("#inventory-movement-date")?.value) {
      $("#inventory-movement-date").value = `${payload.selected_month}-01`;
    }
  }

  function getInventoryPayload() {
    return {
      period_month: monthSelect?.value,
      movement_date: ($("#inventory-movement-date")?.value || "").trim(),
      movement_type: $("#inventory-movement-type")?.value || "inbound",
      sku: ($("#inventory-sku")?.value || "").trim(),
      quantity: ($("#inventory-quantity")?.value || "").trim(),
      unit_cost: ($("#inventory-unit-cost")?.value || "").trim(),
      amount_total: ($("#inventory-amount-total")?.value || "").trim(),
      source_ref: ($("#inventory-source-ref")?.value || "").trim(),
      note: ($("#inventory-note")?.value || "").trim()
    };
  }

  async function saveInventoryMovement() {
    const payload = getInventoryPayload();
    if (!payload.period_month) throw new Error("当前没有可用账期。");
    await jpost("/api/inventory/save", payload);
    setStatus("#inventory-form-status", "库存流水已保存。", "ok");
    const monthQuery = `?${new URLSearchParams({ month: payload.period_month }).toString()}`;
    state.inventory = await jget(`/api/inventory${monthQuery}`);
    state.monthClose = await jget(`/api/month-close${monthQuery}`);
    state.dashboard = await jget(`/api/dashboard${monthQuery}`);
    renderInventory();
    renderMonthClose();
    renderOverview();
    renderProfit();
  }

  function generatedDecisionLabel(row) {
    const decision = row.override_user_choice || "";
    if (decision === "normal" || decision === "normal_timing_difference" || decision === "expected_timing_difference") return "正常差异";
    if (decision === "needs_action") return "待处理异常";
    if (decision) return "已人工处理";
    return "未判断";
  }

  function buildGeneratedDecisionPayload(row, decision) {
    const isNormal = decision === "normal";
    return {
      exception_case_id: row.override_case_id || null,
      period_month: row.period_month,
      exception_code: row.exception_code,
      exception_type: "generated_issue_override",
      source_platform: row.source_platform || "amazon",
      source_store: row.source_store || "",
      source_table: row.source_table || "",
      source_ref: row.source_ref || "",
      order_id: row.order_id || "",
      sku: row.sku || "",
      amount_value: row.amount_value || "",
      system_suggestion: row.system_suggestion || "",
      user_choice: isNormal ? "normal_timing_difference" : "needs_action",
      case_status: isNormal ? "resolved" : "open",
      note: isNormal ? "人工确认：正常时间差，不阻断月结。" : "人工恢复为待处理异常，继续阻断月结。"
    };
  }

  async function refreshExceptionViews(month) {
    const monthQuery = `?${new URLSearchParams({ month }).toString()}`;
    state.exceptions = await jget(`/api/exceptions${monthQuery}`);
    state.monthClose = await jget(`/api/month-close${monthQuery}`);
    state.dashboard = await jget(`/api/dashboard${monthQuery}`);
    renderExceptions();
    renderMonthClose();
    renderOverview();
    renderProfit();
  }

  function getPendingGeneratedCases(payload) {
    return (payload?.generated_cases || []).filter((row) => !row.override_is_normal);
  }

  function buildGeneratedGroups(payload) {
    const groups = new Map();
    getPendingGeneratedCases(payload).forEach((row) => {
      const key = row.exception_code || "unknown";
      if (!groups.has(key)) {
        groups.set(key, {
          exception_code: key,
          total_count: 0,
          blocker_count: 0,
          warning_count: 0,
          total_amount: 0,
          months: new Set(),
          rows: []
        });
      }
      const group = groups.get(key);
      group.total_count += 1;
      group.blocker_count += row.origin === "blocker" ? 1 : 0;
      group.warning_count += row.origin === "warning" ? 1 : 0;
      group.total_amount += Number(row.amount_value || 0);
      group.months.add(row.period_month || "");
      group.rows.push(row);
    });
    return Array.from(groups.values())
      .map((group) => ({
        ...group,
        months: Array.from(group.months).filter(Boolean).sort().reverse()
      }))
      .sort((a, b) => (b.blocker_count - a.blocker_count) || (b.total_count - a.total_count) || a.exception_code.localeCompare(b.exception_code));
  }

  function ensureGeneratedGroupDetailPanel() {
    const host = $("#generated-exception-table");
    if (!host) return null;
    let panel = $("#generated-exception-detail-panel");
    if (!panel) {
      panel = document.createElement("div");
      panel.id = "generated-exception-detail-panel";
      panel.className = "panel inset";
      panel.style.marginTop = "12px";
      panel.innerHTML = `
        <div class="panel-head align-end">
          <div>
            <p class="eyebrow">异常明细</p>
            <h3 id="generated-exception-detail-title">请选择异常类型</h3>
            <p id="generated-exception-detail-subtitle" class="panel-note">上方仅保留待处理异常；认定为正常差异后会从待办列表移除。</p>
          </div>
          <div class="exception-detail-toolbar">
            <label class="control compact-control"><span>账期筛选</span><select id="generated-exception-detail-month-filter"></select></label>
            <div id="generated-exception-detail-bulk-action" class="erp-action-bar">
              <label class="control compact-control"><span>类别处理</span><select id="generated-exception-detail-action-select"><option value="normal">认定正常差异</option><option value="abnormal">保持待处理</option></select></label>
              <button id="generated-exception-detail-action-save" class="ghost-button primary-button" type="button">提交处理</button>
            </div>
            <div id="generated-exception-detail-summary" class="status-panel status-ok">请选择异常类型查看明细</div>
          </div>
        </div>
        <div id="generated-exception-detail-hint" class="status-panel"></div>
        <div id="generated-exception-detail-table" class="table-stack"></div>
      `;
      host.insertAdjacentElement("afterend", panel);
    }
    return panel;
  }

  function ensureExceptionHistoryPanel() {
    const exceptionsPanel = document.querySelector('[data-tab-panel="exceptions"]');
    if (!exceptionsPanel) return;
    const topSection = exceptionsPanel.querySelector(".two-column");
    const generatedArticle = $("#generated-exception-table")?.closest("article");
    const manualArticle = $("#manual-exception-table")?.closest("article");
    const formSection = $("#exception-form-status")?.closest("section");
    if (topSection) {
      topSection.style.gridTemplateColumns = "1fr";
    }
    if (!manualArticle || !formSection) return;
    let historySection = $("#exception-history-section");
    if (!historySection) {
      historySection = document.createElement("section");
      historySection.id = "exception-history-section";
      historySection.className = "reveal compact-top";
      formSection.insertAdjacentElement("afterend", historySection);
    }
    if (!historySection.contains(manualArticle)) {
      historySection.appendChild(manualArticle);
    }
    if (generatedArticle && topSection && !topSection.contains(generatedArticle)) {
      topSection.appendChild(generatedArticle);
    }
  }

  function renderGeneratedDetailSummary(rows) {
    const totalAmount = rows.reduce((sum, row) => sum + Number(row.amount_value || 0), 0);
    const blockerCount = rows.filter((row) => row.origin === "blocker").length;
    const warningCount = rows.filter((row) => row.origin === "warning").length;
    const summary = $("#generated-exception-detail-summary");
    if (!summary) return;
    const status = blockerCount ? "status-blocker" : (warningCount ? "status-warning" : "status-ok");
    summary.className = `status-panel ${status}`;
    summary.innerHTML = `待处理 ${intf(rows.length)} 条，合计金额 ${money(totalAmount)}，Blocker ${intf(blockerCount)}，Warning ${intf(warningCount)}`;
  }

  function renderGeneratedDetailHint(exceptionCode) {
    const hint = $("#generated-exception-detail-hint");
    const subtitle = $("#generated-exception-detail-subtitle");
    const bulkAction = $("#generated-exception-detail-bulk-action");
    if (!hint) return;
    if (exceptionCode === "pending_removal_control") {
      hint.className = "status-panel status-warning";
      hint.innerHTML = "移除费异常需要逐条填写处理结论：先判断 `removal_category`，再选择 `accounting_treatment` 是入费用还是入成本，保存后该条会从待处理异常中移除。";
      if (subtitle) subtitle.textContent = "当前分类需要逐条处理，不支持按类别一键认定。";
      if (bulkAction) bulkAction.style.display = "none";
      return;
    }
    hint.className = "status-panel status-ok";
    hint.innerHTML = "按账期筛选后，可对当前异常类型执行统一处理；提交为正常差异后，会从待处理列表中消失。";
    if (subtitle) subtitle.textContent = "明细区只展示当前分类下仍待处理的异常记录，适合逐类核对。";
    if (bulkAction) bulkAction.style.display = "";
  }

  function renderRemovalDecisionCell(row) {
    const suggestedCategory = row.system_suggestion || "transfer";
    return `
      <div class="exception-ops-inline">
        <label class="control compact-control">
          <span>Removal 类别</span>
          <select data-removal-category="true">
            <option value="transfer"${suggestedCategory === "transfer" ? " selected" : ""}>transfer</option>
            <option value="disposal"${suggestedCategory === "disposal" ? " selected" : ""}>disposal</option>
          </select>
        </label>
        <label class="control compact-control">
          <span>会计处理</span>
          <select data-accounting-treatment="true">
            <option value="" selected>请选择</option>
            <option value="expense">expense</option>
            <option value="capitalize">capitalize</option>
          </select>
        </label>
        <button class="ghost-button primary-button" type="button" data-removal-save="true" data-period-month="${esc(row.period_month || "")}" data-order-id="${esc(row.order_id || "")}" data-sku="${esc(row.sku || "")}">保存处理</button>
        <span class="table-muted">建议类别：${esc(suggestedCategory)}</span>
      </div>
    `;
  }

  function clearGeneratedExceptionGroupPanel(message = "当前没有待处理异常") {
    const title = $("#generated-exception-detail-title");
    const subtitle = $("#generated-exception-detail-subtitle");
    const summary = $("#generated-exception-detail-summary");
    const hint = $("#generated-exception-detail-hint");
    const tableHost = $("#generated-exception-detail-table");
    const monthFilter = $("#generated-exception-detail-month-filter");
    const bulkAction = $("#generated-exception-detail-bulk-action");
    if (title) title.textContent = "当前没有待处理异常";
    if (subtitle) subtitle.textContent = "已认定为正常差异的记录会进入下方人工记录，不再出现在待办区。";
    if (summary) {
      summary.className = "status-panel status-ok";
      summary.innerHTML = message;
    }
    if (hint) {
      hint.className = "status-panel status-ok";
      hint.innerHTML = "请从上方汇总选择异常类型，或等待新的异常生成。";
    }
    if (tableHost) {
      tableHost.innerHTML = '<div class="empty-state">当前没有待处理明细</div>';
    }
    if (monthFilter) monthFilter.innerHTML = '<option value="all">全部账期</option>';
    if (bulkAction) bulkAction.style.display = "none";
  }

  function showGeneratedExceptionGroup(exceptionCode) {
    const payload = state.exceptions;
    if (!payload) return;
    if (exceptionCode && exceptionCode !== state.exceptionGroupCode) {
      state.exceptionGroupMonthFilter = "all";
    }
    state.exceptionGroupCode = exceptionCode;
    const allRows = getPendingGeneratedCases(payload).filter((row) => row.exception_code === exceptionCode);
    if (!allRows.length) {
      clearGeneratedExceptionGroupPanel();
      return;
    }
    const monthFilter = $("#generated-exception-detail-month-filter");
    const months = Array.from(new Set(allRows.map((row) => row.period_month).filter(Boolean))).sort().reverse();
    if (monthFilter) {
      const desired = state.exceptionGroupMonthFilter || "all";
      monthFilter.innerHTML = ['<option value="all">全部账期</option>'].concat(months.map((month) => `<option value="${esc(month)}">${esc(month)}</option>`)).join("");
      monthFilter.value = months.includes(desired) ? desired : "all";
      state.exceptionGroupMonthFilter = monthFilter.value;
    }
    const rows = allRows.filter((row) => !state.exceptionGroupMonthFilter || state.exceptionGroupMonthFilter === "all" || row.period_month === state.exceptionGroupMonthFilter);
    const title = $("#generated-exception-detail-title");
    if (title) title.textContent = `${exceptionCode} 待处理明细（${intf(rows.length)} 条）`;
    renderGeneratedDetailSummary(rows);
    renderGeneratedDetailHint(exceptionCode);
    const actionSelect = $("#generated-exception-detail-action-select");
    if (actionSelect) actionSelect.value = "normal";
    const columns = [
      { label: "账期", key: "period_month" },
      { label: "严重度", render: (r) => `<span class="${r.origin === "blocker" ? "chip alert" : "chip warning"}">${esc(r.origin || "-")}</span>` },
      { label: "订单号/引用", render: (r) => `<div class="table-muted">${esc(r.order_id || r.source_ref || "-")}</div>` },
      { label: "SKU/值", render: (r) => `<div class="table-muted">${esc(r.sku || "-")}</div>` },
      { label: "金额", render: (r) => money(r.amount_value), cls: "number-cell" },
      { label: "系统建议", key: "system_suggestion" },
      { label: "备注", key: "note" }
    ];
    if (exceptionCode === "pending_removal_control") {
      columns.splice(5, 0, { label: "处理录入", render: (r) => renderRemovalDecisionCell(r) });
    }
    table("#generated-exception-detail-table", columns, rows, "当前分类下没有明细");
    const wrap = document.querySelector("#generated-exception-detail-table .preview-table-wrap");
    if (wrap) {
      wrap.style.maxHeight = "420px";
      wrap.style.overflowY = "auto";
      wrap.style.overflowX = "auto";
    }
  }

  function wireDynamicTabButton(button) {
    if (!button || button.dataset.runtimeTabBound === "true") return;
    button.addEventListener("click", async () => {
      try {
        const tab = button.dataset.tab || "overview";
        setTab(tab);
        await loadTabData(tab);
      } catch (err) {
        alert(err.message);
      }
    });
    button.dataset.runtimeTabBound = "true";
  }

  function ensureRuntimeWorkbenchStyles() {
    if (document.getElementById("runtime-exception-workbench-style")) return;
    const style = document.createElement("style");
    style.id = "runtime-exception-workbench-style";
    style.textContent = `
      .primary-button {
        background: linear-gradient(135deg,#18334d 0%,#112336 100%);
        color: #fff8ef;
        border-color: rgba(17,35,54,.92);
        box-shadow: 0 10px 20px rgba(23,50,77,.12);
      }
      .primary-button:hover {
        border-color: rgba(17,35,54,.92);
      }
      .erp-action-bar {
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
      }
      .exception-detail-toolbar {
        display: grid;
        gap: 10px;
        justify-items: end;
      }
      .exception-detail-toolbar .status-panel {
        min-width: min(100%, 440px);
      }
      .exception-ops-card {
        display: grid;
        gap: 8px;
        min-width: 320px;
      }
      .exception-ops-inline {
        display: grid;
        grid-template-columns: minmax(120px, 1fr) minmax(120px, 1fr) auto auto;
        gap: 8px;
        align-items: end;
        min-width: 560px;
      }
      .exception-ops-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 8px;
      }
      .exception-ops-card .table-muted {
        white-space: normal;
      }
      @media (max-width: 980px) {
        .exception-ops-grid {
          grid-template-columns: 1fr;
        }
        .exception-ops-inline {
          grid-template-columns: 1fr;
          min-width: 0;
        }
      }
    `;
    document.head.appendChild(style);
  }

  function ensureTrackingWorkspace() {
    const tabbar = document.querySelector(".tabbar");
    if (tabbar && !document.querySelector('.tab-button[data-tab="tracking"]')) {
      document.querySelector('.tab-button[data-tab="overview"]')?.insertAdjacentHTML(
        "afterend",
        '<button class="tab-button" data-tab="tracking" type="button">订单追踪</button>'
      );
    }
    const trackingButton = document.querySelector('.tab-button[data-tab="tracking"]');
    wireDynamicTabButton(trackingButton);
    let trackingPanel = document.querySelector('[data-tab-panel="tracking"]');
    if (!trackingPanel) {
      trackingPanel = document.createElement("section");
      trackingPanel.className = "tab-panel";
      trackingPanel.dataset.tabPanel = "tracking";
      document.querySelector('[data-tab-panel="overview"]')?.insertAdjacentElement("afterend", trackingPanel);
    }
    const orderTrackingSection = $("#order-state-summary")?.closest("section");
    const orderLookupSection = $("#order-query-result")?.closest("section");
    if (orderTrackingSection && !trackingPanel.contains(orderTrackingSection)) {
      trackingPanel.appendChild(orderTrackingSection);
    }
    if (orderLookupSection && !trackingPanel.contains(orderLookupSection)) {
      trackingPanel.appendChild(orderLookupSection);
    }
  }

  function ensureDatabaseWorkspace() {
    const tabbar = document.querySelector(".tabbar");
    if (tabbar && !document.querySelector('.tab-button[data-tab="database"]')) {
      document.querySelector('.tab-button[data-tab="downloads"]')?.insertAdjacentHTML(
        "afterend",
        '<button class="tab-button" data-tab="database" type="button">数据库</button>'
      );
    }
    const databaseButton = document.querySelector('.tab-button[data-tab="database"]');
    wireDynamicTabButton(databaseButton);
    let panel = document.querySelector('[data-tab-panel="database"]');
    if (!panel) {
      panel = document.createElement("section");
      panel.className = "tab-panel";
      panel.dataset.tabPanel = "database";
      panel.innerHTML = `
        <section class="panel reveal compact-top">
          <div class="panel-head">
            <div><p class="eyebrow">数据库取数</p><h3>最细颗粒度数据</h3></div>
            <p class="panel-note">按整月、订单号或关键词直接预览/下载数据库最细明细，支持利润口径和应收口径。</p>
          </div>
          <div class="download-grid">
            <label class="control"><span>最细明细口径</span><select id="database-basis-select"><option value="pnl">利润口径</option><option value="receivable">应收口径</option></select></label>
            <label class="control"><span>SKU / 产品关键词</span><input id="database-keyword" type="search"></label>
            <label class="control"><span>订单号</span><input id="database-order-id" type="search" placeholder="例如 111-4908959-8585056"></label>
          </div>
          <div class="panel-actions">
            <button id="preview-database-detail-button" class="ghost-button" type="button">预览最细明细</button>
            <button id="download-database-detail-button" class="ghost-button" type="button">下载最细明细</button>
          </div>
          <div id="database-status" class="status-panel"></div>
          <div id="database-preview-meta" class="status-panel"></div>
          <div id="database-preview-table" class="table-stack"></div>
        </section>
      `;
      document.querySelector('[data-tab-panel="downloads"]')?.insertAdjacentElement("afterend", panel);
    }
  }

  async function setGeneratedIssueDecision(caseKey, decision) {
    const payload = state.exceptions;
    const row = (payload?.generated_cases || []).find((item) => item.case_key === caseKey);
    if (!row) throw new Error("未找到对应异常行。");
    const isNormal = decision === "normal";
    await jpost("/api/exception/save", buildGeneratedDecisionPayload(row, decision));
    const month = monthSelect?.value || row.period_month;
    await refreshExceptionViews(month);
    setStatus("#exception-form-status", isNormal ? "已标记为正常差异，不再作为 blocker。" : "已恢复为待处理异常。", isNormal ? "ok" : "warning");
  }

  async function setGeneratedGroupDecision(exceptionCode, decision) {
    const payload = state.exceptions;
    const rows = (payload?.generated_cases || []).filter((item) => item.exception_code === exceptionCode);
    if (!rows.length) throw new Error("当前分类下没有可提交的异常。");
    const isNormal = decision === "normal";
    for (const row of rows) {
      await jpost("/api/exception/save", buildGeneratedDecisionPayload(row, decision));
    }
    const month = monthSelect?.value || rows[0].period_month;
    await refreshExceptionViews(month);
    setStatus("#exception-form-status", isNormal ? `已将 ${exceptionCode} 全部标记为正常差异。` : `已将 ${exceptionCode} 全部恢复为待处理异常。`, isNormal ? "ok" : "warning");
  }

  async function saveRemovalDecision(button) {
    const container = button.closest("td");
    const removalCategory = container?.querySelector("[data-removal-category]")?.value || "";
    const accountingTreatment = container?.querySelector("[data-accounting-treatment]")?.value || "";
    const periodMonth = button.dataset.periodMonth || "";
    const orderId = button.dataset.orderId || "";
    const sku = button.dataset.sku || "";
    if (!removalCategory || !accountingTreatment) {
      throw new Error("请先选择 removal_category 和 accounting_treatment。");
    }
    button.disabled = true;
    const previousText = button.textContent || "保存处理";
    button.textContent = "保存中...";
    setStatus("#exception-form-status", `正在保存 ${orderId || sku || "当前记录"}，并刷新待处理异常...`, "warning");
    try {
      await jpost("/api/removal-controls/save", {
        rows: [
          {
            period_month: periodMonth,
            order_id: orderId,
            sku: sku,
            removal_category: removalCategory,
            accounting_treatment: accountingTreatment,
            source_note: "frontend_exception_detail"
          }
        ]
      });
      if (state.exceptions?.generated_cases) {
        state.exceptions.generated_cases = state.exceptions.generated_cases.filter((row) => !(
          row.period_month === periodMonth
          && (
            (
              row.exception_code === "pending_removal_control"
              && (row.order_id || "") === orderId
              && (row.sku || "") === sku
            )
            || (
              row.exception_code === "removal_fee_control_missing"
              && (row.order_id || "") === orderId
              && (row.sku || "") === sku
            )
          )
        ));
        renderExceptions();
      }
      const month = monthSelect?.value || periodMonth || "";
      state.exceptions = await jget(`/api/exceptions?${new URLSearchParams({ month }).toString()}`);
      renderExceptions();
      setStatus("#exception-form-status", "移除费处理结果已保存，这条待处理异常已从当前列表移除。月结检查正在后台刷新。", "ok");
      setTimeout(() => {
        refreshExceptionViews(month).catch((err) => setStatus("#exception-form-status", err.message, "blocker"));
      }, 1200);
    } finally {
      button.textContent = previousText;
      button.disabled = false;
    }
  }

  function bindGeneratedDecisionControls() {
    document.querySelectorAll("[data-generated-decision-save]").forEach((button) => {
      if (button.dataset.boundGeneratedDecision === "true") return;
      button.addEventListener("click", () => {
        const select = button.closest("tr")?.querySelector("[data-generated-decision-select]");
        setGeneratedIssueDecision(decodeURIComponent(button.dataset.caseKey || ""), select?.value || "abnormal")
          .catch((err) => setStatus("#exception-form-status", err.message, "blocker"));
      });
      button.dataset.boundGeneratedDecision = "true";
    });
    document.querySelectorAll("[data-generated-group-detail]").forEach((button) => {
      if (button.dataset.boundGeneratedGroupDetail === "true") return;
      button.addEventListener("click", () => showGeneratedExceptionGroup(button.dataset.exceptionCode || ""));
      button.dataset.boundGeneratedGroupDetail = "true";
    });
    const bulkSaveButton = $("#generated-exception-detail-action-save");
    if (bulkSaveButton && bulkSaveButton.dataset.boundGeneratedGroupSave !== "true") {
      bulkSaveButton.addEventListener("click", () => {
        if (!state.exceptionGroupCode) {
          setStatus("#exception-form-status", "请先从上方选择一个异常类型。", "warning");
          return;
        }
        const select = $("#generated-exception-detail-action-select");
        setGeneratedGroupDecision(state.exceptionGroupCode, select?.value || "normal")
          .catch((err) => setStatus("#exception-form-status", err.message, "blocker"));
      });
      bulkSaveButton.dataset.boundGeneratedGroupSave = "true";
    }
    document.querySelectorAll("[data-removal-save]").forEach((button) => {
      if (button.dataset.boundRemovalSave === "true") return;
      button.addEventListener("click", () => {
        saveRemovalDecision(button).catch((err) => setStatus("#exception-form-status", err.message, "blocker"));
      });
      button.dataset.boundRemovalSave = "true";
    });
    const monthFilter = $("#generated-exception-detail-month-filter");
    if (monthFilter && monthFilter.dataset.boundGeneratedMonthFilter !== "true") {
      monthFilter.addEventListener("change", () => {
        state.exceptionGroupMonthFilter = monthFilter.value || "all";
        if (state.exceptionGroupCode) showGeneratedExceptionGroup(state.exceptionGroupCode);
      });
      monthFilter.dataset.boundGeneratedMonthFilter = "true";
    }
  }

  function syncBasisControls(source = "top") {
    const topSelect = $("#granular-basis-select");
    const databaseSelect = $("#database-basis-select");
    if (!topSelect || !databaseSelect) return;
    if (source === "database") {
      topSelect.value = databaseSelect.value || "pnl";
    } else {
      databaseSelect.value = topSelect.value || "pnl";
    }
  }

  function wireDatabaseBasisControl() {
    syncBasisControls();
    const topSelect = $("#granular-basis-select");
    const databaseSelect = $("#database-basis-select");
    if (topSelect && !topSelect.dataset.databaseBasisWired) {
      topSelect.addEventListener("change", () => syncBasisControls("top"));
      topSelect.dataset.databaseBasisWired = "true";
    }
    if (databaseSelect && !databaseSelect.dataset.databaseBasisWired) {
      databaseSelect.addEventListener("change", () => syncBasisControls("database"));
      databaseSelect.dataset.databaseBasisWired = "true";
    }
  }

  function databaseDetailParams() {
    return {
      keyword: ($("#database-keyword")?.value || "").trim(),
      order_id: ($("#database-order-id")?.value || $("#order-query-input")?.value || "").trim(),
      basis: $("#database-basis-select")?.value || basis()
    };
  }

  function renderDatabaseDefaults() {
    setStatus("#database-status", "数据库页提供最细颗粒度预览与下载，不受汇总报表布局影响。", "ok");
    $("#database-preview-meta").innerHTML = '<div class="table-muted">选择条件后可以在这里预览数据库最细明细。</div>';
    $("#database-preview-table").innerHTML = '<div class="empty-state">还没有数据库预览内容</div>';
  }

  function renderDatabasePreview(payload) {
    const rows = payload.rows || [];
    const columns = payload.columns || [];
    $("#database-preview-meta").innerHTML = `<div class="table-muted">导出范围：${esc(JSON.stringify(payload.scope || {}))}</div><div class="table-muted">总行数 ${intf(payload.total_rows || 0)}，当前预览 ${intf(payload.preview_limit || rows.length)} 行</div>`;
    if (!rows.length || !columns.length) {
      $("#database-preview-table").innerHTML = '<div class="empty-state">当前条件下没有可预览的数据</div>';
      return;
    }
    $("#database-preview-table").innerHTML = `<div class="preview-table-wrap"><table class="preview-table"><thead><tr>${columns.map((col) => `<th>${esc(col)}</th>`).join("")}</tr></thead><tbody>${rows.map((row) => `<tr>${columns.map((col) => `<td>${esc(row[col] ?? "-")}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
  }

  async function previewDatabaseDetails() {
    const params = databaseDetailParams();
    const payload = await preview("order_line_profit", params);
    renderDatabasePreview(payload);
    const modeText = params.order_id ? "按订单明细" : (params.keyword ? "按关键词筛选明细" : "整月最细明细");
    setStatus("#database-status", `${modeText}预览已更新，当前口径：${params.basis === "pnl" ? "利润口径" : "应收口径"}。`, "ok");
  }

  function downloadDatabaseDetails() {
    const params = databaseDetailParams();
    const modeText = params.order_id ? "订单明细" : (params.keyword ? "筛选明细" : "整月最细明细");
    downloadFile(exportUrl("order_line_profit", params))
      .then(() => setStatus("#database-status", `${modeText}下载已开始，当前口径：${params.basis === "pnl" ? "利润口径" : "应收口径"}。`, "ok"))
      .catch((err) => setStatus("#database-status", err.message, "blocker"));
  }

  function bindDatabaseControls() {
    wireDatabaseBasisControl();
    const previewButton = $("#preview-database-detail-button");
    if (previewButton && previewButton.dataset.databaseBound !== "true") {
      previewButton.addEventListener("click", () => previewDatabaseDetails().catch((err) => setStatus("#database-status", err.message, "blocker")));
      previewButton.dataset.databaseBound = "true";
    }
    const downloadButton = $("#download-database-detail-button");
    if (downloadButton && downloadButton.dataset.databaseBound !== "true") {
      downloadButton.addEventListener("click", () => downloadDatabaseDetails());
      downloadButton.dataset.databaseBound = "true";
    }
  }

  function pruneDownloadPanel() {
    $("#preview-order-detail-button")?.closest("button")?.style.setProperty("display", "none");
    $("#download-order-detail-button")?.closest("button")?.style.setProperty("display", "none");
    const orderIdControl = $("#download-order-id")?.closest(".control");
    if (orderIdControl) orderIdControl.style.display = "none";
    const previewMeta = $("#download-preview-meta");
    if (previewMeta && !$("#download-database-hint")) {
      previewMeta.insertAdjacentHTML(
        "beforebegin",
        '<div id="download-database-hint" class="status-panel status-ok">最细颗粒度数据已迁移到“数据库”页签；当前页保留汇总表与分摊审计下载。</div>'
      );
    }
  }

  function enhanceExceptionScrollArea() {
    const wrap = document.querySelector("#generated-exception-table .preview-table-wrap");
    if (!wrap) return;
    wrap.style.maxHeight = "560px";
    wrap.style.overflowY = "auto";
    wrap.style.overflowX = "auto";
  }

  const baseRenderUploads = renderUploads;
  renderUploads = function renderUploadsWithGovernance() {
    baseRenderUploads();
    const uploads = state.uploads || {};
    table("#upload-batches", [
      { label: "批次类型", key: "batch_type" },
      { label: "目标账期", key: "target_month" },
      { label: "源文件", key: "source_filename" },
      { label: "上传人", key: "uploaded_by" },
      { label: "上传时间", render: (r) => esc(dt(r.uploaded_at)) }
    ], uploads.recent_batches || [], "暂无上传批次记录");
    table("#upload-rule-versions", [
      { label: "规则域", key: "rule_scope" },
      { label: "版本", key: "version_name" },
      { label: "启用时间", render: (r) => esc(dt(r.applied_at)) },
      { label: "说明", key: "notes" }
    ], uploads.rule_versions || [], "暂无规则版本记录");
  };

  const baseLoadTabData = loadTabData;
  loadTabData = async function loadTabDataWithInventory(tab, force = false) {
    const month = monthSelect?.value || state.dashboard?.selected_month;
    const monthQuery = month ? `?${new URLSearchParams({ month }).toString()}` : "";
    if ((tab === "profit" || tab === "tracking") && (force || !state.profit || state.profit.selected_month !== month)) {
      state.profit = await jget(`/api/profit${monthQuery}`);
      renderProfit();
      return;
    }
    if (tab === "tracking" || tab === "profit") {
      renderProfit();
      return;
    }
    if (tab === "database") {
      renderDatabaseDefaults();
      return;
    }
    if (tab === "inventory" && (force || !state.inventory)) {
      state.inventory = await jget(`/api/inventory${monthQuery}`);
      renderInventory();
      return;
    }
    return baseLoadTabData(tab, force);
  };

  const baseResetSecondaryState = resetSecondaryState;
  resetSecondaryState = function resetSecondaryStateWithInventory() {
    baseResetSecondaryState();
    state.inventory = null;
    state.profit = null;
  };

  const baseRenderExceptions = renderExceptions;
  renderExceptions = function renderExceptionsWithDecision() {
    baseRenderExceptions();
    ensureExceptionHistoryPanel();
    const payload = state.exceptions;
    if (!payload) return;
    const groups = buildGeneratedGroups(payload);
    ensureGeneratedGroupDetailPanel();
    table("#generated-exception-table", [
      { label: "异常类型", key: "exception_code" },
      { label: "异常条数", render: (r) => `<strong>${intf(r.total_count || 0)}</strong>` },
      { label: "涉及账期", render: (r) => esc((r.months || []).join(" / ") || "-") },
      { label: "合计金额", render: (r) => money(r.total_amount), cls: "number-cell" },
      { label: "严重度", render: (r) => `<span class="${r.blocker_count ? "chip alert" : "chip warning"}">${esc(`Blocker ${r.blocker_count || 0} / Warning ${r.warning_count || 0}`)}</span>` },
      { label: "处理入口", render: (r) => `<div class="erp-action-bar"><button class="ghost-button primary-button" type="button" data-generated-group-detail="true" data-exception-code="${esc(r.exception_code || "")}">进入处理</button></div>` }
    ], groups, "当前未关账期间没有待处理系统异常");
    table("#manual-exception-table", [
      { label: "账期", key: "period_month" },
      { label: "ID", key: "exception_case_id" },
      { label: "异常编码", key: "exception_code" },
      { label: "异常类型", key: "exception_type" },
      { label: "订单号", key: "order_id" },
      { label: "SKU", key: "sku" },
      { label: "金额", render: (r) => money(r.amount_value), cls: "number-cell" },
      { label: "工单状态", key: "case_status" },
      { label: "审批状态", key: "approval_status" },
      { label: "备注", key: "note" }
    ], payload.manual_cases || [], "当前未关账期间还没有人工工单");
    bindGeneratedDecisionControls();
    enhanceExceptionScrollArea();
    if (state.exceptionGroupCode && !groups.some((group) => group.exception_code === state.exceptionGroupCode)) {
      state.exceptionGroupCode = "";
      state.exceptionGroupMonthFilter = "all";
    }
    if (!state.exceptionGroupCode && groups.length) {
      state.exceptionGroupCode = groups[0].exception_code;
    }
    if (state.exceptionGroupCode) {
      showGeneratedExceptionGroup(state.exceptionGroupCode);
    } else {
      clearGeneratedExceptionGroupPanel("当前没有待处理异常，提交后的正常差异会保留在下方人工记录中。");
    }
  };

  const baseRenderDownloadDefaults = renderDownloadDefaults;
  renderDownloadDefaults = function renderDownloadDefaultsWithGranular() {
    baseRenderDownloadDefaults();
    pruneDownloadPanel();
    renderDatabaseDefaults();
  };

  $("#save-inventory-movement-button")?.addEventListener("click", () => {
    saveInventoryMovement().catch((err) => setStatus("#inventory-form-status", err.message, "blocker"));
  });

  ensureRuntimeWorkbenchStyles();
  ensureTrackingWorkspace();
  ensureDatabaseWorkspace();
  bindDatabaseControls();
  pruneDownloadPanel();
  renderDatabaseDefaults();
  if (state.dashboard) renderOverview();
  if (state.uploads) renderUploads();
  if (params.get("tab") === "tracking" || params.get("tab") === "database") {
    setTab(params.get("tab"));
    loadTabData(params.get("tab"), true).catch((err) => alert(err.message));
  }
  if (state.activeTab === "inventory") {
    loadTabData("inventory", true).catch((err) => alert(err.message));
  }
})();
"""


def query_all(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def query_one(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> dict:
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else {}


def round_money(value: float | int | None) -> float:
    if value is None:
        return 0.0
    return round(float(value), 2)


def build_exception_case_key(
    period_month: str | None,
    exception_code: str | None,
    source_table: str | None,
    source_ref: str | None,
    order_id: str | None,
    sku: str | None,
) -> str:
    parts = [
        (period_month or "").strip(),
        (exception_code or "").strip().lower(),
        (source_table or "").strip().lower(),
        (source_ref or "").strip(),
        (order_id or "").strip(),
        (sku or "").strip(),
    ]
    return "|".join(parts)


def is_normal_override(row: dict | None) -> bool:
    if not row:
        return False
    return str(row.get("case_status", "")).strip().lower() == "resolved" and str(row.get("user_choice", "")).strip().lower() in NORMAL_OVERRIDE_CHOICES


def load_exception_override_map(conn: sqlite3.Connection, months: str | list[str]) -> dict[str, dict]:
    month_list = [months] if isinstance(months, str) else [item for item in months if item]
    if not month_list:
        return {}
    placeholders = ", ".join("?" for _ in month_list)
    rows = query_all(
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
        tuple(month_list),
    )
    override_map: dict[str, dict] = {}
    for row in rows:
        key = build_exception_case_key(
            row.get("period_month"),
            row.get("exception_code"),
            row.get("source_table"),
            row.get("source_ref"),
            row.get("order_id"),
            row.get("sku"),
        )
        override_map.setdefault(key, row)
    return override_map


def get_latest_month_close_state_map(conn: sqlite3.Connection) -> dict[str, str]:
    rows = query_all(
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
    return {row["period_month"]: row["state_code"] for row in rows}


def get_unclosed_issue_months(conn: sqlite3.Connection, anchor_month: str | None = None) -> list[str]:
    state_map = get_latest_month_close_state_map(conn)
    months = get_months(conn)
    month_scope = anchor_month if anchor_month in months else (months[0] if months else None)
    issue_month_rows = query_all(
        conn,
        """
        SELECT DISTINCT period_month
        FROM monthly_close_issue_detail
        ORDER BY period_month DESC
        """,
    )
    issue_months = {row["period_month"] for row in issue_month_rows}
    if month_scope:
        issue_months.add(month_scope)
    open_months = [
        month
        for month in sorted(issue_months, reverse=True)
        if (not month_scope or month <= month_scope) and state_map.get(month) != "closed"
    ]
    return open_months


def query_current_month_close_issues(
    conn: sqlite3.Connection,
    month: str,
    select_sql: str,
    order_by: str | None = None,
) -> list[dict]:
    where_sql = "period_month = ?"
    params: list[object] = [month]
    sql = f"""
        SELECT {select_sql}
        FROM monthly_close_issue_detail
        WHERE {where_sql}
    """
    if order_by:
        sql += f"\nORDER BY {order_by}"
    return query_all(
        conn,
        sql,
        tuple(params),
    )


def query_open_period_month_close_issues(
    conn: sqlite3.Connection,
    anchor_month: str | None,
    select_sql: str,
    order_by: str | None = None,
) -> list[dict]:
    open_months = get_unclosed_issue_months(conn, anchor_month)
    if not open_months:
        return []
    placeholders = ", ".join("?" for _ in open_months)
    sql = f"""
        SELECT {select_sql}
        FROM monthly_close_issue_detail
        WHERE period_month IN ({placeholders})
    """
    if order_by:
        sql += f"\nORDER BY {order_by}"
    return query_all(conn, sql, tuple(open_months))


def get_effective_issue_counts(conn: sqlite3.Connection, month: str) -> dict:
    override_map = load_exception_override_map(conn, month)
    issue_rows = query_current_month_close_issues(
        conn,
        month,
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
        key = build_exception_case_key(
            month,
            row.get("issue_code"),
            row.get("source_table"),
            row.get("source_ref"),
            row.get("issue_key"),
            row.get("issue_value"),
        )
        if is_normal_override(override_map.get(key)):
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


def normalize_detail_basis(value: str | None) -> str:
    basis = (value or "pnl").strip().lower()
    if basis not in {"pnl", "receivable"}:
        raise ValueError("basis must be pnl or receivable")
    return basis


def ensure_month_download_allowed(conn: sqlite3.Connection, month: str | None, dataset: str) -> None:
    blocked_datasets = {"order_line_profit", "order_type_rollup", "allocation_audit", "sku_details", "order_details"}
    if not month or dataset not in blocked_datasets:
        return
    _, pending = get_pending_removal_controls(conn, month)
    if pending:
        raise ValueError(f"{month} ?? {len(pending)} ? removal ???????????????????")
    state_code = get_latest_month_close_state(conn, month) or derive_recommended_close_state(conn, month)
    if state_code in {"mapping_pending", "exception_pending", "receivable_pending", "waiting_upload", "processing"}:
        raise ValueError(f"{month} ??????? {state_code}?????????????")


def project_order_line_rows(rows: list[dict], basis: str) -> list[dict]:
    normalized_basis = normalize_detail_basis(basis)
    common_columns = [
        "order_line_id",
        "detail_source",
        "settlement_line_id",
        "period_month",
        "amazon_order_id",
        "purchase_date",
        "order_status",
        "settlement_state",
        "sales_channel",
        "fulfillment_channel",
        "sku",
        "product_name_cn",
        "asin",
        "order_type",
        "ordered_quantity",
        "ordered_item_price",
        "ordered_shipping_price",
        "ordered_item_promotion_discount",
        "ordered_ship_promotion_discount",
        "settled_quantity",
        "product_sales",
        "shipping_credits",
        "gift_wrap_credits",
        "promotional_rebates",
        "net_sales",
        "selling_fees",
        "fba_fees",
        "other_transaction_fees",
        "marketplace_withheld_tax",
        "settlement_net_total",
        "compensation_income_amount",
        "transfer_quantity",
        "disposal_quantity",
        "test_order_quantity",
        "vine_quantity",
    ]
    pnl_columns = [
        "allocated_storage_fees",
        "allocated_removal_fees",
        "allocated_ad_spend",
        "direct_compensation_income",
        "allocated_test_order_cost",
        "allocated_vine_fee",
        "allocated_subscription_fee",
        "allocated_coupon_participation_fee",
        "allocated_coupon_performance_fee",
        "product_unit_cost",
        "inbound_freight_unit_cost",
        "allocated_product_cost",
        "allocated_inbound_freight_cost",
        "estimated_gross_profit",
    ]
    receivable_columns = [
        "receivable_storage_fees",
        "receivable_removal_fees",
        "receivable_ad_spend",
        "receivable_compensation_income",
        "receivable_subscription_fee",
        "receivable_coupon_participation_fee",
        "receivable_coupon_performance_fee",
        "receivable_vine_fee",
        "inventory_capitalized_cost",
        "receivable_net",
    ]
    columns = common_columns + (pnl_columns if normalized_basis == "pnl" else receivable_columns)
    projected_rows: list[dict] = []
    for row in rows:
        projected_rows.append({column: row.get(column) for column in columns})
    return projected_rows


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def parse_close_notes(raw_notes: str | None) -> dict:
    if not raw_notes:
        return {"blockers": [], "warnings": []}
    try:
        data = json.loads(raw_notes)
    except json.JSONDecodeError:
        return {"blockers": [], "warnings": [raw_notes]}
    return {
        "blockers": data.get("blockers", []),
        "warnings": data.get("warnings", []),
    }


def ensure_manual_templates() -> None:
    MANUAL_DIR.mkdir(parents=True, exist_ok=True)
    ATTACHMENT_DIR.mkdir(parents=True, exist_ok=True)
    for config in MANUAL_FILE_CONFIG.values():
        path = MANUAL_DIR / config["filename"]
        if path.exists():
            continue
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(config["headers"])


def ensure_rule_version(conn: sqlite3.Connection) -> None:
    existing = conn.execute(
        """
        SELECT 1
        FROM rule_version
        WHERE rule_scope = ?
          AND version_name = ?
        LIMIT 1
        """,
        (RULE_VERSION_SCOPE, RULE_VERSION_NAME),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO rule_version (
            rule_scope, version_name, applied_at, notes
        ) VALUES (?, ?, ?, ?)
        """,
        (RULE_VERSION_SCOPE, RULE_VERSION_NAME, now_iso(), RULE_VERSION_NOTES),
    )


def infer_target_month(value: str | None) -> str | None:
    match = re.search(r"(20\d{2})[-_]?([01]\d)", value or "")
    if not match:
        return None
    month = match.group(2)
    if month < "01" or month > "12":
        return None
    return f"{match.group(1)}-{month}"


def register_upload_batch(
    conn: sqlite3.Connection,
    batch_type: str,
    source_filename: str,
    uploaded_by: str = "frontend",
    notes: str | None = None,
) -> None:
    batch_key = f"{batch_type}:{datetime.now().strftime('%Y%m%d%H%M%S%f')}:{Path(source_filename).name}"
    conn.execute(
        """
        INSERT INTO upload_batch (
            batch_key, batch_type, target_month, source_filename, uploaded_by, uploaded_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (batch_key, batch_type, infer_target_month(source_filename), Path(source_filename).name, uploaded_by, now_iso(), notes),
    )


def log_manual_adjustment(
    conn: sqlite3.Connection,
    target_table: str,
    target_key: str,
    adjustment_type: str,
    adjustment_payload: dict | list | str,
    adjusted_by: str = "frontend",
    notes: str | None = None,
) -> None:
    payload_text = (
        adjustment_payload
        if isinstance(adjustment_payload, str)
        else json.dumps(adjustment_payload, ensure_ascii=False, sort_keys=True)
    )
    conn.execute(
        """
        INSERT INTO manual_adjustment_log (
            target_table, target_key, adjustment_type, adjustment_payload, adjusted_by, adjusted_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (target_table, target_key, adjustment_type, payload_text, adjusted_by, now_iso(), notes),
    )


def render_index_html() -> str:
    html = (WEB_DIR / "index.html").read_text(encoding="utf-8")
    if 'data-tab="inventory"' not in html:
        html = html.replace(
            '      <button class="tab-button" data-tab="receivables" type="button">应收核对</button>\n'
            '      <button class="tab-button" data-tab="exceptions" type="button">异常工作台</button>\n',
            '      <button class="tab-button" data-tab="receivables" type="button">应收核对</button>\n'
            f"{INVENTORY_TAB_HTML}"
            '      <button class="tab-button" data-tab="exceptions" type="button">异常工作台</button>\n',
        )
    if 'data-tab-panel="inventory"' not in html:
        html = html.replace(
            '      <section class="tab-panel" data-tab-panel="exceptions">',
            f"{INVENTORY_PANEL_HTML}\n      <section class=\"tab-panel\" data-tab-panel=\"exceptions\">",
        )
    if 'id="upload-batches"' not in html:
        html = html.replace(
            '        <section class="panel reveal compact-top"><div class="panel-head"><div><p class="eyebrow">手工模板</p><h3>手工维护文件</h3></div></div><div id="upload-manual-files" class="table-stack"></div></section>\n'
            '      </section>',
            '        <section class="panel reveal compact-top"><div class="panel-head"><div><p class="eyebrow">手工模板</p><h3>手工维护文件</h3></div></div><div id="upload-manual-files" class="table-stack"></div></section>\n'
            f"{UPLOAD_GOVERNANCE_HTML}"
            '      </section>',
        )
    if '/runtime-app.js' not in html:
        html = html.replace('</body>', '  <script src="/runtime-app.js"></script>\n</body>')
    return html


def ensure_runtime_schema() -> None:
    global RUNTIME_SCHEMA_READY
    if RUNTIME_SCHEMA_READY:
        return
    etl_dir = str(ROOT / "etl")
    inserted_path = False
    if etl_dir not in sys.path:
        sys.path.insert(0, etl_dir)
        inserted_path = True
    try:
        namespace = runpy.run_path(str(ROOT / "etl" / "schema.py"))
        schema_sql = namespace.get("SCHEMA_SQL")
        if not schema_sql:
            raise RuntimeError("SCHEMA_SQL not found during runtime schema setup.")
        conn = sqlite3.connect(DB_PATH, timeout=30)
        try:
            conn.executescript(schema_sql)
            ensure_rule_version(conn)
            conn.commit()
        finally:
            conn.close()
        RUNTIME_SCHEMA_READY = True
    finally:
        if inserted_path:
            try:
                sys.path.remove(etl_dir)
            except ValueError:
                pass


def get_latest_month_close_state(conn: sqlite3.Connection, month: str | None) -> str | None:
    if not month:
        return None
    row = conn.execute(
        """
        SELECT state_code
        FROM month_close_state_log
        WHERE period_month = ?
        ORDER BY state_log_id DESC
        LIMIT 1
        """,
        (month,),
    ).fetchone()
    return row[0] if row else None


def refresh_receivable_snapshot(conn: sqlite3.Connection, month: str) -> dict:
    previous = conn.execute(
        """
        SELECT closing_receivable
        FROM fact_platform_receivable_snapshot
        WHERE period_month < ?
          AND platform_code = 'amazon'
          AND store_code = ''
        ORDER BY period_month DESC
        LIMIT 1
        """,
        (month,),
    ).fetchone()
    opening_receivable = float(previous[0] or 0) if previous else 0.0

    current = query_one(
        conn,
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
        (month,),
    )
    current_receivable = float(current.get("receivable_amount") or 0)
    current_receipts = float(
        conn.execute(
            """
            SELECT COALESCE(SUM(receipt_amount), 0)
            FROM fact_platform_receipts
            WHERE period_month = ?
              AND platform_code = 'amazon'
              AND store_code = ''
            """,
            (month,),
        ).fetchone()[0]
        or 0
    )
    expected_total = opening_receivable + current_receivable
    closing_receivable = expected_total - current_receipts
    unmatched_receipts = max(0.0, current_receipts - expected_total)
    receivable_gap = 0.0 if abs(closing_receivable) <= 0.01 else closing_receivable
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
            month,
            opening_receivable,
            current_receivable,
            current_receipts,
            closing_receivable,
            unmatched_receipts,
            receivable_gap,
            reconciliation_status,
            now_iso(),
            notes,
        ),
    )
    return {
        'period_month': month,
        'platform_code': 'amazon',
        'store_code': '',
        'opening_receivable': round_money(opening_receivable),
        'current_receivable': round_money(current_receivable),
        'current_receipts': round_money(current_receipts),
        'closing_receivable': round_money(closing_receivable),
        'unmatched_receipts': round_money(unmatched_receipts),
        'receivable_gap': round_money(receivable_gap),
        'reconciliation_status': reconciliation_status,
    }


def get_receivable_snapshot(conn: sqlite3.Connection, month: str, refresh_if_missing: bool = False) -> dict | None:
    snapshot = query_one(
        conn,
        """
        SELECT
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
            generated_at
        FROM fact_platform_receivable_snapshot
        WHERE period_month = ?
          AND platform_code = 'amazon'
          AND store_code = ''
        ORDER BY snapshot_id DESC
        LIMIT 1
        """,
        (month,),
    )
    if snapshot:
        for key in ('opening_receivable', 'current_receivable', 'current_receipts', 'closing_receivable', 'unmatched_receipts', 'receivable_gap'):
            snapshot[key] = round_money(snapshot.get(key))
        return snapshot
    if refresh_if_missing:
        return refresh_receivable_snapshot(conn, month)
    return None


def ensure_receivable_snapshots(conn: sqlite3.Connection, months: list[str]) -> None:
    existing_rows = query_all(
        conn,
        """
        SELECT DISTINCT period_month
        FROM fact_platform_receivable_snapshot
        WHERE platform_code = 'amazon'
          AND store_code = ''
        """,
    )
    existing_months = {row['period_month'] for row in existing_rows}
    missing_months = sorted(month for month in months if month not in existing_months)
    for period_month in missing_months:
        refresh_receivable_snapshot(conn, period_month)
    if missing_months:
        conn.commit()


def get_inventory_periods(conn: sqlite3.Connection) -> list[str]:
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


def refresh_inventory_snapshot(conn: sqlite3.Connection, month: str) -> list[dict]:
    opening_rows = query_all(
        conn,
        """
        SELECT current_snapshot.sku, current_snapshot.closing_qty
        FROM fact_inventory_snapshot current_snapshot
        JOIN (
            SELECT sku, MAX(period_month) AS previous_month
            FROM fact_inventory_snapshot
            WHERE period_month < ?
            GROUP BY sku
        ) previous_snapshot
          ON current_snapshot.sku = previous_snapshot.sku
         AND current_snapshot.period_month = previous_snapshot.previous_month
        """,
        (month,),
    )
    opening_map = {row["sku"]: float(row["closing_qty"] or 0) for row in opening_rows}

    movement_rows = query_all(
        conn,
        """
        SELECT
            sku,
            COALESCE(SUM(CASE WHEN movement_type = 'inbound' THEN quantity ELSE 0 END), 0) AS inbound_qty,
            COALESCE(SUM(CASE WHEN movement_type = 'outbound' THEN quantity ELSE 0 END), 0) AS outbound_qty,
            COALESCE(SUM(CASE WHEN movement_type = 'transfer' THEN quantity ELSE 0 END), 0) AS transfer_qty,
            COALESCE(SUM(CASE WHEN movement_type = 'return' THEN quantity ELSE 0 END), 0) AS return_qty,
            COALESCE(SUM(CASE WHEN movement_type = 'adjust' THEN quantity ELSE 0 END), 0) AS adjust_qty
        FROM fact_inventory_movements
        WHERE period_month = ?
          AND COALESCE(trim(sku), '') <> ''
        GROUP BY sku
        """,
        (month,),
    )
    movement_map = {row["sku"]: row for row in movement_rows}

    conn.execute("DELETE FROM fact_inventory_snapshot WHERE period_month = ?", (month,))
    generated_rows: list[dict] = []
    generated_at = now_iso()
    for sku in sorted(set(opening_map) | set(movement_map)):
        opening_qty = float(opening_map.get(sku, 0) or 0)
        movement = movement_map.get(sku, {})
        inbound_qty = float(movement.get("inbound_qty") or 0)
        outbound_qty = float(movement.get("outbound_qty") or 0)
        transfer_qty = float(movement.get("transfer_qty") or 0)
        return_qty = float(movement.get("return_qty") or 0)
        adjust_qty = float(movement.get("adjust_qty") or 0)
        closing_qty = opening_qty + inbound_qty - outbound_qty + transfer_qty + return_qty + adjust_qty
        if not any(abs(value) > 0.000001 for value in (opening_qty, inbound_qty, outbound_qty, transfer_qty, return_qty, adjust_qty, closing_qty)):
            continue
        conn.execute(
            """
            INSERT INTO fact_inventory_snapshot (
                period_month,
                sku,
                opening_qty,
                inbound_qty,
                outbound_qty,
                transfer_qty,
                return_qty,
                adjust_qty,
                closing_qty,
                generated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                month,
                sku,
                opening_qty,
                inbound_qty,
                outbound_qty,
                transfer_qty,
                return_qty,
                adjust_qty,
                closing_qty,
                generated_at,
            ),
        )
        generated_rows.append(
            {
                "sku": sku,
                "opening_qty": round_money(opening_qty),
                "inbound_qty": round_money(inbound_qty),
                "outbound_qty": round_money(outbound_qty),
                "transfer_qty": round_money(transfer_qty),
                "return_qty": round_money(return_qty),
                "adjust_qty": round_money(adjust_qty),
                "closing_qty": round_money(closing_qty),
            }
        )
    return generated_rows


def ensure_inventory_snapshots(conn: sqlite3.Connection, months: list[str] | None = None) -> None:
    target_months = months or get_inventory_periods(conn)
    for period_month in sorted({month for month in target_months if month}):
        refresh_inventory_snapshot(conn, period_month)


def build_inventory_status(conn: sqlite3.Connection, month: str, refresh: bool = False) -> dict:
    if refresh:
        snapshot_exists = query_one(
            conn,
            """
            SELECT COUNT(*) AS total
            FROM fact_inventory_snapshot
            WHERE period_month = ?
            """,
            (month,),
        ).get("total", 0)
        if int(snapshot_exists or 0) == 0:
            periods = get_inventory_periods(conn)
            if month not in periods:
                periods.append(month)
            ensure_inventory_snapshots(conn, periods)
            conn.commit()

    summary = query_one(
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
    movement_count = int(
        query_one(
            conn,
            """
            SELECT COUNT(*) AS total
            FROM fact_inventory_movements
            WHERE period_month = ?
            """,
            (month,),
        ).get("total", 0)
    )
    snapshot_rows = query_all(
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
    movement_rows = query_all(
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
    adjustment_rows = query_all(
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

    issues: list[dict] = []
    snapshot_count = int(summary.get("snapshot_count") or 0)
    negative_sku_count = int(summary.get("negative_sku_count") or 0)
    if snapshot_count == 0:
        issues.append(
            {
                "severity": "warning",
                "issue_code": "inventory_snapshot_missing",
                "note": "当前账期还没有库存快照，请先录入库存流水或导入库存数据。",
            }
        )
    if negative_sku_count > 0:
        issues.append(
            {
                "severity": "blocker",
                "issue_code": "negative_inventory_balance",
                "note": f"{negative_sku_count} 个 SKU 的期末库存为负，月结不能提交审批。",
            }
        )
    if snapshot_count > 0 and movement_count == 0:
        issues.append(
            {
                "severity": "warning",
                "issue_code": "inventory_carry_forward_only",
                "note": "当前账期库存只有结转快照，没有新增库存流水。请确认是否为零变动月份。",
            }
        )

    ready = snapshot_count > 0 and negative_sku_count == 0
    note = (
        "库存核对已完成，可进入利润校验阶段。"
        if ready
        else "库存核对未完成，请先录入库存流水并处理负库存。"
    )
    return {
        "summary": {
            "period_month": month,
            "snapshot_count": snapshot_count,
            "movement_count": movement_count,
            "opening_qty": round_money(summary.get("opening_qty")),
            "inbound_qty": round_money(summary.get("inbound_qty")),
            "outbound_qty": round_money(summary.get("outbound_qty")),
            "transfer_qty": round_money(summary.get("transfer_qty")),
            "return_qty": round_money(summary.get("return_qty")),
            "adjust_qty": round_money(summary.get("adjust_qty")),
            "closing_qty": round_money(summary.get("closing_qty")),
            "negative_sku_count": negative_sku_count,
            "ready": ready,
        },
        "issues": issues,
        "snapshots": snapshot_rows,
        "movements": movement_rows,
        "adjustments": adjustment_rows,
        "note": note,
    }


def build_inventory_payload(month: str | None = None) -> dict:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    months = sorted(set(get_months(conn)) | set(get_inventory_periods(conn)), reverse=True)
    selected_month = month if month in months else (months[0] if months else None)
    if not selected_month:
        conn.close()
        raise RuntimeError("No months available for inventory reconciliation.")
    payload = build_inventory_status(conn, selected_month, refresh=True)
    conn.close()
    return {
        "selected_month": selected_month,
        "available_months": months,
        **payload,
    }


def save_inventory_movement(payload: dict) -> dict:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        period_month = str(payload.get("period_month", "")).strip()
        movement_type = str(payload.get("movement_type", "")).strip().lower()
        movement_date = str(payload.get("movement_date", "")).strip() or None
        sku = str(payload.get("sku", "")).strip()
        source_ref = str(payload.get("source_ref", "")).strip() or None
        note = str(payload.get("note", "")).strip() or None
        if not period_month:
            raise ValueError("period_month is required.")
        if movement_type not in INVENTORY_MOVEMENT_TYPES:
            raise ValueError(f"movement_type must be one of: {', '.join(sorted(INVENTORY_MOVEMENT_TYPES))}.")
        if not sku:
            raise ValueError("sku is required.")

        quantity_raw = payload.get("quantity")
        if quantity_raw in (None, ""):
            raise ValueError("quantity is required.")
        quantity = float(quantity_raw)
        if abs(quantity) <= 0.000001:
            raise ValueError("quantity must be non-zero.")
        if movement_type in {"inbound", "outbound"} and quantity < 0:
            raise ValueError(f"{movement_type} quantity must be positive.")

        unit_cost_raw = payload.get("unit_cost")
        unit_cost = float(unit_cost_raw) if unit_cost_raw not in (None, "") else None
        amount_total_raw = payload.get("amount_total")
        amount_total = float(amount_total_raw) if amount_total_raw not in (None, "") else None
        if amount_total is None and unit_cost is not None:
            amount_total = quantity * unit_cost

        cursor = conn.execute(
            """
            INSERT INTO fact_inventory_movements (
                period_month,
                source_file,
                movement_date,
                movement_type,
                sku,
                quantity,
                unit_cost,
                amount_total,
                source_ref,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (period_month, "manual_entry", movement_date, movement_type, sku, quantity, unit_cost, amount_total, source_ref, now_iso()),
        )
        movement_id = int(cursor.lastrowid)
        log_manual_adjustment(
            conn,
            "fact_inventory_movements",
            f"{period_month}:{movement_id}:{sku}",
            "insert",
            {
                "period_month": period_month,
                "movement_type": movement_type,
                "movement_date": movement_date,
                "sku": sku,
                "quantity": quantity,
                "unit_cost": unit_cost,
                "amount_total": amount_total,
                "source_ref": source_ref,
            },
            notes=note,
        )

        periods = [item for item in get_inventory_periods(conn) if item >= period_month]
        if period_month not in periods:
            periods.append(period_month)
        ensure_inventory_snapshots(conn, periods)
        conn.commit()
        inventory_status = build_inventory_status(conn, period_month, refresh=False)
        return {
            "movement_id": movement_id,
            "period_month": period_month,
            "inventory": inventory_status,
        }
    finally:
        conn.close()


def derive_recommended_close_state(conn: sqlite3.Connection, month: str | None, inventory_ready: bool | None = None) -> str:
    if not month:
        return 'waiting_upload'
    if MONTHLY_JOB.get('status') == 'running' and MONTHLY_JOB.get('target_month') == month:
        return 'processing'
    months = get_months(conn)
    if month not in months:
        return 'waiting_upload'
    if query_one(conn, "SELECT COUNT(*) AS total FROM pending_mapping_queue WHERE status = 'pending'").get('total', 0):
        return 'mapping_pending'
    close_log = query_one(
        conn,
        """
        SELECT close_status, blocker_count, warning_count, receivable_gap
        FROM monthly_close_log
        WHERE period_month = ?
        ORDER BY close_id DESC
        LIMIT 1
        """,
        (month,),
    )
    if not close_log:
        return 'processing'
    effective_issues = get_effective_issue_counts(conn, month)
    if int(effective_issues.get('blocker_count') or 0) > 0:
        return 'exception_pending'
    snapshot = get_receivable_snapshot(conn, month, refresh_if_missing=True)
    if abs(float(snapshot.get('receivable_gap') or 0)) > 0.01:
        return 'receivable_pending'
    if int(effective_issues.get('warning_count') or 0) > 0:
        return 'exception_pending'
    if inventory_ready is None:
        inventory_ready = bool(build_inventory_status(conn, month, refresh=True).get("summary", {}).get("ready"))
    return 'pnl_pending' if inventory_ready else 'inventory_pending'


def record_month_close_state(conn: sqlite3.Connection, month: str, state_code: str, note: str, actor: str = 'system', source: str = 'api') -> None:
    current = get_latest_month_close_state(conn, month)
    if current == state_code:
        return
    conn.execute(
        """
        INSERT INTO month_close_state_log (
            period_month, state_code, state_source, state_note, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (month, state_code, source, note, actor, now_iso()),
    )


def record_month_close_action(
    conn: sqlite3.Connection,
    month: str,
    action_code: str,
    from_state: str | None,
    to_state: str | None,
    result: str,
    note: str,
    actor: str = 'system',
) -> None:
    conn.execute(
        """
        INSERT INTO month_close_action_log (
            period_month, action_code, from_state, to_state, action_result, action_note, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (month, action_code, from_state, to_state, result, note, actor, now_iso()),
    )


def build_receivables_payload(month: str | None = None) -> dict:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    months = get_months(conn)
    selected_month = month if month in months else (months[0] if months else None)
    if not selected_month:
        conn.close()
        raise RuntimeError('No months available for receivables.')

    ensure_receivable_snapshots(conn, months)

    balances = query_all(
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
    for row in balances:
        for key in ('opening_receivable', 'current_receivable', 'current_receipts', 'closing_receivable', 'unmatched_receipts', 'receivable_gap'):
            row[key] = round_money(row.get(key))

    summary = next((row for row in balances if row['period_month'] == selected_month), None) or get_receivable_snapshot(conn, selected_month, refresh_if_missing=True)
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
    unmatched_receipt_rows = query_all(
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
        (selected_month,),
    )
    conn.close()
    return {
        'selected_month': selected_month,
        'available_months': months,
        'summary': summary,
        'balances': balances,
        'aging': aging_rows,
        'unsettled': unsettled_rows,
        'unmatched_receipts': unmatched_receipt_rows,
    }


def build_exceptions_payload(month: str | None = None) -> dict:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    months = get_months(conn)
    selected_month = month if month in months else (months[0] if months else None)
    if not selected_month:
        conn.close()
        raise RuntimeError('No months available for exceptions.')
    open_months = get_unclosed_issue_months(conn, selected_month)
    manual_months = open_months or [selected_month]
    placeholders = ", ".join("?" for _ in manual_months)

    manual_cases = query_all(
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
        tuple(manual_months),
    )

    override_map = load_exception_override_map(conn, manual_months)
    generated_cases = []
    issue_rows = query_open_period_month_close_issues(
        conn,
        selected_month,
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
        if row['issue_code'] == 'removal_fee_control_missing':
            continue
        case_key = build_exception_case_key(
            row['period_month'],
            row['issue_code'],
            row['source_table'],
            row['source_ref'],
            row['issue_key'],
            row['issue_value'],
        )
        override_case = override_map.get(case_key)
        generated_cases.append(
            {
                'case_key': case_key,
                'period_month': row['period_month'],
                'exception_code': row['issue_code'],
                'exception_type': 'generated_issue',
                'source_platform': 'amazon',
                'source_store': '',
                'source_table': row['source_table'],
                'source_ref': row['source_ref'],
                'order_id': row['issue_key'],
                'sku': row['issue_value'],
                'amount_value': row['metric_value'],
                'system_suggestion': row['note'],
                'user_choice': '',
                'case_status': 'open',
                'approval_status': 'not_required',
                'note': row['note'],
                'created_at': row['created_at'],
                'origin': row['severity'],
                'override_case_id': override_case.get('exception_case_id') if override_case else None,
                'override_user_choice': override_case.get('user_choice') if override_case else '',
                'override_case_status': override_case.get('case_status') if override_case else '',
                'override_is_normal': is_normal_override(override_case),
                'override_note': override_case.get('note') if override_case else '',
            }
        )

    for issue_month in manual_months:
        _, pending_removal = get_pending_removal_controls(conn, issue_month)
        for row in pending_removal:
            case_key = build_exception_case_key(
                issue_month,
                'pending_removal_control',
                'fact_removal_monthly_sku',
                row['order_id'],
                row['order_id'],
                row['sku'],
            )
            override_case = override_map.get(case_key)
            generated_cases.append(
                {
                    'case_key': case_key,
                    'period_month': issue_month,
                    'exception_code': 'pending_removal_control',
                    'exception_type': 'removal_control',
                    'source_platform': 'amazon',
                    'source_store': '',
                    'source_table': 'fact_removal_monthly_sku',
                    'source_ref': row['order_id'],
                    'order_id': row['order_id'],
                    'sku': row['sku'],
                    'amount_value': row['removal_fee'],
                    'system_suggestion': row['suggested_category'],
                    'user_choice': '',
                    'case_status': 'open',
                    'approval_status': 'not_required',
                    'note': row['product_name_cn'],
                    'created_at': now_iso(),
                    'origin': 'blocker',
                    'override_case_id': override_case.get('exception_case_id') if override_case else None,
                    'override_user_choice': override_case.get('user_choice') if override_case else '',
                    'override_case_status': override_case.get('case_status') if override_case else '',
                    'override_is_normal': is_normal_override(override_case),
                    'override_note': override_case.get('note') if override_case else '',
                }
            )

    attachment_placeholders = ", ".join("?" for _ in manual_months)
    attachments = query_all(
        conn,
        f"""
        SELECT exception_case_id, attachment_id, file_name, file_path, uploaded_at
        FROM exception_attachment
        WHERE exception_case_id IN (
            SELECT exception_case_id
            FROM manual_exception_case
            WHERE period_month IN ({attachment_placeholders})
        )
        ORDER BY attachment_id DESC
        """,
        tuple(manual_months),
    )
    attachment_map: dict[int, list[dict]] = {}
    for item in attachments:
        attachment_map.setdefault(int(item['exception_case_id']), []).append(item)
    for item in manual_cases:
        item['attachments'] = attachment_map.get(int(item['exception_case_id']), [])

    conn.close()
    return {
        'selected_month': selected_month,
        'available_months': months,
        'open_months': manual_months,
        'manual_cases': manual_cases,
        'generated_cases': generated_cases,
    }


def save_exception_case(payload: dict) -> dict:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        case_id = payload.get('exception_case_id')
        period_month = str(payload.get('period_month', '')).strip()
        exception_code = str(payload.get('exception_code', '')).strip() or 'manual_case'
        exception_type = str(payload.get('exception_type', '')).strip() or 'manual_review'
        source_platform = str(payload.get('source_platform', 'amazon')).strip() or 'amazon'
        order_id = str(payload.get('order_id', '')).strip() or None
        sku = str(payload.get('sku', '')).strip() or None
        amount_value = payload.get('amount_value')
        amount_value = float(amount_value) if amount_value not in (None, '') else None
        system_suggestion = str(payload.get('system_suggestion', '')).strip() or None
        user_choice = str(payload.get('user_choice', '')).strip() or None
        case_status = str(payload.get('case_status', 'open')).strip() or 'open'
        approval_status = str(payload.get('approval_status', 'not_required')).strip() or 'not_required'
        note = str(payload.get('note', '')).strip() or None
        if not period_month:
            raise ValueError('period_month is required.')

        if case_id:
            conn.execute(
                """
                UPDATE manual_exception_case
                SET exception_code = ?,
                    exception_type = ?,
                    source_platform = ?,
                    source_store = ?,
                    source_table = ?,
                    source_ref = ?,
                    order_id = ?,
                    sku = ?,
                    amount_value = ?,
                    system_suggestion = ?,
                    user_choice = ?,
                    case_status = ?,
                    approval_status = ?,
                    note = ?,
                    updated_at = ?,
                    resolved_at = CASE WHEN ? = 'resolved' THEN ? ELSE resolved_at END
                WHERE exception_case_id = ?
                """,
                (
                    exception_code,
                    exception_type,
                    source_platform,
                    str(payload.get('source_store', '')).strip() or None,
                    str(payload.get('source_table', '')).strip() or None,
                    str(payload.get('source_ref', '')).strip() or None,
                    order_id,
                    sku,
                    amount_value,
                    system_suggestion,
                    user_choice,
                    case_status,
                    approval_status,
                    note,
                    now_iso(),
                    case_status,
                    now_iso(),
                    case_id,
                ),
            )
            exception_case_id = int(case_id)
        else:
            cursor = conn.execute(
                """
                INSERT INTO manual_exception_case (
                    period_month, exception_code, exception_type, source_platform, source_store, source_table, source_ref,
                    order_id, sku, amount_value, system_suggestion, user_choice, case_status, approval_status,
                    note, created_by, created_at, updated_at, resolved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    period_month,
                    exception_code,
                    exception_type,
                    source_platform,
                    str(payload.get('source_store', '')).strip() or None,
                    str(payload.get('source_table', '')).strip() or None,
                    str(payload.get('source_ref', '')).strip() or None,
                    order_id,
                    sku,
                    amount_value,
                    system_suggestion,
                    user_choice,
                    case_status,
                    approval_status,
                    note,
                    'frontend',
                    now_iso(),
                    now_iso(),
                    now_iso() if case_status == 'resolved' else None,
                ),
            )
            exception_case_id = int(cursor.lastrowid)

        for attachment in payload.get('attachments', []):
            file_name = str(attachment.get('file_name', '')).strip()
            file_path = str(attachment.get('file_path', '')).strip()
            if not file_name or not file_path:
                continue
            conn.execute(
                """
                INSERT INTO exception_attachment (
                    exception_case_id, file_name, file_path, uploaded_at, uploaded_by
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (exception_case_id, file_name, file_path, now_iso(), 'frontend'),
            )

        approval_action = str(payload.get('approval_action', '')).strip()
        if approval_action:
            conn.execute(
                """
                INSERT INTO exception_approval_log (
                    exception_case_id, action_type, action_by, action_note, acted_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (exception_case_id, approval_action, 'frontend', note, now_iso()),
            )

        conn.commit()
        return {'exception_case_id': exception_case_id, 'period_month': period_month, 'case_status': case_status}
    finally:
        conn.close()


def build_month_close_payload(month: str | None = None) -> dict:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    months = get_months(conn)
    selected_month = month if month in months else (months[0] if months else None)
    if not selected_month:
        conn.close()
        raise RuntimeError('No months available for month close.')

    snapshot = get_receivable_snapshot(conn, selected_month, refresh_if_missing=True)
    inventory_status = build_inventory_status(conn, selected_month, refresh=True)
    inventory_ready = bool(inventory_status.get("summary", {}).get("ready"))
    recommended_state = derive_recommended_close_state(conn, selected_month, inventory_ready=inventory_ready)
    current_state = get_latest_month_close_state(conn, selected_month) or recommended_state
    effective_issues = get_effective_issue_counts(conn, selected_month)
    check_log = query_one(
        conn,
        """
        SELECT period_month, close_status, blocker_count, warning_count, pdf_amount, receivable_gap, notes, closed_at
        FROM monthly_close_log
        WHERE period_month = ?
        ORDER BY close_id DESC
        LIMIT 1
        """,
        (selected_month,),
    )
    check_log["raw_blocker_count"] = int(check_log.get("blocker_count") or 0)
    check_log["raw_warning_count"] = int(check_log.get("warning_count") or 0)
    check_log["blocker_count"] = int(effective_issues.get("blocker_count") or 0)
    check_log["warning_count"] = int(effective_issues.get("warning_count") or 0)
    check_log["overridden_issue_count"] = int(effective_issues.get("overridden_count") or 0)
    state_history = query_all(
        conn,
        """
        SELECT state_code, state_source, state_note, created_by, created_at
        FROM month_close_state_log
        WHERE period_month = ?
        ORDER BY state_log_id DESC
        """,
        (selected_month,),
    )
    action_history = query_all(
        conn,
        """
        SELECT action_code, from_state, to_state, action_result, action_note, created_by, created_at
        FROM month_close_action_log
        WHERE period_month = ?
        ORDER BY action_log_id DESC
        """,
        (selected_month,),
    )
    prerequisites = {
        'mapping_completed': query_one(conn, "SELECT COUNT(*) AS total FROM pending_mapping_queue WHERE status = 'pending'").get('total', 0) == 0,
        'issues_cleared': int(effective_issues.get('blocker_count') or 0) == 0 and int(effective_issues.get('warning_count') or 0) == 0,
        'receivable_balanced': abs(float(snapshot.get('receivable_gap') or 0)) <= 0.01,
        'inventory_ready': inventory_ready,
    }
    conn.close()
    return {
        'selected_month': selected_month,
        'available_months': months,
        'current_state': current_state,
        'recommended_state': recommended_state,
        'check_log': check_log,
        'receivable_snapshot': snapshot,
        'inventory_summary': inventory_status.get('summary', {}),
        'inventory_issues': inventory_status.get('issues', []),
        'prerequisites': prerequisites,
        'state_history': state_history,
        'action_history': action_history,
        'available_actions': ['start_close', 'submit_for_approval', 'approve_close', 'reopen_close'],
        'inventory_note': inventory_status.get('note'),
    }


def perform_month_close_action(month: str, action_code: str, note: str | None = None, actor: str = 'frontend') -> dict:
    ensure_runtime_schema()
    action = action_code.strip()
    if action not in {'start_close', 'submit_for_approval', 'approve_close', 'reopen_close'}:
        raise ValueError('Unsupported month close action.')

    if action == 'start_close':
        run_close_checks([month])

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    from_state: str | None = None
    to_state: str | None = None
    try:
        from_state = get_latest_month_close_state(conn, month) or derive_recommended_close_state(conn, month)
        to_state = from_state
        if action == 'start_close':
            to_state = derive_recommended_close_state(conn, month)
            record_month_close_state(conn, month, to_state, 'month close checks refreshed', actor, 'month_close_action')
        elif action == 'submit_for_approval':
            prerequisites = build_month_close_payload(month).get('prerequisites', {})
            if not prerequisites.get('inventory_ready'):
                raise ValueError('Inventory reconciliation is not complete. Please finish inventory checks before submitting for approval.')
            if from_state != 'pnl_pending':
                raise ValueError('Only pnl_pending months can be submitted for approval.')
            to_state = 'approving'
            record_month_close_state(conn, month, to_state, note or 'submitted for approval', actor, 'month_close_action')
        elif action == 'approve_close':
            if from_state != 'approving':
                raise ValueError('Only approving months can be closed.')
            check_log = query_one(conn, "SELECT blocker_count, warning_count, receivable_gap FROM monthly_close_log WHERE period_month = ? ORDER BY close_id DESC LIMIT 1", (month,))
            effective_issues = get_effective_issue_counts(conn, month)
            if int(effective_issues.get('blocker_count') or 0) > 0:
                raise ValueError('Blockers remain. Close is not allowed.')
            if abs(float(check_log.get('receivable_gap') or 0)) > 0.01:
                raise ValueError('Receivable reconciliation is not complete.')
            inventory_status = build_inventory_status(conn, month, refresh=True)
            if not inventory_status.get('summary', {}).get('ready'):
                raise ValueError('Inventory reconciliation is not complete.')
            to_state = 'closed'
            conn.execute(
                """
                UPDATE monthly_close_log
                SET close_status = 'closed',
                    closed_at = ?
                WHERE period_month = ?
                """,
                (now_iso(), month),
            )
            record_month_close_state(conn, month, to_state, note or 'approved and closed', actor, 'month_close_action')
        elif action == 'reopen_close':
            if from_state not in {'closed', 'approving'}:
                raise ValueError('Only closed or approving months can be reopened.')
            to_state = 'reopened'
            conn.execute(
                """
                UPDATE monthly_close_log
                SET close_status = 'reopened',
                    closed_at = NULL
                WHERE period_month = ?
                """,
                (month,),
            )
            record_month_close_state(conn, month, to_state, note or 'reopened', actor, 'month_close_action')

        record_month_close_action(conn, month, action, from_state, to_state, 'success', note or action, actor)
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        if from_state:
            record_month_close_action(conn, month, action, from_state, to_state or from_state, 'blocked', str(exc), actor)
            conn.commit()
        raise
    finally:
        conn.close()
    return build_month_close_payload(month)


def build_uploads_payload() -> dict:
    ensure_manual_templates()
    ensure_runtime_schema()
    source_files = []
    for path in sorted(ROOT.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not path.is_file() or path.suffix.lower() not in SOURCE_FILE_SUFFIXES:
            continue
        source_files.append(
            {
                'filename': path.name,
                'size': path.stat().st_size,
                'updated_at': datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec='seconds'),
            }
        )
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    recent_batches = query_all(
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
        LIMIT 20
        """,
    )
    rule_versions = query_all(
        conn,
        """
        SELECT
            rule_scope,
            version_name,
            applied_at,
            notes
        FROM rule_version
        ORDER BY rule_version_id DESC
        LIMIT 10
        """,
    )
    conn.close()
    return {
        'targets': ['source', 'attachment'],
        'source_files': source_files[:30],
        'attachment_dir': str(ATTACHMENT_DIR),
        'recent_batches': recent_batches,
        'rule_versions': rule_versions,
    }


def read_csv_with_headers(path: Path, default_headers: list[str]) -> tuple[list[str], list[dict], str]:
    if not path.exists():
        return default_headers, [], "utf-8-sig"
    last_error: Exception | None = None
    for encoding in CSV_ENCODINGS:
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                headers = list(reader.fieldnames or default_headers)
                rows = []
                for raw_row in reader:
                    rows.append({header: (raw_row.get(header) or "") for header in headers})
                return headers, rows, encoding
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error
    return default_headers, [], "utf-8-sig"


def write_csv_rows(path: Path, headers: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})


def sanitize_filename(filename: str) -> str:
    clean = Path(filename).name.strip()
    if not clean:
        raise ValueError("Filename is empty.")
    if clean in {".", ".."}:
        raise ValueError("Invalid filename.")
    return clean


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


def build_overview(conn: sqlite3.Connection, month: str) -> dict:
    overview = query_one(
        conn,
        f"""
        WITH sku AS (
            SELECT
                COALESCE(SUM(net_sales), 0) AS net_sales,
                COALESCE(SUM({GROSS_PROFIT_EXPR}), 0) AS gross_profit,
                COALESCE(SUM(qty_sold), 0) AS units_sold,
                COALESCE(SUM(ad_spend), 0) AS ad_spend,
                COUNT(DISTINCT sku) AS sku_count
            FROM v_monthly_sku_order_type_summary
            WHERE period_month = ?
        ),
        orders AS (
            SELECT
                COUNT(*) AS order_lines,
                COUNT(DISTINCT amazon_order_id) AS order_count,
                SUM(CASE WHEN settlement_state IN ('fully_settled_released', 'fully_settled_unreleased', 'refunded_after_settlement') THEN 1 ELSE 0 END) AS recognized_count,
                SUM(CASE WHEN settlement_state = 'shipped_waiting_settlement' THEN 1 ELSE 0 END) AS waiting_count,
                SUM(CASE WHEN settlement_state = 'cancelled_before_settlement' THEN 1 ELSE 0 END) AS cancelled_count
            FROM v_order_settlement_tracking
            WHERE order_month = ?
        ),
        close_log AS (
            SELECT
                close_status,
                blocker_count,
                warning_count,
                pdf_amount,
                receivable_gap,
                notes,
                closed_at
            FROM monthly_close_log
            WHERE period_month = ?
            ORDER BY closed_at DESC
            LIMIT 1
        )
        SELECT
            sku.net_sales,
            sku.gross_profit,
            sku.units_sold,
            sku.ad_spend,
            sku.sku_count,
            orders.order_lines,
            orders.order_count,
            orders.recognized_count,
            orders.waiting_count,
            orders.cancelled_count,
            close_log.close_status,
            close_log.blocker_count,
            close_log.warning_count,
            close_log.pdf_amount,
            close_log.receivable_gap,
            close_log.notes,
            close_log.closed_at
        FROM sku
        CROSS JOIN orders
        LEFT JOIN close_log ON 1 = 1
        """,
        (month, month, month),
    )

    snapshot = query_one(
        conn,
        """
        SELECT closing_receivable, receivable_gap
        FROM fact_platform_receivable_snapshot
        WHERE period_month = ?
          AND platform_code = 'amazon'
          AND store_code = ''
        ORDER BY snapshot_id DESC
        LIMIT 1
        """,
        (month,),
    )
    effective_issues = get_effective_issue_counts(conn, month)
    net_sales = round_money(overview.get("net_sales"))
    gross_profit = round_money(overview.get("gross_profit"))
    order_lines = int(overview.get("order_lines") or 0)
    recognized_count = int(overview.get("recognized_count") or 0)
    overview["net_sales"] = net_sales
    overview["gross_profit"] = gross_profit
    overview["ad_spend"] = round_money(overview.get("ad_spend"))
    overview["units_sold"] = round_money(overview.get("units_sold"))
    overview["margin_pct"] = round((gross_profit / net_sales) * 100, 2) if net_sales else 0.0
    overview["recognized_rate"] = round((recognized_count / order_lines) * 100, 2) if order_lines else 0.0
    overview["close_notes"] = parse_close_notes(overview.get("notes"))
    overview["raw_blocker_count"] = int(overview.get("blocker_count") or 0)
    overview["raw_warning_count"] = int(overview.get("warning_count") or 0)
    overview["blocker_count"] = int(effective_issues.get("blocker_count") or 0)
    overview["warning_count"] = int(effective_issues.get("warning_count") or 0)
    overview["overridden_issue_count"] = int(effective_issues.get("overridden_count") or 0)
    overview["business_state"] = get_latest_month_close_state(conn, month) or derive_recommended_close_state(conn, month)
    overview["receivable_gap"] = round_money(snapshot.get('receivable_gap', overview.get('receivable_gap')))
    overview["closing_receivable"] = round_money(snapshot.get('closing_receivable'))
    return overview


def build_comparison(current: dict, previous: dict | None, previous_month: str | None) -> dict:
    if not previous or not previous_month:
        return {
            "previous_month": None,
            "net_sales_delta": 0.0,
            "gross_profit_delta": 0.0,
            "order_count_delta": 0,
            "margin_delta": 0.0,
        }
    return {
        "previous_month": previous_month,
        "net_sales_delta": round_money(current.get("net_sales", 0) - previous.get("net_sales", 0)),
        "gross_profit_delta": round_money(current.get("gross_profit", 0) - previous.get("gross_profit", 0)),
        "order_count_delta": int(current.get("order_count", 0) - previous.get("order_count", 0)),
        "margin_delta": round(current.get("margin_pct", 0) - previous.get("margin_pct", 0), 2),
    }



def build_fee_validation_rows(conn: sqlite3.Connection, month: str) -> list[dict]:
    ad_report = conn.execute(
        """
        SELECT COALESCE(SUM(COALESCE(spend, 0)), 0)
        FROM fact_advertising_monthly_sku
        WHERE period_month = ?
        """,
        (month,),
    ).fetchone()[0]
    ad_settlement = conn.execute(
        """
        SELECT COALESCE(SUM(-COALESCE(total, 0)), 0)
        FROM fact_settlement_lines
        WHERE transaction_month = ?
          AND lower(COALESCE(transaction_subtype, '')) = 'cost of advertising'
        """,
        (month,),
    ).fetchone()[0]
    storage_report = conn.execute(
        """
        SELECT COALESCE(SUM(COALESCE(estimated_monthly_storage_fee, 0) - COALESCE(incentive_fee_amount, 0)), 0)
        FROM fact_storage_monthly_sku
        WHERE period_month = ?
        """,
        (month,),
    ).fetchone()[0]
    storage_settlement = conn.execute(
        """
        SELECT COALESCE(SUM(
            CASE
                WHEN lower(COALESCE(transaction_subtype, '')) in ('fba storage fee', 'fba long-term storage fee')
                    THEN -COALESCE(total, 0)
                ELSE 0
            END
        ), 0)
        FROM fact_settlement_lines
        WHERE transaction_month = ?
        """,
        (month,),
    ).fetchone()[0]
    return [
        {
            "fee_code": "advertising",
            "fee_name": "???",
            "report_total": round_money(ad_report),
            "settlement_total": round_money(ad_settlement),
            "difference": round_money(float(ad_settlement or 0) - float(ad_report or 0)),
            "status": "matched" if abs(float(ad_settlement or 0) - float(ad_report or 0)) <= 0.01 else "mismatch",
            "note": "??????????????????????????",
        },
        {
            "fee_code": "storage",
            "fee_name": "???",
            "report_total": round_money(storage_report),
            "settlement_total": round_money(storage_settlement),
            "difference": round_money(float(storage_settlement or 0) - float(storage_report or 0)),
            "status": "matched" if abs(float(storage_settlement or 0) - float(storage_report or 0)) <= 0.01 else "mismatch",
            "note": "??????????????????????????",
        },
    ]

def build_order_type_rollup_rows(
    conn: sqlite3.Connection,
    month: str,
    group_by: str = "sku",
    keyword: str | None = None,
    order_type_filter: str | None = None,
) -> list[dict]:
    if group_by not in {"sku", "product_name", "all"}:
        raise ValueError(f"Unsupported group_by: {group_by}")

    if group_by == "sku":
        metrics_dimension_sql = """
            ms.sku AS sku,
            COALESCE(ds.product_name_cn, '') AS product_name_cn,
            ms.sku AS scope_value
        """
        metrics_group_sql = "ms.sku, COALESCE(ds.product_name_cn, '')"
        counts_dimension_sql = """
            fd.sku AS sku,
            COALESCE(ds.product_name_cn, '') AS product_name_cn,
            fd.sku AS scope_value
        """
        counts_group_sql = "fd.sku, COALESCE(ds.product_name_cn, '')"
    elif group_by == "product_name":
        metrics_dimension_sql = """
            '' AS sku,
            COALESCE(ds.product_name_cn, 'Unmapped Product') AS product_name_cn,
            COALESCE(ds.product_name_cn, 'Unmapped Product') AS scope_value
        """
        metrics_group_sql = "COALESCE(ds.product_name_cn, 'Unmapped Product')"
        counts_dimension_sql = """
            '' AS sku,
            COALESCE(ds.product_name_cn, 'Unmapped Product') AS product_name_cn,
            COALESCE(ds.product_name_cn, 'Unmapped Product') AS scope_value
        """
        counts_group_sql = "COALESCE(ds.product_name_cn, 'Unmapped Product')"
    else:
        metrics_dimension_sql = """
            '' AS sku,
            'All Products' AS product_name_cn,
            'All Products' AS scope_value
        """
        metrics_group_sql = "'All Products'"
        counts_dimension_sql = """
            '' AS sku,
            'All Products' AS product_name_cn,
            'All Products' AS scope_value
        """
        counts_group_sql = "'All Products'"

    metrics_filters = ["ms.period_month = ?"]
    counts_filters = ["fd.period_month = ?"]
    metrics_params: list[object] = [month]
    counts_params: list[object] = [month]

    if keyword:
        keyword_like = f"%{keyword.strip().lower()}%"
        metrics_filters.append("(lower(ms.sku) LIKE ? OR lower(COALESCE(ds.product_name_cn, '')) LIKE ?)")
        counts_filters.append("(lower(fd.sku) LIKE ? OR lower(COALESCE(ds.product_name_cn, '')) LIKE ?)")
        metrics_params.extend([keyword_like, keyword_like])
        counts_params.extend([keyword_like, keyword_like])

    if order_type_filter and order_type_filter != "all":
        metrics_filters.append("ms.order_type = ?")
        counts_filters.append("fd.order_type = ?")
        metrics_params.append(order_type_filter)
        counts_params.append(order_type_filter)

    metrics_sql = f"""
        SELECT
            ms.period_month,
            {metrics_dimension_sql},
            ms.order_type,
            ROUND(SUM(ms.qty_sold), 2) AS qty_sold,
            ROUND(SUM(ms.gmv), 2) AS gmv,
            ROUND(SUM(ms.product_sales), 2) AS product_sales,
            ROUND(SUM(ms.shipping_credits), 2) AS shipping_credits,
            ROUND(SUM(ms.gift_wrap_credits), 2) AS gift_wrap_credits,
            ROUND(SUM(ms.promotional_rebates), 2) AS promotional_rebates,
            ROUND(SUM(ms.net_sales), 2) AS net_sales,
            ROUND(SUM(ms.selling_fees), 2) AS selling_fees,
            ROUND(SUM(ms.fba_fees), 2) AS fba_fees,
            ROUND(SUM(ms.other_transaction_fees), 2) AS other_transaction_fees,
            ROUND(SUM(ms.marketplace_withheld_tax), 2) AS marketplace_withheld_tax,
            ROUND(SUM(ms.transfer_quantity), 2) AS transfer_quantity,
            ROUND(SUM(ms.disposal_quantity), 2) AS disposal_quantity,
            ROUND(SUM(ms.storage_fees), 2) AS storage_fees,
            ROUND(SUM(ms.removal_fees), 2) AS removal_fees,
            ROUND(SUM(ms.removal_fee_capitalized), 2) AS removal_fee_capitalized,
            ROUND(SUM(ms.removal_fee_unclassified), 2) AS removal_fee_unclassified,
            ROUND(SUM(ms.ad_spend), 2) AS ad_spend,
            ROUND(SUM(ms.compensation_income), 2) AS compensation_income,
            ROUND(SUM(ms.review_cost), 2) AS test_order_cost,
            ROUND(SUM(ms.vine_fee), 2) AS vine_fee,
            ROUND(SUM(ms.review_quantity), 2) AS test_order_quantity,
            ROUND(SUM(ms.vine_quantity), 2) AS vine_quantity,
            ROUND(SUM(ms.subscription_fee), 2) AS subscription_fee,
            ROUND(SUM(ms.coupon_participation_fee), 2) AS coupon_participation_fee,
            ROUND(SUM(ms.coupon_performance_fee), 2) AS coupon_performance_fee,
            ROUND(SUM(ms.product_cost), 2) AS product_cost,
            ROUND(SUM(ms.inbound_cost), 2) AS inbound_freight_cost,
            ROUND(SUM(ms.receivable_ad_spend), 2) AS receivable_ad_spend,
            ROUND(SUM(ms.receivable_storage_fees), 2) AS receivable_storage_fees,
            ROUND(SUM(ms.receivable_removal_fees), 2) AS receivable_removal_fees,
            ROUND(SUM(ms.receivable_compensation_income), 2) AS receivable_compensation_income,
            ROUND(SUM(ms.receivable_subscription_fee), 2) AS receivable_subscription_fee,
            ROUND(SUM(ms.receivable_coupon_participation_fee), 2) AS receivable_coupon_participation_fee,
            ROUND(SUM(ms.receivable_coupon_performance_fee), 2) AS receivable_coupon_performance_fee,
            ROUND(SUM(ms.receivable_vine_fee), 2) AS receivable_vine_fee,
            ROUND(SUM(ms.inventory_capitalized_cost), 2) AS inventory_capitalized_cost,
            ROUND(SUM({RECEIVABLE_EXPR}), 2) AS receivable_net,
            ROUND(SUM({GROSS_PROFIT_EXPR}), 2) AS gross_profit
        FROM v_monthly_sku_order_type_summary ms
        LEFT JOIN (
            SELECT sku, MAX(product_name_cn) AS product_name_cn
            FROM dim_sku
            GROUP BY sku
        ) ds
          ON ms.sku = ds.sku
        WHERE {" AND ".join(metrics_filters)}
        GROUP BY ms.period_month, {metrics_group_sql}, ms.order_type
        ORDER BY scope_value, ms.order_type
    """
    metric_rows = query_all(conn, metrics_sql, tuple(metrics_params))

    counts_sql = f"""
        SELECT
            {counts_dimension_sql},
            fd.order_type,
            COUNT(DISTINCT CASE WHEN fd.order_type = 'non_order_fee' THEN NULL ELSE fd.amazon_order_id END) AS order_count
        FROM v_finance_detail_lines fd
        LEFT JOIN (
            SELECT sku, MAX(product_name_cn) AS product_name_cn
            FROM dim_sku
            GROUP BY sku
        ) ds
          ON fd.sku = ds.sku
        WHERE {" AND ".join(counts_filters)}
        GROUP BY {counts_group_sql}, fd.order_type
    """
    count_rows = query_all(conn, counts_sql, tuple(counts_params))
    count_map = {(row["scope_value"], row["order_type"]): int(row["order_count"] or 0) for row in count_rows}

    for row in metric_rows:
        row["order_count"] = count_map.get((row["scope_value"], row["order_type"]), 0)
        row["group_by"] = group_by

    return metric_rows


def build_order_line_profit_rows(
    conn: sqlite3.Connection,
    month: str | None = None,
    sku_filter: str | None = None,
    order_id: str | None = None,
    keyword: str | None = None,
) -> list[dict]:
    where_clauses = ["1 = 1"]
    params: list[object] = []
    if month:
        where_clauses.append("d.period_month = ?")
        params.append(month)
    if sku_filter:
        where_clauses.append("d.sku = ?")
        params.append(sku_filter)
    if order_id:
        where_clauses.append("d.amazon_order_id = ?")
        params.append(order_id)
    outer_where_clauses = ["1 = 1"]
    outer_params: list[object] = []
    if keyword:
        keyword_like = f"%{keyword.strip().lower()}%"
        outer_where_clauses.append(
            "(lower(COALESCE(d.sku, '')) LIKE ? OR lower(COALESCE(ds.product_name_cn, '')) LIKE ? OR lower(COALESCE(d.amazon_order_id, '')) LIKE ?)"
        )
        outer_params.extend([keyword_like, keyword_like, keyword_like])

    sql = f"""
    WITH order_meta AS (
        SELECT
            order_month AS period_month,
            amazon_order_id,
            sku,
            MAX(purchase_date) AS purchase_date,
            MAX(order_status) AS order_status,
            MAX(settlement_state) AS settlement_state,
            MAX(sales_channel) AS sales_channel,
            MAX(fulfillment_channel) AS fulfillment_channel,
            MAX(asin) AS asin,
            SUM(COALESCE(quantity, 0)) AS ordered_quantity,
            SUM(COALESCE(item_price, 0)) AS ordered_item_price,
            SUM(COALESCE(shipping_price, 0)) AS ordered_shipping_price,
            SUM(COALESCE(item_promotion_discount, 0)) AS ordered_item_promotion_discount,
            SUM(COALESCE(ship_promotion_discount, 0)) AS ordered_ship_promotion_discount
        FROM fact_order_lines
        WHERE is_amazon_channel = 1
        GROUP BY order_month, amazon_order_id, sku
    ),
    ranked_detail AS (
        SELECT
            d.*,
            ROW_NUMBER() OVER (
                PARTITION BY d.period_month, COALESCE(d.amazon_order_id, ''), COALESCE(d.sku, '')
                ORDER BY
                    CASE WHEN d.detail_source = 'settlement' THEN 0 ELSE 1 END,
                    COALESCE(d.transaction_datetime, ''),
                    d.detail_line_id
            ) AS detail_rank
        FROM v_finance_detail_lines d
        WHERE {' AND '.join(where_clauses)}
    )
    SELECT
        d.detail_line_id AS order_line_id,
        d.detail_source,
        d.settlement_line_id,
        d.period_month,
        d.amazon_order_id,
        COALESCE(om.purchase_date, d.transaction_datetime) AS purchase_date,
        COALESCE(om.order_status, '') AS order_status,
        COALESCE(om.settlement_state, '') AS settlement_state,
        COALESCE(om.sales_channel, '') AS sales_channel,
        COALESCE(om.fulfillment_channel, d.fulfillment, '') AS fulfillment_channel,
        d.sku,
        COALESCE(ds.product_name_cn, '') AS product_name_cn,
        COALESCE(om.asin, '') AS asin,
        d.order_type,
        ROUND(CASE WHEN d.detail_rank = 1 THEN COALESCE(om.ordered_quantity, 0) ELSE 0 END, 2) AS ordered_quantity,
        ROUND(CASE WHEN d.detail_rank = 1 THEN COALESCE(om.ordered_item_price, 0) ELSE 0 END, 2) AS ordered_item_price,
        ROUND(CASE WHEN d.detail_rank = 1 THEN COALESCE(om.ordered_shipping_price, 0) ELSE 0 END, 2) AS ordered_shipping_price,
        ROUND(CASE WHEN d.detail_rank = 1 THEN COALESCE(om.ordered_item_promotion_discount, 0) ELSE 0 END, 2) AS ordered_item_promotion_discount,
        ROUND(CASE WHEN d.detail_rank = 1 THEN COALESCE(om.ordered_ship_promotion_discount, 0) ELSE 0 END, 2) AS ordered_ship_promotion_discount,
        ROUND(COALESCE(d.qty_sold, 0), 2) AS settled_quantity,
        ROUND(COALESCE(d.product_sales, 0), 2) AS product_sales,
        ROUND(COALESCE(d.shipping_credits, 0), 2) AS shipping_credits,
        ROUND(COALESCE(d.gift_wrap_credits, 0), 2) AS gift_wrap_credits,
        ROUND(COALESCE(d.promotional_rebates, 0), 2) AS promotional_rebates,
        ROUND(COALESCE(d.net_sales, 0), 2) AS net_sales,
        ROUND(COALESCE(d.selling_fees, 0), 2) AS selling_fees,
        ROUND(COALESCE(d.fba_fees, 0), 2) AS fba_fees,
        ROUND(COALESCE(d.other_transaction_fees, 0), 2) AS other_transaction_fees,
        ROUND(COALESCE(d.marketplace_withheld_tax, 0), 2) AS marketplace_withheld_tax,
        ROUND(COALESCE(d.settlement_net_total, 0), 2) AS settlement_net_total,
        ROUND(COALESCE(d.compensation_income, 0), 6) AS compensation_income_amount,
        ROUND(COALESCE(d.transfer_quantity, 0), 2) AS transfer_quantity,
        ROUND(COALESCE(d.disposal_quantity, 0), 2) AS disposal_quantity,
        ROUND(COALESCE(d.review_quantity, 0), 2) AS test_order_quantity,
        ROUND(COALESCE(d.vine_quantity, 0), 2) AS vine_quantity,
        ROUND(COALESCE(d.storage_fees, 0), 6) AS allocated_storage_fees,
        ROUND(COALESCE(d.removal_fees, 0), 6) AS allocated_removal_fees,
        ROUND(COALESCE(d.ad_spend, 0), 6) AS allocated_ad_spend,
        ROUND(COALESCE(d.compensation_income, 0), 6) AS direct_compensation_income,
        ROUND(COALESCE(d.review_cost, 0), 6) AS allocated_test_order_cost,
        ROUND(COALESCE(d.vine_fee, 0), 6) AS allocated_vine_fee,
        ROUND(COALESCE(d.subscription_fee, 0), 6) AS allocated_subscription_fee,
        ROUND(COALESCE(d.coupon_participation_fee, 0), 6) AS allocated_coupon_participation_fee,
        ROUND(COALESCE(d.coupon_performance_fee, 0), 6) AS allocated_coupon_performance_fee,
        ROUND(COALESCE(d.product_unit_cost, 0), 6) AS product_unit_cost,
        ROUND(COALESCE(d.inbound_unit_cost, 0), 6) AS inbound_freight_unit_cost,
        ROUND(COALESCE(d.product_cost, 0), 6) AS allocated_product_cost,
        ROUND(COALESCE(d.inbound_cost, 0), 6) AS allocated_inbound_freight_cost,
        ROUND(COALESCE(d.receivable_ad_spend, 0), 6) AS receivable_ad_spend,
        ROUND(COALESCE(d.receivable_storage_fees, 0), 6) AS receivable_storage_fees,
        ROUND(COALESCE(d.receivable_removal_fees, 0), 6) AS receivable_removal_fees,
        ROUND(COALESCE(d.receivable_compensation_income, 0), 6) AS receivable_compensation_income,
        ROUND(COALESCE(d.receivable_subscription_fee, 0), 6) AS receivable_subscription_fee,
        ROUND(COALESCE(d.receivable_coupon_participation_fee, 0), 6) AS receivable_coupon_participation_fee,
        ROUND(COALESCE(d.receivable_coupon_performance_fee, 0), 6) AS receivable_coupon_performance_fee,
        ROUND(COALESCE(d.receivable_vine_fee, 0), 6) AS receivable_vine_fee,
        ROUND(COALESCE(d.inventory_capitalized_cost, 0), 6) AS inventory_capitalized_cost,
        ROUND(
            COALESCE(d.net_sales, 0)
            - COALESCE(d.selling_fees, 0)
            - COALESCE(d.fba_fees, 0)
            - COALESCE(d.other_transaction_fees, 0)
            - COALESCE(d.marketplace_withheld_tax, 0)
            - COALESCE(d.storage_fees, 0)
            - COALESCE(d.removal_fees, 0)
            - COALESCE(d.ad_spend, 0)
            + COALESCE(d.compensation_income, 0)
            - COALESCE(d.review_cost, 0)
            - COALESCE(d.subscription_fee, 0)
            - COALESCE(d.coupon_participation_fee, 0)
            - COALESCE(d.coupon_performance_fee, 0)
            - COALESCE(d.vine_fee, 0)
            - COALESCE(d.product_cost, 0)
            - COALESCE(d.inbound_cost, 0),
            6
        ) AS estimated_gross_profit,
        ROUND(
            COALESCE(d.net_sales, 0)
            - COALESCE(d.selling_fees, 0)
            - COALESCE(d.fba_fees, 0)
            - COALESCE(d.other_transaction_fees, 0)
            - COALESCE(d.marketplace_withheld_tax, 0)
            - COALESCE(d.receivable_storage_fees, 0)
            - COALESCE(d.receivable_removal_fees, 0)
            - COALESCE(d.receivable_ad_spend, 0)
            + COALESCE(d.receivable_compensation_income, 0)
            - COALESCE(d.receivable_subscription_fee, 0)
            - COALESCE(d.receivable_coupon_participation_fee, 0)
            - COALESCE(d.receivable_coupon_performance_fee, 0)
            - COALESCE(d.receivable_vine_fee, 0),
            6
        ) AS receivable_net
    FROM ranked_detail d
    LEFT JOIN order_meta om
      ON d.period_month = om.period_month
     AND COALESCE(d.amazon_order_id, '') = COALESCE(om.amazon_order_id, '')
     AND COALESCE(d.sku, '') = COALESCE(om.sku, '')
    LEFT JOIN (
        SELECT sku, MAX(product_name_cn) AS product_name_cn
        FROM dim_sku
        GROUP BY sku
    ) ds
      ON d.sku = ds.sku
    WHERE {" AND ".join(outer_where_clauses)}
    ORDER BY purchase_date DESC, d.amazon_order_id, d.sku, d.detail_line_id
    """
    return query_all(conn, sql, tuple(params + outer_params))


def build_allocation_audit_rows(
    conn: sqlite3.Connection,
    month: str,
    keyword: str | None = None,
    order_type_filter: str | None = None,
) -> list[dict]:
    filters = [
        "d.period_month = ?",
        "d.detail_source = 'settlement'",
        "d.order_type <> 'non_order_fee'",
        "ABS(COALESCE(d.alloc_share, 0)) > 0.000001",
    ]
    params: list[object] = [month]
    if keyword:
        keyword_like = f"%{keyword.strip().lower()}%"
        filters.append("(lower(d.sku) LIKE ? OR lower(COALESCE(ds.product_name_cn, '')) LIKE ? OR lower(COALESCE(d.amazon_order_id, '')) LIKE ?)")
        params.extend([keyword_like, keyword_like, keyword_like])
    if order_type_filter and order_type_filter != "all":
        filters.append("d.order_type = ?")
        params.append(order_type_filter)

    sql = f"""
    WITH order_meta AS (
        SELECT
            order_month AS period_month,
            amazon_order_id,
            sku,
            MAX(purchase_date) AS purchase_date
        FROM fact_order_lines
        WHERE is_amazon_channel = 1
        GROUP BY order_month, amazon_order_id, sku
    ),
    eligible_detail AS (
        SELECT
            d.detail_line_id,
            d.period_month,
            d.amazon_order_id,
            d.sku,
            d.order_type,
            ABS(COALESCE(d.net_sales, 0)) AS row_abs_net_sales,
            COALESCE(d.alloc_share, 0) AS alloc_share,
            COALESCE(d.ad_spend, 0) AS allocated_ad_spend,
            COALESCE(d.subscription_fee, 0) AS allocated_subscription_fee,
            COALESCE(d.coupon_participation_fee, 0) AS allocated_coupon_participation_fee,
            COALESCE(d.coupon_performance_fee, 0) AS allocated_coupon_performance_fee,
            COALESCE(d.receivable_ad_spend, 0) AS receivable_ad_spend,
            COALESCE(d.receivable_removal_fees, 0) AS receivable_removal_fees,
            COALESCE(d.receivable_subscription_fee, 0) AS receivable_subscription_fee,
            COALESCE(d.receivable_coupon_participation_fee, 0) AS receivable_coupon_participation_fee,
            COALESCE(d.receivable_coupon_performance_fee, 0) AS receivable_coupon_performance_fee,
            d.settlement_line_id
        FROM v_finance_detail_lines d
        LEFT JOIN (
            SELECT sku, MAX(product_name_cn) AS product_name_cn
            FROM dim_sku
            GROUP BY sku
        ) ds
          ON d.sku = ds.sku
        WHERE {" AND ".join(filters)}
    ),
    sku_totals AS (
        SELECT period_month, sku, SUM(ABS(COALESCE(net_sales, 0))) AS sku_abs_net_sales
        FROM v_finance_detail_lines
        WHERE period_month = ?
          AND detail_source = 'settlement'
          AND order_type <> 'non_order_fee'
          AND lower(COALESCE(order_type, '')) NOT LIKE '%refund'
        GROUP BY period_month, sku
    ),
    month_totals AS (
        SELECT period_month, SUM(ABS(COALESCE(net_sales, 0))) AS month_abs_net_sales
        FROM v_finance_detail_lines
        WHERE period_month = ?
          AND detail_source = 'settlement'
          AND order_type <> 'non_order_fee'
          AND lower(COALESCE(order_type, '')) NOT LIKE '%refund'
        GROUP BY period_month
    )
    SELECT
        d.period_month,
        COALESCE(om.purchase_date, '') AS purchase_date,
        d.amazon_order_id,
        d.sku,
        COALESCE(ds.product_name_cn, '') AS product_name_cn,
        d.order_type,
        ROUND(d.row_abs_net_sales, 6) AS allocation_basis_abs_net_sales,
        ROUND(COALESCE(st.sku_abs_net_sales, 0), 6) AS sku_abs_net_sales_base,
        ROUND(COALESCE(mt.month_abs_net_sales, 0), 6) AS month_abs_net_sales_base,
        ROUND(d.alloc_share, 6) AS alloc_share,
        ROUND(CASE WHEN COALESCE(mt.month_abs_net_sales, 0) = 0 THEN 0 ELSE d.row_abs_net_sales * 1.0 / mt.month_abs_net_sales END, 6) AS month_alloc_share,
        ROUND(d.allocated_ad_spend, 6) AS allocated_ad_spend,
        ROUND(d.allocated_subscription_fee, 6) AS allocated_subscription_fee,
        ROUND(d.allocated_coupon_participation_fee, 6) AS allocated_coupon_participation_fee,
        ROUND(d.allocated_coupon_performance_fee, 6) AS allocated_coupon_performance_fee,
        ROUND(d.receivable_ad_spend, 6) AS receivable_ad_spend,
        ROUND(d.receivable_removal_fees, 6) AS receivable_removal_fees,
        ROUND(d.receivable_subscription_fee, 6) AS receivable_subscription_fee,
        ROUND(d.receivable_coupon_participation_fee, 6) AS receivable_coupon_participation_fee,
        ROUND(d.receivable_coupon_performance_fee, 6) AS receivable_coupon_performance_fee,
        d.settlement_line_id
    FROM eligible_detail d
    LEFT JOIN sku_totals st
      ON d.period_month = st.period_month
     AND d.sku = st.sku
    LEFT JOIN month_totals mt
      ON d.period_month = mt.period_month
    LEFT JOIN order_meta om
      ON d.period_month = om.period_month
     AND COALESCE(d.amazon_order_id, '') = COALESCE(om.amazon_order_id, '')
     AND COALESCE(d.sku, '') = COALESCE(om.sku, '')
    LEFT JOIN (
        SELECT sku, MAX(product_name_cn) AS product_name_cn
        FROM dim_sku
        GROUP BY sku
    ) ds
      ON d.sku = ds.sku
    ORDER BY d.sku, d.amazon_order_id, d.detail_line_id
    """
    return query_all(conn, sql, tuple(params + [month, month]))


def build_order_lookup_payload(order_id: str) -> dict:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = build_order_line_profit_rows(conn, order_id=order_id)
    if not rows:
        conn.close()
        return {"order_id": order_id, "found": False, "summary": {}, "rows": []}

    numeric_fields = [
        'ordered_quantity', 'ordered_item_price', 'ordered_shipping_price', 'ordered_item_promotion_discount',
        'ordered_ship_promotion_discount', 'settled_quantity', 'product_sales', 'shipping_credits', 'gift_wrap_credits',
        'promotional_rebates', 'selling_fees', 'fba_fees', 'other_transaction_fees', 'marketplace_withheld_tax',
        'net_sales', 'settlement_net_total', 'allocated_storage_fees', 'allocated_removal_fees', 'allocated_ad_spend',
        'direct_compensation_income', 'allocated_test_order_cost', 'allocated_vine_fee', 'allocated_subscription_fee',
        'allocated_coupon_participation_fee', 'allocated_coupon_performance_fee', 'allocated_product_cost',
        'allocated_inbound_freight_cost', 'receivable_ad_spend', 'receivable_storage_fees', 'receivable_removal_fees',
        'receivable_compensation_income', 'receivable_subscription_fee', 'receivable_coupon_participation_fee',
        'receivable_coupon_performance_fee', 'receivable_vine_fee', 'inventory_capitalized_cost', 'receivable_net',
        'estimated_gross_profit'
    ]
    summary = {field: round(sum(float(row.get(field) or 0) for row in rows), 6) for field in numeric_fields}
    summary['line_count'] = len(rows)
    summary['sku_count'] = len({row['sku'] for row in rows if row.get('sku')})
    summary['period_months'] = sorted({row['period_month'] for row in rows if row.get('period_month')})
    conn.close()
    return {'order_id': order_id, 'found': True, 'summary': summary, 'rows': rows}

def build_dashboard_payload(month: str | None) -> dict:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    months = get_months(conn)
    if not months:
        raise RuntimeError("No dashboard months available in the database.")

    selected_month = month if month in months else months[0]
    previous_month = months[months.index(selected_month) + 1] if months.index(selected_month) + 1 < len(months) else None

    overview = build_overview(conn, selected_month)
    previous_overview = build_overview(conn, previous_month) if previous_month else None
    comparison = build_comparison(overview, previous_overview, previous_month)

    close_timeline = query_all(
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
    for item in close_timeline:
        item["close_notes"] = parse_close_notes(item.get("notes"))
        item["pdf_amount"] = round_money(item.get("pdf_amount"))
        item["receivable_gap"] = round_money(item.get("receivable_gap"))

    top_skus = query_all(
        conn,
        f"""
        SELECT
            sku,
            ROUND(SUM(qty_sold), 2) AS qty_sold,
            ROUND(SUM(net_sales), 2) AS net_sales,
            ROUND(SUM(ad_spend), 2) AS ad_spend,
            ROUND(SUM({GROSS_PROFIT_EXPR}), 2) AS gross_profit,
            ROUND(CASE WHEN SUM(net_sales) = 0 THEN 0 ELSE (SUM({GROSS_PROFIT_EXPR}) / SUM(net_sales)) * 100 END, 2) AS margin_pct
        FROM v_monthly_sku_order_type_summary
        WHERE period_month = ?
        GROUP BY sku
        ORDER BY gross_profit DESC, net_sales DESC
        LIMIT 8
        """,
        (selected_month,),
    )

    alerts = query_all(
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
        (selected_month,),
    )

    fee_validations = build_fee_validation_rows(conn, selected_month)
    receivable_summary = get_receivable_snapshot(conn, selected_month, refresh_if_missing=True)
    inventory_summary = build_inventory_status(conn, selected_month, refresh=True).get("summary", {})

    conn.close()

    return {
        "generated_at": now_iso(),
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


def build_profit_payload(month: str | None) -> dict:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    months = get_months(conn)
    if not months:
        conn.close()
        raise RuntimeError("No profit-analysis months available in the database.")
    selected_month = month if month in months else months[0]

    sku_details = query_all(
        conn,
        f"""
        SELECT
            sku,
            ROUND(SUM(qty_sold), 2) AS qty_sold,
            ROUND(SUM(net_sales), 2) AS net_sales,
            ROUND(SUM(ad_spend), 2) AS ad_spend,
            ROUND(SUM({GROSS_PROFIT_EXPR}), 2) AS gross_profit,
            ROUND(CASE WHEN SUM(net_sales) = 0 THEN 0 ELSE (SUM({GROSS_PROFIT_EXPR}) / SUM(net_sales)) * 100 END, 2) AS margin_pct,
            ROUND(CASE WHEN SUM(net_sales) = 0 THEN 0 ELSE (SUM(ad_spend) / SUM(net_sales)) * 100 END, 2) AS acos_pct
        FROM v_monthly_sku_order_type_summary
        WHERE period_month = ?
        GROUP BY sku
        ORDER BY gross_profit DESC, net_sales DESC
        """,
        (selected_month,),
    )

    order_details = query_all(
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
        (selected_month,),
    )
    conn.close()
    return {
        "generated_at": now_iso(),
        "selected_month": selected_month,
        "available_months": months,
        "sku_details": sku_details,
        "order_details": order_details,
    }


def export_dataset(
    month: str | None,
    dataset: str,
    sku_filter: str | None = None,
    order_id: str | None = None,
    group_by: str | None = None,
    keyword: str | None = None,
    order_type_filter: str | None = None,
    basis: str | None = None,
) -> tuple[str, list[dict]]:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    months = get_months(conn)
    if not months:
        raise RuntimeError("No exportable months available.")
    selected_month = month if month in months else months[0]
    ensure_month_download_allowed(conn, selected_month, dataset)

    if dataset == "order_line_profit":
        detail_basis = normalize_detail_basis(basis)
        detail_month = selected_month
        if order_id and month not in months:
            detail_month = None
        rows = build_order_line_profit_rows(conn, month=detail_month, sku_filter=sku_filter, order_id=order_id, keyword=keyword)
        rows = project_order_line_rows(rows, detail_basis)
        suffix = sku_filter or order_id or keyword or 'all'
        safe_suffix = suffix.replace("/", "_").replace("\\", "_").replace(" ", "_")
        month_label = detail_month or "all_months"
        filename = f"amazon_order_line_profit_{detail_basis}_{month_label}_{safe_suffix}.csv"
        conn.close()
        return filename, rows

    if dataset == "order_type_rollup":
        export_group_by = group_by or "sku"
        rows = build_order_type_rollup_rows(
            conn,
            selected_month,
            group_by=export_group_by,
            keyword=keyword,
            order_type_filter=order_type_filter,
        )
        suffix_parts = [selected_month, export_group_by]
        if keyword:
            suffix_parts.append(keyword)
        if order_type_filter and order_type_filter != "all":
            suffix_parts.append(order_type_filter)
        safe_suffix = "_".join(part.replace("/", "_").replace("\\", "_").replace(" ", "_") for part in suffix_parts)
        filename = f"amazon_order_type_rollup_{safe_suffix}.csv"
        conn.close()
        return filename, rows

    if dataset == "allocation_audit":
        rows = build_allocation_audit_rows(
            conn,
            selected_month,
            keyword=keyword,
            order_type_filter=order_type_filter,
        )
        suffix_parts = [selected_month, "allocation_audit"]
        if keyword:
            suffix_parts.append(keyword)
        if order_type_filter and order_type_filter != "all":
            suffix_parts.append(order_type_filter)
        safe_suffix = "_".join(part.replace("/", "_").replace("\\", "_").replace(" ", "_") for part in suffix_parts)
        filename = f"amazon_allocation_audit_{safe_suffix}.csv"
        conn.close()
        return filename, rows

    datasets: dict[str, tuple[str, str]] = {
        "sku_details": (
            f"amazon_sku_details_{selected_month}.csv",
            f"""
            SELECT
                sku,
                ROUND(SUM(qty_sold), 2) AS qty_sold,
                ROUND(SUM(net_sales), 2) AS net_sales,
                ROUND(SUM(ad_spend), 2) AS ad_spend,
                ROUND(SUM({GROSS_PROFIT_EXPR}), 2) AS gross_profit,
                ROUND(CASE WHEN SUM(net_sales) = 0 THEN 0 ELSE (SUM({GROSS_PROFIT_EXPR}) / SUM(net_sales)) * 100 END, 2) AS margin_pct,
                ROUND(CASE WHEN SUM(net_sales) = 0 THEN 0 ELSE (SUM(ad_spend) / SUM(net_sales)) * 100 END, 2) AS acos_pct
            FROM v_monthly_sku_order_type_summary
            WHERE period_month = ?
            GROUP BY sku
            ORDER BY gross_profit DESC, net_sales DESC
            """,
        ),
        "order_details": (
            f"amazon_order_tracking_{selected_month}.csv",
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
        ),
        "alerts": (
            f"amazon_month_close_alerts_{selected_month}.csv",
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
            """,
        ),
    }

    if dataset not in datasets:
        conn.close()
        raise ValueError(f"Unsupported export dataset: {dataset}")

    filename, sql = datasets[dataset]
    rows = query_all(conn, sql, (selected_month,))
    conn.close()
    return filename, rows


def build_download_preview(
    month: str | None,
    dataset: str,
    sku_filter: str | None = None,
    order_id: str | None = None,
    group_by: str | None = None,
    keyword: str | None = None,
    order_type_filter: str | None = None,
    basis: str | None = None,
    limit: int = 50,
) -> dict:
    ensure_runtime_schema()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    months = get_months(conn)
    if not months:
        conn.close()
        raise RuntimeError("No previewable months available.")
    selected_month = month if month in months else months[0]
    ensure_month_download_allowed(conn, selected_month, dataset)

    if dataset == "order_type_rollup":
        rows = build_order_type_rollup_rows(
            conn,
            selected_month,
            group_by=group_by or "sku",
            keyword=keyword,
            order_type_filter=order_type_filter,
        )
        scope = {
            "mode": "month_rollup",
            "selected_month": selected_month,
            "group_by": group_by or "sku",
            "keyword": keyword or "",
            "order_type": order_type_filter or "all",
        }
    elif dataset == "allocation_audit":
        rows = build_allocation_audit_rows(
            conn,
            selected_month,
            keyword=keyword,
            order_type_filter=order_type_filter,
        )
        scope = {
            "mode": "allocation_audit",
            "selected_month": selected_month,
            "keyword": keyword or "",
            "order_type": order_type_filter or "all",
        }
    elif dataset == "order_line_profit":
        detail_basis = normalize_detail_basis(basis)
        detail_month = selected_month
        if order_id and month not in months:
            detail_month = None
        rows = build_order_line_profit_rows(conn, month=detail_month, sku_filter=sku_filter, order_id=order_id, keyword=keyword)
        rows = project_order_line_rows(rows, detail_basis)
        scope = {
            "mode": "order_detail" if order_id else "line_detail",
            "selected_month": detail_month,
            "sku": sku_filter or "",
            "order_id": order_id or "",
            "keyword": keyword or "",
            "basis": detail_basis,
        }
    else:
        conn.close()
        raise ValueError(f"Unsupported preview dataset: {dataset}")

    conn.close()
    columns = list(rows[0].keys()) if rows else []
    return {
        "dataset": dataset,
        "columns": columns,
        "rows": rows[:limit],
        "total_rows": len(rows),
        "preview_limit": limit,
        "scope": scope,
    }


def build_operations_payload(month: str | None = None) -> dict:
    ensure_runtime_schema()
    ensure_manual_templates()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    manual_files = []
    for key, config in MANUAL_FILE_CONFIG.items():
        path = MANUAL_DIR / config["filename"]
        headers, rows, encoding = read_csv_with_headers(path, config["headers"])
        manual_files.append(
            {
                "key": key,
                "label": config["label"],
                "filename": config["filename"],
                "headers": headers,
                "rows": rows,
                "row_count": len(rows),
                "encoding": encoding,
                "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds") if path.exists() else None,
            }
        )

    worklists = []
    for path in sorted(MANUAL_DIR.glob("worklist_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True):
        worklists.append(
            {
                "filename": path.name,
                "size": path.stat().st_size,
                "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
            }
        )

    source_files = []
    for path in sorted(ROOT.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not path.is_file():
            continue
        if path.suffix.lower() not in SOURCE_FILE_SUFFIXES:
            continue
        source_files.append(
            {
                "filename": path.name,
                "size": path.stat().st_size,
                "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
            }
        )
    source_files = source_files[:20]
    selected_month, pending_removal_controls = get_pending_removal_controls(conn, month)
    conn.close()

    with JOB_LOCK:
        job_snapshot = json.loads(json.dumps(MONTHLY_JOB, ensure_ascii=False))

    return {
        "selected_month": selected_month,
        "manual_files": manual_files,
        "worklists": worklists,
        "source_files": source_files,
        "pending_removal_controls": pending_removal_controls,
        "monthly_job": job_snapshot,
        "upload_targets": ["source", "attachment"],
    }


def save_manual_file(file_key: str, rows: list[dict]) -> dict:
    ensure_manual_templates()
    if file_key not in MANUAL_FILE_CONFIG:
        raise ValueError(f"Unsupported manual file: {file_key}")
    config = MANUAL_FILE_CONFIG[file_key]
    headers = config["headers"]
    normalized_rows = []
    for row in rows:
        normalized = {header: str(row.get(header, "")).strip() for header in headers}
        if not any(normalized.values()):
            continue
        normalized_rows.append(normalized)
    path = MANUAL_DIR / config["filename"]
    write_csv_rows(path, headers, normalized_rows)
    return {
        "file_key": file_key,
        "filename": config["filename"],
        "row_count": len(normalized_rows),
        "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
    }


def get_pending_removal_controls(conn: sqlite3.Connection, month: str | None) -> tuple[str | None, list[dict]]:
    months = get_months(conn)
    selected_month = month if month in months else (months[0] if months else month)
    if not selected_month:
        return None, []

    sql = """
        SELECT
            r.period_month,
            r.order_id,
            r.sku,
            COALESCE(ds.product_name_cn, '') AS product_name_cn,
            COALESCE(r.order_source, '') AS order_source,
            COALESCE(r.removal_order_type, '') AS removal_order_type,
            COALESCE(r.order_status, '') AS order_status,
            COALESCE(r.disposition, '') AS disposition,
            ROUND(SUM(COALESCE(r.requested_quantity, 0)), 2) AS requested_quantity,
            ROUND(SUM(COALESCE(r.cancelled_quantity, 0)), 2) AS cancelled_quantity,
            ROUND(SUM(COALESCE(r.disposed_quantity, 0)), 2) AS disposed_quantity,
            ROUND(SUM(COALESCE(r.shipped_quantity, 0)), 2) AS shipped_quantity,
            ROUND(SUM(COALESCE(r.removal_fee, 0)), 2) AS removal_fee,
            CASE
                WHEN lower(COALESCE(r.removal_order_type, '')) = 'disposal' OR COALESCE(r.disposed_quantity, 0) > 0
                    THEN 'disposal'
                ELSE 'transfer'
            END AS suggested_category
        FROM fact_removal_monthly_sku r
        LEFT JOIN (
            SELECT sku, MAX(product_name_cn) AS product_name_cn
            FROM dim_sku
            GROUP BY sku
        ) ds
          ON r.sku = ds.sku
        LEFT JOIN manual_removal_fee_controls c
          ON r.period_month = c.period_month
         AND r.order_id = c.order_id
        WHERE r.period_month = ?
          AND r.sku IS NOT NULL
          AND ABS(COALESCE(r.removal_fee, 0)) > 0.000001
          AND lower(COALESCE(r.removal_order_type, '')) <> 'disposal'
          AND COALESCE(r.disposed_quantity, 0) = 0
          AND c.order_id IS NULL
        GROUP BY
            r.period_month,
            r.order_id,
            r.sku,
            COALESCE(ds.product_name_cn, ''),
            COALESCE(r.order_source, ''),
            COALESCE(r.removal_order_type, ''),
            COALESCE(r.order_status, ''),
            COALESCE(r.disposition, '')
        ORDER BY SUM(COALESCE(r.removal_fee, 0)) DESC, r.order_id
    """
    return selected_month, query_all(conn, sql, (selected_month,))


def rebuild_reporting_views() -> None:
    etl_dir = str(ROOT / "etl")
    inserted_path = False
    if etl_dir not in sys.path:
        sys.path.insert(0, etl_dir)
        inserted_path = True
    try:
        namespace = runpy.run_path(str(ROOT / "etl" / "16_build_monthly_finance_views.py"))
        view_sql = namespace.get("VIEW_SQL")
        if not view_sql:
            raise RuntimeError("VIEW_SQL not found when rebuilding reporting views.")
        conn = sqlite3.connect(DB_PATH, timeout=30)
        try:
            conn.executescript(view_sql)
            conn.commit()
        finally:
            conn.close()
    finally:
        if inserted_path:
            try:
                sys.path.remove(etl_dir)
            except ValueError:
                pass


def run_close_checks(months: list[str]) -> None:
    for month in sorted({item for item in months if item}):
        result = subprocess.run(
            [sys.executable, str(ROOT / "etl" / "17_run_month_close_checks.py"), month],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError((result.stdout or "") + ("\n" + result.stderr if result.stderr else ""))


def run_close_checks_async(months: list[str]) -> None:
    target_months = sorted({item for item in months if item})
    if not target_months:
        return

    def worker() -> None:
        try:
            run_close_checks(target_months)
        except Exception as exc:  # noqa: BLE001
            print(f"[removal-controls] background close check failed for {target_months}: {exc}")

    threading.Thread(target=worker, daemon=True).start()


def save_removal_control_rows(rows: list[dict]) -> dict:
    ensure_manual_templates()
    config = MANUAL_FILE_CONFIG["manual_removal_fee_controls"]
    path = MANUAL_DIR / config["filename"]
    headers, existing_rows, _ = read_csv_with_headers(path, config["headers"])
    row_map: dict[tuple[str, str], dict] = {}
    for row in existing_rows:
        key = (str(row.get("period_month", "")).strip(), str(row.get("order_id", "")).strip())
        if key[0] and key[1]:
            row_map[key] = {header: str(row.get(header, "")).strip() for header in headers}

    upserted_rows: list[dict] = []
    affected_months: set[str] = set()
    for raw_row in rows:
        period_month = str(raw_row.get("period_month", "")).strip()
        order_id = str(raw_row.get("order_id", "")).strip()
        sku = str(raw_row.get("sku", "")).strip()
        removal_category = str(raw_row.get("removal_category", "")).strip().lower()
        accounting_treatment = str(raw_row.get("accounting_treatment", "")).strip().lower()
        source_note = str(raw_row.get("source_note", "")).strip() or "frontend_confirmation"
        if not period_month or not order_id:
            raise ValueError("period_month 和 order_id 不能为空。")
        if removal_category not in {"transfer", "disposal"}:
            raise ValueError(f"{order_id} 的 removal_category 无效。")
        if accounting_treatment not in {"expense", "capitalize"}:
            raise ValueError(f"{order_id} 的 accounting_treatment 必须填写 expense 或 capitalize。")
        record = {
            "period_month": period_month,
            "order_id": order_id,
            "sku": sku,
            "removal_category": removal_category,
            "accounting_treatment": accounting_treatment,
            "source_note": source_note,
        }
        row_map[(period_month, order_id)] = record
        upserted_rows.append(record)
        affected_months.add(period_month)

    ordered_rows = sorted(row_map.values(), key=lambda item: (item["period_month"], item["order_id"]))
    write_csv_rows(path, headers, ordered_rows)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_removal_fee_controls (
                control_id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_month TEXT NOT NULL,
                order_id TEXT NOT NULL,
                sku TEXT,
                removal_category TEXT NOT NULL,
                accounting_treatment TEXT NOT NULL,
                source_note TEXT,
                created_at TEXT,
                UNIQUE(period_month, order_id)
            )
            """
        )
        now_text = now_iso()
        conn.executemany(
            """
            INSERT INTO manual_removal_fee_controls (
                period_month, order_id, sku, removal_category, accounting_treatment, source_note, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(period_month, order_id) DO UPDATE SET
                sku = excluded.sku,
                removal_category = excluded.removal_category,
                accounting_treatment = excluded.accounting_treatment,
                source_note = excluded.source_note,
                created_at = excluded.created_at
            """,
            [
                (
                    row["period_month"],
                    row["order_id"],
                    row["sku"] or None,
                    row["removal_category"],
                    row["accounting_treatment"],
                    row["source_note"],
                    now_text,
                )
                for row in upserted_rows
            ],
        )
        conn.commit()
    finally:
        conn.close()

    run_close_checks_async(list(affected_months))

    return {
        "saved_count": len(upserted_rows),
        "affected_months": sorted(affected_months),
        "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
    }


def append_job_log(message: str) -> None:
    with JOB_LOCK:
        MONTHLY_JOB["logs"].append(message)
        MONTHLY_JOB["logs"] = MONTHLY_JOB["logs"][-200:]


def run_monthly_job(job_id: str, target_month: str, skip_init: bool) -> None:
    command = [sys.executable, str(ETL_RUNNER), target_month]
    if skip_init:
        command.append("--skip-init")
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")

    append_job_log(f"[{now_iso()}] Starting: {' '.join(command)}")
    process = subprocess.Popen(
        command,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )

    assert process.stdout is not None
    for line in process.stdout:
        append_job_log(line.rstrip())

    exit_code = process.wait()
    with JOB_LOCK:
        MONTHLY_JOB["exit_code"] = exit_code
        MONTHLY_JOB["finished_at"] = now_iso()
        MONTHLY_JOB["status"] = "success" if exit_code == 0 else "failed"


def start_monthly_job(target_month: str, skip_init: bool) -> dict:
    with JOB_LOCK:
        if MONTHLY_JOB["status"] == "running":
            raise RuntimeError("A monthly ETL job is already running.")
        job_id = datetime.now().strftime("%Y%m%d%H%M%S")
        MONTHLY_JOB.update(
            {
                "job_id": job_id,
                "status": "running",
                "target_month": target_month,
                "skip_init": skip_init,
                "started_at": now_iso(),
                "finished_at": None,
                "exit_code": None,
                "logs": [],
            }
        )

    thread = threading.Thread(target=run_monthly_job, args=(job_id, target_month, skip_init), daemon=True)
    thread.start()
    with JOB_LOCK:
        return json.loads(json.dumps(MONTHLY_JOB, ensure_ascii=False))


class DashboardHandler(SimpleHTTPRequestHandler):
    extensions_map = {
        **SimpleHTTPRequestHandler.extensions_map,
        **TEXT_TYPES,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(WEB_DIR), **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"", "/", "/index.html"}:
            self.send_text(render_index_html(), "text/html; charset=utf-8")
            return
        if parsed.path == "/runtime-app.js":
            self.send_text(RUNTIME_APP_JS, "application/javascript; charset=utf-8")
            return
        if parsed.path == "/api/dashboard":
            self.handle_dashboard(parsed.query)
            return
        if parsed.path == "/api/profit":
            self.handle_profit(parsed.query)
            return
        if parsed.path == "/api/operations":
            self.handle_operations()
            return
        if parsed.path == "/api/inventory":
            self.handle_inventory(parsed.query)
            return
        if parsed.path == "/api/receivables":
            self.handle_receivables(parsed.query)
            return
        if parsed.path == "/api/exceptions":
            self.handle_exceptions(parsed.query)
            return
        if parsed.path == "/api/month-close":
            self.handle_month_close(parsed.query)
            return
        if parsed.path == "/api/uploads":
            self.handle_uploads()
            return
        if parsed.path == "/api/order-lookup":
            self.handle_order_lookup(parsed.query)
            return
        if parsed.path == "/api/download-preview":
            self.handle_download_preview(parsed.query)
            return
        if parsed.path == "/api/export":
            self.handle_export(parsed.query)
            return
        if parsed.path == "/api/health":
            self.send_json({"ok": True})
            return
        return super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/upload":
            self.handle_upload(parsed.query)
            return
        if parsed.path == "/api/run-monthly":
            self.handle_run_monthly()
            return
        if parsed.path == "/api/manual/save":
            self.handle_manual_save()
            return
        if parsed.path == "/api/exception/save":
            self.handle_exception_save()
            return
        if parsed.path == "/api/inventory/save":
            self.handle_inventory_save()
            return
        if parsed.path == "/api/month-close/action":
            self.handle_month_close_action()
            return
        if parsed.path == "/api/removal-controls/save":
            self.handle_removal_controls_save()
            return
        self.send_json({"error": "Unsupported endpoint."}, status=HTTPStatus.NOT_FOUND)

    def guess_type(self, path: str) -> str:
        suffix = Path(path).suffix
        if suffix in TEXT_TYPES:
            return TEXT_TYPES[suffix]
        guessed = super().guess_type(path)
        if guessed == "application/octet-stream":
            return mimetypes.guess_type(path)[0] or guessed
        return guessed

    def log_message(self, fmt: str, *args) -> None:
        print(f"[{self.log_date_time_string()}] {fmt % args}")

    def read_body(self) -> bytes:
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        return self.rfile.read(content_length) if content_length > 0 else b""

    def read_json(self) -> dict:
        body = self.read_body()
        if not body:
            return {}
        return json.loads(body.decode("utf-8"))

    def handle_dashboard(self, query: str) -> None:
        params = parse_qs(query)
        month = params.get("month", [None])[0]
        try:
            payload = build_dashboard_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_operations(self) -> None:
        params = parse_qs(urlparse(self.path).query)
        month = params.get("month", [None])[0]
        try:
            payload = build_operations_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_receivables(self, query: str) -> None:
        params = parse_qs(query)
        month = params.get("month", [None])[0]
        try:
            payload = build_receivables_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_profit(self, query: str) -> None:
        params = parse_qs(query)
        month = params.get("month", [None])[0]
        try:
            payload = build_profit_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_inventory(self, query: str) -> None:
        params = parse_qs(query)
        month = params.get("month", [None])[0]
        try:
            payload = build_inventory_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_exceptions(self, query: str) -> None:
        params = parse_qs(query)
        month = params.get("month", [None])[0]
        try:
            payload = build_exceptions_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_month_close(self, query: str) -> None:
        params = parse_qs(query)
        month = params.get("month", [None])[0]
        try:
            payload = build_month_close_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_uploads(self) -> None:
        try:
            payload = build_uploads_payload()
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_order_lookup(self, query: str) -> None:
        params = parse_qs(query)
        order_id = params.get("order_id", [""])[0].strip()
        if not order_id:
            self.send_json({"error": "order_id is required."}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            payload = build_order_lookup_payload(order_id)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_download_preview(self, query: str) -> None:
        params = parse_qs(query)
        month = params.get("month", [None])[0]
        dataset = params.get("dataset", ["order_type_rollup"])[0]
        sku_filter = params.get("sku", [None])[0]
        order_id = params.get("order_id", [None])[0]
        group_by = params.get("group_by", [None])[0]
        keyword = params.get("keyword", [None])[0]
        order_type_filter = params.get("order_type", [None])[0]
        basis = params.get("basis", [None])[0]
        try:
            payload = build_download_preview(
                month,
                dataset,
                sku_filter=sku_filter,
                order_id=order_id,
                group_by=group_by,
                keyword=keyword,
                order_type_filter=order_type_filter,
                basis=basis,
            )
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_export(self, query: str) -> None:
        params = parse_qs(query)
        month = params.get("month", [None])[0]
        dataset = params.get("dataset", ["sku_details"])[0]
        sku_filter = params.get("sku", [None])[0]
        order_id = params.get("order_id", [None])[0]
        group_by = params.get("group_by", [None])[0]
        keyword = params.get("keyword", [None])[0]
        order_type_filter = params.get("order_type", [None])[0]
        basis = params.get("basis", [None])[0]
        try:
            filename, rows = export_dataset(
                month,
                dataset,
                sku_filter=sku_filter,
                order_id=order_id,
                group_by=group_by,
                keyword=keyword,
                order_type_filter=order_type_filter,
                basis=basis,
            )
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_csv(filename, rows)

    def handle_upload(self, query: str) -> None:
        params = parse_qs(query)
        target = params.get("target", ["source"])[0]
        filename_param = params.get("filename", [""])[0]
        try:
            ensure_manual_templates()
            filename = sanitize_filename(filename_param)
            body = self.read_body()
            if not body:
                raise ValueError("Upload body is empty.")
            if target == "source":
                destination = ROOT / filename
            elif target == "attachment":
                destination = ATTACHMENT_DIR / filename
            else:
                raise ValueError("Unsupported upload target.")
            destination.write_bytes(body)
            ensure_runtime_schema()
            conn = sqlite3.connect(DB_PATH, timeout=30)
            try:
                register_upload_batch(conn, target, filename, notes=str(destination))
                conn.commit()
            finally:
                conn.close()
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self.send_json(
            {
                "ok": True,
                "filename": filename,
                "target": target,
                "size": len(body),
                "saved_to": str(destination),
                "updated_at": datetime.fromtimestamp(destination.stat().st_mtime).isoformat(timespec="seconds"),
            }
        )

    def handle_run_monthly(self) -> None:
        try:
            payload = self.read_json()
            target_month = str(payload.get("target_month", "")).strip()
            skip_init = bool(payload.get("skip_init", True))
            if len(target_month) != 7 or target_month[4] != "-":
                raise ValueError("target_month must use YYYY-MM format.")
            job = start_monthly_job(target_month, skip_init)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except RuntimeError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        self.send_json({"ok": True, "job": job})

    def handle_manual_save(self) -> None:
        try:
            payload = self.read_json()
            file_key = str(payload.get("file_key", "")).strip()
            rows = payload.get("rows", [])
            if not isinstance(rows, list):
                raise ValueError("rows must be a list.")
            result = save_manual_file(file_key, rows)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True, "result": result})

    def handle_exception_save(self) -> None:
        try:
            payload = self.read_json()
            result = save_exception_case(payload)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True, "result": result})

    def handle_inventory_save(self) -> None:
        try:
            payload = self.read_json()
            result = save_inventory_movement(payload)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True, "result": result})

    def handle_month_close_action(self) -> None:
        try:
            payload = self.read_json()
            month = str(payload.get("month", "")).strip()
            action_code = str(payload.get("action_code", "")).strip()
            note = str(payload.get("note", "")).strip() or None
            if not month:
                raise ValueError('month is required.')
            result = perform_month_close_action(month, action_code, note)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True, "result": result})

    def handle_removal_controls_save(self) -> None:
        try:
            payload = self.read_json()
            rows = payload.get("rows", [])
            if not isinstance(rows, list) or not rows:
                raise ValueError("rows must be a non-empty list.")
            result = save_removal_control_rows(rows)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True, "result": result})

    def send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, body: str, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def send_csv(self, filename: str, rows: list[dict]) -> None:
        buffer = io.StringIO()
        if rows:
            writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        body = buffer.getvalue().encode("utf-8-sig")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    ensure_runtime_schema()
    ensure_manual_templates()
    parser = argparse.ArgumentParser(description="Amazon finance dashboard server")
    parser.add_argument("--host", default="127.0.0.1", help="bind host")
    parser.add_argument("--port", type=int, default=8000, help="bind port")
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"Dashboard running at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
