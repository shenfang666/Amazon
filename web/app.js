
const ORDER_TYPE_LABELS = {
  normal_sale: "正常销售",
  normal_refund: "正常退款",
  vine_sale: "Vine 销售",
  vine_refund: "Vine 退款",
  test_order_sale: "测评订单销售",
  test_order_refund: "测评订单退款",
  non_order_fee: "非订单费用"
};

const STATE_LABELS = {
  fully_settled_released: "已完全结算（已放款）",
  fully_settled_unreleased: "已完全结算（未放款）",
  cancelled_before_settlement: "结算前取消",
  shipped_waiting_settlement: "已发货待结算",
  pending_not_shipped: "待发货",
  refunded_after_settlement: "结算后退款",
  unknown: "未知"
};

const CLOSE_STATE_LABELS = {
  not_started: "未开始",
  waiting_upload: "待上传",
  processing: "处理中",
  mapping_pending: "映射待确认",
  exception_pending: "异常待处理",
  receivable_pending: "应收待完成",
  inventory_pending: "库存待完成",
  pnl_pending: "利润校验完成",
  approving: "审批中",
  closed: "已关账",
  reopened: "已重开"
};

const state = {
  activeTab: "overview",
  dashboard: null,
  receivables: null,
  exceptions: null,
  monthClose: null,
  uploads: null,
  operations: null,
  pendingAttachments: []
};

const params = new URLSearchParams(location.search);
const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));
const moneyFormatter = new Intl.NumberFormat("zh-CN", { style: "currency", currency: "USD", maximumFractionDigits: 2 });
const intFormatter = new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 0 });
const decFormatter = new Intl.NumberFormat("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const pctFormatter = new Intl.NumberFormat("zh-CN", { minimumFractionDigits: 1, maximumFractionDigits: 1 });

const monthSelect = $("#month-select");
const basisSelect = $("#granular-basis-select");

function esc(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function money(value) { return moneyFormatter.format(Number(value || 0)); }
function intf(value) { return intFormatter.format(Number(value || 0)); }
function dec(value) { return decFormatter.format(Number(value || 0)); }
function pct(value) { return `${pctFormatter.format(Number(value || 0))}%`; }
function dt(value) { return value ? String(value).replace("T", " ").slice(0, 19) : "-"; }
function basis() { return basisSelect?.value || "pnl"; }
function orderType(value) { return ORDER_TYPE_LABELS[value] || value || "-"; }
function settlementState(value) { return STATE_LABELS[value] || value || "-"; }
function closeState(value) { return CLOSE_STATE_LABELS[value] || value || "-"; }

function sClass(value) {
  if (["blocker", "failed", "exception_pending", "receivable_pending", "mapping_pending"].includes(value)) return "status-blocker";
  if (["warning", "running", "approving", "processing", "inventory_pending", "reopened"].includes(value)) return "status-warning";
  return "status-ok";
}

function chipClass(value) {
  if (["shipped_waiting_settlement", "pending_not_shipped", "mismatch", "pending"].includes(value)) return "chip warning";
  if (["refunded_after_settlement", "unknown", "blocker"].includes(value)) return "chip alert";
  return "chip";
}

function syncUrl() {
  const q = new URLSearchParams(location.search);
  q.set("tab", state.activeTab);
  if (monthSelect?.value) q.set("month", monthSelect.value);
  history.replaceState({}, "", `${location.pathname}?${q.toString()}`);
}

async function jget(url, options) {
  const response = await fetch(url, { cache: "no-store", ...options });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || `请求失败: ${response.status}`);
  return payload;
}

async function jpost(url, body) {
  return jget(url, {
    method: "POST",
    headers: { "Content-Type": "application/json; charset=utf-8" },
    body: JSON.stringify(body || {})
  });
}

async function uploadBinary(url, file) {
  const response = await fetch(url, { method: "POST", body: file });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || `上传失败: ${response.status}`);
  return payload;
}

async function downloadFile(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || `下载失败: ${response.status}`);
  }
  const blob = await response.blob();
  const disposition = response.headers.get("Content-Disposition") || "";
  const match = /filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i.exec(disposition);
  const name = decodeURIComponent((match && (match[1] || match[2])) || "download.csv");
  const href = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = href;
  link.download = name;
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(href), 1000);
}

function setStatus(target, message, kind = "ok") {
  const el = $(target);
  if (!el) return;
  el.className = `status-panel ${sClass(kind)}`;
  el.innerHTML = esc(message);
}

function table(target, cols, rows, empty = "暂无数据") {
  const el = $(target);
  if (!el) return;
  if (!rows || !rows.length) {
    el.innerHTML = `<div class="empty-state">${esc(empty)}</div>`;
    return;
  }
  el.innerHTML = `<div class="preview-table-wrap"><table class="preview-table"><thead><tr>${cols.map((c) => `<th>${esc(c.label)}</th>`).join("")}</tr></thead><tbody>${rows.map((row) => `<tr>${cols.map((c) => `<td class="${c.cls || ""}">${c.render ? c.render(row) : esc(row[c.key] ?? "-")}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
}

function setTab(tab) {
  state.activeTab = tab;
  $$(".tab-button").forEach((button) => button.classList.toggle("active", button.dataset.tab === tab));
  $$(".tab-panel").forEach((panel) => panel.classList.toggle("active", panel.dataset.tabPanel === tab));
  syncUrl();
}

function exportUrl(dataset, extra = {}) {
  const query = new URLSearchParams({ dataset });
  if (monthSelect?.value) query.set("month", monthSelect.value);
  Object.entries(extra).forEach(([key, value]) => {
    if (value !== null && value !== undefined && String(value).trim() !== "") query.set(key, String(value).trim());
  });
  return `/api/export?${query.toString()}`;
}

async function preview(dataset, extra = {}) {
  const query = new URLSearchParams({ dataset });
  if (monthSelect?.value) query.set("month", monthSelect.value);
  Object.entries(extra).forEach(([key, value]) => {
    if (value !== null && value !== undefined && String(value).trim() !== "") query.set(key, String(value).trim());
  });
  return jget(`/api/download-preview?${query.toString()}`);
}

function renderOverview() {
  const payload = state.dashboard;
  if (!payload) return;
  const overview = payload.overview || {};
  $("#hero-title").textContent = `${payload.selected_month} 财务概览`;
  $("#hero-description").textContent = `净销售 ${money(overview.net_sales)}，毛利 ${money(overview.gross_profit)}，毛利率 ${pct(overview.margin_pct)}，期末应收 ${money(overview.closing_receivable)}。`;
  $("#capabilities").innerHTML = (payload.capabilities || []).map((item) => `<span class="chip">${esc(item)}</span>`).join("");
  $("#close-status-card").innerHTML = `<div class="close-stat ${sClass(overview.business_state || overview.close_status)}"><p>当前月结状态</p><strong>${esc(closeState(overview.business_state || overview.close_status || "-"))}</strong><span>Blocker ${intf(overview.blocker_count || 0)} / Warning ${intf(overview.warning_count || 0)}</span><span>应收差异 ${money(overview.receivable_gap || 0)}</span></div>`;
  const metrics = [
    ["净销售", money(overview.net_sales), `订单数 ${intf(overview.order_count || 0)}`],
    ["毛利", money(overview.gross_profit), `毛利率 ${pct(overview.margin_pct || 0)}`],
    ["销量", dec(overview.units_sold), `SKU 数 ${intf(overview.sku_count || 0)}`],
    ["广告费", money(overview.ad_spend), `已结算率 ${pct(overview.recognized_rate || 0)}`],
    ["应收差异", money(overview.receivable_gap), `业务状态 ${closeState(overview.business_state || "-")}`]
  ];
  $("#metrics-grid").innerHTML = metrics.map((item) => `<article class="metric-card"><span class="metric-label">${esc(item[0])}</span><strong class="metric-value">${item[1]}</strong><small class="metric-meta">${esc(item[2])}</small></article>`).join("");
  const comparison = payload.comparison || {};
  const compareCards = [
    ["较上月净销售变化", money(comparison.net_sales_delta || 0)],
    ["较上月毛利变化", money(comparison.gross_profit_delta || 0)],
    ["毛利率变化", pct(comparison.margin_delta || 0)],
    ["订单数变化", intf(comparison.order_count_delta || 0)]
  ];
  $("#comparison-strip").innerHTML = compareCards.map((item) => `<article class="compare-card"><span>${esc(item[0])}</span><strong>${item[1]}</strong><small>${esc(comparison.previous_month || "无上月对比")}</small></article>`).join("");
  table("#top-skus", [
    { label: "SKU", key: "sku" },
    { label: "销量", render: (r) => dec(r.qty_sold), cls: "number-cell" },
    { label: "净销售", render: (r) => money(r.net_sales), cls: "number-cell" },
    { label: "广告费", render: (r) => money(r.ad_spend), cls: "number-cell" },
    { label: "毛利", render: (r) => money(r.gross_profit), cls: "number-cell" },
    { label: "毛利率", render: (r) => pct(r.margin_pct), cls: "number-cell" }
  ], payload.top_skus || [], "暂无 Top SKU 数据");
  const alerts = payload.alerts || [];
  $("#alerts").innerHTML = alerts.length ? alerts.map((item) => `<div class="status-panel ${sClass(item.severity)}"><strong>${esc(item.issue_code)}</strong><div class="table-muted">${esc(item.note || "-")}</div><div class="table-muted">值 ${esc(String(item.issue_value || item.metric_value || "-"))} / 时间 ${esc(dt(item.created_at))}</div></div>`).join("") : '<div class="empty-state">当前账期没有新的预警</div>';
  table("#fee-validations", [
    { label: "费用项", key: "fee_name" },
    { label: "报表金额", render: (r) => money(r.report_total), cls: "number-cell" },
    { label: "结算金额", render: (r) => money(r.settlement_total), cls: "number-cell" },
    { label: "差异", render: (r) => money(r.difference), cls: "number-cell" },
    { label: "状态", render: (r) => `<span class="${chipClass(r.status)}">${esc(r.status)}</span>` },
    { label: "说明", key: "note" }
  ], payload.fee_validations || [], "暂无费用校验数据");
}
function renderProfit() {
  const payload = state.dashboard;
  if (!payload) return;
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
  if (!$("#order-query-result").innerHTML.trim()) {
    $("#order-query-result").innerHTML = '<div class="empty-state">输入订单号后可查看该订单的完整财务明细。</div>';
  }
}

function renderReceivables() {
  const payload = state.receivables;
  if (!payload) return;
  const summary = payload.summary || {};
  $("#receivable-summary").innerHTML = [
    ["期初应收", money(summary.opening_receivable)],
    ["本期发生", money(summary.current_receivable)],
    ["本期回款", money(summary.current_receipts)],
    ["期末应收", money(summary.closing_receivable)],
    ["差异", money(summary.receivable_gap)]
  ].map((item) => `<article class="compare-card"><span>${esc(item[0])}</span><strong>${item[1]}</strong></article>`).join("");
  table("#receivable-balance-table", [
    { label: "账期", key: "period_month" },
    { label: "期初应收", render: (r) => money(r.opening_receivable), cls: "number-cell" },
    { label: "本期发生", render: (r) => money(r.current_receivable), cls: "number-cell" },
    { label: "本期回款", render: (r) => money(r.current_receipts), cls: "number-cell" },
    { label: "期末应收", render: (r) => money(r.closing_receivable), cls: "number-cell" },
    { label: "状态", key: "reconciliation_status" }
  ], payload.balances || [], "暂无应收余额数据");
  table("#receivable-aging-table", [
    { label: "账期", key: "period_month" },
    { label: "账龄桶", key: "aging_bucket" },
    { label: "期末应收", render: (r) => money(r.closing_receivable), cls: "number-cell" },
    { label: "差异", render: (r) => money(r.receivable_gap), cls: "number-cell" },
    { label: "状态", key: "reconciliation_status" }
  ], payload.aging || [], "暂无账龄数据");
  table("#receivable-unsettled-table", [
    { label: "账期", key: "period_month" },
    { label: "期末应收", render: (r) => money(r.closing_receivable), cls: "number-cell" },
    { label: "差异", render: (r) => money(r.receivable_gap), cls: "number-cell" },
    { label: "状态", key: "reconciliation_status" }
  ], payload.unsettled || [], "当前没有未结清应收");
  table("#receivable-unmatched-table", [
    { label: "回款日期", render: (r) => esc(String(r.receipt_date || "").slice(0, 10)) },
    { label: "回款参考", key: "receipt_reference" },
    { label: "结算单号", key: "settlement_id" },
    { label: "金额", render: (r) => money(r.receipt_amount), cls: "number-cell" },
    { label: "类型", key: "receipt_type" },
    { label: "备注", key: "memo" }
  ], payload.unmatched_receipts || [], "当前没有未匹配回款");
}

function renderExceptions() {
  const payload = state.exceptions;
  if (!payload) return;
  table("#generated-exception-table", [
    { label: "来源", key: "origin" },
    { label: "异常编码", key: "exception_code" },
    { label: "订单号/引用", render: (r) => esc(r.order_id || r.source_ref || "-") },
    { label: "SKU/值", render: (r) => esc(r.sku || "-") },
    { label: "金额", render: (r) => money(r.amount_value), cls: "number-cell" },
    { label: "系统建议", key: "system_suggestion" },
    { label: "备注", key: "note" }
  ], payload.generated_cases || [], "当前账期没有系统生成异常");
  table("#manual-exception-table", [
    { label: "ID", key: "exception_case_id" },
    { label: "异常编码", key: "exception_code" },
    { label: "异常类型", key: "exception_type" },
    { label: "订单号", key: "order_id" },
    { label: "SKU", key: "sku" },
    { label: "金额", render: (r) => money(r.amount_value), cls: "number-cell" },
    { label: "工单状态", key: "case_status" },
    { label: "审批状态", key: "approval_status" },
    { label: "备注", key: "note" }
  ], payload.manual_cases || [], "当前账期还没有人工工单");
  const attachments = state.pendingAttachments || [];
  $("#uploaded-attachments").innerHTML = attachments.length ? attachments.map((item) => `<div class="status-panel status-ok"><strong>${esc(item.file_name)}</strong><div class="table-muted">${esc(item.file_path)}</div></div>`).join("") : '<div class="empty-state">暂未上传待挂载附件</div>';
}
function renderMonthClose() {
  const payload = state.monthClose;
  if (!payload) return;
  $("#month-close-title").textContent = `${payload.selected_month} 月结中心`;
  $("#month-close-description").textContent = `当前状态 ${closeState(payload.current_state)}，推荐状态 ${closeState(payload.recommended_state)}，应收差异 ${money(payload.receivable_snapshot?.receivable_gap)}。${payload.inventory_note || ""}`;
  $("#month-close-actions").innerHTML = (payload.available_actions || []).map((item) => `<span class="chip">${esc(item)}</span>`).join("");
  $("#month-close-current").innerHTML = `<div class="close-stat ${sClass(payload.current_state)}"><p>当前状态</p><strong>${esc(closeState(payload.current_state || "-"))}</strong><span>检查结果 ${esc(payload.check_log?.close_status || "-")}</span><span>应收差异 ${money(payload.receivable_snapshot?.receivable_gap || 0)}</span></div>`;
  const prereqLabels = { mapping_completed: "映射已完成", issues_cleared: "异常已清理", receivable_balanced: "应收已对平", inventory_ready: "库存已接入" };
  $("#month-close-prereq").innerHTML = Object.entries(payload.prerequisites || {}).map(([key, value]) => `<article class="compare-card"><span>${esc(prereqLabels[key] || key)}</span><strong>${typeof value === "boolean" ? (value ? "已满足" : "未满足") : esc(String(value))}</strong></article>`).join("");
  table("#month-close-state-history", [
    { label: "状态", render: (r) => esc(closeState(r.state_code)) },
    { label: "来源", key: "state_source" },
    { label: "说明", key: "state_note" },
    { label: "操作人", key: "created_by" },
    { label: "时间", render: (r) => esc(dt(r.created_at)) }
  ], payload.state_history || [], "暂无状态历史");
  table("#month-close-action-history", [
    { label: "动作", key: "action_code" },
    { label: "from", render: (r) => esc(closeState(r.from_state)) },
    { label: "to", render: (r) => esc(closeState(r.to_state)) },
    { label: "结果", key: "action_result" },
    { label: "说明", key: "action_note" },
    { label: "操作人", key: "created_by" },
    { label: "时间", render: (r) => esc(dt(r.created_at)) }
  ], payload.action_history || [], "暂无动作历史");
}

function renderUploads() {
  const operations = state.operations || {};
  const uploads = state.uploads || {};
  table("#upload-source-files", [
    { label: "文件名", key: "filename" },
    { label: "大小", render: (r) => intf(r.size), cls: "number-cell" },
    { label: "更新时间", render: (r) => esc(dt(r.updated_at)) }
  ], uploads.source_files || operations.source_files || [], "暂无最近上传源文件");
  table("#upload-worklists", [
    { label: "文件名", key: "filename" },
    { label: "大小", render: (r) => intf(r.size), cls: "number-cell" },
    { label: "更新时间", render: (r) => esc(dt(r.updated_at)) }
  ], operations.worklists || [], "暂无工作清单");
  table("#upload-manual-files", [
    { label: "模板", key: "label" },
    { label: "文件名", key: "filename" },
    { label: "行数", render: (r) => intf(r.row_count), cls: "number-cell" },
    { label: "更新时间", render: (r) => esc(dt(r.updated_at)) }
  ], operations.manual_files || [], "暂无手工模板文件");
  setStatus("#run-monthly-status", `最近月跑状态：${operations.monthly_job?.status || "idle"} / 目标账期：${operations.monthly_job?.target_month || "-"}`, operations.monthly_job?.status || "ok");
}

function renderDownloadDefaults() {
  setStatus("#download-status", "可先预览再下载；如果当前账期仍有 blocker 或应收差异，受控报表会被系统阻止。", "ok");
  $("#download-preview-meta").innerHTML = '<div class="table-muted">选择数据集后可以在这里预览导出范围和样例数据。</div>';
  $("#download-preview-table").innerHTML = '<div class="empty-state">还没有预览内容</div>';
}

function renderPreview(payload) {
  const rows = payload.rows || [];
  const columns = payload.columns || [];
  $("#download-preview-meta").innerHTML = `<div class="table-muted">导出范围：${esc(JSON.stringify(payload.scope || {}))}</div><div class="table-muted">总行数 ${intf(payload.total_rows || 0)}，当前预览 ${intf(payload.preview_limit || rows.length)} 行</div>`;
  if (!rows.length || !columns.length) {
    $("#download-preview-table").innerHTML = '<div class="empty-state">当前条件下没有可预览的数据</div>';
    return;
  }
  $("#download-preview-table").innerHTML = `<div class="preview-table-wrap"><table class="preview-table"><thead><tr>${columns.map((col) => `<th>${esc(col)}</th>`).join("")}</tr></thead><tbody>${rows.map((row) => `<tr>${columns.map((col) => `<td>${esc(row[col] ?? "-")}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
}

async function runOrderLookup() {
  const orderId = ($("#order-query-input")?.value || "").trim();
  if (!orderId) {
    $("#order-query-result").innerHTML = '<div class="status-panel status-warning">请先输入订单号。</div>';
    return;
  }
  const payload = await jget(`/api/order-lookup?${new URLSearchParams({ order_id: orderId }).toString()}`);
  if (!payload.found) {
    $("#order-query-result").innerHTML = `<div class="status-panel status-warning">没有找到订单 ${esc(orderId)} 的财务明细。</div>`;
    return;
  }
  const summary = payload.summary || {};
  const headerHtml = [
    ["明细行数", intf(summary.line_count || 0)],
    ["涉及 SKU", intf(summary.sku_count || 0)],
    ["结算数量", dec(summary.settled_quantity || 0)],
    ["预计毛利", money(summary.estimated_gross_profit || 0)]
  ].map((item) => `<div class="lookup-card"><h4>${esc(item[0])}</h4><div>${item[1]}</div></div>`).join("");
  const cols = [
    { label: "订单号", key: "amazon_order_id" },
    { label: "日期", render: (r) => esc(String(r.purchase_date || "").slice(0, 10)) },
    { label: "SKU", key: "sku" },
    { label: "产品名", key: "product_name_cn" },
    { label: "订单类型", render: (r) => esc(orderType(r.order_type || "unknown")) },
    { label: "结算状态", render: (r) => esc(settlementState(r.settlement_state || "unknown")) },
    { label: "数量", render: (r) => dec(r.ordered_quantity), cls: "number-cell" },
    { label: "销售收入", render: (r) => money(Number(r.product_sales || 0) + Number(r.shipping_credits || 0) + Number(r.gift_wrap_credits || 0) + Number(r.promotional_rebates || 0)), cls: "number-cell" },
    { label: "平台费用", render: (r) => money(Number(r.selling_fees || 0) + Number(r.fba_fees || 0) + Number(r.other_transaction_fees || 0) + Number(r.marketplace_withheld_tax || 0)), cls: "number-cell" },
    { label: "货品成本", render: (r) => money(Number(r.allocated_product_cost || 0) + Number(r.allocated_inbound_freight_cost || 0)), cls: "number-cell" },
    { label: "预计毛利", render: (r) => money(r.estimated_gross_profit), cls: "number-cell" }
  ];
  const rows = payload.rows || [];
  const tableHtml = rows.length ? `<div class="preview-table-wrap"><table class="preview-table"><thead><tr>${cols.map((c) => `<th>${esc(c.label)}</th>`).join("")}</tr></thead><tbody>${rows.map((row) => `<tr>${cols.map((c) => `<td class="${c.cls || ""}">${c.render ? c.render(row) : esc(row[c.key] ?? "-")}</td>`).join("")}</tr>`).join("")}</tbody></table></div>` : '<div class="empty-state">没有明细数据</div>';
  $("#order-query-result").innerHTML = `<div class="lookup-stack"><div class="lookup-summary-grid">${headerHtml}</div>${tableHtml}</div>`;
}
function getExceptionPayload() {
  return {
    exception_case_id: ($("#exception-id")?.value || "").trim() || null,
    period_month: monthSelect?.value,
    exception_code: ($("#exception-code")?.value || "").trim(),
    exception_type: ($("#exception-type")?.value || "").trim(),
    source_platform: "amazon",
    order_id: ($("#exception-order-id")?.value || "").trim(),
    sku: ($("#exception-sku")?.value || "").trim(),
    amount_value: ($("#exception-amount")?.value || "").trim(),
    system_suggestion: ($("#exception-suggestion")?.value || "").trim(),
    user_choice: ($("#exception-choice")?.value || "").trim(),
    case_status: $("#exception-status")?.value || "open",
    approval_action: $("#exception-approval-action")?.value || "",
    note: ($("#exception-note")?.value || "").trim(),
    attachments: state.pendingAttachments
  };
}

async function saveExceptionCase() {
  const payload = getExceptionPayload();
  if (!payload.period_month) throw new Error("当前没有可用账期，无法保存工单。");
  const response = await jpost("/api/exception/save", payload);
  state.pendingAttachments = [];
  setStatus("#exception-form-status", `工单已保存，ID: ${response.result?.exception_case_id || "-"}`, "ok");
  state.exceptions = await jget(`/api/exceptions?${new URLSearchParams({ month: monthSelect.value }).toString()}`);
  renderExceptions();
}

async function uploadAttachment() {
  const file = $("#attachment-file")?.files?.[0];
  if (!file) throw new Error("请先选择附件文件。");
  const result = await uploadBinary(`/api/upload?${new URLSearchParams({ target: "attachment", filename: file.name }).toString()}`, file);
  state.pendingAttachments = [...state.pendingAttachments, { file_name: result.filename, file_path: result.saved_to }];
  $("#attachment-file").value = "";
  setStatus("#exception-form-status", `附件已上传：${result.filename}`, "ok");
  renderExceptions();
}

async function uploadSourceFile() {
  const file = $("#source-upload-file")?.files?.[0];
  if (!file) throw new Error("请先选择要上传的文件。");
  const target = $("#upload-target")?.value || "source";
  const filename = ($("#upload-filename")?.value || "").trim() || file.name;
  const result = await uploadBinary(`/api/upload?${new URLSearchParams({ target, filename }).toString()}`, file);
  $("#source-upload-file").value = "";
  $("#upload-filename").value = "";
  setStatus("#upload-status", `上传成功：${result.filename}`, "ok");
  if (state.activeTab === "uploads") {
    const month = monthSelect?.value || state.dashboard?.selected_month;
    const monthQuery = month ? `?${new URLSearchParams({ month }).toString()}` : "";
    const [uploads, operations] = await Promise.all([jget("/api/uploads"), jget(`/api/operations${monthQuery}`)]);
    state.uploads = uploads;
    state.operations = operations;
    renderUploads();
  }
}

async function startMonthlyRun() {
  const targetMonth = ($("#run-month-input")?.value || "").trim();
  const skipInit = ($("#run-skip-init")?.value || "true") === "true";
  const result = await jpost("/api/run-monthly", { target_month: targetMonth, skip_init: skipInit });
  setStatus("#run-monthly-status", `月跑已启动：${result.job?.target_month || targetMonth}`, "processing");
  resetSecondaryState();
  await loadDashboard(monthSelect?.value || targetMonth);
  if (state.activeTab === "uploads") {
    await loadTabData("uploads", true);
  }
}

async function monthCloseAction(actionCode) {
  const month = monthSelect?.value;
  const note = ($("#month-close-note")?.value || "").trim();
  if (!month) throw new Error("当前没有可用账期。");
  const response = await jpost("/api/month-close/action", { month, action_code: actionCode, note });
  state.monthClose = response.result;
  setStatus("#month-close-action-status", `动作执行成功：${actionCode}`, "ok");
  renderMonthClose();
  state.dashboard = await jget(`/api/dashboard?${new URLSearchParams({ month }).toString()}`);
  renderOverview();
  renderProfit();
}

async function loadDashboard(month) {
  const monthQuery = month ? `?${new URLSearchParams({ month }).toString()}` : "";
  const dashboard = await jget(`/api/dashboard${monthQuery}`);
  state.dashboard = dashboard;
  const months = dashboard.available_months || [];
  monthSelect.innerHTML = months.map((item) => `<option value="${esc(item)}">${esc(item)}</option>`).join("");
  monthSelect.value = dashboard.selected_month;
  syncUrl();
  const settlementStates = ["all", ...Object.keys(STATE_LABELS).filter((key) => (dashboard.order_details || []).some((row) => row.settlement_state === key))];
  $("#state-filter").innerHTML = settlementStates.map((item) => `<option value="${esc(item)}">${item === "all" ? "全部状态" : esc(settlementState(item))}</option>`).join("");
  $("#run-month-input").value = dashboard.selected_month;
  renderOverview();
  renderProfit();
  renderDownloadDefaults();
}

async function loadTabData(tab, force = false) {
  const month = monthSelect?.value || state.dashboard?.selected_month;
  const monthQuery = month ? `?${new URLSearchParams({ month }).toString()}` : "";
  if (tab === "receivables" && (force || !state.receivables)) {
    state.receivables = await jget(`/api/receivables${monthQuery}`);
    renderReceivables();
  }
  if (tab === "exceptions" && (force || !state.exceptions)) {
    state.exceptions = await jget(`/api/exceptions${monthQuery}`);
    renderExceptions();
  }
  if (tab === "month-close" && (force || !state.monthClose)) {
    state.monthClose = await jget(`/api/month-close${monthQuery}`);
    renderMonthClose();
  }
  if (tab === "uploads" && (force || !state.uploads || !state.operations)) {
    const [uploads, operations] = await Promise.all([jget("/api/uploads"), jget(`/api/operations${monthQuery}`)]);
    state.uploads = uploads;
    state.operations = operations;
    renderUploads();
  }
}

function resetSecondaryState() {
  state.receivables = null;
  state.exceptions = null;
  state.monthClose = null;
  state.uploads = null;
  state.operations = null;
}

function wireEvents() {
  $("#refresh-button")?.addEventListener("click", async () => {
    try {
      resetSecondaryState();
      await loadDashboard(monthSelect?.value);
      await loadTabData(state.activeTab, true);
    } catch (err) { alert(err.message); }
  });
  monthSelect?.addEventListener("change", async () => {
    try {
      resetSecondaryState();
      await loadDashboard(monthSelect.value);
      await loadTabData(state.activeTab, true);
    } catch (err) { alert(err.message); }
  });
  $$(".tab-button").forEach((button) => button.addEventListener("click", async () => {
    try {
      const tab = button.dataset.tab || "overview";
      setTab(tab);
      await loadTabData(tab);
    } catch (err) { alert(err.message); }
  }));
  $("#sku-search")?.addEventListener("input", () => renderProfit());
  $("#order-search")?.addEventListener("input", () => renderProfit());
  $("#state-filter")?.addEventListener("change", () => renderProfit());
  $("#order-query-button")?.addEventListener("click", () => runOrderLookup().catch((err) => { $("#order-query-result").innerHTML = `<div class="status-panel status-blocker">${esc(err.message)}</div>`; }));
  $("#order-query-export-button")?.addEventListener("click", () => {
    const orderId = $("#order-query-input").value.trim();
    if (!orderId) return setStatus("#download-status", "请先输入订单号。", "warning");
    downloadFile(exportUrl("order_line_profit", { order_id: orderId, basis: basis() })).catch((err) => setStatus("#download-status", err.message, "blocker"));
  });
  $("#export-sku-granular-button")?.addEventListener("click", () => {
    const keyword = $("#sku-search").value.trim();
    downloadFile(exportUrl("order_line_profit", keyword ? { sku: keyword, basis: basis() } : { basis: basis() })).catch((err) => alert(err.message));
  });
  $("#preview-rollup-button")?.addEventListener("click", async () => {
    try {
      renderPreview(await preview("order_type_rollup", { group_by: $("#download-group-by").value, keyword: $("#download-keyword").value, order_type: $("#download-order-type").value }));
      setStatus("#download-status", "汇总表预览已更新。", "ok");
    } catch (err) { setStatus("#download-status", err.message, "blocker"); }
  });
  $("#download-rollup-button")?.addEventListener("click", () => downloadFile(exportUrl("order_type_rollup", { group_by: $("#download-group-by").value, keyword: $("#download-keyword").value, order_type: $("#download-order-type").value })).catch((err) => setStatus("#download-status", err.message, "blocker")));
  $("#preview-order-detail-button")?.addEventListener("click", async () => {
    try {
      const orderId = $("#download-order-id").value.trim() || $("#order-query-input").value.trim();
      if (!orderId) return setStatus("#download-status", "请先输入订单号。", "warning");
      renderPreview(await preview("order_line_profit", { order_id: orderId, basis: basis() }));
      setStatus("#download-status", "订单明细预览已更新。", "ok");
    } catch (err) { setStatus("#download-status", err.message, "blocker"); }
  });
  $("#download-order-detail-button")?.addEventListener("click", () => {
    const orderId = $("#download-order-id").value.trim() || $("#order-query-input").value.trim();
    if (!orderId) return setStatus("#download-status", "请先输入订单号。", "warning");
    downloadFile(exportUrl("order_line_profit", { order_id: orderId, basis: basis() })).catch((err) => setStatus("#download-status", err.message, "blocker"));
  });
  $("#preview-allocation-audit-button")?.addEventListener("click", async () => {
    try {
      renderPreview(await preview("allocation_audit", { keyword: $("#download-keyword").value, order_type: $("#download-order-type").value }));
      setStatus("#download-status", "分摊审计预览已更新。", "ok");
    } catch (err) { setStatus("#download-status", err.message, "blocker"); }
  });
  $("#download-allocation-audit-button")?.addEventListener("click", () => downloadFile(exportUrl("allocation_audit", { keyword: $("#download-keyword").value, order_type: $("#download-order-type").value })).catch((err) => setStatus("#download-status", err.message, "blocker")));
  $("#save-exception-button")?.addEventListener("click", () => saveExceptionCase().catch((err) => setStatus("#exception-form-status", err.message, "blocker")));
  $("#upload-attachment-button")?.addEventListener("click", () => uploadAttachment().catch((err) => setStatus("#exception-form-status", err.message, "blocker")));
  $("#upload-source-button")?.addEventListener("click", () => uploadSourceFile().catch((err) => setStatus("#upload-status", err.message, "blocker")));
  $("#run-monthly-button")?.addEventListener("click", () => startMonthlyRun().catch((err) => setStatus("#run-monthly-status", err.message, "blocker")));
  $$(".month-close-action").forEach((button) => button.addEventListener("click", () => monthCloseAction(button.dataset.action).catch((err) => setStatus("#month-close-action-status", err.message, "blocker"))));
}

wireEvents();
setTab(params.get("tab") || "overview");
loadDashboard(params.get("month")).then(() => loadTabData(state.activeTab)).catch((err) => alert(err.message));
