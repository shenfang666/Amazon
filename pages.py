"""HTML fragments injected into the base template by the Python page renderer.

This module exposes constants used by `server.py` to assemble the full page.
Each constant is self-contained HTML that is embedded at render time.
"""

from __future__ import annotations

from pathlib import Path

WEB_DIR: Path | None = None

# Tab button for the "库存核对" tab (renders <button data-tab="inventory">).
INVENTORY_TAB_HTML = """      <button class="tab-button" data-tab="inventory" type="button">库存核对</button>
"""
# Full tab panel for the "库存核对" tab.
# Hero header, 收发存 summary, exception list, manual-entry form,
# two-column snapshot/movement tables, and adjustment audit log.
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
# Governance sub-panel appended to the "上传管理" operations tab.
# Shows recent upload batches and active ETL rule versions.
UPLOAD_GOVERNANCE_HTML = """
        <section class="two-column reveal compact-top">
          <article class="panel"><div class="panel-head"><div><p class="eyebrow">批次治理</p><h3>最近上传批次</h3></div></div><div id="upload-batches" class="table-stack"></div></article>
          <article class="panel"><div class="panel-head"><div><p class="eyebrow">规则版本</p><h3>当前已启用规则</h3></div></div><div id="upload-rule-versions" class="table-stack"></div></article>
        </section>
"""
# Extra runtime JS injected after web/app.js.
# Initializes per-tab state (inventory, profit) and patches response shapes
# to handle missing/null values before render functions execute.
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
    const manualTable = $("#manual-exception-table");
    if (manualTable && !$("#manual-summary-container")) {
      const summaryContainer = document.createElement("div");
      summaryContainer.id = "manual-summary-container";
      summaryContainer.className = "table-stack";
      manualTable.insertAdjacentElement("beforebegin", summaryContainer);
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

  function renderManualSummary(payload) {
    const host = $("#manual-summary-container");
    if (!host) return;
    const manualSummary = payload?.manual_summary || [];
    const removalSummary = payload?.removal_control_summary || [];
    host.innerHTML = `
      <div class="panel inset" style="margin-bottom:12px;">
        <div class="panel-head" style="margin-bottom:10px;">
          <div>
            <p class="eyebrow">人工汇总</p>
            <h3 style="font-size:1.05rem;">人工处理记录汇总</h3>
          </div>
          <p class="panel-note">按类别聚合显示条数、金额和最后更新时间。</p>
        </div>
        <div id="manual-removal-summary-table" class="table-stack"></div>
        <div id="manual-category-summary-table" class="table-stack" style="margin-top:12px;"></div>
      </div>
    `;
    table("#manual-removal-summary-table", [
      { label: "账期", key: "period_month" },
      { label: "transfer", render: (r) => intf(r.transfer_count || 0), cls: "number-cell" },
      { label: "disposal", render: (r) => intf(r.disposal_count || 0), cls: "number-cell" },
      { label: "expense", render: (r) => intf(r.expense_count || 0), cls: "number-cell" },
      { label: "capitalize", render: (r) => intf(r.capitalize_count || 0), cls: "number-cell" },
      { label: "合计金额", render: (r) => money(r.total_amount || 0), cls: "number-cell" },
      { label: "最后更新", render: (r) => esc(dt(r.latest_updated)) }
    ], removalSummary, "暂无移除费人工处理汇总");
    table("#manual-category-summary-table", [
      { label: "异常类型", key: "exception_type" },
      { label: "异常编码", key: "exception_code" },
      { label: "总数", render: (r) => intf(r.count || 0), cls: "number-cell" },
      { label: "已解决", render: (r) => intf(r.resolved_count || 0), cls: "number-cell" },
      { label: "待处理", render: (r) => intf(r.open_count || 0), cls: "number-cell" },
      { label: "合计金额", render: (r) => money(r.total_amount || 0), cls: "number-cell" },
      { label: "最后更新", render: (r) => esc(dt(r.latest_updated)) }
    ], manualSummary.filter((row) => row.exception_code !== "pending_removal_control"), "暂无其他人工处理汇总");
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
    renderManualSummary(payload);
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


def render_index_html(web_dir: Path | None = None) -> str:
    """Render the index.html with injected panels.
    
    Args:
        web_dir: Path to web directory. If None, uses the global WEB_DIR.
    """
    effective_web_dir = web_dir if web_dir is not None else WEB_DIR
    if effective_web_dir is None:
        raise RuntimeError("WEB_DIR not initialized. Call set_web_dir() or pass web_dir parameter.")
    html = (effective_web_dir / "index.html").read_text(encoding="utf-8")
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


def set_web_dir(web_dir: Path) -> None:
    global WEB_DIR
    WEB_DIR = web_dir
