# Amazon财务系统项目书（当前版）
**版本**: vCurrent-2026-03-14
**编制日期**: 2026-03-14
**适用范围**: Amazon平台财务系统一期当前落地版本
**项目目录**: `E:/输出/5. 财务系统搭建/1. Amazon平台/`

---

## 1. 项目定位
本项目用于沉淀 Amazon 平台最细颗粒度财务数据，并在此基础上输出订单结算追踪、SKU 财务明细、订单类型分析、费用拆分和月结核对结果，作为后续多平台财务数据底座的第一期。

当前版本不是概念规划版，而是已经完成数据库、ETL、人工控制表、月结检查和问题清单导出的可运行版本。

---

## 2. 当前建设目标
当前版本聚焦 4 件核心事情：

1. 准确识别订单是否已经进入结算，并可追踪未结算/已结算/已退款状态。
2. 准确区分正常销售、正常退款、Vine 销售、测评销售等订单类型。
3. 输出 `月份 × SKU × order_type × 费用项` 颗粒的财务明细。
4. 建立可复跑、可审计、可阻断的月结机制。

---

## 3. 当前范围
### 3.1 已纳入范围
- Amazon 订单报告
- Amazon 结算明细
- 广告报告
- FBA 仓储费
- 移除费
- 赔偿/报销
- 平台费用（订阅费、优惠券费、Vine Enrollment Fee）
- 测评订单台账
- SKU 主数据
- SKU 月度成本表
- 人工 Vine 分配表
- 人工 SKU 别名表

### 3.2 暂未完成范围
- 多平台共摊正式入账
- `manual_shared_costs` 自动分摊到最终明细
- PDF 月结对账闭环
- Vine 扣款月 / 归属月双口径
- Web 看板与前端展示

---

## 4. 当前系统结构
### 4.1 数据层
当前数据库为 `amazon_finance.db`，已建立以下核心对象：

- 主数据：`dim_sku`、`dim_sku_alias`、`dim_cost_monthly`、`dim_platform_monthly_base`
- 订单与结算：`fact_order_lines`、`fact_settlement_lines`、`bridge_orderline_settlement`
- 费用与补充事实：`fact_advertising_monthly_sku`、`fact_storage_monthly_sku`、`fact_removal_monthly_sku`、`fact_compensation_monthly_sku`、`fact_platform_fee_lines`、`fact_review_orders`
- 人工控制：`manual_vine_fee_allocations`、`manual_shared_costs`、`manual_sku_alias`
- 审计与阻断：`pending_mapping_queue`、`monthly_close_log`、`monthly_close_issue_detail`、`etl_run_log`、`file_import_log`

### 4.2 视图层
当前可直接出数的核心视图：

- `v_order_settlement_tracking`
- `v_monthly_sku_fee_pool`
- `v_monthly_sku_order_type_summary`

截至当前库状态：
- `v_order_settlement_tracking`：2087 行
- `v_monthly_sku_fee_pool`：85 行
- `v_monthly_sku_order_type_summary`：50 行

---

## 5. 当前 ETL 实施结果
当前一期 ETL 已形成完整月跑链路：

- `00_init_db.py`
- `01_load_sku_master.py`
- `02_load_sku_cost.py`
- `03_load_order_lines.py`
- `04_load_settlement_lines.py`
- `05_build_order_settlement_bridge.py`
- `06_load_review_orders.py`
- `07_classify_order_types.py`
- `08_load_advertising.py`
- `09_load_storage_fees.py`
- `10_load_removal_fees.py`
- `11_load_compensations.py`
- `12_load_platform_fees.py`
- `13_load_manual_controls.py`
- `14_load_platform_monthly_base.py`
- `16_build_monthly_finance_views.py`
- `17_run_month_close_checks.py`
- `18_export_manual_worklists.py`
- `99_run_monthly.py`

系统已支持以下稳定性能力：

- 重复月跑
- 原始行去重
- 历史年文件回退读取
- 中文编码 CSV 自动识别
- 手工控制表自动导入
- 阻断项与警告项自动记录
- 待处理清单自动导出到 `manual/`

---

## 6. 当前业务规则落地情况
### 6.1 订单粒度
已改为订单行粒度，不再使用订单号作为唯一业务主键。

### 6.2 结算状态识别
当前已落地的结算状态包括：
- `fully_settled_released`
- `fully_settled_unreleased`
- `cancelled_before_settlement`
- `shipped_waiting_settlement`
- `pending_not_shipped`
- `refunded_after_settlement`

以 `2026-02` 为例，Amazon 渠道订单状态分布为：
- `fully_settled_released`: 391
- `cancelled_before_settlement`: 61
- `shipped_waiting_settlement`: 25
- `pending_not_shipped`: 2
- `refunded_after_settlement`: 1

### 6.3 订单类型识别
当前已落地的订单类型包括：
- `normal_sale`
- `normal_refund`
- `vine_sale`
- `vine_refund`（逻辑预留，当前样本中未形成重点量）
- `review_sale`
- `review_refund`（逻辑预留，当前样本中未形成重点量）

真实数据验证结果：
- `2025-10`：`normal_sale=583`、`vine_sale=40`、`normal_refund=12`、`review_sale=6`
- `2025-12`：`normal_sale=1604`、`vine_sale=2`、`normal_refund=27`、`review_sale=2`
- `2026-02`：`normal_sale=428`、`normal_refund=4`

### 6.4 Vine 费用处理
当前已支持：
- Vine Enrollment Fee 从结算明细中抽取到平台费用层
- 人工 Vine 分配表导入
- 中文品名 / SKU / 别名三种方式录入 Vine 分配
- 同名 SKU 在 Vine 场景下按当月真实 Vine 结算自动消歧
- Vine 费用落到 `vine_sale` 明细中

已验证落地结果：
- `2025-10`：`NMN-Liver-90ct`、`NMN-ManEnergy-90ct`、`NMN-Skin Brightening-90ct`、`NMN-WOE&I-90ct` 已在 `vine_sale` 上承接 Vine 费用
- `2025-12`：`Neumina-AKK-30Capsule` 已在 `vine_sale` 上承接 Vine 费用

注意：当前仍采用“财务结算月”口径处理 Vine 费用，`扣款月 / 归属月` 双口径尚未正式实现。

---

## 7. 当前月结状态
截至当前数据库结果，以下月份已无 blocker：

| 月份 | 状态 | Blocker | Warning |
|---|---|---:|---:|
| 2025-10 | warning | 0 | 1 |
| 2025-12 | warning | 0 | 1 |
| 2026-02 | warning | 0 | 1 |

当前仅剩的 warning 为：
- `shipped_waiting_settlement`

说明：这代表部分订单已发货但尚未在当前导入时点进入结算，属于需要持续跟踪的正常观察状态，不属于系统阻断错误。

对应月结清单文件：
- `manual/worklist_month_close_2025-10.csv`
- `manual/worklist_month_close_2025-12.csv`
- `manual/worklist_month_close_2026-02.csv`

---

## 8. 当前已经解决的关键问题
当前版本已经解决以下此前阻断问题：

1. 多 SKU 订单被订单号主键压扁的问题。
2. 结算文件重复行导致金额重复入账的问题。
3. 历史移除费 / 赔偿文件只有年度文件时无法导入的问题。
4. 测评订单 SKU 无法通过结算或订单报告回挂的问题。
5. SKU 成本缺失、组合品误映射、人工别名无法复跑的问题。
6. Vine 分配 CSV 编码、正负号、中文品名映射的问题。
7. 月结异常只能看汇总、不能定位到明细的问题。

---

## 9. 当前尚未完成事项
以下事项属于二阶段重点，不影响当前 Amazon 一期月跑，但会影响后续财务深度和多平台扩展：

1. `manual_shared_costs` 尚未真正分摊进入 `v_monthly_sku_order_type_summary`。
2. 多平台共摊分母虽然已有结构预留，但尚未进入实跑口径。
3. PDF 月结金额尚未纳入正式对账阻断。
4. Vine 费用尚未拆分为 `扣款月` 与 `归属月` 双口径。
5. 仓储、平台费、共摊费用的更精细归因规则尚可继续增强。
6. 报表层目前以数据库视图和 CSV 工作清单为主，尚未形成正式 Web 看板。

---

## 10. 当前版本可支持的业务问题
当前版本已经可以回答以下类型问题：

- 某月某 SKU 的销售数量、销售金额、平台佣金、FBA 配送费、仓储费、广告费、赔偿收入、产品成本、头程成本分别是多少。
- 某订单是否已经进入结算，当前处于哪一种结算状态。
- 某月是否存在 Vine 订单、测评订单、正常订单，各自数量和金额分别是多少。
- 某月是否存在待结算订单、异常订单、需人工关注订单。
- 某月月结是否还有 blocker，问题明细是什么。

---

## 11. 当前版本的使用方式
### 11.1 月跑命令
```powershell
python etl/99_run_monthly.py 2026-02 --skip-init
```

### 11.2 人工控制文件
位于 `manual/` 目录：
- `manual_sku_aliases.csv`
- `manual_vine_fee_allocations.csv`
- `manual_shared_costs.csv`
- `manual_platform_monthly_base.csv`

### 11.3 月结输出
月跑完成后自动输出：
- `monthly_close_log`
- `monthly_close_issue_detail`
- `manual/worklist_month_close_YYYY-MM.csv`
- `manual/worklist_pending_aliases.csv`

---

## 12. 下一阶段实施重点
下一阶段建议按以下顺序推进：

1. 实现 `manual_shared_costs` 自动分摊，并进入 SKU/order_type 财务明细。
2. 增加 PDF 月结对账与 settlement / SKU 汇总三方核对。
3. 增加 Vine `扣款月 / 归属月` 双口径。
4. 增加面向经营层的利润表、应收平台款、SKU 经营分析报表。
5. 在 Amazon 一期稳定后，再复制同样方法扩展到其他平台。

---

## 13. 当前结论
截至 2026-03-14，Amazon 财务系统一期已经从“规划方案”进入“可运行、可复跑、可审计”的当前版本。

当前系统已经具备：
- 订单结算追踪能力
- SKU 财务明细能力
- Vine / 测评 / 正常订单分类能力
- 月结阻断与清单导出能力

当前系统尚未完全具备：
- 多平台共摊正式入账能力
- PDF 对账闭环能力
- Vine 双口径归因能力
- 最终看板层能力

因此，当前版本可以作为 Amazon 一期的正式财务底座继续推进，但仍应按第二阶段计划补齐共摊、对账和归因闭环。
