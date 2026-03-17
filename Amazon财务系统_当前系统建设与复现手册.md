# Amazon 财务系统当前系统建设与复现手册

## 1. 文档目的

本文档以当前代码与当前数据库行为为唯一准绳，固化 Amazon 财务系统一期截至目前的真实实现状态，目标是：

- 让系统可以被稳定复制、稳定复跑、稳定核对。
- 让后续维护者能够按同一套财务口径继续建设。
- 让财务数据的收入、成本、费用、赔偿、退款、月结校验逻辑不因人员变化而失真。

本文档描述的是当前工作区 `E:\输出\5. 财务系统搭建\1. Amazon平台` 内已经实现并验证过的系统，不描述尚未落地的理想方案。

---

## 2. 当前系统边界

### 2.1 系统形态

当前系统是一个基于 `SQLite + Python ETL + 轻量 Web Server + 浏览器前端` 的单机财务系统。

系统入口文件：

- `app.py`

前端文件：

- `web/index.html`
- `web/app.js`
- `web/styles.css`

数据库文件：

- `amazon_finance.db`

ETL 目录：

- `etl/`

人工控制目录：

- `manual/`

### 2.2 当前系统不是

当前系统不是多用户 SaaS，不带用户权限，不带账号登录，不带外部 API 网关，不带容器化部署，也没有把共享成本、PDF 对账闭环做完。

---

## 3. 当前目录结构

### 3.1 核心目录

- `etl/`: 所有初始化、装载、分类、汇总、月结检查、工作清单导出脚本
- `manual/`: 人工控制表与工作清单导出
- `web/`: 浏览器前端页面

### 3.2 关键源文件

当前系统按固定命名规则读取源文件：

- `99_SKU_MASTER.xlsx`
- `98_SKU Cost Table_Amazon.xlsx`
- `2_Order report_AmazonYYYYMM.txt`
- `3_Settlement Details_AmazonYYYYMM.csv`
- `4_Ad Spend_AmazonYYYYMM.xlsx`
- `5_Test Orders_Amazon.xlsx`
- `6_FBA Storage Fees_AmazonYYYYMM.csv`
- `8_Removal Fees_AmazonYYYYMM.csv`
- `8_Removal Fees_AmazonYYYY.csv`
- `9_Reimbursements_AmazonYYYYMM.csv`
- `9_Reimbursements_AmazonYYYY.csv`

部分文件支持年度文件回退逻辑，详见后文。

---

## 4. 当前数据库模型

### 4.1 日志与运行控制表

- `schema_version`
- `etl_run_log`
- `file_import_log`
- `monthly_close_issue_detail`
- `monthly_close_log`

### 4.2 维表

- `dim_sku`
- `dim_sku_alias`
- `manual_sku_alias`
- `dim_cost_monthly`
- `dim_platform_monthly_base`

### 4.3 事实表

- `fact_order_lines`
- `fact_settlement_lines`
- `bridge_orderline_settlement`
- `fact_review_orders`
- `fact_advertising_monthly_sku`
- `fact_storage_monthly_sku`
- `fact_removal_monthly_sku`
- `fact_compensation_monthly_sku`
- `fact_platform_fee_lines`

### 4.4 人工控制表

- `manual_vine_fee_allocations`
- `manual_shared_costs`
- `manual_removal_fee_controls`

### 4.5 待处理与人工调整

- `pending_mapping_queue`
- `manual_adjustment_log`

---

## 5. ETL 总体链路

当前月跑由 `etl/99_run_monthly.py` 驱动，脚本执行顺序固定如下：

1. `00_init_db.py`
2. `01_load_sku_master.py`
3. `02_load_sku_cost.py`
4. `03_load_order_lines.py`
5. `04_load_settlement_lines.py`
6. `05_build_order_settlement_bridge.py`
7. `06_load_review_orders.py`
8. `07_classify_order_types.py`
9. `08_load_advertising.py`
10. `09_load_storage_fees.py`
11. `10_load_removal_fees.py`
12. `11_load_compensations.py`
13. `12_load_platform_fees.py`
14. `13_load_manual_controls.py`
15. `14_load_platform_monthly_base.py`
16. `16_build_monthly_finance_views.py`
17. `17_run_month_close_checks.py`
18. `18_export_manual_worklists.py`

说明：

- `00_init_db.py` 可以通过 `--skip-init` 跳过。
- 其余步骤顺序不应擅自调整，因为后续步骤依赖前一步的结果。

---

## 6. 各 ETL 步骤当前真实逻辑

### 6.1 `01_load_sku_master.py`

输入：

- `99_SKU_MASTER.xlsx`

逻辑：

- 读取首个工作表，从第 2 行开始。
- 只取 `sku` 和 `product_name_cn`。
- `sku` 为空则跳过。
- `product_name_cn` 为空则报错，不允许继续。
- 写入 `dim_sku`。
- 同时把 `product_name_cn.lower()` 写入 `dim_sku_alias`，`alias_type='product_name_cn'`。

结果：

- 建立 SKU 主数据。
- 建立产品名到 SKU 的自动映射基础。

### 6.2 `02_load_sku_cost.py`

输入：

- `98_SKU Cost Table_Amazon.xlsx`
- 工作表：`2.9 SKU Cost Table`

逻辑：

- 从第 3 行起读取。
- 提取 `cost_month`、`product_name`、`product_unit_cost`、`inbound_unit_cost`。
- 产品名优先走 `manual_sku_alias`，否则走 `dim_sku_alias`。
- 找不到 SKU 的成本行不入 `dim_cost_monthly`，写入 `pending_mapping_queue`。

结果：

- 成本表以 `sku + cost_month` 为主键。

### 6.3 `03_load_order_lines.py`

输入：

- `2_Order report_AmazonYYYYMM.txt`

逻辑：

- 以制表符读取。
- 每行做稳定哈希，去除完全重复行。
- 仅 `sales-channel == 'Amazon.com'` 的行记为 `is_amazon_channel = 1`。
- 初次写入时 `settlement_state = NULL`，后续由桥接脚本更新。

结果：

- 原始订单明细进入 `fact_order_lines`。

### 6.4 `04_load_settlement_lines.py`

输入：

- `3_Settlement Details_AmazonYYYYMM.csv`

逻辑：

- 自动检测 header 行，识别标记为 `"date/time","settlement id","type","order id"`。
- 每行做稳定哈希，去除完全重复行。
- 写入收入口径、平台费用、税费、运费、优惠、总额等全部 settlement 字段。
- `marketplace` 为 `Amazon.com` / `amazon.com` 时记为 Amazon 渠道。
- 同源 settlement 文件会先删旧再重载。

结果：

- 原始 settlement 明细进入 `fact_settlement_lines`。

### 6.5 `05_build_order_settlement_bridge.py`

逻辑：

- 订单行与 settlement 行按 `amazon_order_id + sku + 月份` 精确匹配。
- 匹配成功后写入 `bridge_orderline_settlement`。

`settlement_state` 判定规则：

- `Cancelled` 且无 bridge：`cancelled_before_settlement`
- `Pending` 或 `Shipping` 且无 bridge：`pending_not_shipped`
- 无 bridge 且不是以上状态：`shipped_waiting_settlement`
- 同时存在 `Order` 和 `Refund` settlement：`refunded_after_settlement`
- 有 bridge 且全部 `transaction_status='Released'`：`fully_settled_released`
- 有 bridge 但未全部 Released：`fully_settled_unreleased`
- 其余情况：`exception_needs_review`

### 6.6 `06_load_review_orders.py`

输入：

- `5_Test Orders_Amazon.xlsx`
- 工作表：`Sheet1`

逻辑：

- 以 Excel 读取测评订单。
- SKU 解析优先级：
  1. `manual_sku_alias`
  2. `dim_sku_alias`
  3. 同订单号在订单/结算中的唯一 SKU
  4. 产品名模糊匹配唯一 SKU
- 不能唯一映射的行写入 `pending_mapping_queue`。

### 6.7 `07_classify_order_types.py`

当前 order type 分类规则完全如下：

- `vine_sale`
  - `transaction_type='Order'`
  - Amazon 渠道
  - 有 `order_id`
  - `product_sales > 0`
  - `ABS(product_sales - ABS(promotional_rebates)) < 0.01`
- `vine_refund`
  - `transaction_type='Refund'`
  - `order_id` 出现在已判定为 `vine_sale` 的订单中
- `review_sale`
  - `transaction_type='Order'`
  - `order_id` 出现在 `fact_review_orders`
  - 且此前未被判定
- `review_refund`
  - `transaction_type='Refund'`
  - `order_id` 出现在 `fact_review_orders`
  - 且此前未被判定
- `normal_refund`
  - `transaction_type='Refund'`
  - 且此前未被判定
- `normal_sale`
  - `transaction_type='Order'`
  - 且此前未被判定

### 6.8 `08_load_advertising.py`

输入：

- `4_Ad Spend_AmazonYYYYMM.xlsx`

工作表优先级：

- 明细表优先：
  - `Sponsored_Products_Advertised_p`
  - `商品推广_推广的商品_报告`
- 明细表无可用数据时回退：
  - `Sheet2`

SKU 解析规则：

- 有 `Advertised SKU` / `广告SKU` 则直接用。
- 没有则用产品名映射唯一 SKU。

结果：

- 按 `period_month + sku` 聚合入 `fact_advertising_monthly_sku`。

### 6.9 `09_load_storage_fees.py`

输入：

- `6_FBA Storage Fees_AmazonYYYYMM.csv`

SKU 解析规则：

- 优先 `fnsku == sku`
- 否则在订单表中找唯一同名 SKU
- 否则用 `asin` 在订单表中找唯一 SKU
- 再否则用 `asin` 联合 settlement/订单信息找唯一 SKU

金额逻辑：

- 原始仓储费采用：
  - `estimated_monthly_storage_fee`
  - `incentive_fee_amount`
- 后续汇总视图中的净仓储费口径为：
  - `estimated_monthly_storage_fee - incentive_fee_amount`

### 6.10 `10_load_removal_fees.py`

输入：

- 优先月文件：`8_Removal Fees_AmazonYYYYMM.csv`
- 无月文件时回退年文件：`8_Removal Fees_AmazonYYYY.csv`
- 使用年文件时按 `request-date[:7] == target_month` 过滤

写入字段：

- `order_id`
- `order_source`
- `removal_order_type`
- `order_status`
- `sku`
- `fnsku`
- `disposition`
- `requested_quantity`
- `cancelled_quantity`
- `disposed_quantity`
- `shipped_quantity`
- `in_process_quantity`
- `removal_fee`

注意：

- 此脚本会自动补齐 `fact_removal_monthly_sku` 缺失字段。
- 此脚本也会确保 `manual_removal_fee_controls` 表存在。

### 6.11 `11_load_compensations.py`

输入：

- 优先月文件：`9_Reimbursements_AmazonYYYYMM.csv`
- 无月文件时回退年文件：`9_Reimbursements_AmazonYYYY.csv`

字段：

- `amazon_order_id`
- `sku`
- `reason`
- `amount_total`
- `quantity_reimbursed_cash`
- `quantity_reimbursed_inventory`

### 6.12 `12_load_platform_fees.py`

平台费用抽取只提取以下四类：

- `subscription_fee`
  - `transaction_type='Service Fee'`
  - `transaction_subtype='Subscription'`
- `coupon_participation_fee`
  - `transaction_type='Amazon Fees'`
  - `transaction_subtype='Coupon Participation Fee'`
- `coupon_performance_fee`
  - `transaction_type='Amazon Fees'`
  - `transaction_subtype='Coupon Performance Based Fee'`
- `vine_enrollment_fee_source`
  - `transaction_type='Amazon Fees'`
  - `transaction_subtype='Vine Enrollment Fee'`

说明：

- 原始金额直接从 settlement `total` 写入 `fact_platform_fee_lines.amount_total`。
- 后续在汇总视图中统一转为正向费用口径：`-amount_total`。

### 6.13 `13_load_manual_controls.py`

当前支持以下人工控制文件：

- `manual_sku_aliases.csv`
- `manual_vine_fee_allocations.csv`
- `manual_shared_costs.csv`
- `manual_platform_monthly_base.csv`
- `manual_removal_fee_controls.csv`

当前真实作用：

- `manual_sku_aliases.csv`: 参与 SKU 映射
- `manual_vine_fee_allocations.csv`: 参与 Vine fee 分摊
- `manual_platform_monthly_base.csv`: 可写入平台月基表
- `manual_removal_fee_controls.csv`: 控制调仓 removal 费用入成本还是费用化

当前未进入最终利润明细的控制表：

- `manual_shared_costs.csv`

### 6.14 `14_load_platform_monthly_base.py`

逻辑：

- 从 `fact_order_lines` 与 `v_monthly_sku_order_type_summary` 推导平台月度基表。
- 当前一期中该表主要作为平台层预留底座。

### 6.15 `16_build_monthly_finance_views.py`

这是整个财务口径的核心汇总层，创建 3 个关键视图：

- `v_order_settlement_tracking`
- `v_monthly_platform_fee_pool`
- `v_monthly_sku_fee_pool`
- `v_monthly_sku_order_type_summary`

详细规则见第 7 节。

### 6.16 `17_run_month_close_checks.py`

当前会写入 `monthly_close_issue_detail` 与 `monthly_close_log`。

详细校验规则见第 10 节。

### 6.17 `18_export_manual_worklists.py`

导出：

- `manual/worklist_month_close_YYYY-MM.csv`
- `manual/worklist_pending_aliases.csv`

---

## 7. 当前财务核心规则

### 7.1 利润公式

当前系统统一利润表达式定义在 `app.py`：

```sql
gross_profit =
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
```

### 7.2 费用正负号统一规则

当前系统已经统一为：

- 费用支出为正数
- 费用冲回或返还为负数
- 利润计算统一按收入减费用处理

具体体现：

- settlement 中 `selling_fees`、`fba_fees`、`other_transaction_fees`、`marketplace_withheld_tax` 原始通常是负数
- 进入汇总视图时统一变为正向费用：
  - `SUM(-selling_fees)`
  - `SUM(-fba_fees)`
  - `SUM(-other_transaction_fees)`
  - `SUM(-marketplace_withheld_tax)`
- 若退款行出现 fee reversal，则最终在汇总中会表现为负费用

### 7.3 收入口径

当前净销售额定义：

```sql
net_sales = product_sales + shipping_credits + gift_wrap_credits + promotional_rebates
```

说明：

- 促销折扣 `promotional_rebates` 在原始 settlement 中通常为负值
- 买家代收运费 `shipping_credits` 已纳入收入口径
- Gift Wrap `gift_wrap_credits` 已纳入收入口径

### 7.4 退款数量规则

当前退款数量口径：

```sql
qty_sold = SUM(CASE WHEN transaction_type IN ('Order', 'Refund') THEN quantity ELSE 0 END)
```

说明：

- 退款单数量不再被错误记为 0

### 7.5 退款订单分摊规则

当前退款订单不分摊以下项目：

- `storage_fees`
- `removal_fees`
- `ad_spend`
- `review_cost`
- `vine_fee`
- `subscription_fee`
- `coupon_participation_fee`
- `coupon_performance_fee`
- `product_cost`
- `inbound_cost`

退款订单当前仅体现：

- settlement 本身的收入、折扣、费用、税
- 与该退款订单直接关联的赔偿

### 7.6 赔偿规则

赔偿分两类：

1. 可直接挂到退款订单的赔偿
2. 无法直接挂到退款订单的赔偿

规则如下：

- 若 `fact_compensation_monthly_sku` 能按 `period_month + amazon_order_id + sku` 匹配到退款订单，则分配到对应 refund order type
- 若不能匹配到 refund，则进入 `unassigned_compensation`
- 这部分会落入 `non_order_fee` 行，而不是强行分配到销售订单

当前设计原则：

- 宁可保守挂在 `non_order_fee`
- 不把无法证明归属的赔偿强行分摊进订单利润

### 7.7 平台费用分摊规则

平台月度费用池来自 `v_monthly_platform_fee_pool`：

- `subscription_fee`
- `coupon_participation_fee`
- `coupon_performance_fee`

分摊规则：

- 按 SKU 月度 `abs_net_sales` 占全月 `abs_net_sales` 的比例分到 SKU
- 再按该 SKU 各 `order_type` 的 `alloc_share` 分到订单类型
- refund 不参与这类 pooled fee 分摊

### 7.8 Review 与 Vine 分摊规则

`review_cost`：

- 只在 `review_sale` 类订单内部分摊
- 分摊基础为 review 类型的 `abs_net_sales`

`vine_fee`：

- 来源于 `manual_vine_fee_allocations`
- 只在 `vine_sale` 类订单内部分摊
- 优先按 `qty_sold` 分摊
- 若总数量为 0，则按记录数平均分摊

### 7.9 仓储费规则

SKU 月度仓储费池：

```sql
storage_fees = SUM(estimated_monthly_storage_fee - incentive_fee_amount)
```

分摊逻辑：

- 有 eligible sales 的 SKU：按该 SKU 下订单类型 `alloc_share` 分摊
- 没有 eligible sales 的 SKU：不丢失，进入 `non_order_fee`

目的：

- 月度汇总金额必须与仓储原始报表合计一致
- 不允许因为当月没有销售行而漏掉仓储费

### 7.10 Removal 费用规则

Removal 费用在当前系统中分成 3 个口径字段：

- `removal_fees`
- `removal_fee_capitalized`
- `removal_fee_unclassified`

判定逻辑：

1. 明确报废

- `removal_order_type='disposal'`
- 或 `disposed_quantity > 0`
- 进入 `removal_fees`

2. 非报废且人工指定 `expense`

- 进入 `removal_fees`

3. 非报废且人工指定 `capitalize`

- 进入 `removal_fee_capitalized`

4. 非报废且未人工确认

- 进入 `removal_fee_unclassified`

系统行为：

- `removal_fees` 参与利润
- `removal_fee_capitalized` 不直接进入当期利润
- `removal_fee_unclassified` 不进入利润，但会进入 `non_order_fee` 行展示，并触发月结 blocker

### 7.11 Removal 必填确认规则

当前系统要求以下 removal 必须确认：

- 当月有 `removal_fee`
- `removal_order_type <> 'disposal'`
- `disposed_quantity = 0`
- 且 `manual_removal_fee_controls` 中没有同 `period_month + order_id` 的确认记录

必须确认字段：

- `removal_category`
  - `transfer`
  - `disposal`
- `accounting_treatment`
  - `expense`
  - `capitalize`

### 7.12 `non_order_fee` 行的含义

`v_monthly_sku_order_type_summary` 中可能出现 `order_type='non_order_fee'`。

其含义不是订单，而是“未能安全分配到某订单类型，但必须保留在月度口径中”的财务项目，主要包括：

- 未直连到退款的赔偿
- 无 eligible sales 的 SKU 仓储费
- 无 eligible sales 的 SKU removal 费用
- `removal_fee_capitalized`
- `removal_fee_unclassified`

---

## 8. 当前浏览器前端模块

### 8.1 页面入口

启动命令：

```powershell
python -u app.py --host 127.0.0.1 --port 8765
```

访问地址：

- `http://127.0.0.1:8765`

### 8.2 当前页签

- `总览`
- `SKU 经营`
- `订单追踪`
- `数据下载`
- `操作中心`

### 8.3 总览

展示：

- 月度净销售、毛利、毛利率、订单数
- 月度趋势
- 月结时间线
- 订单类型结构
- 结算状态结构
- 数据规模

### 8.4 SKU 经营

展示：

- Top SKU
- SKU 全量经营明细
- 支持搜索 SKU
- 支持下载当前月份订单行级最细颗粒度明细

### 8.5 订单追踪

展示：

- 月结告警
- 未完全闭环订单
- 订单号直查
- 全量订单追踪

订单号直查支持：

- 输入订单号
- 查询订单收入、成本、费用、数量
- 导出该订单最细明细

### 8.6 数据下载

支持统一筛选下载：

- 汇总维度
  - `sku`
  - `product_name`
  - `all`
- 订单类型筛选
- SKU / 产品名关键词
- 订单号

支持动作：

- 预览月度累计统计
- 下载月度累计统计
- 预览订单号明细
- 下载订单号明细

### 8.7 操作中心

支持：

- 源文件上传
- 触发月跑
- 查看月跑日志
- 浏览与保存 `manual/*.csv`
- 查看工作清单
- 前台确认 removal 必填项

当前 removal 前台确认面板的页面标签是英文：

- `Required Confirmations`
- `Removal Cost Confirmation`
- `Save Confirmations`

这是当前页面真实状态，文档以此为准。

---

## 9. 当前后端接口

### 9.1 `GET /api/dashboard`

用途：

- 返回首页驾驶舱数据

关键输出：

- `selected_month`
- `available_months`
- `overview`
- `comparison`
- `trend`
- `close_timeline`
- `order_types`
- `settlement_states`
- `top_skus`
- `sku_details`
- `alerts`
- `watch_orders`
- `order_details`
- `source_counts`

### 9.2 `GET /api/order-lookup?order_id=...`

用途：

- 按订单号查询订单级经营明细

关键输出：

- `found`
- `summary`
- `rows`

### 9.3 `GET /api/download-preview`

支持数据集：

- `order_type_rollup`
- `order_line_profit`

### 9.4 `GET /api/export`

支持数据集：

- `sku_details`
- `order_details`
- `alerts`
- `order_type_rollup`
- `order_line_profit`

### 9.5 `GET /api/operations`

用途：

- 返回操作中心数据

关键输出：

- `selected_month`
- `manual_files`
- `worklists`
- `source_files`
- `pending_removal_controls`
- `monthly_job`

### 9.6 `POST /api/upload`

用途：

- 上传源文件到项目根目录

### 9.7 `POST /api/run-monthly`

用途：

- 触发 `etl/99_run_monthly.py`

### 9.8 `POST /api/manual/save`

用途：

- 保存 `manual/*.csv`

### 9.9 `POST /api/removal-controls/save`

用途：

- 保存前台填写的 removal 必填确认项

真实行为：

1. 写入 `manual/manual_removal_fee_controls.csv`
2. Upsert 到数据库 `manual_removal_fee_controls`
3. 重建 `16_build_monthly_finance_views.py` 视图
4. 对涉及月份重跑 `17_run_month_close_checks.py`

---

## 10. 当前月结检查规则

当前 `17_run_month_close_checks.py` 会写入 blocker / warning。

### 10.1 blocker 条件

1. `pending_mapping_queue` 有 pending 记录
2. 有销量但 `product_unit_cost = 0`
3. Vine source fee 与 `manual_vine_fee_allocations` 不一致
4. 缺少 removal 必填确认项
5. refund 数量与汇总视图不一致
6. 仓储费原始合计与汇总视图不一致
7. 赔偿原始合计与汇总视图不一致
8. `shipping_credits` 原始合计与汇总视图不一致
9. `gift_wrap_credits` 原始合计与汇总视图不一致

### 10.2 warning 条件

1. `shipped_waiting_settlement`
2. storage fee 中 `sku is null`
3. subscription fee source 缺失或为 0

### 10.3 月结状态判定

- 有 blocker：`blocked`
- 无 blocker 但有 warning：`warning`
- 其余：`ready`

---

## 11. 当前人工控制文件

### 11.1 `manual/manual_sku_aliases.csv`

用途：

- 补充 SKU 映射

字段：

- `alias_type`
- `alias_value`
- `sku`
- `source_note`
- `is_active`

### 11.2 `manual/manual_vine_fee_allocations.csv`

用途：

- 指定 Vine 费用分配到 SKU

字段：

- `period_month`
- `sku`
- `fee_amount`
- `source_note`

### 11.3 `manual/manual_shared_costs.csv`

用途：

- 记录共享费用

注意：

- 当前一期已能维护，但尚未进入最终利润明细分摊

### 11.4 `manual/manual_platform_monthly_base.csv`

用途：

- 平台层月度基表补录

### 11.5 `manual/manual_removal_fee_controls.csv`

用途：

- 调仓/报废费用分类
- 决定 removal 费用是否费用化或资本化

字段：

- `period_month`
- `order_id`
- `sku`
- `removal_category`
- `accounting_treatment`
- `source_note`

---

## 12. 当前导出能力

### 12.1 SKU 汇总导出

数据集：

- `sku_details`

### 12.2 订单追踪导出

数据集：

- `order_details`

### 12.3 月结告警导出

数据集：

- `alerts`

### 12.4 月度累计统计导出

数据集：

- `order_type_rollup`

字段包含：

- `period_month`
- `sku`
- `product_name_cn`
- `scope_value`
- `order_type`
- `order_count`
- `qty_sold`
- `gmv`
- `product_sales`
- `shipping_credits`
- `gift_wrap_credits`
- `promotional_rebates`
- `net_sales`
- `selling_fees`
- `fba_fees`
- `other_transaction_fees`
- `marketplace_withheld_tax`
- `storage_fees`
- `removal_fees`
- `removal_fee_capitalized`
- `removal_fee_unclassified`
- `ad_spend`
- `compensation_income`
- `review_cost`
- `vine_fee`
- `subscription_fee`
- `coupon_participation_fee`
- `coupon_performance_fee`
- `product_cost`
- `inbound_cost`
- `gross_profit`

### 12.5 订单最细颗粒度导出

数据集：

- `order_line_profit`

字段包含：

- `order_line_id`
- `period_month`
- `amazon_order_id`
- `purchase_date`
- `order_status`
- `settlement_state`
- `sales_channel`
- `fulfillment_channel`
- `sku`
- `product_name_cn`
- `asin`
- `order_type`
- `ordered_quantity`
- `ordered_item_price`
- `ordered_shipping_price`
- `ordered_item_promotion_discount`
- `ordered_ship_promotion_discount`
- `settled_quantity`
- `product_sales`
- `shipping_credits`
- `gift_wrap_credits`
- `promotional_rebates`
- `net_sales`
- `selling_fees`
- `fba_fees`
- `other_transaction_fees`
- `marketplace_withheld_tax`
- `settlement_net_total`
- `alloc_share`
- `allocated_storage_fees`
- `allocated_removal_fees`
- `allocated_ad_spend`
- `direct_compensation_income`
- `allocated_review_cost`
- `allocated_vine_fee`
- `allocated_subscription_fee`
- `allocated_coupon_participation_fee`
- `allocated_coupon_performance_fee`
- `product_unit_cost`
- `inbound_unit_cost`
- `allocated_product_cost`
- `allocated_inbound_cost`
- `estimated_gross_profit`

---

## 13. 当前复现步骤

### 13.1 初始化或全量重建

```powershell
python etl/00_init_db.py
python etl/01_load_sku_master.py
python etl/02_load_sku_cost.py
python etl/06_load_review_orders.py
python etl/13_load_manual_controls.py
```

然后按月份运行月跑。

### 13.2 单月月跑

```powershell
python etl/99_run_monthly.py 2026-02
```

如数据库已初始化，可跳过初始化：

```powershell
python etl/99_run_monthly.py 2026-02 --skip-init
```

### 13.3 启动浏览器服务

```powershell
python -u app.py --host 127.0.0.1 --port 8765
```

### 13.4 浏览器访问

- `http://127.0.0.1:8765`

### 13.5 当前建议的月度复核动作

每月跑完后至少做以下核对：

1. refund 数量是否与原始 settlement 一致
2. 仓储费汇总是否与 storage report 一致
3. 赔偿汇总是否与 reimbursements 一致
4. `shipping_credits` / `gift_wrap_credits` 是否完整计入收入
5. 是否存在 `removal_fee_unclassified`
6. 是否仍有 `shipped_waiting_settlement`

---

## 14. 当前已验证状态

### 14.1 已加载月份

- `2026-02`
- `2025-12`
- `2025-11`
- `2025-10`
- `2025-09`

### 14.2 当前数据库规模

当前库内记录量：

- `fact_order_lines`: `2087`
- `fact_settlement_lines`: `3197`
- `bridge_orderline_settlement`: `1487`
- `dim_sku where is_active=1`: `13`

### 14.3 已核对通过的财务事实

以 `2026-02` 为例，当前系统已核对通过：

- refund 原始数量 `4`，汇总数量 `4.0`
- storage 原始合计 `44.70`，汇总合计 `44.70`
- compensation 原始合计 `133.21`，汇总合计 `133.21`
- shipping credits 原始合计 `167.80`，汇总合计 `167.80`
- gift wrap credits 原始合计 `3.99`，汇总合计 `3.99`
- refund 不再分摊广告、订阅费、优惠券费、测评费、Vine 费、仓储费、removal 费、产品成本、头程成本

### 14.4 当前最近月结状态

最近月份 `2026-02` 当前月结状态：

- `close_status = blocked`
- `blocker_count = 1`
- `warning_count = 1`

当前 blocker 为：

- `removal_fee_control_missing`
- `order_id = 26021145R`
- `sku = 4C-3ZX7-A1SH`
- `metric_value = 1522.35`
- 备注：`removal_order_type=Return; disposition=Sellable`

当前 warning 主要为：

- `shipped_waiting_settlement=25`

---

## 15. 当前已知限制

以下限制是当前真实状态，不应在复刻时误以为已完成：

1. `manual_shared_costs.csv` 已可维护，但尚未进入最终利润分摊
2. PDF 月结对账闭环尚未做完
3. 平台经营层利润表与应收平台款报表尚未做完
4. Vine 扣款月 / 归属月双口径尚未做完
5. SKU 主数据中仍存在脏产品名，可能影响按产品名筛选
6. 当前 removal 前台确认面板标签是英文，不影响逻辑

---

## 16. 维护原则

后续任何改动都必须遵守以下原则：

1. 先保证原始合计与汇总合计可对平，再谈展示与体验
2. 不能证明归属的金额，宁可落 `non_order_fee`，不能强行分摊
3. refund 不得分摊正常销售侧 pooled 费用
4. 调仓类 removal 未确认前，必须继续阻断月结
5. 文档、代码、数据库口径三者必须同步更新

---

## 17. 文档维护要求

后续只要发生以下任一变化，本文档必须同步修订：

- ETL 脚本顺序变化
- 利润公式变化
- 收入口径变化
- 退款分摊规则变化
- removal 分类规则变化
- 月结 blocker / warning 规则变化
- 前台入口变化
- 导出字段变化

如果代码与文档冲突，以代码和数据库实际行为为准，并应立即修正文档。
