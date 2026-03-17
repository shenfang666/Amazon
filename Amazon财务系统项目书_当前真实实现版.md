# Amazon财务系统项目书（当前真实实现版）
**版本**: vCurrent-Real-2026-03-16  
**编制日期**: 2026-03-16  
**适用范围**: Amazon 平台财务系统一期当前真实落地版本  
**项目目录**: `E:/输出/5. 财务系统搭建/1. Amazon平台/`

---

## 1. 项目背景与定位

本项目用于沉淀 Amazon 平台财务数据，并在统一财务底座上输出浏览器驾驶舱、订单追踪、最细颗粒度下载、月度累计统计、费用分摊验证报表和月结阻断结果，作为后续多平台财务数据底座建设的第一期。

当前版本不是规划稿，也不是纯原型，而是已经具备以下能力的真实系统：

- 本地数据库持久化
- 完整 ETL 月跑链路
- 浏览器访问与交互页面
- 最细颗粒度与汇总下载
- 前台人工控制项填写
- 月结 blocker / warning 输出

但当前版本也不是“已完全关账可冻结版本”。截至 2026-03-16，最近月份 `2026-02` 仍处于 `blocked` 状态，说明系统已具备识别与阻断错误的能力，但本月数据尚未完全收口。

---

## 2. 项目目标

当前项目的真实目标分为四层。

### 2.1 数据沉淀目标

- 将 Amazon 原始文件导入统一数据库
- 建立可复跑的月度财务底座
- 支持订单、结算、广告、仓储、移除费、赔偿、平台费、测评订单、SKU 成本和人工控制表的统一管理

### 2.2 财务分析目标

- 输出统一最细颗粒度财务明细
- 输出按 `月份 × SKU × order_type` 的标准汇总
- 同时支持利润表口径与应收款口径

### 2.3 业务操作目标

- 在浏览器中查看总览、SKU 经营、订单追踪、数据下载、操作中心
- 在前台完成 `Removal` 必填确认项，而不是要求后台改 Excel/CSV
- 在前台触发月跑、查看工作清单、浏览源文件和手工控制表

### 2.4 治理目标

- 建立 blocker / warning 机制
- 将费用报告与结算报告核对纳入月结确认
- 将错误月份阻断在下载前，而不是下载后才发现问题

---

## 3. 当前范围

### 3.1 已纳入范围

当前一期已纳入以下数据与模块：

- Amazon 订单报告
- Amazon 结算明细
- 广告报告
- FBA 仓储费报告
- 移除费报告
- 赔偿 / 报销报告
- 平台费用抽取
- 测评订单台账
- SKU 主数据
- SKU 月度成本表
- 人工 Vine 分配表
- 人工 SKU 别名表
- 人工平台月度基表
- 人工 `Removal` 费用分类控制表
- 浏览器驾驶舱
- 数据下载模块
- 操作中心
- 月结问题清单导出

### 3.2 当前暂未完成范围

以下能力在当前代码中尚未完成或仅为结构预留：

- `manual_shared_costs` 自动分摊入最终明细
- PDF 月结金额正式纳入关账阻断
- Vine `扣款月 / 归属月` 双口径
- 多平台共摊正式入账
- 完整 `open / warning / blocked / closed` 月结状态机
- “任一 blocker 存在时统一禁止下载”的完整下载治理

---

## 4. 当前系统形态

### 4.1 架构形态

当前系统是单机架构：

- 数据存储：`SQLite`
- 数据处理：`Python ETL`
- 服务入口：`app.py`
- 前端页面：`web/index.html`、`web/app.js`、`web/styles.css`
- 数据库文件：`amazon_finance.db`

### 4.2 当前目录职责

- `etl/`
  - 初始化、导入、桥接、分类、汇总、月结检查、工作清单导出
- `manual/`
  - 人工控制表与月结工作清单
- `web/`
  - 浏览器页面与交互脚本
- 根目录原始文件
  - Amazon 月度源文件、成本表、SKU 主数据、PDF 文件

### 4.3 当前浏览器模块

当前页面包含 5 个主页签：

- `总览`
- `SKU经营`
- `订单追踪`
- `数据下载`
- `操作中心`

页面已不是“未完成状态”，而是已可运行的浏览器系统。

---

## 5. 当前数据底座与 ETL 链路

### 5.1 当前核心数据底座

当前系统的统一财务底座为：

- 明细底座：`v_finance_detail_lines`
- 汇总视图：`v_monthly_sku_order_type_summary`

原则是：

- 看板从汇总视图取数
- 最细颗粒度下载从明细底座取数
- 月结检查同时校验明细与汇总
- 后续报表应继续基于这两层构建

### 5.2 当前 ETL 顺序

当前月跑由 `etl/99_run_monthly.py` 驱动，顺序如下：

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

### 5.3 当前系统已支持的稳定性能力

- 可重复月跑
- 原始文件导入日志
- 中文 CSV 编码兼容
- 年度文件回退读取
- 手工控制表自动加载
- blocker / warning 自动入库
- 工作清单自动导出

---

## 6. 当前数据库真实状态

以下数据来自当前库 `amazon_finance.db` 的真实状态。

### 6.1 已加载月份

- `2026-02`
- `2025-12`
- `2025-11`
- `2025-10`
- `2025-09`

### 6.2 当前记录规模

- `fact_order_lines`: `2087`
- `fact_settlement_lines`: `3200`
- `bridge_orderline_settlement`: `1090`
- `v_finance_detail_lines`: `2584`
- `v_monthly_sku_order_type_summary`: `80`
- `dim_sku where is_active = 1`: `13`

### 6.3 当前月结日志状态

| 月份 | 状态 | Blocker | Warning |
|---|---|---:|---:|
| 2026-02 | blocked | 5 | 1 |
| 2025-12 | warning | 0 | 1 |
| 2025-10 | warning | 0 | 1 |

说明：

- 当前数据库没有显示 `2025-11` 和 `2025-09` 的月结日志行，不代表无数据，而是说明当前日志表中没有对应关闭记录。
- 最近月 `2026-02` 当前真实状态为 `blocked`，这与旧版项目书中“warning 且无 blocker”的表述不一致，旧表述已过期。

---

## 7. 当前财务模型与口径

### 7.1 双口径模型

当前系统已形成两套并行口径：

- 利润表口径
- 应收款口径

两套口径共用同一最细颗粒度底座，但字段集合不同。

### 7.2 当前最细颗粒度基础

当前最细颗粒度主底座已收敛到结算明细驱动，统一输出为 `v_finance_detail_lines`。  
它由以下几类行组成：

- `settlement_rows`
- `settlement_removal_rows`
- `synthetic_comp_rows`
- `synthetic_fee_rows`

这意味着当前系统已经从“订单表思路”纠偏到“结算事实底座”。

### 7.3 当前成本字段

当前对外字段定义：

- `product_unit_cost`
  - 单件产品成本
- `allocated_product_cost`
  - 当前最细行承接的产品成本
- `inbound_freight_unit_cost`
  - 单件头程成本
- `allocated_inbound_freight_cost`
  - 当前最细行承接的头程成本

说明：

- 当前 `inbound_freight_*` 对应的是成本表中的“头程费用”
- 当前前台对外已经使用 `Inbound Freight Cost` 命名

### 7.4 当前退款规则

当前系统中：

- 退款数量不允许被压成 `0`
- 退款订单不分摊广告、订阅费、优惠券服务费、测评费、Vine 费等
- 退款产品成本和头程成本按负数冲回，不是强制写 `0`

### 7.5 当前广告费与仓储费规则

当前已明确：

- 广告费
  - 利润表口径：来自广告费报告
  - 应收款口径：来自结算表 `Cost of Advertising`
- 仓储费
  - 利润表口径：来自仓储费报告
  - 应收款口径：来自结算表仓储扣款

当前这两类费用被视为存在时间性差异，需要纳入月结核对。

### 7.6 当前 `Removal` 规则

当前 `Removal` 已拆分为：

- 费用化部分
- 资本化部分
- 未确认部分

且要求前台填写：

- `Removal 类别`
  - `Transfer`
  - `Disposal`
- `会计处理`
  - `Expense`
  - `Capitalize`

未确认时：

- 进入 blocker
- 阻断相关下载

### 7.7 当前 `non_order_fee`

`non_order_fee` 是系统正式定义的业务行类型，用于承接：

- 仓储费
- 无法挂到订单销售行的费用
- 结算表中的 `Removal` 扣款行
- 未挂单赔偿
- 资本化费用行

### 7.8 当前 `alloc_share`

`alloc_share` 是分摊验证用字段，不是业务主字段。  
当前系统已把它从主下载语义中降级，并通过单独的“费用分摊验证报表”对外展示。

---

## 8. 当前浏览器能力

### 8.1 总览

已支持：

- KPI
- 趋势
- 月结状态
- 月结告警
- 费用报告与结算报告核对

### 8.2 SKU经营

已支持：

- SKU 级净销售、销量、广告费、毛利、毛利率、ACOS
- 搜索 SKU
- 导出 SKU 明细

### 8.3 订单追踪

已支持：

- 订单状态筛选
- 订单号查询
- 单订单最细明细预览与导出

### 8.4 数据下载

已支持：

- 月度累计统计预览和下载
- 最细颗粒度预览和下载
- 利润表口径 / 应收款口径切换
- 费用分摊验证报表

### 8.5 操作中心

已支持：

- 月跑状态展示
- 源文件浏览
- 工作清单浏览
- 手工控制表浏览
- 前台填写并保存 `Removal` 确认项

---

## 9. 当前后端接口

当前已对外提供这些主要接口：

- `GET /api/dashboard`
- `GET /api/operations`
- `GET /api/order-lookup`
- `GET /api/download-preview`
- `GET /api/export`
- `GET /api/health`
- `POST /api/upload`
- `POST /api/run-monthly`
- `POST /api/manual/save`
- `POST /api/removal-controls/save`

说明：

- `app.py` 当前同时承担 HTTP、业务聚合、导出和月跑触发职责
- 这在当前版本是事实，也属于后续重构重点

---

## 10. 当前月结状态与问题

### 10.1 最近月 `2026-02` 当前真实状态

当前 `2026-02` 的月结状态为：

- `close_status = blocked`
- `blocker_count = 5`
- `warning_count = 1`

### 10.2 当前 blocker

当前 `2026-02` 的 blocker 为：

- `unknown_order_type_in_settlement_detail = 432`
- `ad_report_settlement_mismatch`
  - `report = 5168.84`
  - `settlement = 5520.32`
- `storage_report_settlement_mismatch`
  - `report = 44.70`
  - `settlement = 610.49`
- `shipping_credits_mismatch`
  - `raw = 167.80`
  - `summary = 0.00`
- `gift_wrap_credits_mismatch`
  - `raw = 3.99`
  - `summary = 0.00`

### 10.3 当前 warning

- `shipped_waiting_settlement = 25`

### 10.4 这意味着什么

说明当前系统已经具备：

- 把错误月份识别为 `blocked`
- 把费用报告与结算报告差异展示出来
- 把明细和汇总不一致问题阻断下来

但也说明：

- 当前 `2026-02` 还不能视为已完成正式月结
- 旧版项目书中的“当前仅有 warning”结论已经失效

---

## 11. 当前已解决与未解决事项

### 11.1 当前已解决的关键问题

当前版本已经解决或完成了以下能力：

1. 建立了浏览器系统，不再只是数据库和脚本。
2. 建立了统一最细颗粒度底座 `v_finance_detail_lines`。
3. 建立了利润表口径与应收款口径双口径模型。
4. 将 `Removal` 前台确认项纳入系统，而不是只靠后台文件修改。
5. 提供了订单级查询、月度累计统计、费用分摊验证报表。
6. 将广告费和仓储费的“报告 vs 结算”核对纳入月结检查。
7. 下载已与关键控制项阻断发生联动。

### 11.2 当前尚未完成事项

以下事项在当前实现中仍属于后续阶段：

1. `manual_shared_costs` 自动分摊入最终财务底座。
2. PDF 月结对账闭环。
3. Vine 双口径。
4. 完整月结状态机与正式关账冻结机制。
5. 将所有 blocker 统一纳入下载放行逻辑。
6. 清理前端少量残留文案 / 编码问题。

---

## 12. 当前系统可回答的问题

按当前代码和当前页面，系统已经可以回答：

- 某订单是否已进入结算、处于什么结算状态
- 某订单的最细颗粒度金额、成本、费用、数量是什么
- 某月按 SKU、产品名或订单类型汇总后的销量、净销售、费用和毛利是什么
- 某月广告费和仓储费在利润表口径与应收款口径下有何差异
- 某月是否仍存在 blocker / warning
- 某月哪些订单或费用项仍需人工关注

---

## 13. 当前使用方式

### 13.1 月跑命令

```powershell
python etl/99_run_monthly.py 2026-02 --skip-init
```

### 13.2 启动服务

```powershell
python -u app.py --host 127.0.0.1 --port 8765
```

### 13.3 浏览器访问

- `http://127.0.0.1:8765`

### 13.4 手工控制文件

位于 `manual/`：

- `manual_sku_aliases.csv`
- `manual_vine_fee_allocations.csv`
- `manual_shared_costs.csv`
- `manual_platform_monthly_base.csv`
- `manual_removal_fee_controls.csv`

### 13.5 月结工作清单

当前已导出的工作清单包括：

- `manual/worklist_month_close_2025-10.csv`
- `manual/worklist_month_close_2025-12.csv`
- `manual/worklist_month_close_2026-02.csv`
- `manual/worklist_pending_aliases.csv`

---

## 14. 当前风险与限制

当前系统已经进入“可运行、可下载、可阻断”阶段，但仍有以下限制：

- 当前 `2026-02` 数据仍被 blocker 阻断，不能视为已完全收口
- 月结状态仍未形成 `closed` 终态
- 下载阻断仍未完全与全部 blocker 统一
- 部分前端文案仍存在编码或占位残留
- `app.py` 单文件职责过重，继续迭代的风险较高

---

## 15. 当前结论

截至 2026-03-16，Amazon 财务系统一期已经不是概念项目，也不是仅供验证的 ETL 原型，而是一个真实可运行的本地财务系统，已经具备：

- 统一数据底座
- 浏览器看板与交互
- 最细颗粒度与汇总下载
- 前台人工控制项确认
- 月结 blocker / warning 机制

同时，当前系统也明确处于“已进入治理阶段、尚未完全收口”的状态。  
因此，当前项目书的正确定位应是：

- 这是一个已落地、可继续运行、可继续重构的真实一期系统
- 不是未开始开发的方案稿
- 也不是已经完全关账冻结的最终版财务平台

---

## 16. 参考文档

- [Amazon财务系统_技术设计与重构优化方案.md](E:/输出/5.%20财务系统搭建/1.%20Amazon平台/Amazon财务系统_技术设计与重构优化方案.md)
- [Amazon财务系统_月结核对SOP.md](E:/输出/5.%20财务系统搭建/1.%20Amazon平台/Amazon财务系统_月结核对SOP.md)
- [Amazon财务系统_当前系统建设与复现手册.md](E:/输出/5.%20财务系统搭建/1.%20Amazon平台/Amazon财务系统_当前系统建设与复现手册.md)
- [Amazon财务系统项目书_当前版.md](E:/输出/5.%20财务系统搭建/1.%20Amazon平台/Amazon财务系统项目书_当前版.md)
