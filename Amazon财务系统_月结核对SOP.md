# Amazon财务系统月结核对SOP

## 1. 目的

本文档用于指导财务或研发按当前系统的真实实现完成单月核对、问题处理、前台确认和下载放行。  
本文档只描述当前系统已经具备的操作链路，不写尚未落地的理想流程。

适用范围：

- 当前工作区：`E:\输出\5. 财务系统搭建\1. Amazon平台`
- 当前系统入口：`app.py`
- 当前数据库：`amazon_finance.db`

---

## 2. 月结前准备

每次月结前，先确认以下输入已准备齐全：

- `2_Order report_AmazonYYYYMM.txt`
- `3_Settlement Details_AmazonYYYYMM.csv`
- `4_Ad Spend_AmazonYYYYMM.xlsx`
- `6_FBA Storage Fees_AmazonYYYYMM.csv`
- `8_Removal Fees_AmazonYYYY.csv` 或对应年度文件
- `9_Reimbursements_AmazonYYYY.csv` 或对应年度文件
- `98_SKU Cost Table_Amazon.xlsx`
- `99_SKU_MASTER.xlsx`

同时确认 `manual/` 目录中的控制表存在：

- `manual_sku_aliases.csv`
- `manual_vine_fee_allocations.csv`
- `manual_shared_costs.csv`
- `manual_platform_monthly_base.csv`
- `manual_removal_fee_controls.csv`

---

## 3. 标准月结流程

### 3.1 执行月跑

首次初始化或全量重建：

```powershell
python etl/00_init_db.py
python etl/01_load_sku_master.py
python etl/02_load_sku_cost.py
python etl/06_load_review_orders.py
python etl/13_load_manual_controls.py
```

单月月跑：

```powershell
python etl/99_run_monthly.py 2026-02 --skip-init
```

### 3.2 启动浏览器系统

```powershell
python -u app.py --host 127.0.0.1 --port 8765
```

打开：

- `http://127.0.0.1:8765`

### 3.3 在总览页核对月结状态

进入 `总览` 页签，重点看：

- 月结状态
- Blocker 数
- Warning 数
- 应收差额
- 月结告警
- 费用报告与结算报告核对

如果首页显示：

- `blocked`
  - 本月不得视为完成月结
- `warning`
  - 本月可继续人工复核，但仍未正式收口
- `ready`
  - 当前代码语义是“无 blocker 且无 warning”，不是“已正式 closed”

### 3.4 在操作中心处理必填控制项

进入 `操作中心` 页签，检查：

- 月跑状态
- 待确认 `Removal` 控制项
- 源文件列表
- 工作清单
- 手工控制表

如果有待确认 `Removal`：

1. 在前台选择 `Removal 类别`
   - `Transfer`
   - `Disposal`
2. 选择 `会计处理`
   - `Expense`
   - `Capitalize`
3. 点击 `保存必填确认项`
4. 等待系统刷新当前月份数据

当前系统规则：

- 这一步没有完成时，该月最细颗粒度和月度累计下载会被阻断

### 3.5 在数据下载页做预览核对

进入 `数据下载` 页签，先预览，再下载。

应至少核对以下两类输出：

- 月度累计统计
- 单订单最细明细

如果需要复核分摊逻辑，再看：

- 费用分摊验证报表

当前页面支持两种最细颗粒度口径：

- 利润表口径
- 应收款口径

### 3.6 在订单追踪页抽样检查订单

进入 `订单追踪` 页签，按订单号抽查至少以下类型：

- 正常发货单
- 退款单
- 待结算单
- `Removal` 相关订单

应确认：

- 订单类型是否正确
- 结算状态是否正确
- 金额与最细颗粒度是否可对应

---

## 4. 当前必须核对的财务检查点

以下检查点应作为每月最少核对项。

### 4.1 最细颗粒度与汇总一致

原则：

- 月度汇总必须等于最细颗粒度各字段求和
- 当前系统已通过 `detail_rollup_mismatch` 进行 blocker 检查

### 4.2 退款数量

原则：

- 退款订单数量必须等于结算明细退款数量
- 当前系统已通过 `refund_qty_mismatch` 进行 blocker 检查

### 4.3 仓储费

要核对两层：

- 利润表仓储费是否等于仓储费报告
- 仓储费报告是否等于结算仓储扣款

对应当前检查：

- `storage_fee_mismatch`
- `storage_report_settlement_mismatch`

### 4.4 广告费

要核对：

- 广告费报告合计是否等于结算报告 `Cost of Advertising`

对应当前检查：

- `ad_report_settlement_mismatch`

### 4.5 赔偿收入

要核对：

- 赔偿报告合计是否等于汇总中的赔偿收入

对应当前检查：

- `compensation_mismatch`

### 4.6 买家运费与 Gift Wrap

要核对：

- `shipping_credits`
- `gift_wrap_credits`

对应当前检查：

- `shipping_credits_mismatch`
- `gift_wrap_credits_mismatch`

### 4.7 `Removal`

要核对：

- 是否存在未确认的 `Removal`
- 已确认的 `Transfer / Expense / Capitalize / Disposal` 是否符合业务事实

对应当前检查：

- `removal_fee_control_missing`

### 4.8 订单类型

要核对：

- 结算表明细中不得存在 `unknown order type`

对应当前检查：

- `unknown_order_type_in_settlement_detail`

---

## 5. 当前 blocker 与 warning 解释

### 5.1 blocker

blocker 表示本月数据存在不能直接接受的财务或系统问题。  
当前常见 blocker 包括：

- 产品成本缺失
- `Removal` 未确认
- 结算明细存在 `unknown order type`
- 退款数量不一致
- 广告费报告与结算不一致
- 仓储费报告与结算不一致
- 仓储费、赔偿、运费、Gift Wrap 与汇总不一致
- 最细颗粒度与汇总不一致
- Vine 费用未分配

### 5.2 warning

warning 表示本月仍有待关注事项，但不一定代表计算错误。  
当前常见 warning 包括：

- `shipped_waiting_settlement`
- `storage_unmapped`
- `subscription_fee_source_missing_or_zero`

---

## 6. 当前下载放行规则

当前真实实现不是“所有 blocker 都禁止下载”，而是更保守但不完整的版本：

- 如果该月存在待确认 `Removal` 控制项
  - `order_line_profit`
  - `order_type_rollup`
  - `allocation_audit`
  - `sku_details`
  - `order_details`
  这些数据集会被阻断下载

这意味着：

- 当前系统已经做到“关键控制项未完成时禁止下载”
- 但还没有做到“任一 blocker 存在时一律禁止下载”

因此本月即使页面允许部分查看，也不能把“能点下载”理解成“可以正式关账”。

---

## 7. 当前建议的月结执行顺序

建议每月按以下顺序操作：

1. 放齐原始文件和控制表
2. 执行单月月跑
3. 打开浏览器，先看 `总览`
4. 处理 `操作中心` 中的必填确认项
5. 回到 `总览` 复核 blocker / warning
6. 在 `数据下载` 先做预览
7. 在 `订单追踪` 抽样检查关键订单
8. 再执行正式下载
9. 保存当月工作清单和核对结论

---

## 8. 研发与财务分工建议

### 财务负责

- 判断 `Removal` 的业务性质和会计处理
- 复核广告费和仓储费报告是否可接受
- 确认赔偿、退款、买家运费、Gift Wrap 是否符合业务理解

### 研发负责

- 保证 ETL 可复跑
- 保证前台与下载统一从同一底座取数
- 保证 blocker / warning 规则执行稳定
- 保证保存控制项后能正确重建和刷新页面

---

## 9. 结论

当前系统的月结方式已经不是“导出 Excel 后人工拼表”，而是：

- 先把数据沉淀为统一底座
- 再通过月结规则阻断错误月份
- 再由前台完成必要控制项确认
- 最后再开放下载与复核

这份 SOP 的核心作用不是增加步骤，而是确保每个月都按同一套顺序、同一套口径、同一套阻断机制执行，避免同月数据在不同报表中出现不一致。
