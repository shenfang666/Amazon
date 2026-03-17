# Amazon财务系统财务级修订方案

**版本**: v1.0  
**编制日期**: 2026-03-13  
**适用范围**: Amazon 平台财务系统一期实施修订  
**对应文档**:
- `Amazon财务系统项目规划书.md`
- `Amazon财务系统设计规范.md`

---

## 1. 修订结论

现有规划书和设计规范具备较好的业务方向，但若目标是：

- 准确识别订单是否已经结算
- 准确区分正常销售/退款、Vine、测评订单
- 下钻到 `月份 × SKU × 订单类型 × 费用项` 的最细颗粒
- 将平台内费用、多平台共摊费用稳定拆分到 SKU
- 结果稳定可复现，满足财务使用要求

则当前方案仍未达到可直接实施上线的标准。

本修订方案的目标不是推翻原方案，而是在保留其总体方向的基础上，将系统提升为可审计、可复跑、可月结的财务级方案。

---

## 2. 修订原则

### 2.1 财务级原则

1. 所有结果必须可追溯到原始文件、原始行、原始字段。
2. 任何无法唯一归属的记录，不允许自动猜测归类后直接入正式账。
3. 所有跨表映射、分摊、补录、人工修正都必须留痕。
4. 同一批输入在相同规则版本下重复运行，必须产出完全一致的结果。
5. 报表口径必须区分：
   - 经营分析口径
   - 平台结算口径
   - 供应链/管理口径

### 2.2 实施边界

一期仅覆盖 Amazon 平台，但数据模型必须预留多平台扩展能力。  
当前货币以 USD 为主，跨平台分摊如涉及其他平台，必须通过统一分母表或手工录入表进入系统。

---

## 3. 必须修订的核心问题

### 3.1 订单表粒度必须改为订单行粒度

当前方案将 `amazon_order_id` 作为订单表主键，这不成立。  
真实订单报告中已经存在：

- 同一订单号多行
- 同一订单号下多个 SKU
- 同一订单号下多个数量和不同促销分配

因此必须将订单事实表拆为：

- `fact_order_lines`：订单行明细
- `fact_order_headers`：订单头汇总，可选

**正式口径以订单行表为准，不以订单头表为准。**

### 3.2 结算表必须以原始行哈希去重，不允许业务键去重

真实结算文件中，同一 `settlement id + date/time + order id + sku + type` 可以合法出现多行。  
原因包括但不限于：

- 促销拆分
- 佣金拆分
- 同一订单行的金额拆分
- 移除费/赔偿费的多笔记录

因此：

- 唯一去重键必须是 `source_row_hash`
- 业务键只能用于分析，不可用于物理去重

### 3.3 SKU 歧义必须阻断，不得自动猜测

若一个中文产品名可映射多个 SKU，系统必须：

- 优先通过订单号回查结算记录确定 SKU
- 其次通过 ASIN/FNSKU 确定 SKU
- 若仍无法唯一确定，进入 `pending_mapping_queue`
- 不允许默认选一个 SKU 继续入正式账

### 3.4 “是否已结算”必须改为状态机

`is_settled = 0/1` 不足以支持财务追踪。  
必须引入结算状态机，至少包括：

- `unshipped_or_pending`
- `shipped_waiting_settlement`
- `partially_settled`
- `fully_settled`
- `refunded_after_settlement`
- `cancelled_before_settlement`
- `exception_needs_review`

### 3.5 共摊费用必须先有稳定分母，才能谈自动分摊

“多平台共摊”不可直接依赖未来系统外部数据。  
必须新增统一分母表，至少支持：

- 平台月度净销售额
- 平台月度订单数
- 平台月度出货件数

若分母缺失，则该月共摊费用不得进入正式损益。

---

## 4. 修订后的目标产出

系统一期必须能稳定产出以下四类正式结果：

### 4.1 订单结算追踪表

按订单行展示：

- 下单时间
- 发货状态
- 订单行金额
- 已进入结算金额
- 已释放金额
- 结算批次
- 当前结算状态
- 账龄天数
- 是否超出预计结算窗口

### 4.2 月度 SKU 财务明细表

最小正式分析粒度：

- `period_month`
- `sku`
- `order_type`
- `fee_type`

能够下钻到：

- 销售数量
- 销售金额
- 促销折扣
- 平台佣金
- FBA 配送费
- 仓储费
- 移除费
- 优惠券费用
- 广告费
- Vine 费用
- 测评费用
- 赔偿收入
- 采购成本
- 头程成本
- 平台内部分摊费用
- 多平台共摊费用

### 4.3 应收平台款项表

必须区分：

- 已产生未释放
- 已释放未到账
- 已到账
- 负向扣款
- 信用卡扣款
- 需追踪差异项

### 4.4 月结审计包

每月系统必须自动生成审计结果：

- 导入文件清单
- 文件哈希
- 规则版本
- 异常记录清单
- 未决映射清单
- 月结核对差异清单

---

## 5. 修订后的数据模型

## 5.1 主数据表

### `dim_sku`

```sql
CREATE TABLE dim_sku (
    sku                  TEXT PRIMARY KEY,
    asin                 TEXT,
    fnsku                TEXT,
    product_name_cn      TEXT NOT NULL,
    product_name_en      TEXT,
    product_group        TEXT,
    is_active            INTEGER DEFAULT 1,
    created_at           TEXT,
    updated_at           TEXT
);
```

### `dim_sku_alias`

用于解决产品名、广告名、人工台账名等映射问题。

```sql
CREATE TABLE dim_sku_alias (
    alias_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    alias_type           TEXT NOT NULL,   -- product_name_cn / ad_name / manual_name / fnsku / asin
    alias_value          TEXT NOT NULL,
    sku                  TEXT NOT NULL,
    is_unique_mapping    INTEGER NOT NULL DEFAULT 1,
    created_at           TEXT,
    UNIQUE(alias_type, alias_value, sku)
);
```

### `dim_cost_monthly`

```sql
CREATE TABLE dim_cost_monthly (
    sku                  TEXT NOT NULL,
    cost_month           TEXT NOT NULL,   -- YYYY-MM
    product_unit_cost    REAL NOT NULL,
    inbound_unit_cost    REAL NOT NULL,
    total_unit_cost      REAL GENERATED ALWAYS AS (product_unit_cost + inbound_unit_cost) STORED,
    source_file          TEXT NOT NULL,
    source_row_ref       TEXT,
    PRIMARY KEY (sku, cost_month)
);
```

### `dim_platform_monthly_base`

用于多平台共摊分母。

```sql
CREATE TABLE dim_platform_monthly_base (
    period_month         TEXT NOT NULL,
    platform             TEXT NOT NULL,
    net_sales            REAL NOT NULL,
    shipped_qty          REAL,
    order_line_count     REAL,
    source_type          TEXT NOT NULL,   -- system / manual
    source_note          TEXT,
    PRIMARY KEY (period_month, platform)
);
```

## 5.2 订单与结算事实表

### `fact_order_lines`

```sql
CREATE TABLE fact_order_lines (
    order_line_id             TEXT PRIMARY KEY,
    source_file               TEXT NOT NULL,
    source_row_hash           TEXT NOT NULL UNIQUE,
    amazon_order_id           TEXT NOT NULL,
    purchase_date             TEXT NOT NULL,
    last_updated_date         TEXT,
    order_month               TEXT NOT NULL,
    order_status              TEXT NOT NULL,
    fulfillment_channel       TEXT,
    sales_channel             TEXT,
    sku                       TEXT,
    asin                      TEXT,
    quantity                  REAL,
    currency                  TEXT,
    item_price                REAL,
    item_tax                  REAL,
    shipping_price            REAL,
    shipping_tax              REAL,
    item_promotion_discount   REAL,
    ship_promotion_discount   REAL,
    promotion_ids             TEXT,
    is_amazon_channel         INTEGER NOT NULL,
    created_at                TEXT
);
```

`order_line_id` 建议按以下方式生成：

```text
SHA256(source_file + source_row_hash)
```

### `fact_settlement_lines`

```sql
CREATE TABLE fact_settlement_lines (
    settlement_line_id        TEXT PRIMARY KEY,
    source_file               TEXT NOT NULL,
    source_row_hash           TEXT NOT NULL UNIQUE,
    transaction_datetime      TEXT NOT NULL,
    transaction_month         TEXT NOT NULL,
    settlement_id             TEXT NOT NULL,
    transaction_type          TEXT NOT NULL,
    transaction_subtype       TEXT,
    order_id                  TEXT,
    sku                       TEXT,
    quantity                  REAL,
    marketplace               TEXT,
    fulfillment               TEXT,
    product_sales             REAL DEFAULT 0,
    product_sales_tax         REAL DEFAULT 0,
    shipping_credits          REAL DEFAULT 0,
    shipping_credits_tax      REAL DEFAULT 0,
    gift_wrap_credits         REAL DEFAULT 0,
    gift_wrap_credits_tax     REAL DEFAULT 0,
    regulatory_fee            REAL DEFAULT 0,
    regulatory_fee_tax        REAL DEFAULT 0,
    promotional_rebates       REAL DEFAULT 0,
    promotional_rebates_tax   REAL DEFAULT 0,
    marketplace_withheld_tax  REAL DEFAULT 0,
    selling_fees              REAL DEFAULT 0,
    fba_fees                  REAL DEFAULT 0,
    other_transaction_fees    REAL DEFAULT 0,
    other_amount              REAL DEFAULT 0,
    total                     REAL DEFAULT 0,
    transaction_status        TEXT,
    transaction_release_date  TEXT,
    is_amazon_channel         INTEGER NOT NULL,
    created_at                TEXT
);
```

### `bridge_orderline_settlement`

用于订单行与结算行的映射留痕。

```sql
CREATE TABLE bridge_orderline_settlement (
    bridge_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    order_line_id             TEXT NOT NULL,
    settlement_line_id        TEXT NOT NULL,
    match_method              TEXT NOT NULL,   -- exact_order_sku / order_only / manual
    matched_at                TEXT,
    UNIQUE(order_line_id, settlement_line_id)
);
```

## 5.3 其他费用事实表

必须保留来源字段和月度归属字段：

- `fact_advertising_monthly_sku`
- `fact_storage_monthly_sku`
- `fact_removal_monthly_sku`
- `fact_compensation_monthly_sku`
- `fact_review_orders`
- `manual_vine_fee_allocations`
- `manual_shared_costs`

所有表均必须包含：

- `source_file` 或 `source_type`
- `period_month`
- `sku`（如无法归属则为空并进入待处理队列）

---

## 6. 修订后的核心业务规则

## 6.1 Amazon 有效渠道识别

### 订单报告

仅保留：

```text
sales-channel = 'Amazon.com'
```

### 结算明细

对 `Order`、`Refund` 等订单类流水，仅保留：

```text
marketplace IN ('amazon.com', 'Amazon.com')
```

非 Amazon 渠道记录可入库，但必须标识为：

- `is_amazon_channel = 0`
- 不进入 Amazon 正式损益和应收报表

## 6.2 Vine 订单识别

### Vine 销售

```text
transaction_type = 'Order'
AND is_amazon_channel = 1
AND order_id IS NOT NULL
AND product_sales > 0
AND ABS(product_sales - ABS(promotional_rebates)) < 0.01
```

说明：

- 在当前数据中，该规则已被验证有效
- `promotional_rebates` 在源文件中为负数
- 该规则等价于 `ABS(product_sales + promotional_rebates) < 0.01`

### Vine 退款

```text
transaction_type = 'Refund'
AND order_id 对应的原始销售记录已标记为 vine_sale
```

### Vine 审计校验

系统必须同时检查：

1. 当月或相邻月份是否存在 `Vine Enrollment Fee`
2. Vine 销售数量与 Vine 手工分配费用是否存在明显异常
3. Vine 订单是否误入测评台账

若校验失败：

- 不阻断识别
- 但进入黄色异常清单

## 6.3 测评订单识别

### 识别顺序

1. 先识别 `cancelled`
2. 再识别 `vine_sale / vine_refund`
3. 再识别 `review_sale / review_refund`
4. 其余为正常销售或正常退款

### 测评订单要求

测评台账必须至少包含：

- `amazon_order_id`
- `order_date`
- `product_name`
- `sale_amount`
- `review_cost`

若台账无法唯一映射 SKU，则阻断正式入账。

## 6.4 订单结算状态机

建议定义如下：

| 状态 | 含义 |
|------|------|
| `cancelled_before_settlement` | 订单取消且无结算 |
| `pending_not_shipped` | 订单未发货 |
| `shipped_waiting_settlement` | 已发货但尚未进入结算 |
| `partially_settled` | 已部分进入结算 |
| `fully_settled_unreleased` | 已全量进入结算，但未释放 |
| `fully_settled_released` | 已释放 |
| `refunded_after_settlement` | 已结算后又退款 |
| `exception_needs_review` | 金额不平、匹配异常、状态异常 |

### 预计结算窗口

必须增加参数表：

- `expected_settlement_days_min`
- `expected_settlement_days_max`

超出最大窗口仍未进入结算的订单，才允许标记为“疑似漏结算”。

---

## 7. 修订后的费用归集与分摊规则

## 7.1 费用分类

### 直接可归因到 SKU 的费用

- 平台佣金
- FBA 配送费
- 退款相关负向金额
- 仓储费
- 移除费
- 赔偿收入
- 广告费
- Vine 费用
- 测评费用
- 采购成本
- 头程成本

### 需分摊到 SKU 的费用

- Amazon 月租费
- 优惠券参与费
- 优惠券绩效费
- 其他无法直接落 SKU 的平台费用
- 平台内部共摊费用
- 多平台共摊费用

## 7.2 平台内分摊顺序

对于 Amazon 平台内部无法直接归属 SKU 的费用，推荐默认分摊顺序：

1. 若费用能关联订单号，则优先按订单落 SKU
2. 若费用能关联活动或商品，则按已关联 SKU 落账
3. 若费用只能到月度平台层，则按月度 `net_sales` 分摊到 SKU
4. 若业务认为应按销量分摊，则必须单独配置该费用类型的分摊规则

## 7.3 多平台共摊顺序

### 第一步：从全平台总额分到 Amazon

```text
Amazon应承担金额
= 费用总额 × Amazon月度分母 / 全平台月度分母
```

### 第二步：从 Amazon 分到 SKU

默认：

```text
SKU分摊金额
= Amazon应承担金额 × SKU月度净销售额 / Amazon月度净销售额
```

### 分摊前置条件

以下任一条件不满足，则该月共摊不入正式损益：

- 全平台分母缺失
- Amazon 分母缺失
- 分摊规则未配置
- 人工确认未完成

## 7.4 优惠券费用处理

以下费用必须单独建字段或单独费用类型：

- `coupon_participation_fee`
- `coupon_performance_fee`
- `coupon_other_fee`

不得混入“其他 Amazon 费用”后在报表层丢失。

---

## 8. 修订后的报表口径

## 8.1 月度 SKU 财务明细表正式字段

建议正式输出为长表结构：

| 字段 | 说明 |
|------|------|
| `period_month` | 月份 |
| `sku` | SKU |
| `order_type` | 订单类型 |
| `metric_type` | 指标类型 |
| `amount` | 金额 |
| `qty` | 数量 |
| `source_layer` | 来源层 |

其中 `metric_type` 至少包括：

- `qty_sold`
- `gmv`
- `promotional_rebates`
- `net_sales`
- `selling_fees`
- `fba_fees`
- `storage_fees`
- `removal_fees`
- `coupon_participation_fee`
- `coupon_performance_fee`
- `subscription_fee`
- `ad_spend`
- `vine_fee`
- `review_cost`
- `compensation_income`
- `product_cost`
- `inbound_cost`
- `platform_shared_cost`
- `cross_platform_shared_cost`
- `gross_profit`

## 8.2 毛利润公式

```text
gross_profit
= net_sales
+ compensation_income
+ selling_fees              (原始为负数)
+ fba_fees                  (原始为负数)
- storage_fees
- removal_fees
- coupon_participation_fee
- coupon_performance_fee
- subscription_fee
- ad_spend
- vine_fee
- review_cost
- product_cost
- inbound_cost
- platform_shared_cost
- cross_platform_shared_cost
```

## 8.3 应收平台款项口径

按 `settlement_id` 统计：

```text
released_receivable
= SUM(total) of released non-transfer lines

transferred_amount
= SUM(total) of transfer lines

debt_charged
= SUM(total) of debt lines

outstanding_cash_gap
= released_receivable - transferred_amount + debt_charged
```

说明：

- `Debt` 为负值时，表示 Amazon 已从其他渠道扣走款项，应减少可收回金额
- `FBA Inventory Fee - Reversal` 应单独追踪，不应混入正常经营毛利

---

## 9. 修订后的数据质量与审计机制

## 9.1 红线阻断项

出现以下情况必须阻断正式月结：

- 新 SKU 无主数据
- SKU 无当月成本
- SKU 映射不唯一
- 共摊分母缺失
- Vine 费用需人工分配但未完成
- 月结 PDF 核对金额缺失
- 关键原始文件重复导入但哈希不一致
- 原始文件结构变化导致关键列无法识别

## 9.2 黄色待审项

允许入库但不得自动关账：

- 订单超预计结算窗口仍未结算
- 当月存在大量部分结算
- Vine 订单出现但相邻月份无 Vine 费用
- 广告、仓储、赔偿等费用无法完全映射 SKU
- 月度应收与 PDF 差异超过阈值

## 9.3 审计留痕

必须新增以下留痕对象：

- `etl_run_log`
- `file_import_log`
- `manual_adjustment_log`
- `mapping_override_log`
- `monthly_close_log`

每次月结应能回答：

1. 用了哪些原始文件
2. 每个文件的哈希是多少
3. 用了哪一版规则
4. 哪些记录被阻断
5. 哪些记录被人工修正
6. 本月与上次重跑是否一致

---

## 10. 修订后的月结流程

## 10.1 导入阶段

1. 导入 SKU 主数据
2. 导入成本表
3. 导入订单报告
4. 导入结算明细
5. 导入广告/仓储/移除/赔偿/测评台账
6. 导入 Vine 费用和共摊费用
7. 导入当月平台分母和跨平台分母

## 10.2 清洗与映射阶段

1. 标记 Amazon 有效渠道
2. 建立订单行与结算行桥接
3. 识别 Vine / 测评 / 正常订单类型
4. 汇总到月度 SKU 费用层
5. 执行平台内分摊
6. 执行多平台分摊

## 10.3 审计阶段

1. 检查红线阻断项
2. 输出黄色异常清单
3. 与 MonthlySummary 进行核对
4. 与人工台账核对 Vine 费用与共摊费用

## 10.4 关账阶段

仅当以下条件全部满足时，允许标记为正式月结：

- 红线阻断项为 0
- 所有人工补录完成
- 月结核对差异在阈值内
- 复跑结果一致

---

## 11. 修订后的实施建议

## 11.1 第一优先级

必须先做：

1. 重构订单模型为订单行粒度
2. 重构结算去重逻辑为原始行哈希
3. 建立订单结算桥接表
4. 建立正式结算状态机

## 11.2 第二优先级

随后完成：

1. Vine / 测评分类闭环
2. 优惠券费用单列
3. SKU 映射阻断机制
4. 月度 SKU 财务明细长表

## 11.3 第三优先级

最后完成：

1. 多平台分母表
2. 共摊自动分摊模块
3. 月结审计包
4. Streamlit 报表层

---

## 12. 对现有文档的具体修订建议

### 对《项目规划书》的修订建议

- 将“订单明细表”改为“订单行明细表”
- 删除 `amazon_order_id` 作为主键的设计
- 将 `is_settled` 改为“结算状态机”
- 在月度 SKU 损益中补充优惠券费用、平台共摊、多平台共摊
- 将“可视化/看板”放到三期，不作为一期交付的核心验收标准

### 对《设计规范》的修订建议

- 保留 `source_row_hash` 唯一去重设计
- 将 SKU 歧义处理从“黄灯继续”改为“阻断正式入账”
- 增加 `fact_order_lines` 与 `bridge_orderline_settlement`
- 增加 `dim_platform_monthly_base`
- 增加正式月结审计和关账机制

---

## 13. 验收标准

只有满足以下条件，才能认定系统达到财务级一期验收标准：

1. 任意一条报表金额都可追溯到原始文件和原始行
2. 同月数据重复运行结果完全一致
3. 任意 SKU 的月度毛利计算公式透明、可审计
4. 订单漏结算识别基于状态机和账龄，不是简单 0/1
5. 所有无法唯一归属的记录都被阻断或留痕，不被自动猜测
6. 月结能输出正式异常清单和核对差异清单

---

**结论**：  
建议以后续实施以本修订方案为准，将现有《项目规划书》和《设计规范》视为业务方向文档，而不是最终实施依据。若进入开发阶段，应先基于本修订方案重写数据库模型、ETL 流程和月结规则，再开始编码。
