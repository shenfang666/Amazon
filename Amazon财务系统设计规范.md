# Amazon 平台财务系统 · 系统设计规范

**版本**: v1.0 FINAL
**制定日期**: 2026-03-13
**状态**: 设计封版，待编码
**数据目录**: `E:/输出/5. 财务系统搭建/1. Amazon平台/`

> **最高原则**：本系统处理的全部是财务数据，所有逻辑必须有明确依据，不得有模糊假设。每一条规则均标注来源与验证方式。产出必须稳定可复现，在相同输入下永远得到相同结果。

---

## 目录

1. [系统概述](#1-系统概述)
2. [数据范围与约束](#2-数据范围与约束)
3. [原始文件规范](#3-原始文件规范)
4. [数据模型](#4-数据模型)
5. [核心业务逻辑规则](#5-核心业务逻辑规则)
6. [ETL处理规范](#6-etl处理规范)
7. [手工录入模块规范](#7-手工录入模块规范)
8. [数据验证与告警规则](#8-数据验证与告警规则)
9. [报告规范](#9-报告规范)
10. [已知数据质量问题](#10-已知数据质量问题)
11. [系统扩展预留](#11-系统扩展预留)

---

## 1. 系统概述

### 1.1 建设目标

| 目标 | 说明 |
|------|------|
| 数据沉淀 | 将所有Amazon原始报告解析入库，保留最细粒度（订单行级别） |
| 订单结算追踪 | 每笔订单与结算数据双向比对，识别未结算、结算差异 |
| 损益分析 | 按月份 × SKU × 订单类型，精确计算含所有费用的毛利润 |
| 应收款追踪 | 追踪Amazon平台应付款项，识别在途、未到账金额 |
| 报告输出 | 支持多维度筛选，可导出Excel |

### 1.2 技术选型

| 组件 | 选型 | 说明 |
|------|------|------|
| 数据库 | SQLite | 本地文件数据库，无需服务器 |
| ETL | Python 3.14 + pandas + openpyxl | 已验证可用 |
| Web应用 | Streamlit | 本地浏览器访问，支持筛选器和Excel导出 |
| 部署 | 本地运行 | `streamlit run app.py` → http://localhost:8501 |

### 1.3 平台范围

**当前版本**：仅处理 Amazon.com（美国站）数据。
**未来扩展**：系统预留 `platform` 和 `marketplace` 字段，支持后续接入其他平台。

---

## 2. 数据范围与约束

### 2.1 时间范围

| 范围 | 说明 |
|------|------|
| **有效起始月份** | **2025年10月（含）** |
| 排除原因 | 2025年4-9月存在大量TikTok平台MCF跨平台发货数据，与Amazon财务数据混杂，不纳入本系统 |
| 年度文件处理 | 含有2025年全年数据的文件（file 7/8/9/10），加载时自动过滤，仅保留 `date >= 2025-10-01` 的行 |

### 2.2 渠道范围

**Amazon订单的识别标准**（两个维度均须满足）：

| 数据源 | 过滤条件 | 排除的内容 |
|--------|---------|----------|
| 结算明细（type=Order） | `marketplace IN ('amazon.com', 'Amazon.com')` | `sim1.stores.amazon.com` 及其他非Amazon渠道（TikTok MCF等）|
| 订单报告 | `sales-channel = 'Amazon.com'` | `Non-Amazon`、`Non-Amazon US` |

**依据**：经数据核查，`marketplace = 'sim1.stores.amazon.com'` 的订单为其他平台通过Amazon MCF发货，其`item-price`为空（NaN），与Amazon财务无关。

### 2.3 货币

当前数据全部为USD。系统预留货币字段，多币种场景通过汇率表处理（见第7节）。

---

## 3. 原始文件规范

### 3.1 文件命名规范

```
月度文件：{序号}_{类型}_{平台}{YYYYMM}.{扩展名}
主数据文件：{序号}_{类型}_{平台}.{扩展名}
年度文件：{序号}_{类型}_{平台}{YYYY}.{扩展名}

示例：
  3_Settlement Details_Amazon202602.csv
  99_SKU_MASTER.xlsx
  7_FBA customer returns_Amazon2025.csv
```

### 3.2 文件清单与规格

#### File 01：MonthlySummary（月度汇总PDF）

| 属性 | 值 |
|------|-----|
| 文件名模式 | `1_MonthlySummary_Amazon{YYYYMM}.pdf` |
| 格式 | PDF（Amazon Seller Central下载） |
| 用途 | 应收款核对基准：用户从PDF中读取总结算金额，手工录入系统 |
| 系统处理 | 不做自动解析，仅作为应收款核对的对标值 |
| 覆盖月份 | 202510 – 至今 |

#### File 02：订单报告

| 属性 | 值 |
|------|-----|
| 文件名模式 | `2_Order report_Amazon{YYYYMM}.txt` |
| 格式 | TSV（制表符分隔），UTF-8编码 |
| 有效数据起始行 | 第1行（含标题行） |
| 关键列 | `amazon-order-id`, `purchase-date`, `last-updated-date`, `order-status`, `fulfillment-channel`, `sales-channel`, `sku`, `asin`, `quantity`, `item-price`, `promotion-ids` |
| 过滤规则 | 仅保留 `sales-channel = 'Amazon.com'` 的行 |
| 覆盖月份 | 202510 – 至今（按 `purchase-date` 归月） |

#### File 03：结算明细

| 属性 | 值 |
|------|-----|
| 文件名模式 | `3_Settlement Details_Amazon{YYYYMM}.csv` |
| 格式 | CSV，编码和header行数因月份而异（见下表） |
| 关键列 | 见3.3节完整列定义 |
| 过滤规则 | type=Order 时需 `marketplace IN ('amazon.com','Amazon.com')` |
| 覆盖月份 | 202510 – 至今 |

**结算文件格式差异记录**（已核查，按月份）：

| 月份 | 编码 | 有效Header行（skiprows） | 备注 |
|------|------|----------------------|------|
| 202510 | UTF-8-sig | 7 | 英文列名，29列 |
| 202511 | UTF-8-sig | 9（已修正）| 已由用户修正为标准格式 |
| 202512 | UTF-8-sig | 7 | 英文列名，29列 |
| 202601 | UTF-8-sig | 9 | 英文列名，31列（含Transaction Status/Release Date）|
| 202602 | UTF-8-sig | 9 | 英文列名，31列 |

**格式自动检测算法**：

```
1. 依次尝试编码：UTF-8-sig → GBK → cp1252
2. 对成功读取的内容，逐行扫描第0-14行
3. 找到同时包含 'type' 和 'order id' 的行 → 该行为header行
4. 数据从header行+1开始
5. 若未找到 → 触发告警，拒绝导入
```

**结算文件完整列定义**（202601+格式，含所有已知列）：

```
date/time                  | 交易时间（出库日期，用于月份归属）
settlement id              | 结算批次号
type                       | 交易类型（见5.1节）
order id                   | Amazon订单号
sku                        | 卖家SKU
description                | 交易描述
quantity                   | 数量
marketplace                | 平台标识（渠道过滤字段）
fulfillment                | 履行方式
order city/state/postal    | 收货地址
tax collection model       | 税收模式
product sales              | 产品销售额
product sales tax          | 销售税
shipping credits           | 运费补贴
shipping credits tax       | 运费税
gift wrap credits          | 礼品包装费
giftwrap credits tax       | 礼品包装税
Regulatory Fee             | 监管费
Tax On Regulatory Fee      | 监管费税
promotional rebates        | 促销折扣（通常为负数）
promotional rebates tax    | 促销折扣税
marketplace withheld tax   | 平台代扣税
selling fees               | 平台佣金（负数）
fba fees                   | FBA配送费（负数）
other transaction fees     | 其他交易费
other                      | 其他
total                      | 本行净额
Transaction Status         | Released / 空（仅202601+有此列）
Transaction Release Date   | 释放日期（仅202601+有此列）
```

#### File 04：广告报告

| 属性 | 值 |
|------|-----|
| 文件名模式 | `4_Ad Spend_Amazon{YYYYMM}.xlsx` |
| 格式 | XLSX，多Sheet结构（见下表） |
| 有效Sheet | 见下表，按月份不同 |
| 覆盖月份 | 202510 – 至今 |

**广告报告各月格式差异**（已全部核查）：

| 月份 | 有效Sheet | 列数 | SKU列 | Spend列 | 格式类型 |
|------|----------|------|------|--------|--------|
| 202510 | [0]（中文静态名）| 27 | 无`Advertised SKU`，**用产品名[0]→SKU映射** | [16]`花费` | 关键词级 |
| 202511 | `Sponsored_Products_Advertised_p` | 26 | [9]`Advertised SKU` | [15]`Spend` | 产品级 |
| 202512 | `Sponsored_Products_Advertised_p` | 26 | [9]`Advertised SKU` | [15]`Spend` | 产品级（中文静态列名）|
| 202601 | `Sponsored_Products_Advertised_p` | 26 | [9]`Advertised SKU` | [15]`Spend` | 产品级（含XLOOKUP公式）|
| 202602 | 中文静态名 | 26 | [9]`Advertised SKU` | [15]`Spend` | 产品级（含XLOOKUP公式）|
| **202603+** | `Sponsored_Products_Advertised_p` | 26 | [9]`Advertised SKU` | [15]`Spend` | **标准格式（同202601）** |

**广告ETL逻辑**：
- 所有月份只提取：SKU、Spend、Start Date、End Date
- 202510特殊处理：[0]产品名列 → 映射至SKU主表 → 得到SKU；花费在[16]列
- 202601含XLOOKUP公式：读取时使用 `openpyxl data_only=True` 获取公式计算值
- 汇总粒度：按 `sku + 月份` 求和，得到月度SKU广告花费

#### File 05：测评台账

| 属性 | 值 |
|------|-----|
| 文件名模式 | `5_Test Orders_Amazon.xlsx`（无月份后缀，为持续台账）|
| 格式 | XLSX，Sheet1，10列 |
| 维护方式 | 手工录入 |
| SKU获取方式 | `产品名` 列 → 匹配 `99_SKU_MASTER.xlsx` 的产品名列 → 得到SKU |

**台账字段定义**（按实际列顺序）：

| 列序 | 内部字段名 | 说明 |
|------|-----------|------|
| 0 | `seq` | 序号 |
| 1 | `operator` | 操作人员（如Helen、祝老师）|
| 2 | `platform` | 平台（Amazon）|
| 3 | `order_date` | 订单日期（Excel序列号或datetime均支持）|
| 4 | `amazon_order_id` | Amazon订单号（关联结算数据的主键）|
| 5 | `customer` | 测评师名称 |
| 6 | `product_name` | 产品名（中文，用于映射SKU）|
| 7 | `currency` | 货币 |
| 8 | `sale_amount` | 订单销售金额 |
| 9 | `review_cost` | 测评费用（0或正数）|

**SKU映射规则**：
- `product_name` 严格匹配 `sku_master.product_name`（大小写不敏感，去除首尾空格）
- 若无法匹配：**阻断导入**，生成告警，要求在SKU主表中补充该产品名
- 特殊情况：`NMN-ManEnergy-90ct` 与 `NMN-Man E&I-90ct` 共用同一中文产品名，系统无法自动区分。对此，优先通过 `amazon_order_id` 关联结算数据中的SKU字段获取精确SKU；若结算数据中未找到该订单，则保留两个SKU中按字母顺序排列的第一个，并生成黄色警告供人工核查

#### File 06：FBA仓储费

| 属性 | 值 |
|------|-----|
| 文件名模式 | `6_FBA Storage Fees_Amazon{YYYYMM}.csv` |
| 格式 | CSV，UTF-8-sig |
| 关键列 | `asin`, `fnsku`, `month_of_charge`, `estimated_monthly_storage_fee` |
| 归因粒度 | FNSKU级别，通过FNSKU→SKU主表映射到SKU |
| 覆盖月份 | 202510 – 至今 |

#### File 07：退货报告

| 属性 | 值 |
|------|-----|
| 文件名模式（月度）| `7_Return report_Amazon{YYYYMM}.csv` |
| 文件名模式（年度）| `7_FBA customer returns_Amazon{YYYY}.csv` |
| 格式 | CSV，月度为UTF-8-sig，年度为cp1252 |
| 两者关系 | 月度文件是年度文件的子集，系统加载时去重（基于`order-id + return-date + sku`）|
| 年度文件过滤 | 仅保留 `return-date >= 2025-10-01` 的行 |
| 关键列 | `return-date`, `order-id`, `sku`, `quantity`, `reason`, `detailed-disposition` |

#### File 08：移除报告

| 属性 | 值 |
|------|-----|
| 文件名模式（月度）| `8_Removal Fees_Amazon{YYYYMM}.csv` |
| 文件名模式（年度）| `8_Removal Fees_Amazon{YYYY}.csv` |
| 格式 | CSV，UTF-8-sig |
| 两者关系 | 月度是年度子集，系统去重（基于`order-id`）|
| 年度文件过滤 | 仅保留 `request-date >= 2025-10-01` 的行 |
| 关键列 | `request-date`, `order-id`, `sku`, `shipped-quantity`, `removal-fee` |

#### File 09：赔偿报告

| 属性 | 值 |
|------|-----|
| 文件名模式（月度）| `9_Reimbursements_Amazon{YYYYMM}.csv` |
| 文件名模式（年度）| `9_Reimbursements_Amazon{YYYY}.csv` |
| 格式 | CSV，UTF-8-sig |
| 两者关系 | 月度是年度子集，系统去重（基于`reimbursement-id`）|
| 年度文件过滤 | 仅保留 `approval-date >= 2025-10-01` 的行 |
| 关键列 | `approval-date`, `reimbursement-id`, `amazon-order-id`, `sku`, `reason`, `amount-total` |

#### File 10：FBA入库分仓费

| 属性 | 值 |
|------|-----|
| 文件名模式 | `10_FBA inbound placement service fees_Amazon{YYYY}.csv` |
| 格式 | CSV，UTF-8-sig |
| 用途 | Amazon实际收取的入库分仓费，用于与供应链口径核对（见7.3节）|
| 年度文件过滤 | 仅保留 `Transaction date >= 2025-10-01` 的行 |
| 关键列 | `Transaction date`, `FBA shipment ID`, `FNSKU`, `ASIN`, `Total FBA inbound placement service fee charge`, `Total charges` |
| FNSKU映射 | 通过FNSKU→SKU主表映射到SKU |

#### File 98：SKU成本表

| 属性 | 值 |
|------|-----|
| 文件名模式 | `98_SKU Cost Table_Amazon.xlsx`（无月份后缀，覆盖所有月份）|
| 格式 | XLSX，Sheet名 `2.9 SKU Cost Table`，**必须使用 `data_only=True` 读取**（部分单元格为公式）|
| 列定义 | `月份`（文本，格式`YYYY-MM`），`产品`（中文名），`产品成本`（正数，美元），`头程`（正数，美元）|
| SKU映射 | `产品名` → `sku_master.product_name` → `sku` |
| 月份格式 | 统一为文本 `YYYY-MM`（已确认，不再是浮点数）|

**已知问题（见第10节）**：
- 部分单元格为Excel公式字符串（如 `=(-9.39...-0.2)*-1`），须以 `data_only=True` 读取缓存值
- 存在合并产品行（如 `NAD+女性营养素`），此类行**不对应任何单一SKU，必须跳过**
- 合并行识别规则：若产品名在SKU主表中找不到匹配，跳过该行并记录告警

#### File 99：SKU主数据表

| 属性 | 值 |
|------|-----|
| 文件名 | `99_SKU_MASTER.xlsx` |
| 格式 | XLSX，Sheet1，2列 |
| 列定义 | `sku`（卖家SKU，唯一键），`product_name`（中文产品名）|
| 当前记录数 | 13个SKU（含重复中文名情况，见第10节）|
| 维护规则 | 新增SKU必须先在此表添加，否则系统拒绝导入含新SKU的数据 |

---

## 4. 数据模型

### 4.1 维度表

#### `dim_sku` — SKU主数据

```sql
CREATE TABLE dim_sku (
    sku             TEXT PRIMARY KEY,
    product_name    TEXT NOT NULL,     -- 中文产品名
    is_active       INTEGER DEFAULT 1, -- 1=有效，0=停用
    created_at      TEXT,
    updated_at      TEXT
);
```

#### `dim_sku_cost` — SKU月度成本

```sql
CREATE TABLE dim_sku_cost (
    sku              TEXT NOT NULL,
    cost_month       TEXT NOT NULL,   -- YYYY-MM
    unit_cost        REAL NOT NULL,   -- 产品采购成本（美元/件，正数）
    inbound_fee      REAL NOT NULL,   -- 头程费（美元/件，正数）
    total_unit_cost  REAL GENERATED ALWAYS AS (unit_cost + inbound_fee) STORED,
    source_formula   TEXT,            -- 原始公式字符串（仅当来源为公式时记录，供审计）
    PRIMARY KEY (sku, cost_month)
);
```

#### `dim_exchange_rate` — 月度汇率

```sql
CREATE TABLE dim_exchange_rate (
    period_month    TEXT NOT NULL,    -- YYYY-MM
    from_currency   TEXT NOT NULL,    -- 源货币（如CNY）
    to_currency     TEXT NOT NULL,    -- 目标货币（如USD）
    rate            REAL NOT NULL,    -- 月末汇率（1单位源货币=多少目标货币）
    PRIMARY KEY (period_month, from_currency, to_currency)
);
```

### 4.2 核心事实表

#### `fact_settlement` — 结算流水（最细粒度，每行对应Amazon结算表一行）

```sql
CREATE TABLE fact_settlement (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    -- 来源追踪
    source_file              TEXT NOT NULL,   -- 原始文件名，用于审计和去重
    source_row_hash          TEXT NOT NULL,   -- 整行内容的SHA256哈希（去重用）
    -- 时间维度
    transaction_datetime     TEXT NOT NULL,   -- 原始时间字符串（出库日期）
    transaction_month        TEXT NOT NULL,   -- YYYY-MM（从transaction_datetime提取）
    -- 结算信息
    settlement_id            TEXT NOT NULL,
    transaction_type         TEXT NOT NULL,   -- 见5.1节类型定义
    transaction_subtype      TEXT,            -- description字段
    -- 订单信息
    order_id                 TEXT,            -- Amazon订单号（部分类型为空）
    sku                      TEXT,            -- 卖家SKU（部分类型为空）
    quantity                 INTEGER,
    marketplace              TEXT,            -- 渠道标识
    -- 金额字段（均为原始值，正负号保留原样）
    product_sales            REAL DEFAULT 0,
    product_sales_tax        REAL DEFAULT 0,
    shipping_credits         REAL DEFAULT 0,
    promotional_rebates      REAL DEFAULT 0,
    selling_fees             REAL DEFAULT 0,  -- 平台佣金，通常为负
    fba_fees                 REAL DEFAULT 0,  -- FBA配送费，通常为负
    other_fees               REAL DEFAULT 0,
    total                    REAL DEFAULT 0,  -- 本行净额
    -- 状态（仅202601+文件有此字段）
    transaction_status       TEXT,            -- Released / 空
    transaction_release_date TEXT,
    -- 系统打标
    order_type               TEXT,            -- 见5.2节：7种订单类型
    is_amazon_channel        INTEGER,         -- 1=Amazon渠道，0=非Amazon渠道
    loaded_at                TEXT DEFAULT (datetime('now')),
    UNIQUE(source_row_hash)                   -- 绝对防止重复导入
);
```

#### `fact_orders` — 订单报告

```sql
CREATE TABLE fact_orders (
    amazon_order_id    TEXT PRIMARY KEY,
    source_file        TEXT NOT NULL,
    purchase_date      TEXT NOT NULL,
    order_month        TEXT NOT NULL,        -- YYYY-MM
    last_updated_date  TEXT,
    order_status       TEXT NOT NULL,        -- Shipped/Cancelled/Pending/Shipping
    fulfillment_channel TEXT,
    sales_channel      TEXT,
    sku                TEXT,
    asin               TEXT,
    quantity           INTEGER,
    item_price         REAL,
    promotion_ids      TEXT,
    -- 关联结算（ETL后期补充）
    is_settled         INTEGER DEFAULT 0,    -- 0=未结算，1=已结算
    settlement_id      TEXT,
    settled_gross      REAL,                 -- 结算中product_sales合计
    settled_net        REAL,                 -- 结算中total合计
    settlement_status  TEXT,                 -- Released/Pending/未结算
    loaded_at          TEXT DEFAULT (datetime('now'))
);
```

#### `fact_advertising` — 广告月度消耗

```sql
CREATE TABLE fact_advertising (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file    TEXT NOT NULL,
    ad_month       TEXT NOT NULL,            -- YYYY-MM（从Start Date提取）
    sku            TEXT NOT NULL,
    campaign_name  TEXT,
    spend          REAL NOT NULL DEFAULT 0,  -- 广告花费（月度SKU合计）
    impressions    INTEGER,
    clicks         INTEGER,
    sales_7d       REAL,
    acos           REAL,
    loaded_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(ad_month, sku, campaign_name)
);
```

#### `fact_storage_fees` — 月度仓储费

```sql
CREATE TABLE fact_storage_fees (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file                 TEXT NOT NULL,
    charge_month                TEXT NOT NULL,   -- YYYY-MM
    asin                        TEXT,
    fnsku                       TEXT,
    sku                         TEXT,            -- 通过FNSKU映射
    avg_qty_on_hand             REAL,
    estimated_monthly_storage_fee REAL NOT NULL DEFAULT 0,
    total_incentive_fee         REAL DEFAULT 0,
    loaded_at                   TEXT DEFAULT (datetime('now')),
    UNIQUE(charge_month, fnsku)
);
```

#### `fact_removals` — 移除订单

```sql
CREATE TABLE fact_removals (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file      TEXT NOT NULL,
    order_id         TEXT NOT NULL,
    request_date     TEXT,
    removal_month    TEXT,                    -- YYYY-MM
    sku              TEXT,
    shipped_quantity INTEGER DEFAULT 0,
    removal_fee      REAL DEFAULT 0,
    currency         TEXT DEFAULT 'USD',
    loaded_at        TEXT DEFAULT (datetime('now')),
    UNIQUE(order_id)
);
```

#### `fact_returns` — 退货记录

```sql
CREATE TABLE fact_returns (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file          TEXT NOT NULL,
    return_date          TEXT NOT NULL,
    return_month         TEXT NOT NULL,       -- YYYY-MM
    order_id             TEXT,
    sku                  TEXT,
    asin                 TEXT,
    fnsku                TEXT,
    quantity             INTEGER DEFAULT 0,
    fulfillment_center   TEXT,
    detailed_disposition TEXT,
    reason               TEXT,
    status               TEXT,
    loaded_at            TEXT DEFAULT (datetime('now')),
    UNIQUE(order_id, return_date, sku)        -- 防重复（年度+月度文件去重）
);
```

#### `fact_compensations` — 赔偿记录

```sql
CREATE TABLE fact_compensations (
    reimbursement_id   TEXT PRIMARY KEY,
    source_file        TEXT NOT NULL,
    approval_date      TEXT NOT NULL,
    comp_month         TEXT NOT NULL,         -- YYYY-MM
    amazon_order_id    TEXT,
    sku                TEXT,
    asin               TEXT,
    reason             TEXT,
    amount_total       REAL NOT NULL DEFAULT 0,
    qty_cash           INTEGER DEFAULT 0,
    qty_inventory      INTEGER DEFAULT 0,
    loaded_at          TEXT DEFAULT (datetime('now'))
);
```

#### `fact_fba_inbound_fees` — FBA入库分仓费（Amazon收取）

```sql
CREATE TABLE fact_fba_inbound_fees (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file        TEXT NOT NULL,
    transaction_date   TEXT NOT NULL,
    fee_month          TEXT NOT NULL,         -- YYYY-MM
    shipment_id        TEXT,
    fnsku              TEXT,
    asin               TEXT,
    sku                TEXT,                  -- 通过FNSKU映射
    charge_amount      REAL NOT NULL DEFAULT 0,
    currency           TEXT DEFAULT 'USD',
    loaded_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(shipment_id, fnsku)
);
```

#### `fact_review_orders` — 测评台账

```sql
CREATE TABLE fact_review_orders (
    amazon_order_id  TEXT PRIMARY KEY,
    source_file      TEXT NOT NULL,
    order_date       TEXT,
    order_month      TEXT,                    -- YYYY-MM
    operator         TEXT,
    platform         TEXT DEFAULT 'Amazon',
    product_name     TEXT NOT NULL,           -- 原始产品名
    sku              TEXT,                    -- 通过产品名映射，可为空（映射失败时）
    currency         TEXT DEFAULT 'USD',
    sale_amount      REAL DEFAULT 0,
    review_cost      REAL DEFAULT 0,
    loaded_at        TEXT DEFAULT (datetime('now'))
);
```

### 4.3 手工录入表

#### `manual_vine_enrollment_fee` — Vine计划注册费手工录入

```sql
CREATE TABLE manual_vine_enrollment_fee (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month   TEXT NOT NULL,             -- YYYY-MM（该费用所属月份）
    sku            TEXT NOT NULL,             -- 必填，Vine注册的SKU
    amount         REAL NOT NULL,             -- 正数，美元
    settlement_ref TEXT,                      -- 对应结算表中的行描述（供核查）
    notes          TEXT,
    created_at     TEXT DEFAULT (datetime('now')),
    updated_at     TEXT,
    UNIQUE(period_month, sku)
);
```

#### `manual_fba_inbound_reconciliation` — 入库费核对手工录入

```sql
CREATE TABLE manual_fba_inbound_reconciliation (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month            TEXT NOT NULL,    -- YYYY-MM
    sku                     TEXT,             -- 可为空（按批次无法精确到SKU时）
    shipment_id             TEXT,
    -- 三个层次的金额
    system_amazon_charged   REAL,             -- Layer 1：系统从file 10自动计算
    system_amazon_reversed  REAL DEFAULT 0,   -- Layer 2：系统从settlement中FBA Inventory Fee - Reversal自动计算
    system_net              REAL,             -- = L1 - L2（系统计算净值）
    manual_supply_chain     REAL,             -- Layer 3：用户填写，供应链确认金额（必填）
    difference              REAL,             -- = system_net - manual_supply_chain（系统自动计算）
    -- 差异处理
    adjustment_status       TEXT DEFAULT 'pending', -- pending/confirmed/disputed
    error_charge_flag       INTEGER DEFAULT 0,-- 1=确认为Amazon错误扣款（影响应收款）
    notes                   TEXT,
    created_at              TEXT DEFAULT (datetime('now')),
    updated_at              TEXT
);
```

#### `manual_shared_costs` — 共摊费用手工录入

```sql
CREATE TABLE manual_shared_costs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    period_month      TEXT NOT NULL,           -- YYYY-MM
    cost_type         TEXT NOT NULL,           -- 人员/研发/检测/其他
    description       TEXT,
    total_amount      REAL NOT NULL,           -- 总金额
    currency          TEXT NOT NULL DEFAULT 'USD',
    platforms         TEXT DEFAULT 'all',      -- all / Amazon / 逗号分隔
    allocation_method TEXT NOT NULL DEFAULT 'revenue_share', -- revenue_share/direct/custom
    direct_sku        TEXT,                    -- 仅allocation_method=direct时填写
    custom_pct_json   TEXT,                    -- 仅custom时填写，JSON格式 {"SKU-A":0.6}
    created_at        TEXT DEFAULT (datetime('now'))
);
```

#### `manual_monthly_summary_amount` — MonthlySummary核对金额

```sql
CREATE TABLE manual_monthly_summary_amount (
    period_month       TEXT PRIMARY KEY,       -- YYYY-MM
    pdf_net_amount     REAL NOT NULL,          -- 用户从PDF读取的结算净额
    notes              TEXT,
    created_at         TEXT DEFAULT (datetime('now'))
);
```

### 4.4 系统管理表

#### `data_load_log` — 数据导入日志

```sql
CREATE TABLE data_load_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    load_time       TEXT DEFAULT (datetime('now')),
    file_name       TEXT NOT NULL,
    file_type       TEXT NOT NULL,
    period_month    TEXT,
    rows_loaded     INTEGER,
    rows_skipped    INTEGER,
    rows_duplicate  INTEGER,
    status          TEXT,                      -- success/failed/warning
    error_message   TEXT,
    validation_log  TEXT                       -- JSON，记录所有验证结果
);
```

#### `schema_fingerprints` — 文件格式指纹

```sql
CREATE TABLE schema_fingerprints (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    file_type       TEXT NOT NULL,             -- settlement/orders/ads/...
    period_month    TEXT,
    column_hash     TEXT NOT NULL,             -- 列名组合的哈希
    column_names    TEXT NOT NULL,             -- 实际列名（JSON数组）
    encoding        TEXT,
    skiprows        INTEGER,
    first_seen      TEXT,
    last_seen       TEXT
);
```

---

## 5. 核心业务逻辑规则

### 5.1 Transaction Type 完整分类

| type值 | description值 | 财务含义 | P&L归属 | 应收款影响 |
|--------|-------------|---------|--------|---------|
| `Order` | 产品描述 | 销售收入（含佣金、FBA费等扣款）| 按order_type分类 | ✅ 计入 |
| `Refund` | 产品描述 | 退款（负向销售）| 按order_type分类 | ✅ 计入 |
| `FBA Inventory Fee` | `FBA Long-Term Storage Fee` | 长期仓储费 | 仓储费 | ✅ 计入 |
| `FBA Inventory Fee` | `FBA storage fee` | 月度仓储费（结算表版）| 仓储费 | ✅ 计入 |
| `FBA Inventory Fee` | `FBA Removal Order: Return Fee` | 移除退回费 | 移除费 | ✅ 计入 |
| `FBA Inventory Fee` | `FBA Removal Order: Disposal Fee` | 移除销毁费 | 移除费 | ✅ 计入 |
| `FBA Inventory Fee - Reversal` | NaN | 入库分仓费退回（见7.3节）| **不计入P&L** | ✅ 计入（冲减应收）|
| `Service Fee` | `Cost of Advertising` | 广告费扣款（结算口径）| 不计入P&L（用广告报告口径）| ✅ 计入应收 |
| `Service Fee` | `FBA Inbound Placement Service Fee` | 入库分仓费（结算表版）| 见7.3节三层逻辑 | ✅ 计入 |
| `Service Fee` | `Subscription` | Amazon月租费（$39.99）| 平台固定费用 | ✅ 计入 |
| `Service Fee` | `Coupon Redemption Fee: *` | 优惠券费用 | 平台费用 | ✅ 计入 |
| `Amazon Fees` | `Vine Enrollment Fee` | Vine计划注册费（见7.2节）| 平台费用（按SKU，手工录入）| ✅ 计入 |
| `Amazon Fees` | `Coupon Participation Fee` | 优惠券参与费 | 平台费用 | ✅ 计入 |
| `Amazon Fees` | `Coupon Performance Based Fee` | 优惠券绩效费 | 平台费用 | ✅ 计入 |
| `Amazon Fees - Reversal` | — | Amazon费用冲回（之前多收）| 平台费用冲抵（正向）| ✅ 计入 |
| `Amazon Fees - Correction` | — | Amazon费用更正 | 平台费用调整 | ✅ 计入 |
| `Adjustment` | `FBA Inventory Reimbursement - *` | 库存赔偿（见赔偿报告）| 赔偿收入 | ✅ 计入 |
| `Transfer` | `To your account ending in: *` | 实际打款至卖家账户 | 不影响P&L | ✅ 计入（应收款减少）|
| `Debt` | `amzn1.cam.v1.sgid.*` | Amazon余额不足，从信用卡扣款 | 不影响P&L（已从信用卡收取）| ✅ 计入应收（负向，即卖家欠Amazon）|

### 5.2 订单类型分类（order_type）

**判断顺序**（严格按以下优先级，满足第一个即停止）：

```
Step 1: 取消订单
  条件：订单在 fact_orders 中 order_status = 'Cancelled'
  → order_type = 'cancelled'
  注意：取消订单不出现在结算表中，仅在订单报告中体现

Step 2: Vine订单（销售）
  条件：
    fact_settlement.type = 'Order'
    AND fact_settlement.marketplace IN ('amazon.com', 'Amazon.com')
    AND fact_settlement.order_id IS NOT NULL
    AND fact_settlement.product_sales > 0
    AND ABS(fact_settlement.product_sales + fact_settlement.promotional_rebates) < 0.01
  → order_type = 'vine_sale'
  依据：Vine计划为消费者提供免费产品，平台通过promotional_rebates完全抵消product_sales
  验证：当月或相邻月份应存在 Vine Enrollment Fee 记录（软警告，不阻断）

Step 3: Vine退款
  条件：
    fact_settlement.type = 'Refund'
    AND 对应原始订单（order_id）已被标记为 vine_sale
  → order_type = 'vine_refund'
  注意：历史上未发生过，如发生需单独核查

Step 4: 测评订单（销售）
  条件：
    fact_settlement.order_id 存在于 fact_review_orders.amazon_order_id
    AND fact_settlement.type = 'Order'
  → order_type = 'review_sale'

Step 5: 测评订单（退款）
  条件：
    fact_settlement.order_id 存在于 fact_review_orders.amazon_order_id
    AND fact_settlement.type = 'Refund'
  → order_type = 'review_refund'

Step 6: 正常退款
  条件：fact_settlement.type = 'Refund'（Step 3/5均未命中）
  → order_type = 'normal_refund'

Step 7: 正常销售
  条件：以上所有均未命中，fact_settlement.type = 'Order'
  → order_type = 'normal_sale'
```

### 5.3 月份归属规则

**统一使用结算数据中的 `date/time` 字段（出库日期）作为月份归属依据。**

- `date/time` 格式：`Nov 1, 2025 12:11:18 AM PDT`（美国太平洋时间）
- 提取月份时转换为UTC后取年月，格式 `YYYY-MM`
- 不使用 settlement_id 的起止日期来归属月份

### 5.4 去重规则

| 数据表 | 去重键 | 逻辑 |
|--------|--------|------|
| fact_settlement | `source_row_hash`（整行SHA256）| 完全相同的行才算重复，不以settlement_id为准 |
| fact_orders | `amazon_order_id` | 主键唯一 |
| fact_returns | `order_id + return_date + sku` | 防年度+月度文件重复 |
| fact_removals | `order_id` | 主键唯一 |
| fact_compensations | `reimbursement_id` | 主键唯一 |
| fact_fba_inbound_fees | `shipment_id + fnsku` | 联合唯一 |

### 5.5 FNSKU → SKU 映射规则

1. 优先从 `fact_fba_inbound_fees` 或 `fact_storage_fees` 的历史记录中查找同一FNSKU对应的SKU
2. 若无历史记录，从 `fact_settlement` 中查找同一FNSKU出现过的SKU
3. 若仍无法映射：**黄色告警**，记录未映射FNSKU，等待人工补充
4. 约束：一个FNSKU必须唯一对应一个SKU；若发现一对多，**红色告警，阻断导入**

---

## 6. ETL处理规范

### 6.1 每月导入执行顺序（必须按序）

```
Step 1: 导入 File 99（SKU主表）         ← 基准，无则后续全部失败
Step 2: 导入 File 98（成本表）          ← 依赖SKU主表
Step 3: 导入 File 03（结算明细）        ← 核心
Step 4: 导入 File 02（订单报告）        ← 依赖结算（关联结算状态）
Step 5: 导入 File 05（测评台账）        ← 依赖SKU主表
Step 6: 执行 订单类型分类逻辑           ← 依赖Step 3/4/5
Step 7: 导入 File 04（广告报告）        ← 依赖SKU主表
Step 8: 导入 File 06（仓储费）          ← 依赖FNSKU映射
Step 9: 导入 File 08（移除报告）
Step 10: 导入 File 07（退货报告）
Step 11: 导入 File 09（赔偿报告）
Step 12: 导入 File 10（FBA入库费）      ← 依赖FNSKU映射
Step 13: 手工录入核对（用户操作）       ← 见第7节
Step 14: 运行全量数据验证
Step 15: 生成报告
```

### 6.2 结算文件读取算法（详细）

```python
def load_settlement(filepath):
    # 1. 编码检测
    for encoding in ['utf-8-sig', 'gbk', 'cp1252']:
        try:
            raw = open(filepath, encoding=encoding, errors='strict').read(10000)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"无法识别文件编码: {filepath}")

    # 2. Header行定位
    lines = raw.split('\n')
    skiprows = None
    for i, line in enumerate(lines[:15]):
        fields = line.lower()
        if 'type' in fields and 'order id' in fields:
            skiprows = i
            break
    if skiprows is None:
        raise ValueError(f"未找到header行，请人工检查文件格式: {filepath}")

    # 3. 读取数据
    df = pd.read_csv(filepath, skiprows=skiprows, encoding=encoding,
                     on_bad_lines='skip', low_memory=False)

    # 4. 列名标准化（中英文统一映射）
    df = normalize_column_names(df)  # 见字段映射表

    # 5. 渠道过滤
    df = df[df['marketplace'].isin(['amazon.com', 'Amazon.com']) |
            df['type'].isin(['FBA Inventory Fee', 'Service Fee', 'Amazon Fees',
                             'Amazon Fees - Reversal', 'Amazon Fees - Correction',
                             'FBA Inventory Fee - Reversal', 'Adjustment',
                             'Transfer', 'Debt'])]
    return df
```

### 6.3 成本表读取注意事项

```python
# 必须使用 data_only=True 获取公式计算值
wb = openpyxl.load_workbook(filepath, data_only=True)
ws = wb.active

for row in ws.iter_rows(min_row=2, values_only=True):
    month, product_name, unit_cost, inbound_fee = row[0], row[1], row[2], row[3]

    # 跳过空行
    if not month or not product_name:
        continue

    # 跳过合并产品行（产品名在SKU主表中找不到）
    sku = lookup_sku_by_name(product_name)
    if sku is None:
        log_warning(f"成本表产品名 [{product_name}] 在SKU主表中无匹配，已跳过")
        continue

    # 验证值为数字（非公式字符串）
    if isinstance(unit_cost, str) or isinstance(inbound_fee, str):
        log_error(f"成本表 [{month}][{product_name}] 包含未计算的公式字符串，"
                  f"请在Excel中重新保存后再导入")
        raise ValueError("成本表包含未计算公式")

    # 验证为正数
    if unit_cost < 0 or inbound_fee < 0:
        log_error(f"成本值为负数: {month} {product_name}")
        raise ValueError("成本值不得为负数")
```

---

## 7. 手工录入模块规范

### 7.1 汇率录入

- **时机**：每月导入数据前完成
- **内容**：月末汇率，从指定汇率表格读取
- **格式**：每行一条，字段：`period_month, from_currency, to_currency, rate`
- **验证**：rate必须为正数；period_month格式必须为YYYY-MM

### 7.2 Vine Enrollment Fee录入（必填）

**触发条件**：每次结算数据导入后，系统检测到 `Amazon Fees → Vine Enrollment Fee` 记录时，**阻断当月报告生成**，强制用户完成录入。

**录入页面字段**：

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| period_month | 下拉选择 | 必填 | 当前导入月份 |
| sku | 下拉选择（来自SKU主表）| 必填 | 该费用归属的SKU |
| amount | 数字输入 | 必填，正数 | 美元金额（与结算表原值一致）|
| settlement_ref | 文本 | 可选 | 结算表中对应的描述（供核查）|
| notes | 文本 | 可选 | 备注 |

**逻辑约束**：
- 所有 `Vine Enrollment Fee` 行的金额合计，必须等于用户录入条目的金额合计（差额 < $0.01），否则不允许保存
- 同一月份同一SKU可录入多条（对应多次注册）

### 7.3 FBA入库费核对（三层逻辑）

**执行时机**：File 10和结算数据导入完成后，系统自动计算Layer 1和Layer 2，生成核对工单。

**三个层次**：

| 层次 | 来源 | 含义 | 系统/手工 |
|------|------|------|---------|
| Layer 1（Amazon收取）| `fact_fba_inbound_fees`（来自file 10）| Amazon实际扣款金额 | **系统自动计算** |
| Layer 2（Amazon退回）| `fact_settlement` 中 `type='FBA Inventory Fee - Reversal'` | Amazon退回的金额 | **系统自动计算** |
| Layer 3（供应链口径）| `manual_fba_inbound_reconciliation.manual_supply_chain` | 供应链实际发生金额 | **用户手工输入** |

**计算规则**：

```
系统净值 = Layer 1 - Layer 2
差异金额 = 系统净值 - Layer 3

若 差异金额 ≠ 0：
  → 系统生成差异提示，要求用户确认
  → 用户可标记差异原因：
    a) 正常时间差（下月退款）→ 标记为 pending，追踪至下月
    b) Amazon错误扣款 → 标记为 error_charge，计入应收款追踪项
    c) 供应链填写有误 → 修正Layer 3
```

**P&L影响**：
- 进入P&L的入库费 = **Layer 3（供应链口径）**
- Layer 2（退款）不影响P&L，仅影响应收款
- 差异追踪项在报告中**单独列示一行**

### 7.4 共摊费用录入

**支持的分摊方式**：

| 方式 | 代码 | 逻辑 |
|------|------|------|
| 按净销售额比例 | `revenue_share` | 各SKU当月净销售额 / Amazon平台净销售额合计 |
| 直接归因 | `direct` | 指定单一SKU承担全部金额 |
| 自定义比例 | `custom` | JSON格式指定各SKU比例，合计须等于1.0 |

**多平台分摊**：若 `platforms='all'`，先按Amazon平台当月净销售额在全平台净销售额中的占比，计算Amazon应承担份额；再在Amazon内部按上述方式分摊到SKU。

### 7.5 MonthlySummary金额录入

**时机**：每月完成全部数据导入后
**内容**：用户打开PDF，找到"Net Proceeds"或"Total Settlement Amount"，填入系统
**用途**：应收款报告中的核对项

---

## 8. 数据验证与告警规则

### 8.1 告警级别定义

| 级别 | 标识 | 含义 | 系统行为 |
|------|------|------|---------|
| 红色 | 🔴 | 数据无法安全处理，有计算错误风险 | **阻断导入，必须人工处理后重试** |
| 黄色 | 🟡 | 数据可能不完整，但不影响已有数据 | **显示警告，用户确认后可继续** |
| 蓝色 | 🔵 | 信息提示，无需操作 | 记录日志，不打断流程 |

### 8.2 文件导入时验证

| 检查项 | 级别 | 触发条件 | 说明 |
|--------|------|---------|------|
| 关键列缺失 | 🔴 | 文件中找不到type/order id/sku/total等核心列（即使经AI映射后仍无法确认）| 无法处理 |
| 金额列非数字 | 🔴 | total/product sales等列包含无法转为数字的值 | 数据损坏 |
| 空文件 | 🔴 | 数据行数为0 | 文件异常 |
| 文件编码无法识别 | 🔴 | UTF-8-sig/GBK/cp1252三种均失败 | 需手动转换 |
| 文件名月份与数据月份不一致 | 🟡 | 文件名中的YYYYMM与数据中最多的月份不符 | 可能上传错误 |
| 列名变更（AI映射置信度<90%）| 🟡 | 关键列无法精确匹配，AI映射建议置信度不足 | 需人工确认映射 |
| 重复导入（整行hash匹配）| 🔵 | 与已入库数据完全一致 | 跳过，记录跳过数量 |

### 8.3 业务逻辑验证

| 检查项 | 级别 | 触发条件 |
|--------|------|---------|
| 新SKU未在主表中 | 🔴 | 结算/订单/广告中出现SKU主表没有的SKU |
| SKU成本缺失 | 🔴 | 当月结算中有该SKU的Order记录，但dim_sku_cost中无对应月份成本 |
| FNSKU一对多 | 🔴 | 同一FNSKU对应多个SKU |
| Vine费用未录入 | 🔴 | 结算中有Vine Enrollment Fee，但manual_vine_enrollment_fee中无本月记录 |
| 成本表包含公式字符串 | 🔴 | data_only=True读取后值仍为字符串 |
| Vine识别无对应费用 | 🟡 | 识别出Vine订单，但前后2个月内无Vine Enrollment Fee |
| FNSKU映射失败 | 🟡 | 仓储/入库费中有FNSKU无法映射到SKU |
| 测评台账产品名无匹配 | 🔴 | 台账中产品名在SKU主表中找不到 |
| FBA入库费差异 | 🟡 | Layer 3与system_net不一致 |
| 应收款核对差异 | 🟡 | 系统计算结算净额与PDF录入金额差异 > $1.00 |
| 月度订单量异常 | 🟡 | 当月Order类型行数与同SKU历史均值偏差 > 200% |
| Vine订单比例异常 | 🟡 | 当月Vine订单占总Order比例与历史偏差 > 50% |
| 成本表合并行 | 🟡 | 产品名在SKU主表无匹配（跳过并记录）|

### 8.4 AI辅助字段映射规范

**触发条件**：精确列名匹配失败时启用

**处理流程**：
```
1. 提取文件所有列名 + 每列前3行样本值
2. 与标准字段映射库（中英文双语）进行模糊匹配
3. 对无法匹配的列，调用AI语义判断
4. 输出：字段名 + 映射建议 + 置信度
5. 置信度 ≥ 90%：自动采用，记录日志
   置信度 60%-89%：UI显示建议，需用户点击确认
   置信度 < 60%：标记为 🔴，拒绝导入，要求人工处理
6. 确认的映射写入 schema_fingerprints 表，供后续复用
```

**标准字段映射库（部分示例）**：

| 标准内部名 | 已知英文变体 | 已知中文变体 |
|-----------|------------|------------|
| `sku` | Advertised SKU, seller-sku | 所在SKU、广告商品SKU |
| `spend` | Spend, Cost | 花费, 广告花费 |
| `start_date` | Start Date, date/time | 开始日期, 交易时间 |
| `product_sales` | product sales | 产品销售额 |
| `total` | total | 总计 |
| `impressions` | Impressions | 展示量 |

---

## 9. 报告规范

### 9.1 报告A：订单结算比对表

**用途**：追踪每笔Amazon订单的结算状态，识别漏结算、结算异常

**数据来源**：`fact_orders` LEFT JOIN `fact_settlement`

**核心字段**：

| 字段 | 说明 |
|------|------|
| amazon_order_id | 订单号 |
| order_month | 下单月份（YYYY-MM）|
| sku + 产品名 | 商品 |
| order_status | 订单状态 |
| order_type | 系统分类的订单类型 |
| item_price | 下单金额 |
| is_settled | 是否在结算表中出现 |
| settlement_id | 结算批次 |
| settled_product_sales | 结算中的product_sales |
| settled_net | 结算净额（total字段）|
| transaction_status | Released/Pending/未结算 |
| price_gap | item_price - settled_product_sales |

**筛选维度**：月份、SKU、订单状态、是否结算、订单类型

---

### 9.2 报告B：月度SKU损益明细

**用途**：最细粒度损益分析，支持按月份×SKU×订单类型任意组合查询

**数据来源**：多表聚合

**核心字段**：

| 字段 | 来源 | 说明 |
|------|------|------|
| period_month | fact_settlement.transaction_month | 月份 |
| sku + 产品名 | dim_sku | 商品 |
| order_type | fact_settlement | 订单类型（7种）|
| qty_sold | fact_settlement.quantity | 销售数量 |
| gmv | SUM(product_sales) | 毛销售额 |
| promotional_rebates | SUM(promotional_rebates) | 促销折扣 |
| net_sales | gmv + promotional_rebates | 净销售额 |
| selling_fees | SUM(selling_fees) | 平台佣金 |
| fba_fees | SUM(fba_fees) | FBA配送费 |
| storage_fees | fact_storage_fees | 月度仓储费 |
| removal_fees | fact_removals | 移除费 |
| vine_enrollment_fee | manual_vine_enrollment_fee | Vine注册费（按SKU）|
| subscription_fee | fact_settlement（Service Fee/Subscription）| 月租费（$39.99，按SKU净销售额分摊）|
| ad_spend | fact_advertising | 广告花费（广告报告口径）|
| compensation | fact_compensations | 赔偿收入 |
| review_cost | fact_review_orders | 测评费用 |
| unit_cost | dim_sku_cost | 单位成本 |
| inbound_fee_per_unit | dim_sku_cost | 头程费/件 |
| cogs | unit_cost × qty_sold | 销售成本 |
| gross_profit | 见下方公式 | 毛利润 |
| gross_margin_pct | gross_profit / net_sales | 毛利率 |

**毛利润计算公式**：
```
gross_profit = net_sales
             + selling_fees          (负数)
             + fba_fees              (负数)
             - storage_fees          (正数，减)
             - removal_fees          (正数，减)
             - vine_enrollment_fee   (正数，减)
             - subscription_fee      (正数，减)
             - ad_spend              (正数，减)
             + compensation          (正数，加)
             - review_cost           (正数，减)
             - cogs                  (正数，减)
```

---

### 9.3 报告C：应收平台款项报告

**用途**：追踪Amazon平台欠卖家的款项，识别已到账/未到账金额

**计算逻辑**：

```
每个settlement_id（结算批次）：

应收总额 = SUM(total) of all Released transactions
         （含Order + Refund + Adjustment + 各类费用，不含Transfer）

已到账   = SUM(total) of type='Transfer'（实际打款）

Debt扣款 = SUM(total) of type='Debt'（信用卡扣款，为负值，减少应收）

未到账   = 应收总额 - 已到账 + Debt扣款

FBA入库费退款追踪 = manual_fba_inbound_reconciliation 中
                    error_charge_flag=1 且 adjustment_status='disputed'
                    的金额（单独列示）
```

**报告字段**：

| 字段 | 说明 |
|------|------|
| settlement_id | 结算批次号 |
| settlement_period | 结算周期（最早date - 最晚date）|
| gross_revenue | 销售总额（product_sales合计）|
| total_amazon_fees | 平台各类费用合计 |
| total_adjustments | 调整项合计 |
| net_receivable | 应收净额 |
| transferred | 已到账（Transfer合计）|
| debt_charged | Debt扣款 |
| outstanding | 未到账余额 |
| pdf_amount | MonthlySummary PDF录入金额 |
| reconciliation_gap | outstanding - pdf_amount |
| fba_reversal_tracking | 入库费退款追踪金额 |
| status | 已结清 / 未结清 |

---

### 9.4 报告D：指定期间利润表（P&L）

**用途**：指定任意起止月份，生成完整的经营利润表，支持按SKU汇总或拆分

**利润表结构**：

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  利润表  [YYYY-MM] 至 [YYYY-MM]  |  SKU: [全部/指定]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

一、营业收入
  1. 正常销售净额（normal_sale）          xxx
  2. 测评订单净额（review_sale）          xxx
  3. Vine订单（vine_sale，净额=$0）         0
  4. 退款（normal_refund + review_refund） (xxx)
  5. 促销折扣（promotional_rebates）      (xxx)
  ─────────────────────────────────────
     合计营业收入                         xxx

二、销售成本（COGS）
  6. 产品采购成本（unit_cost × qty）      (xxx)
  7. 头程费分摊（inbound_fee × qty）      (xxx)
  ─────────────────────────────────────
     毛利润（一 - 二）                    xxx   毛利率 xx%

三、Amazon平台费用
  8. 平台佣金（selling fees）            (xxx)
  9. FBA配送费（fba fees）               (xxx)
  10. 月度仓储费                         (xxx)
  11. 移除费                             (xxx)
  12. Vine注册费（按SKU手工分配）         (xxx)
  13. Amazon月租费（$39.99分摊）          (xxx)
  14. 优惠券费用                         (xxx)
  15. 其他Amazon Fees                    (xxx)
  16. Amazon Fees冲抵（Reversal/Correction） xxx
  ─────────────────────────────────────
     平台费用合计                        (xxx)

四、FBA入库分仓费（供应链口径）
  17. 入库分仓费（Layer 3，手工确认）     (xxx)
  18. 差异追踪项（Layer 1-L2 vs L3）      [单独列示，不计入利润]

五、广告费用
  19. 广告花费（广告报告实际消耗）        (xxx)

六、测评费用
  20. 站外测评费用（review_cost）         (xxx)

七、共摊费用（Amazon应承担部分）
  21. 人员费用分摊                       (xxx)
  22. 研发费用分摊                       (xxx)
  23. 检测费用分摊                       (xxx)
  24. 其他共摊费用                       (xxx)

八、其他收入
  25. 赔偿收入（FBA库存赔偿）             xxx

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
     经营利润                            xxx   利润率 xx%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

**重要说明**：
- 广告费用使用**广告报告口径**（实际消耗），不使用结算表中的`Cost of Advertising`（结算口径用于应收款报告）
- 入库分仓费差异追踪项**单独列示，不计入利润表**

---

### 9.5 报告E：月度经营看板

**展示要素**：
- 月度KPI卡片：总销售额、毛利润、毛利率、广告ACOS、退货率、取消率
- 各SKU销售额/利润趋势折线图
- 费用结构饼图（佣金/FBA费/广告费/仓储费占比）
- 订单类型分布（正常/测评/Vine/退款/取消）
- 本月数据质量状态（告警汇总）

---

## 10. 已知数据质量问题

| # | 问题描述 | 影响范围 | 处理方案 |
|---|---------|---------|---------|
| DQ-01 | `NMN-ManEnergy-90ct` 与 `NMN-Man E&I-90ct` 共用同一中文产品名（均为男性活力营养素，均有效）| 测评台账SKU映射、成本表映射、广告202510映射 | 优先通过order_id关联结算数据获取精确SKU；无法确认时保留两个SKU中第一个并生成黄色告警 |
| DQ-02 | 成本表部分行为Excel公式字符串（如`=(-9.39...-0.2)*-1`），`data_only=True`读取依赖Excel缓存值 | dim_sku_cost计算 | 要求每次修改成本表后在Excel中手动Save，确保缓存更新；系统检测到字符串时报红色告警 |
| DQ-03 | 成本表中存在合并产品行（如`NAD+女性营养素`，为多SKU合计行）| dim_sku_cost | 系统自动识别（产品名不在SKU主表）并跳过，记录告警 |
| DQ-04 | 广告报告202510为关键词级别数据（27列），无`Advertised SKU`列 | fact_advertising | 通过[0]产品名列映射SKU，SKU维度汇总后精度与其他月份一致 |
| DQ-05 | `FBA Inventory Fee - Reversal`无SKU和描述（202601两笔：$110.50和$352.15）| 应收款计算 | 归入Layer 2，全额计入应收款冲减项，与手工录入供应链金额比对 |
| DQ-06 | 测评台账早期行的订单日期为Excel序列号（如45925）而非日期格式 | fact_review_orders.order_date | 系统自动识别数字类型并转换（Excel序列号起始1900-01-01）|

---

## 11. 系统扩展预留

### 11.1 多平台扩展

所有核心表均预留以下字段（当前版本默认值）：
```sql
platform    TEXT DEFAULT 'Amazon',
marketplace TEXT DEFAULT 'amazon.com'
```

新增平台时：创建独立ETL模块，共用分析层和报告层。

### 11.2 多币种扩展

所有金额字段均为原始货币（当前为USD）。跨平台损益汇总时通过 `dim_exchange_rate` 表统一换算。

### 11.3 多账号/多站点扩展

预留 `seller_account` 字段，支持同一平台不同账号的数据隔离与合并分析。

---

*文档结束。本文档为系统编码的唯一依据，任何实现细节如与本文档不符，以本文档为准。*
