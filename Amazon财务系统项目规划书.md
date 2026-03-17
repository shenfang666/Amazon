# Amazon 平台财务系统项目规划书

**版本**: v2.0（已纳入确认事项）
**编制日期**: 2026-03-13
**工作目录**: `E:/输出/5. 财务系统搭建/1. Amazon平台/`

---

## 一、项目目标

| # | 目标 | 交付物 |
|---|------|--------|
| 1 | 最细粒度数据沉淀（订单行级别） | 数据库 + ETL管道 |
| 2 | 订单-结算状态双向追踪 | 订单结算比对报告 |
| 3 | 月度多维度损益分析（SKU × 月份 × 订单类型） | 月度损益明细表 |
| 4 | 应收平台金额追踪 | 应收平台款项报告 |
| 5 | 指定期间利润表 | 利润表 |
| 6 | 可按维度展示的报告/看板，支持导出Excel | Web应用（Streamlit） |

---

## 二、数据源结构（已核实）

### 2.1 数据源清单

| 文件 | 格式 | 核心字段 | 更新方式 |
|------|------|----------|----------|
| `1 Amazon 订单报告.txt` | TSV | order-id, sku, purchase-date, order-status, quantity, item-price, promotion-ids | 月度导出 |
| `2 Amazon 结算明细表.csv` | CSV（跳过前9行） | settlement-id, type, order-id, sku, product-sales, selling-fees, fba-fees, promotional-rebates, total, Transaction Status | 月度导出 |
| `3 广告报告.xlsx` | XLSX | SKU, 广告活动, 开始/结束日期, 花费(CPC), 展示量, 点击量, ACOS, ROAS, 7天销售额 | 月度导出 |
| `4 Amazon 仓储报告_YYYYMM.csv` | CSV | asin, fnsku, month-of-charge, estimated-monthly-storage-fee | 月度导出 |
| `5 Amazon 移除报告_YYYYMMDD.csv` | CSV | order-id, sku, shipped-quantity, removal-fee | 月度导出 |
| `6 Amazon 退货报告_YYYYMMDD.csv` | CSV | return-date, order-id, sku, quantity, reason, detailed-disposition | 月度导出 |
| `7 Amazon 赔偿报告_YYYYMMDD.csv` | CSV | reimbursement-id, amazon-order-id, sku, reason, amount-total | 月度导出 |
| `8 亚马逊测评订单台账.xlsx` | XLSX | **含订单号**，用于识别测评/Vine之外的站外测评订单 | 手动维护 |
| `98 SKU成本表.xlsx` | XLSX | 月份, SKU, 采购成本/件, 头程费/件 | 手动维护 |
| `99 SKU.xls` | XLS | SKU, ASIN, 产品名称 | 手动维护 |

### 2.2 广告费双源架构（重要）

```
广告费来源A：结算明细表（Service Fee → Cost of Advertising）
├── 用途：核算应收平台款（Amazon收了多少广告费）
├── 特点：按结算周期汇总，不区分SKU，金额约 $500/条
└── 字段：type='Service Fee', description='Cost of Advertising', total

广告费来源B：广告报告.xlsx（SKU × 广告活动 × 日期）
├── 用途：计算当月实际利润（实际消耗的广告费）
├── 特点：按SKU细分，反映实际花费时间
└── 字段：SKU, 花费(spend), 开始日期, 结束日期

时间差处理原则：
  利润表 → 使用广告报告B（当月实际消耗）
  应收平台报告 → 使用结算明细A（Amazon实际结算扣款）
```

---

## 三、订单类型分类逻辑

### 3.1 七种订单类型

| order_type | 含义 | 判断条件 |
|------------|------|----------|
| `cancelled` | 取消订单 | 订单报告中 order-status = 'Cancelled' |
| `vine_sale` | Vine销售 | 结算type='Order' **且** product_sales > 0 **且** \|product_sales + promotional_rebates\| < $0.01 |
| `vine_refund` | Vine退款 | 结算type='Refund' **且** 对应原始订单为vine_sale |
| `review_sale` | 测评销售 | 订单号存在于测评台账 **且** 结算type='Order' |
| `review_refund` | 测评退款 | 订单号存在于测评台账 **且** 结算type='Refund' |
| `normal_refund` | 正常退款 | 结算type='Refund'（非测评） |
| `normal_sale` | 正常销售 | 以上均不满足的 type='Order' |

### 3.2 Vine识别逻辑验证

从现有结算数据核实：
- Vine订单特征：`product_sales + promotional_rebates ≈ 0`（消费者0元获取，平台折扣抵消售价）
- FBA费用和佣金仍正常扣除
- 当前数据中符合条件的记录：66条（需进一步过滤掉无订单号行）

### 3.3 判断优先级（代码执行顺序）

```
1. 取消订单  ← 来自订单报告
2. Vine订单  ← 来自结算数据的金额特征（最高优先级覆盖）
3. 测评订单  ← 来自测评台账订单号比对
4. 正常退款/销售 ← 其余
```

---

## 四、数据模型

### 4.1 维度表

#### `dim_sku` — SKU主数据
```sql
CREATE TABLE dim_sku (
    sku             TEXT PRIMARY KEY,
    asin            TEXT,
    product_name_cn TEXT,    -- 中文名
    product_name_en TEXT,    -- 英文名
    product_category TEXT
);
```

#### `dim_sku_cost` — SKU月度成本
```sql
CREATE TABLE dim_sku_cost (
    sku              TEXT,
    cost_month       TEXT,   -- 格式 YYYY-MM
    unit_cost        REAL,   -- 采购成本/件（美元）
    inbound_fee      REAL,   -- 头程费/件
    total_unit_cost  REAL,   -- = unit_cost + inbound_fee
    PRIMARY KEY (sku, cost_month)
);
```

### 4.2 核心事实表

#### `fact_settlement` — 结算流水（最细粒度）
```sql
CREATE TABLE fact_settlement (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    settlement_id         TEXT,     -- 结算批次号
    transaction_datetime  TEXT,     -- 交易时间
    transaction_month     TEXT,     -- YYYY-MM（交易发生月）
    settlement_month      TEXT,     -- YYYY-MM（结算所属月，按settlement_id归属）
    transaction_type      TEXT,     -- Order/Refund/Adjustment/Service Fee/FBA Inventory Fee/Transfer
    transaction_subtype   TEXT,     -- description字段（Cost of Advertising / FBA Inventory Reimbursement等）
    order_id              TEXT,     -- Amazon订单号
    sku                   TEXT,
    quantity              INTEGER,
    product_sales         REAL,
    shipping_credits      REAL,
    promotional_rebates   REAL,     -- 促销折扣（负数）
    selling_fees          REAL,     -- 平台佣金（负数）
    fba_fees              REAL,     -- FBA配送费（负数）
    other_fees            REAL,
    total                 REAL,     -- 本行净额
    transaction_status    TEXT,     -- Released / 空
    order_type            TEXT,     -- 七种分类标签（见3.1节）
    UNIQUE(settlement_id, transaction_datetime, order_id, sku, transaction_type)
);
```

#### `fact_orders` — 订单明细
```sql
CREATE TABLE fact_orders (
    amazon_order_id   TEXT PRIMARY KEY,
    purchase_date     TEXT,
    order_month       TEXT,         -- YYYY-MM
    order_status      TEXT,         -- Shipped/Cancelled/Pending
    sku               TEXT,
    asin              TEXT,
    quantity          INTEGER,
    item_price        REAL,
    promotion_ids     TEXT,
    is_settled        INTEGER,      -- 0/1
    settlement_id     TEXT,
    settled_amount    REAL,
    settlement_status TEXT          -- Released/Pending/未结算
);
```

#### `fact_advertising` — 广告实际消耗（来自广告报告）
```sql
CREATE TABLE fact_advertising (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_month        TEXT,           -- YYYY-MM
    campaign_name   TEXT,
    sku             TEXT,
    asin            TEXT,
    impressions     INTEGER,
    clicks          INTEGER,
    ctr             REAL,
    cpc             REAL,
    spend           REAL,           -- 广告花费（利润计算使用）
    sales_7d        REAL,
    acos            REAL,
    roas            REAL
);
```

#### `fact_storage_fees` — 月度仓储费
```sql
CREATE TABLE fact_storage_fees (
    id                           INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_month                 TEXT,   -- YYYY-MM
    asin                         TEXT,
    fnsku                        TEXT,
    sku                          TEXT,   -- 关联dim_sku（通过fnsku匹配）
    avg_qty_on_hand              REAL,
    estimated_monthly_storage_fee REAL,
    total_incentive_fee          REAL    -- 库龄折扣
);
```

#### `fact_removals` — 移除订单
```sql
CREATE TABLE fact_removals (
    order_id          TEXT PRIMARY KEY,
    request_date      TEXT,
    removal_month     TEXT,
    sku               TEXT,
    shipped_quantity  INTEGER,
    removal_fee       REAL
);
```

#### `fact_returns` — 退货记录
```sql
CREATE TABLE fact_returns (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    return_date         TEXT,
    return_month        TEXT,
    order_id            TEXT,
    sku                 TEXT,
    quantity            INTEGER,
    reason              TEXT,
    detailed_disposition TEXT
);
```

#### `fact_compensations` — 赔偿/报销
```sql
CREATE TABLE fact_compensations (
    reimbursement_id   TEXT PRIMARY KEY,
    approval_date      TEXT,
    comp_month         TEXT,
    amazon_order_id    TEXT,
    sku                TEXT,
    reason             TEXT,
    amount_total       REAL,
    qty_cash           INTEGER,
    qty_inventory      INTEGER
);
```

#### `fact_review_orders` — 测评台账
```sql
CREATE TABLE fact_review_orders (
    amazon_order_id  TEXT PRIMARY KEY,
    order_date       TEXT,
    sku              TEXT,
    platform         TEXT,   -- 测评平台名称
    order_type       TEXT,   -- 'review'（站外测评）
    sale_amount      REAL,
    review_cost      REAL    -- 测评费用（现金成本）
);
```
> **注**：Vine订单不在此表中维护，通过结算数据金额特征自动识别。

---

## 五、分析报告设计

### 5.1 报告A：订单结算比对表

**用途**：追踪每笔订单是否已完成结算，识别异常

| 字段 | 说明 |
|------|------|
| amazon_order_id | 订单号 |
| order_month | 下单月份 |
| sku / 产品名 | 商品 |
| order_status | 订单状态（Shipped/Cancelled/Pending）|
| item_price | 下单金额 |
| is_settled | 是否已结算 |
| settlement_id | 结算批次 |
| settled_net | 结算净额（扣除佣金、FBA费等） |
| settlement_status | Released / Pending / 未结算 |
| gap | item_price - settled_gross（金额差异） |
| order_type | 订单类型 |

**筛选维度**：月份、SKU、订单状态、是否已结算

---

### 5.2 报告B：月度SKU损益明细（核心）

**用途**：最细粒度分析，如"2026年2月镁产品各类订单的费用"

| 字段 | 说明 | 数据来源 |
|------|------|----------|
| period_month | 月份 | 结算月份 |
| sku / 产品名 | SKU | dim_sku |
| order_type | 订单类型（7种） | 逻辑判断 |
| qty_sold | 销售数量 | fact_settlement |
| gmv | 毛销售额 | product_sales |
| promotional_rebates | 促销折扣 | fact_settlement |
| net_sales | 净销售额 = gmv + rebates | 计算 |
| selling_fees | 平台佣金 | fact_settlement |
| fba_fees | FBA配送费 | fact_settlement |
| storage_fees | 月度仓储费 | fact_storage_fees |
| removal_fees | 移除费 | fact_removals |
| ad_spend | 广告花费 | fact_advertising |
| compensation | 赔偿收入 | fact_compensations |
| unit_cost | 单位成本 | dim_sku_cost |
| cogs | 销售成本 = unit_cost × qty | 计算 |
| review_cost | 测评费用 | fact_review_orders |
| **gross_profit** | **毛利润** | **计算** |
| **gross_margin%** | **毛利率** | **计算** |

**筛选维度**：月份（范围）、SKU、订单类型

---

### 5.3 报告C：应收平台款项报告

**用途**：追踪Amazon平台欠卖家多少钱

```
应收平台款 = 结算中Released的净收入 - 已收到的Transfer金额

每个结算批次（settlement_id）：
  + 销售净额（Order类 released）
  - 平台佣金
  - FBA费用
  - 广告费（Cost of Advertising）
  - 仓储费（FBA Inventory Fee）
  - 其他扣款
  = 本批次应收净额
  - Transfer（已到账）
  = 本批次未到账余额
```

| 字段 | 说明 |
|------|------|
| settlement_id | 结算批次 |
| settlement_period | 结算周期（起止日期） |
| gross_revenue | 销售总额 |
| total_deductions | 总扣款（佣金+FBA+广告+仓储） |
| net_receivable | 应收净额 |
| transferred_amount | 已到账（Transfer记录） |
| outstanding | 未到账余额 |
| status | 已结清 / 未结清 |

---

### 5.4 报告D：指定期间利润表（P&L）

**用途**：管理层月度/季度/年度经营报告

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
         利润表（YYYY-MM 至 YYYY-MM）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

一、营业收入
  1. 正常销售净额              xxx
  2. 测评订单净额              xxx
  3. Vine订单净额（$0）          0
  4. 退款（负数）              (xxx)
  ────────────────────────────
     合计营业收入              xxx

二、销售成本（COGS）
  5. 产品采购成本              (xxx)
  6. 头程运费分摊              (xxx)
  ────────────────────────────
     毛利润（一 - 二）          xxx   毛利率 xx%

三、平台费用
  7.  平台佣金（Selling Fees）   (xxx)
  8.  FBA配送费                (xxx)
  9.  月度仓储费               (xxx)
  10. 移除费                   (xxx)
  11. Vine计划注册费            (xxx)
  12. Amazon月租费（$39.99）    (xxx)
  13. FBA入库分仓费             (xxx)
  ────────────────────────────
     平台费用合计              (xxx)

四、广告费用（实际消耗）
  14. 广告花费（来自广告报告）   (xxx)

五、测评费用
  15. 站外测评费用              (xxx)

六、其他收入
  16. 赔偿收入                  xxx

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
     经营利润（二至六合计）      xxx   利润率 xx%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

**筛选维度**：起止月份、SKU（全部/指定）、订单类型

---

## 六、系统技术方案

### 6.1 技术选型

| 层次 | 技术 | 说明 |
|------|------|------|
| 数据库 | **SQLite**（本地）或 **DuckDB**（分析加速） | 无需服务器，文件即数据库 |
| ETL处理 | **Python + pandas** | 已确认可用（Python 3.14.2） |
| Excel读写 | **openpyxl** | 已安装 |
| Web应用/看板 | **Streamlit** | Python原生，可做筛选器、图表、表格，支持导出Excel |
| 部署方式 | 本地运行（`streamlit run app.py`） | 访问 http://localhost:8501 |

### 6.2 Streamlit应用功能规划

```
侧边栏：
├── 数据导入
│   └── 上传本月文件 → 一键ETL → 入库
│
└── 报告选择
    ├── 📋 订单结算比对
    ├── 📊 月度SKU损益明细
    ├── 💰 应收平台款项
    └── 📈 利润表

报告页面（每个报告均有）：
├── 筛选器（月份、SKU、订单类型等）
├── 汇总指标卡（KPI tiles）
├── 数据明细表
├── 趋势图（可选）
└── [下载 Excel] 按钮
```

### 6.3 项目目录结构

```
E:/输出/5. 财务系统搭建/1. Amazon平台/
│
├── 原始数据/                        ← 每月原始报告存放
│   ├── 202601/
│   │   ├── 1 Amazon 订单报告.txt
│   │   └── ...
│   └── 202602/
│
├── amazon_finance.db                ← SQLite数据库（自动生成）
│
├── etl/                             ← 数据处理脚本
│   ├── 00_init_db.py                ── 初始化表结构
│   ├── 01_load_sku_master.py        ── 加载SKU主数据
│   ├── 02_load_orders.py            ── 加载订单报告
│   ├── 03_load_settlement.py        ── 加载结算明细（含订单类型打标）
│   ├── 04_load_ads.py               ── 加载广告报告
│   ├── 05_load_storage.py           ── 加载仓储报告
│   ├── 06_load_removals.py          ── 加载移除报告
│   ├── 07_load_returns.py           ── 加载退货报告
│   ├── 08_load_compensations.py     ── 加载赔偿报告
│   ├── 09_load_review_orders.py     ── 加载测评台账
│   ├── 10_classify_orders.py        ── 订单类型分类（跑完所有load后执行）
│   └── 99_run_monthly.py            ── 每月一键导入（传入月份参数）
│
├── app.py                           ← Streamlit主应用入口
│
├── pages/                           ← Streamlit多页面
│   ├── 1_订单结算比对.py
│   ├── 2_月度SKU损益.py
│   ├── 3_应收平台款项.py
│   └── 4_利润表.py
│
└── output/                          ← 导出的Excel报告
```

---

## 七、开发阶段计划

### Phase 1：数据基础（本周）
- [ ] P1-1 `00_init_db.py`：建库建表
- [ ] P1-2 `01_load_sku_master.py`：SKU主数据 + 成本表入库
- [ ] P1-3 `03_load_settlement.py`：结算明细入库（核心）
- [ ] P1-4 `02_load_orders.py`：订单报告入库
- [ ] P1-5 `10_classify_orders.py`：实现7种订单类型分类（含Vine识别、测评台账关联）

**里程碑**：能查询到"2026年2月 NMN-magnesium-90ct 的正常销售订单，数量X、净销售额$Y、佣金$Z、FBA费$W"

### Phase 2：其他数据源
- [ ] P2-1 加载广告报告（SKU维度）
- [ ] P2-2 加载仓储报告
- [ ] P2-3 加载移除、退货、赔偿报告
- [ ] P2-4 加载测评台账

**里程碑**：所有数据源均可查询，毛利润字段可计算

### Phase 3：报告生成
- [ ] P3-1 订单结算比对报告（含Excel导出）
- [ ] P3-2 月度SKU损益明细（含Excel导出）
- [ ] P3-3 应收平台款项报告
- [ ] P3-4 利润表（指定期间）

### Phase 4：Web应用（Streamlit）
- [ ] P4-1 安装Streamlit，搭建基础框架
- [ ] P4-2 实现各报告页面的筛选器和表格展示
- [ ] P4-3 Excel下载功能
- [ ] P4-4 趋势图（月度走势）

### Phase 5：自动化
- [ ] P5-1 每月一键导入脚本（`99_run_monthly.py`）
- [ ] P5-2 数据去重保护（同一文件不重复导入）

---

## 八、待确认事项

### ❓ Q1（新增）：软件部署方式

方案A（推荐）：**Streamlit本地应用**
- 在你的电脑上运行，浏览器访问 `localhost:8501`
- Python一行命令启动

方案B：**纯Excel报告**
- 每月运行脚本，输出格式化Excel
- 无需启动服务

**→ 你倾向于哪种方式？或者你是否已有指定的BI软件/工具？**

### ❓ Q2：测评台账字段确认

建议测评台账包含以下字段（**请确认是否与你现有格式一致**）：

| 字段 | 必须 | 说明 |
|------|------|------|
| amazon_order_id | ✅ | 用于关联结算数据 |
| order_date | ✅ | 订单日期 |
| sku | ✅ | 产品SKU |
| platform | ✅ | 测评平台（AKK等） |
| sale_amount | ✅ | 销售金额（用于结算比对） |
| review_cost | ✅ | 测评费用（成本） |
| order_type | 建议 | review（统一，Vine单独识别） |

### ❓ Q3：仓储费分摊粒度

仓储报告提供的是 **FNSKU级月度总仓储费**。
- 方案A：SKU月度汇总（直接用，简单）
- 方案B：按当月销售数量分摊到每笔订单（更精准）

**→ 你倾向于哪种方式？**（建议方案A，足够精准且易维护）

---

## 九、数据关联关系图

```
dim_sku ─────────────────────────────────────────────────┐
    │                                                      │
dim_sku_cost ──────────────────────────────────────────── │
    │                                                      │
fact_orders ──── (order_id) ──── fact_settlement ─────────┤
                                      │                   │
fact_review_orders ── (order_id) ─────┘                   │
                                                          │
fact_advertising ─── (sku + month) ───────────────────────┤
fact_storage_fees ── (sku + month) ───────────────────────┤
fact_removals ─────── (sku + month) ──────────────────────┤
fact_compensations ── (sku + month) ──────────────────────┤
                                                          ▼
                                            v_monthly_pnl_sku
                                            v_order_settlement_match
                                            v_pnl_statement
                                            v_receivables
```

---

*规划书将随开发进展持续更新。Phase 1 可立即开始，待Q1-Q3确认后并行推进Phase 4。*
