# 计算型期权分析数据

本文说明 Autotrade v0.9.0 的计算型期权 Greeks 和 IVX 数据资源。

v0.9.0 起，Forward、Greeks 和 IVX 的算法实现直接复用从 cfutures
复制的模块，目标是与历史 `row_data/opt_panel` 和 `ivx_data` 逐值一致。

## 1. 设计目标

计算型期权分析数据使用与 RiceQuant 数据模块一致的调用方式：

```python
service.get(mode=FetchMode.DB_ONLY, ...)
service.get(mode=FetchMode.SOURCE_ONLY, ...)
service.get(mode=FetchMode.DB_THEN_SOURCE, ...)
```

但它与 RiceQuant 官方 `options.get_greeks` 数据严格分开：

- `OptionGreeksService`：RiceQuant 官方 Greeks；
- `CalculatedOptionGreeksService`：Autotrade 使用 Black97 现场计算的
  IV、Greeks 和高阶 Greeks。

纯计算逻辑位于 `autotrade.analytics.options`，不访问数据库、不调用
RiceQuant，也不处理策略字段映射。`autotrade.data.ricequant` 负责：

- 获取计算所需的完整期权截面；
- 构造 Forward；
- 调用纯计算引擎；
- 将完整计算截面写入 ClickHouse；
- 根据调用者的查询条件裁剪返回结果。

## 2. 模块位置

```text
autotrade/
├── analytics/options/
│   ├── opt_forward_curve.py
│   ├── cal_opt_greek.py
│   └── cal_ivx.py
└── data/ricequant/
    ├── datasource/calculated_options.py
    ├── repository/calculated_options.py
    ├── service/calculated_options.py
    └── spec/calculated_options.py
```

对应的 ClickHouse 表为：

```text
rq_option_data.calculated_option_greeks_1d
```

当前版本只支持日频 `frequency="1d"` 和收盘价
`price_type="close"`。

## 3. Black97 输入

直接使用复制自 cfutures 的函数：

```python
from autotrade.analytics.options import calculate_option_greeks_for_dates
```

输入 DataFrame 必须包含：

```text
order_book_id
date
close
forward_price
strike_price
T_days
r
option_type
```

其中：

- `close`：期权市场价格；
- `forward_price`：期权对应的 Forward；
- `strike_price`：行权价；
- `T_days`：剩余日历天数；
- `r`：小数形式的年化无风险利率；
- `option_type`：`C` 或 `P`。

输出在原始输入后增加：

```text
iv
delta
gamma
vega
theta
rho
vanna
vomma
charm
```

计算口径：

- Vega：对小数波动率的导数；
- Theta：对年化时间的导数；
- Rho：对小数利率的导数；
- Vanna：`d(delta) / d(iv_decimal)`；
- Vomma：`d(vega_decimal) / d(iv_decimal)`；
- Charm：一年化时间流逝引起的 Delta 变化。

## 4. Forward 构造

### 4.1 期货期权

期货期权使用每张期权实际对应的期货合约收盘价：

```text
forward_price = underlying future close
forward_method = "future_close"
```

例如 AU 期权对应 AU 月份期货合约，而不是使用 `AU888` 计算 Greeks。

### 4.2 ETF 和指数期权

ETF、指数期权按交易日、剩余期限和行权价配对 Call/Put：

```text
F = K + exp(rT) × (CallPrice - PutPrice)
```

同一交易日、同一剩余期限存在多组配对时，与 cfutures 一致：

```text
pair_weight = (call_volume + put_volume) / 2
forward_price = weighted_mean(forward_candidate, pair_weight)
forward_method = "cfutures_implied_weighted_mean"
```

缺少有效平价组合时使用标的现货 Carry 兜底；缺失期限使用 log-linear
插值，曲线边缘使用平端外推。

## 5. Service 使用方法

导入：

```python
from autotrade.data.ricequant.base import FetchMode
from autotrade.data.ricequant.service.calculated_options import (
    CalculatedOptionGreeksService,
)

service = CalculatedOptionGreeksService()
```

### 5.1 DB_ONLY

已经落库的数据可以直接按合约查询：

```python
result = service.get(
    mode=FetchMode.DB_ONLY,
    order_book_ids=["AU2608C1000"],
    start_date="2026-07-10",
    end_date="2026-07-10",
    frequency="1d",
)
```

也可以按品种查询：

```python
result = service.get(
    mode=FetchMode.DB_ONLY,
    opt_symbol="AU",
    start_date="2026-07-10",
    end_date="2026-07-10",
)
```

`DB_ONLY` 不进行现场计算，也不会调用 RiceQuant。

### 5.2 SOURCE_ONLY 按品种计算

```python
result = service.get(
    mode=FetchMode.SOURCE_ONLY,
    opt_symbol="AU",
    start_date="2026-07-10",
    end_date="2026-07-10",
    persist=True,
)
```

它会计算并返回完整 AU 期权截面。

### 5.3 SOURCE_ONLY 按合约请求

```python
result = service.get(
    mode=FetchMode.SOURCE_ONLY,
    order_book_ids=["AU2608C1000"],
    start_date="2026-07-10",
    end_date="2026-07-10",
    persist=True,
)
```

内部流程：

```text
请求 order_book_ids
    → SOURCE_ONLY 获取期权合约信息
    → 解析所属 opt_symbol
    → SOURCE_ONLY 获取该品种完整期权截面
    → SOURCE_ONLY 获取所需期货价格
    → 计算完整截面
    → 完整截面写入 ClickHouse
    → 最后按请求 order_book_ids 裁剪返回
```

即使调用者只请求一张期权，持久化的仍然是计算所需的完整品种截面。

### 5.4 SOURCE_ONLY 模式传播

外层使用 `SOURCE_ONLY` 时，内部服务也必须使用 `SOURCE_ONLY`：

```text
CalculatedOptionGreeksService SOURCE_ONLY
├── OptionInstrumentService SOURCE_ONLY
├── OptionPriceService SOURCE_ONLY
└── FuturePriceService SOURCE_ONLY（期货期权需要时）
```

内部输入查询统一使用 `persist=False`，避免现场计算过程中顺带修改基础行情
和基础信息表。计算结果是否写入由外层 `persist` 控制。

## 6. ClickHouse 字段说明

`calculated_option_greeks_1d` 字段：

| 字段 | 含义 |
|---|---|
| `order_book_id` | 期权合约代码 |
| `date` | 交易日 |
| `opt_symbol` | 期权品种或标的代码 |
| `underlying_order_book_id` | 实际标的/期货合约代码 |
| `maturity_date` | 到期日 |
| `strike_price` | 行权价 |
| `option_type` | `C` 或 `P` |
| `option_price` | 用于反解 IV 的期权价格 |
| `forward_price` | Black97 Forward 输入 |
| `risk_free_rate` | 年化无风险利率，小数形式 |
| `t_days` | 剩余日历天数 |
| `iv` | 隐含波动率 |
| `delta` | Delta |
| `gamma` | Gamma |
| `vega` | Vega |
| `theta` | Theta |
| `rho` | Rho |
| `vanna` | Vanna |
| `vomma` | Vomma |
| `charm` | Charm |
| `forward_method` | `future_close` 或 `cfutures_implied_weighted_mean` |
| `price_type` | 当前为 `close` |
| `frequency` | 当前为 `1d` |
| `market` | 当前为 `cn` |
| `model_id` | 当前默认 `black97` |
| `model_version` | 当前默认 `cfutures_v1` |
| `ingest_time` | ClickHouse 写入版本时间 |

表使用：

```text
ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(date)
ORDER BY (date, order_book_id, model_id, model_version)
```

Repository 查询统一使用 `FINAL`。

## 7. 初始化

创建计算型 Greeks 表：

```python
from autotrade.coreutils.config import load_env
from autotrade.data.ricequant.init_rq_data import (
    create_calculated_option_greeks_tables,
)

load_env()
create_calculated_option_greeks_tables()
```

完整的 RiceQuant option 初始化入口也会创建该表。

## 8. 当前验证

已完成真实端到端验证：

- AU `2026-07-10`：
  - `SOURCE_ONLY` 计算 732 行；
  - 使用 `future_close` Forward；
  - 请求单合约时完整 732 行先落库；
  - `DB_ONLY` 成功返回请求合约。
- 510050 `2026-07-10`：
  - `SOURCE_ONLY` 计算 96 行；
  - v0.9.0 起使用 cfutures volume-weighted Forward。

示例 AU 合约：

```text
order_book_id = AU2608C1000
forward_price = 897.94
iv = 0.272818
delta = 0.035962
model_version = cfutures_v1
```

## 9. IVX 接口

IVX 已作为独立日度资源实现：

```text
CalculatedOptionIVXService
rq_option_data.calculated_option_ivx_1d
```

IVX 是品种级指标，不能使用单个 `order_book_id` 现场计算。调用时必须传入
`opt_symbol`，内部会获取该品种完整当日期权截面。

### 9.1 使用方法

```python
from autotrade.data.ricequant.base import FetchMode
from autotrade.data.ricequant.service.calculated_options import (
    CalculatedOptionIVXService,
)

service = CalculatedOptionIVXService()

source_result = service.get(
    mode=FetchMode.SOURCE_ONLY,
    opt_symbol="AU",
    start_date="2026-07-10",
    end_date="2026-07-10",
    persist=True,
)

db_result = service.get(
    mode=FetchMode.DB_ONLY,
    opt_symbol="AU",
    start_date="2026-07-10",
    end_date="2026-07-10",
)
```

`SOURCE_ONLY` 内部的 `OptionInstrumentService` 和 `OptionPriceService`
同样强制使用 `SOURCE_ONLY, persist=False`。

### 9.2 `opt_symbol` 如何解析期权集合

当前实现先通过 RiceQuant 获取指定市场的全部 Option 合约信息，再使用
`underlying_symbol` 匹配 `opt_symbol`：

```text
AU     -> underlying_symbol == "AU"
510050 -> underlying_symbol == "510050" 或 "510050.XSHG"
```

随后保留与请求日期区间存在交集的合约：

```text
listed_date <= end_date
maturity_date >= start_date
```

筛选得到完整的 `order_book_id` 集合后，再使用 `OptionPriceService` 一次性
查询这些合约在日期区间内的收盘价。因此 IVX 的计算输入始终是对应品种的
完整有效期权截面，而不是调用者指定的单张合约。

### 9.3 计算口径

- Call/Put 平价按到期日估计 Forward；
- 排除剩余期限不超过 `min_days=7` 的月份；
- 使用 OTM 期权价格积分计算每个到期月份的年化方差；
- 将近月和次近月方差插值到固定的 `target_days=30`；
- IVX 使用波动率点表示，例如 `27.2` 表示 `27.2%`。

为与 cfutures 完全一致，`target_days` 固定为 30，`min_days` 固定为 7。
如果最短有效到期月份已经不短于目标期限，则直接使用该到期月份的波动率，
与原 `cfutures/opt_tools/cal_ivx.py` 口径保持一致。

### 9.4 ClickHouse 字段

| 字段 | 含义 |
|---|---|
| `date` | 交易日 |
| `opt_symbol` | 期权品种 |
| `ivx` | IVX，单位为波动率点 |
| `target_days` | 目标期限，默认 30 天 |
| `min_days` | 最短有效期限阈值，默认 7 天 |
| `near_t_days` | cfutures 原函数不返回，当前为 NULL |
| `next_t_days` | cfutures 原函数不返回，当前为 NULL |
| `near_variance` | cfutures 原函数不返回，当前为 NULL |
| `next_variance` | cfutures 原函数不返回，当前为 NULL |
| `option_count` | 当日输入期权行数 |
| `risk_free_rate` | 年化无风险利率 |
| `method` | 默认 `model_free_variance` |
| `price_type` | 当前为 `close` |
| `frequency` | 当前为 `1d` |
| `market` | 当前为 `cn` |
| `model_version` | 算法版本 |
| `ingest_time` | ClickHouse 写入版本时间 |

纯计算入口：

```python
from autotrade.analytics.options import cal_ivx
```

输入字段：

```text
date, price, T_days, K, flag, r
```

### 9.5 验证结果

AU `2026-07-10` 使用 732 行完整期权截面完成
`SOURCE_ONLY -> ClickHouse -> DB_ONLY`：

```text
ivx = 27.239487230465492
```

该结果与历史 `row_data/ivx_data/AU.pkl` 完全一致。

## 10. 已知限制

- 当前只支持日频和收盘价；
- `DB_THEN_SOURCE` 仍使用基础框架的“数据库非空即返回”语义；
- 尚未增加完整截面 coverage 表；
- 计算失败的 IV/Greeks 使用 `NULL`，不会丢弃原始合约行；
- `underlying_price` 尚未存入计算表；
- `time_to_expiry` 可由 `t_days / 365` 推导；
- 若修改 Forward、利率或 Greeks 口径，应升级 `model_version`。
