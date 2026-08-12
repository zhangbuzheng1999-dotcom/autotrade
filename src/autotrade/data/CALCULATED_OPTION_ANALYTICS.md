# 计算型期权分析数据

本文说明 Autotrade v0.10.0 的计算型期权 Greeks 和 IVX 数据资源。

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

纯计算逻辑位于 `autotrade.option.analytics`，不访问数据库、不调用
RiceQuant，也不处理策略字段映射。`autotrade.data.ricequant` 负责：

- 获取计算所需的完整期权截面；
- 构造 Forward；
- 调用纯计算引擎；
- 将完整计算截面写入 ClickHouse；
- 根据调用者的查询条件裁剪返回结果。

## 2. 模块位置

```text
autotrade/
├── option/analytics/
│   ├── forward_curve.py
│   ├── greeks.py
│   └── ivx.py
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
from autotrade.option.analytics import calculate_option_greeks_for_dates
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

v0.10.0 起不再区分期货、ETF或指数期权。所有期权都按交易日、剩余期限和
行权价配对 Call/Put：

```text
F = K + exp(rT) × (CallPrice - PutPrice)
```

同一交易日、同一剩余期限存在多组配对时：

```text
pair_weight = (call_volume + put_volume) / 2
forward_price = weighted_mean(forward_candidate, pair_weight)
forward_method = "put_call_parity_weighted_mean"
```

缺失期限可以使用同一交易日其他有效平价期限进行 log-linear 插值，曲线
边缘使用平端外推。禁止使用期货收盘价或 Spot Carry 兜底；如果当日没有
任何有效 Call/Put 平价锚点，Forward 和对应 Greeks 保持 NULL。

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

`mode` 控制 Greeks 本身的获取方式。现场计算时，可用 `input_mode`
单独控制计算所需的期权合约信息和行情数据来源：

```python
# 读取 ClickHouse/MySQL 中已有的 instruments 和 price，在本地计算 Greeks
result = service.get(
    mode=FetchMode.SOURCE_ONLY,
    input_mode=FetchMode.DB_ONLY,
    opt_symbol="AU",
    start_date="2026-07-10",
    end_date="2026-07-10",
    persist=False,
)
```

`input_mode` 只接受 `FetchMode.DB_ONLY` 和 `FetchMode.SOURCE_ONLY`：

- 不传时默认为 `FetchMode.SOURCE_ONLY`，保持原有行为；
- `DB_ONLY`：合约信息和期权行情均从本地数据库读取，再现场计算；
- `SOURCE_ONLY`：合约信息和期权行情均从 RiceQuant 源读取，再现场计算。

两个底层服务始终使用同一个 `input_mode`，不会混用数据库合约信息和源行情。
`strike_price` 优先采用行情数据中的值；行情源未提供该字段时，才回退到合约信息。

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
    → Call/Put 利率平价构造 Forward
    → 计算完整 Greeks 截面
    → 完整截面写入 ClickHouse
    → 最后按请求 order_book_ids 裁剪返回
```

即使调用者只请求一张期权，持久化的仍然是计算所需的完整品种截面。

### 5.4 现场计算与输入数据源分层

外层 `mode` 与底层 `input_mode` 是两个独立层次：

```text
mode
├── DB_ONLY：直接读取 calculated_option_greeks_1d
└── SOURCE_ONLY：现场计算 Greeks
    └── input_mode
        ├── DB_ONLY：MySQL instruments + ClickHouse option price
        └── SOURCE_ONLY：RiceQuant instruments + RiceQuant option price
```

`input_mode` 不接受 `DB_THEN_SOURCE`，避免一次计算中的底层数据来源随数据库
命中情况变化。合约信息和行情始终使用同一个 `input_mode`。

分钟 Greeks 使用相同的现场计算语义，并把 `frequency` 与闭区间
`time_slice` 原样传递给期权行情服务：

```python
result = service.get(
    mode=FetchMode.SOURCE_ONLY,
    input_mode=FetchMode.SOURCE_ONLY,
    persist=False,
    opt_symbol="AU",
    start_date="2026-07-10",
    end_date="2026-07-10",
    frequency="1m",
    time_slice=("11:00", "11:30"),
)
```

每个 `datetime` 独立构造完整 Call/Put 截面的成交量加权 Forward，并以
`order_book_id + datetime` 为唯一计算键。分钟结果存储在
`rq_option_data.calculated_option_greeks_1m`，同时保留 `trading_date` 处理夜盘。

内部输入查询统一使用 `persist=False`，避免现场计算过程中顺带修改基础行情
和基础信息表。计算结果是否写入由外层 `persist` 控制。

### 5.5 Strike Price 来源

现场计算使用的 `strike_price` 按以下优先级解析：

1. 优先使用 `OptionPriceService` 行情数据中的 `strike_price`；
2. 行情源没有该列或该行为空时，回退到 `OptionInstrumentService` 合约信息；
3. `input_mode=DB_ONLY` 时，正常情况下即使用 ClickHouse 期权行情表的
   `strike_price`。

合并前只保留计算所需的行情列，避免 ClickHouse 完整行中的
`strike_price`、`contract_multiplier` 等字段与合约快照产生 `_x`、`_y`
后缀，保证 DB 和 RiceQuant 两条输入路径形成相同的计算 schema。

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
| `forward_method` | `put_call_parity_weighted_mean` |
| `price_type` | 当前为 `close` |
| `frequency` | 当前为 `1d` |
| `market` | 当前为 `cn` |
| `model_id` | 当前默认 `black97` |
| `model_version` | Greeks 当前默认 `parity_v1` |
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

v0.10.0 已完成真实 SOURCE_ONLY 验证：

- AU `2026-07-10`：
  - `SOURCE_ONLY` 计算 732 行；
  - 732 行 Forward 全部由 Call/Put 平价得到；
  - 680 行成功反解 IV。
- HO `2026-07-10`：
  - `SOURCE_ONLY` 计算 172 行；
  - 172 行 Forward 全部由 Call/Put 平价得到；
  - 162 行成功反解 IV。

同时完成 AU `2026-07-10` 的数据库输入现场计算验证：

```python
service.get(
    mode=FetchMode.SOURCE_ONLY,
    input_mode=FetchMode.DB_ONLY,
    persist=False,
    opt_symbol="AU",
    start_date="2026-07-10",
    end_date="2026-07-10",
)
```

验证结果：

```text
status = success
rows = 732
forward_price non-null = 732
iv non-null = 680
ClickHouse strike_price exact match = 732 / 732
strike_price null mismatch = 0
```

示例 AU 合约：

```text
forward_method = put_call_parity_weighted_mean
model_version = parity_v1
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
from autotrade.option.analytics import cal_ivx
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

## 10. v0.10.0 变更记录

2026-08-02：

- Greeks 接口新增 `input_mode`，不修改公共 `FetchMode`，也不影响其他数据接口；
- `mode=SOURCE_ONLY, input_mode=DB_ONLY` 支持利用本地数据库输入现场计算；
- `mode=SOURCE_ONLY, input_mode=SOURCE_ONLY` 保留原有全 RiceQuant 输入行为；
- `input_mode` 默认值为 `SOURCE_ONLY`，已有调用保持向后兼容；
- 禁止 Greeks 的 `input_mode=DB_THEN_SOURCE`，确保同一次计算输入来源确定；
- DB 输入链路使用宿主机 MySQL 合约信息和 ClickHouse 期权行情；
- `strike_price` 改为行情优先、合约信息兜底，DB 输入时以 ClickHouse 为准；
- 修复 ClickHouse 完整行情字段与 instruments 合并后产生重名后缀的问题；
- 增加 SOURCE_ONLY 默认传播、DB_ONLY 输入传播和 Strike 优先级测试；
- 使用 AU 2026-07-10 的 732 行真实数据完成端到端验证。

## 11. 已知限制

- Greeks 当前支持 `1d` 和 `1m`，价格类型仍只支持收盘价；
- `DB_THEN_SOURCE` 仍使用基础框架的“数据库非空即返回”语义；
- 尚未增加完整截面 coverage 表；
- 计算失败的 IV/Greeks 使用 `NULL`，不会丢弃原始合约行；
- `underlying_price` 尚未存入计算表；
- `time_to_expiry` 可由 `t_days / 365` 推导；
- 若修改 Forward、利率或 Greeks 口径，应升级 `model_version`。
