# Option 模块：风险状态、策略面板与回测归因

autotrade.option 是期权领域能力的单一落点。它不改变通用 Security、
SecurityManager、OMS 或 TimeSlice 的数据模型；而是在这些通用运行时对象之上，
维护期权及其他资产的可选 Greeks 风险状态，并提供回测快照和 PnL 归因。

正式结构：

~~~
autotrade.option
├── analytics/
│   ├── forward_curve.py      # put-call parity forward curve
│   ├── greeks.py             # Black97 IV / Greeks 批量计算
│   └── ivx.py                # IVX 计算
├── strategy.py               # OptionStrategy 与 option panel 组装
├── greek_risk_manager.py     # 运行时最新 GreekRiskState
├── backtest_analysis.py      # 回测快照与区间 PnL 归因
└── reporting.py              # 期权回测表、Greek 汇总与 Excel 导出扩展
~~~

autotrade.strategy 只保留通用策略基类；期权策略应从 autotrade.option 导入。

## 1. 架构和职责边界

~~~
TimeSlice
 ├─ 行情 / 合约状态 ─────────────→ SecurityManager ──→ Security（最新价格、multiplier）
 ├─ option_analytics ───────────→ GreekRiskManager ─┐
 └─ custom_data[non_option...] ─→ GreekRiskManager ─┤→ GreekRiskState（最新风险记录）
                                                       │
OMS ──────────────────────────────────────────────────┤→ 当前单资产 / 组合暴露
                                                       │
OptionBacktestAnalyzer ← trade / valuation event ──────┘→ 逐资产历史快照、区间 PnL 归因
                                                        │
OptionBacktestReporting ← Analyzer + BacktestReporting ┘→ 期权表、组合表、Excel 报告
~~~

| 组件 | 负责 | 不负责 |
| --- | --- | --- |
| SecurityManager | 每个资产的最新 Security、价格、合约乘数和元数据 | Greeks、风险因子、PnL 归因历史 |
| OMS | 当前持仓及方向 | 模型 Greeks、价格状态快照 |
| GreekRiskManager | 每个 instrument_id 的最新风险记录；按 OMS 聚合当前暴露 | 计算 IV/Greeks；更新行情；修改 Security；保存历史 |
| OptionBacktestAnalyzer | 保存逐资产不等距快照、按同一资产的相邻快照归因 | 维护实时风险；对资产时钟做补齐；计算组合聚合 |
| OptionBacktestReporting | 输出风险/归因明细及按时间点的组合聚合、Excel 导出 | 修改 Analyzer 事实记录；对缺失快照做最近值补齐 |
| option.analytics | Forward、Black97 Greeks、IVX 等纯计算 | 访问 OMS、维护实盘状态、写回 Security |
| OptionStrategy | 把 Slice.option_analytics 与 OptionContract 拼成策略面板 | 代替 GreekRiskManager 维护组合风险 |

实盘和回测共用完全相同的风险状态语义：两者都将风险数据放入 Slice，
由应用层调用同一个 GreekRiskManager.on_slice()。只有历史记录和事后归因
属于回测分析层。

## 2. GreekRiskState：单资产的最新风险视图

GreekRiskState 只有两个来源：

~~~
GreekRiskState(
    security=security_manager.get(instrument_id),
    analytics=latest_risk_record_or_none,
)
~~~

它不复制、不扩展 Security。因此：

- state.price 是当前 Security.price，即 Security.value。它可以来自 tick、
  bar 或其他行情更新；风险模块不会区分 close 与 tick。
- state.multiplier 是当前 Security.multiplier。
- state.delta、gamma、vega、theta、rho、vanna、vomma、charm 来自最新风险
  记录；未提供时均为 None，而不是伪造为零或一。
- `state.factor_price` / `state.driver_price` 是归因中的风险因子价格。对于期权，
  它严格取 manager 初始化时声明的 `forward_price` 或 `underlying_price`；两者
  不存在时为 None，**绝不**退回到期权权利金。对于非期权，则依次取 custom
  `factor_price`、custom `forward_price`、自身 Security.price。
- 不再提供含义含混的 `unit_exposure()` 或 `unit_dollar_delta_1pct`。应调用
  `state.exposure(level="contract")` 获得每张合约的敏感度，或调用
  `state.exposure(level="contract_cash")` 获得命名明确的标准 shock 现金 PnL。

None 表示“未知/未提供”，与数值 0.0 明确不同。这样 ETF、股票、期货和
期权共用同一个状态类型：线性资产通常只有 delta，期权可以有完整 Greeks，
任意字段都允许不存在。

## 3. GreekRiskManager：维护最新风险状态

初始化：

~~~python
from autotrade.option import GreekRiskManager

risk_manager = GreekRiskManager(
    security_manager=security_manager,
    oms=oms,
    option_factor_price="forward",  # Black-97 默认；也可显式选择 "underlying"
)
~~~

内部只保存一个私有映射：

~~~python
_analytics: dict[str, Any]  # instrument_id -> 最新完整风险记录
~~~

### 3.1 更新模型

update(record) 读取 record.instrument_id，并以这条记录整体替换该资产此前的
风险记录。它不区分 ETF、期货、股票或期权，也不计算 beta：上游传来什么字段，
它就保存什么字段。

所以每次记录应当是该时点的完整模型输出。若新记录没有 gamma，该字段会成为
None，不会沿用旧记录的 gamma；这样不会把不同时间的 Greeks 混在一起。

get(instrument_id) 在读取时才组合“当前 Security + 已缓存风险记录”。只要
SecurityManager 已处理本时点行情，读取到的价格和 multiplier 就是最新的；
风险记录只在收到新的 analytics/custom data 时才改变。

GreekRiskManager 本身不订阅事件，也不改 TimeSliceDriver。调用方应在
SecurityManager 完成本 Slice 的行情更新后显式执行：

~~~python
def on_data(self, slice_):
    self.risk_manager.on_slice(slice_)
    # 此后可读取最新 state 或组合暴露
~~~

回测和实盘都使用相同顺序。没有风险数据的 slice 也可调用；它不会清掉已经
缓存的上一份风险记录。

### 3.2 两条输入通道

管理器故意将“期权模型输出”和“非期权风险因子”置于不同 Slice 容器，但进入
管理器后使用完全相同的字段协议。

| 数据来源 | Slice 容器 | 默认 data name | 风险记录类型 |
| --- | --- | --- | --- |
| 期权模型输出 | slice_.option_analytics | option_analytics | OptionAnalyticsData |
| ETF / 股票 / 期货 / 自定义风险模型 | slice_.custom_data | non_option_greek_risk | CustomData(payload=...) |

可按数据源修改 data name：

~~~python
risk_manager.on_slice(
    slice_,
    option_analytics_data_name="my_option_analytics",
    non_option_greek_risk_data_name="my_risk_factor",
)
~~~

#### 期权：OptionAnalyticsData

OptionAnalyticsData 是标准期权模型输出。除 instrument_id、time 外，
构造时需要非空 model_id、model_version；可提供 forward_price、surface_iv、
risk_free_rate 及全部 Greeks。

~~~python
from autotrade.coreutils.object import OptionAnalyticsData, Slice

option_risk = OptionAnalyticsData(
    instrument_id="510050C2506M03000",
    time=when,
    value=option_price,
    forward_price=3.02,
    surface_iv=0.18,
    risk_free_rate=0.02,
    delta=0.46,
    gamma=1.12,
    vega=0.09,
    theta=-0.21,
    model_id="black97_surface",
    model_version="v1",
)
slice_ = Slice.from_named_data(when, [("option_analytics", option_risk)])
risk_manager.on_slice(slice_)
~~~

#### 非期权：CustomData

非期权风险数据无需伪装为 OptionAnalyticsData。用 CustomData.payload 放同名
字段即可；推荐固定使用 NON_OPTION_GREEK_RISK_DATA_NAME =
"non_option_greek_risk" 作为 data name，以避免和普通 custom data 混用。

~~~python
from autotrade.coreutils.object import CustomData, Slice
from autotrade.option.greek_risk_manager import (
    NON_OPTION_GREEK_RISK_DATA_NAME,
)

etf_risk = CustomData(
    instrument_id="510050.XSHG",
    time=when,
    value=0.0,  # 管理器不使用；价格仍取 Security.price
    custom_type=NON_OPTION_GREEK_RISK_DATA_NAME,
    payload={
        "factor_id": "000852.XSHG",
        "factor_price": 6500.0,
        "delta": 1.0,              # 或 beta × ETF价格 / 因子价格
        # "gamma": ...,
        # "surface_iv": ...,       # 需要 vega/vanna/vomma 归因时提供
        # "risk_free_rate": ...,   # 需要 rho 归因时提供
    },
)
slice_ = Slice.from_named_data(
    when,
    [(NON_OPTION_GREEK_RISK_DATA_NAME, etf_risk)],
)
risk_manager.on_slice(slice_)
~~~

Slice.custom_data 的实际结构为：

~~~
dict[data_name, dict[instrument_id, list[CustomData]]]
~~~

同一资产同一 slice 出现多条风险记录时，on_slice 会按列表顺序更新，最后一条
成为最新状态。因此正常情况下，每资产每时点应只投递一条完整风险记录。
这里由 Slice 的 data name 决定管理器是否消费这条 CustomData；custom_type 只是
记录自身的元数据。推荐两者都设为 non_option_greek_risk，便于检查和排错。

### 3.3 读取单资产和组合暴露

~~~python
state = risk_manager.get("510050.XSHG")
print(state.factor_id, state.factor_price, state.delta)

cash = risk_manager.exposure("510050.XSHG", level="position_cash")
print(cash.delta_cash_1pct, cash.gamma_cash_1pct)

by_factor = risk_manager.portfolio_exposure(level="position_cash")
print(by_factor["000852.XSHG"].delta_cash_1pct)
~~~

`exposure()` 以 `level` 显式选择层级：`raw`、`contract`、`position`、
`contract_cash`、`position_cash`。前三级分别是模型导数、乘数调整敏感度和
仓位敏感度；后两级是标准 shock 下的现金 PnL。标准 shock 由 `GreekShock`
统一定义：标的 1%、IV 1 vol point、利率 1bp、时间 1 天。

`portfolio_exposure()` 返回 `{factor_id: GreekExposure}`，因此不同风险因子不
会被错误相加。没有 custom 风险记录的非期权默认是自身因子、delta=1、其他 Greek=0。

风险因子价格按运行时 `Security` 类型确定，而不按代码字符串猜测：

| 类型 | factor_id | factor_price | 默认 Delta |
| --- | --- | --- | --- |
| `OptionContract` | analytics 的 `underlying_instrument_id`（否则合约 underlying） | manager 明确选择的 `forward_price` 或 `underlying_price` | 必须来自 analytics |
| `FutureContract` | custom `factor_id`，否则自身 | custom `factor_price`，否则自身价格 | 1 |
| `EquitySecurity`（ETF/股票） | custom `factor_id`，否则自身 | custom `factor_price`，否则自身价格 | 1 |

期权不会读取 custom `factor_price`，也绝不会用期权权利金作为 factor price；所选
`forward_price` / `underlying_price` 缺失时，该风险状态的含标的因子归因会标记为无效。

## 4. OptionBacktestAnalyzer：快照和归因

OptionBacktestAnalyzer 是回测分析组件，依赖同一个 GreekRiskManager，但不
参与实时状态维护。它保存的是**每个资产自己的不等距时间序列**，而不是预先
对齐后的组合时间序列：

~~~python
from autotrade.option import OptionBacktestAnalyzer

analyzer = OptionBacktestAnalyzer(risk_manager)

# 在风险数据已 on_slice 后，每个需要估值/输出归因的时点调用。
analyzer.record(when, instrument_ids=["MO2409C5000"])
~~~

record() 的职责：

1. 对指定资产读取 OMS 的权威带符号仓位（平仓后可为 0）和 manager 状态；
2. 深拷贝 GreekRiskState，冻结价格、multiplier 与风险记录；
3. 将快照追加到 `instrument_snapshots[instrument_id]`；
4. 若该资产已有上一快照，使用该资产的期初仓位、期初 Greeks 生成
   `InstrumentPnlAttribution`，追加到
   `instrument_attributions_by_instrument[instrument_id]`。

默认不传 `instrument_ids` 时，`record()` 仍记录当前 OMS 的全部持仓，用于兼容
旧调用；事件驱动接线应明确传入发生变化或需要估值的资产。Analyzer 不做最近
快照补齐，也不做组合级归因；时间对齐和组合口径属于 Reporter。

可订阅成交事件：

~~~python
# OmsBase 必须先于 analyzer 注册到同一个 event_engine，保证成交后 OMS
# 已更新，Analyzer 才读取 post-fill 的权威仓位。
analyzer.subscribe_trade_events(event_engine)
~~~

该订阅按 `tradeid` 去重，并忽略未被 OMS 接纳的无效成交。完全平仓时 OMS 会
删除该持仓，但 Analyzer 仍以成交的 `instrument_id` 记录 `position=0` 的终点
快照，确保最后一段归因不丢失。回测开始已有持仓时，仍须显式记录一条初始快照；
持仓期间没有成交时，还应在每日/估值/回测结束时额外 record，以覆盖浮动 PnL。

### 4.1 归因口径

区间 t0 → t1 使用 **t0 持仓和 t0 Greeks**，即 lagged-position /
start-of-period exposure 口径。令：

~~~
q       = t0 带符号持仓
m       = t0 multiplier
scale   = q × m
dF      = driver_price(t1) - driver_price(t0)
dIV     = surface_iv(t1) - surface_iv(t0)
dr      = risk_free_rate(t1) - risk_free_rate(t0)
dt      = (t1 - t0) / 365 年
~~~

对于期权，driver_price 由 manager 的 `option_factor_price` 明确选择为
forward 或 underlying；缺失即为 None 并使该段归因无效，绝不使用权利金。
对于非期权，缺失显式因子价格时才使用自身 Security.price。
对单个资产，实际 PnL 与各分量为：

~~~
actual PnL = scale × (security_price(t1) - security_price(t0))

delta = scale × delta(t0) × dF
gamma = 0.5 × scale × gamma(t0) × dF²
vega  = scale × vega(t0) × dIV
theta = scale × theta(t0) × dt
rho   = scale × rho(t0) × dr
vanna = scale × vanna(t0) × dF × dIV
vomma = 0.5 × scale × vomma(t0) × dIV²
charm = scale × charm(t0) × dF × dt

approximate PnL = Σ Greek components
residual PnL    = actual PnL - approximate PnL
~~~

成交手续费当前由通用 AccountLedger 记账；若要使期权归因同 equity 严格对账，
应在报告接线中将每笔成交手续费分配到相应的资产/区间，再计算扣费后的 PnL。

希腊字母单位必须与上述公式一致。当前 Black97 计算器输出的 vega、theta、rho
已转换为分别对“小数 IV”、“年化时间”和“小数利率”的导数。

### 4.2 缺失值和线性资产默认规则

风险状态层始终保持严格的 None；但 PnL 归因有一个有意限定的线性资产约定：

- 非 OptionContract 且起点没有 delta 风险记录：归因时默认 delta = 1.0。
  如果没有 forward_price，则用该资产自身价格变化作为 dF。
- OptionContract 起点没有 delta：这是模型输入缺失，绝不默认 delta=1。
  区间标记 valid=False，missing 中出现 "instrument_id:option_delta"，
  approximate_pnl 与 residual_pnl 为 None。
- gamma、vega、theta、rho、vanna、vomma、charm 未提供时，该分量保持 0；
  当前实现不会仅因此使归因失效。
- 缺少期初/期末价格、multiplier、状态或 delta 所需 driver price 时，归因
  标为无效，具体原因写入 PnlAttribution.missing。

这样同时保证：普通线性资产不提供风险数据时仍可做基本 delta PnL；期权不会
因缺少模型 delta 而被伪装成线性资产。

## 5. OptionBacktestReporting：期权回测表与 Excel 导出

`OptionBacktestReporting` 继承通用 `BacktestReporting`，通过 Analyzer 的逐资产
事实记录生成表，不改变基础回测账本：

~~~python
from autotrade.option import OptionBacktestReporting

reporting = OptionBacktestReporting(
    recorder=gateway.recorder,
    analyzer=performance_analyzer,
    oms=oms,
    option_analyzer=option_analyzer,
)
reporting.export_xlsx("output/option_backtest_report.xlsx")
~~~

它提供以下 DataFrame：

| 方法 | 索引 | 内容 |
| --- | --- | --- |
| `get_position_cash_greeks_df()` | `(asof, instrument_id)` | 逐资产持仓、合约特征、标准化现金 Greeks |
| `get_instrument_greek_pnl_df()` | `(end, factor_id, instrument_id)` | 各资产相邻快照区间的实际 PnL、各 Greek PnL、近似 PnL、残差、有效性 |
| `get_portfolio_cash_greeks_df()` | `(date, factor_id)` | 同一风险因子、同一事件时间的标准化现金 Greek 加总；不做最近值补齐 |
| `get_portfolio_greek_pnl_df()` | `(date, factor_id)` | 按逐资产归因区间的 `end` 时间、风险因子加总 |
| `get_portfolio_greek_pnl_analysis_df()` | `greek` | 标准化现金风险与 Greek PnL 的统计特征 |

标准化现金 Greek 使用：标的 ±1%、IV ±1 vol point、利率 ±1bp、时间 1 天；
风险统计表将每段 Greek PnL 配对到其**期初**标准化现金风险，输出平均/最大绝对
风险、累计/平均 PnL、PnL 波动与正收益比例。归因无效的组合时点不进入统计。

期权 Excel 报告在基础四张表 `performance`、`account_daily`、`trade_log`、
`position_daily` 上额外输出：

~~~text
position_cash_greeks
instrument_greek_pnl
portfolio_cash_greeks
portfolio_greek_pnl
greek_pnl_analysis
~~~

## 6. 典型接线方式

下面是策略/应用层的推荐顺序；无需修改 TimeSliceDriver：

~~~python
class MyOptionStrategy(OptionStrategy):
    def __init__(self, event_engine, security_manager, oms):
        super().__init__(event_engine, security_manager)
        self.risk_manager = GreekRiskManager(security_manager, oms)
        self.analyzer = OptionBacktestAnalyzer(self.risk_manager)

    def on_data(self, slice_):
        # StrategyBase 与 OptionStrategy 处理通用事件 / 可选期权面板。
        super().on_data(slice_)

        # Slice 已由运行时组装；SecurityManager 应已反映本时点行情。
        self.risk_manager.on_slice(slice_)

        exposure = self.risk_manager.portfolio_exposure()
        # 用 exposure 做风控、下单限制或监控。

        if should_value(slice_.time):
            # 日终/估值快照应明确指定要估值的资产；该策略在这里对当前
            # OMS 全部持仓估值，成交事件则由 subscribe_trade_events 记录。
            self.analyzer.record(slice_.time)

    def on_option_panel(self, panel, slice_):
        # 仅策略自己的期权横截面逻辑。
        pass
~~~

若 on_option_panel 需要读取最新风险，可在应用层先执行
risk_manager.on_slice(slice_) 再调用面板组装逻辑，或覆写 on_data 调整顺序。
OptionStrategy 与 GreekRiskManager 是协作关系而非隐式依赖，目的是让风险层
适用于所有期权策略（双卖、套利、方向性等），也覆盖组合中的线性对冲资产。

## 7. 当前边界与后续扩展点

- 风险记录只按 instrument_id 缓存最新版本；不保存时间序列。历史只由
  OptionBacktestAnalyzer.record() 形成。
- manager.items() 只枚举“收到过风险记录”的资产；若要取得没有风险记录的线性
  资产状态，应按 instrument_id 调用 manager.get()。组合暴露则始终按 OMS 持仓
  逐项计算。
- 管理器不决定 beta、factor price 或模型选择。上游若希望 beta 映射，应将
  映射后的 delta 与可选 forward_price 写入 CustomData.payload。
- 当前 GreekExposure 是 raw Greek exposure，不是 VaR、情景损失或保证金。
  将来可在 manager 之上添加独立 calculator/report，而无需修改 Security 或
  输入数据协议。
- 当前归因使用同一资产相邻快照之间的期初持仓。成交事件会切分持仓段；但若要
  覆盖无成交期间的浮动 PnL，仍需要独立的日终/估值/回测结束快照。
- 报告的组合 Greek 只按完全相同的事件时间聚合。若需要“最近有效风险暴露延续”
  的组合曲线，应在 Reporter 增加明确的 as-of 对齐策略，而不应写进 Analyzer。
- 外部 pandas 数据必须在 Reader 中将 `NaN` 转为 `None`；期权 Greeks 数据文件中
  常见的 `iv` 会由标准 Reader 映射为 `surface_iv`。

## 8. 本次重构的设计决策与使用准则

本节记录当前接口的最终设计意图。核心原则是：**风险状态、历史事实、组合对齐
和展示报告是四件不同的事，不能互相代替。**

### 8.1 一份状态，一个显式层级

`GreekRiskState` 只描述“某个 instrument 此刻的模型输入和 raw Greeks”。
`GreekExposure` 才描述“以什么层级、对多少仓位、在什么标准 shock 下观察它”。
这避免了旧式 `delta_notional`、`delta_pnl_1pct` 等名称同时混合了单位、价格、
仓位和 shock 的问题。

统一链路为：

~~~text
Raw Greek
  -> Contract Greek       = Raw × multiplier
  -> Position Greek       = Contract × signed quantity
  -> Contract Cash Greek  = Contract Greek × named standard shock × Taylor coefficient
  -> Position Cash Greek  = Contract Cash Greek × signed quantity
~~~

`raw`、`contract`、`position` 返回的列名仍是 `delta`、`gamma` 等数学导数；
`contract_cash`、`position_cash` 返回下列固定命名的现金 PnL 风险：

| 字段 | 含义 |
| --- | --- |
| `delta_cash_1pct` | 标的变动 1% 的一阶现金 PnL |
| `gamma_cash_1pct` | 标的变动 1% 的二阶现金 PnL，含 1/2 |
| `vega_cash_1vol` | IV 变动 1 vol point 的现金 PnL |
| `theta_cash_1d` | 经过 1 天的现金 PnL |
| `rho_cash_1bp` | 利率变动 1 bp 的现金 PnL |
| `vanna_cash_1pct_1vol` | 1% 标的 × 1 vol point 的交叉现金 PnL |
| `vomma_cash_1vol` | 1 vol point 的二阶 vol 现金 PnL，含 1/2 |
| `charm_cash_1pct_1d` | 1% 标的 × 1 天的交叉现金 PnL |

默认 shock 为 spot return=0.01、vol change=0.01、rate change=0.0001、
time=1/365 年；需要不同情景时传入 `GreekShock`，而不是修改 raw Greek。

~~~python
from autotrade.option import GreekShock

one_contract = risk_manager.exposure(option_id, quantity=1, level="contract_cash")
held = risk_manager.exposure(option_id, level="position_cash")
stress = risk_manager.exposure(
    option_id, level="position_cash",
    shock=GreekShock(spot_return=0.03, vol_change=0.02),
)
~~~

对冲数量应直接比较同一风险因子下的现金 Delta，而非自行拼接
`delta × multiplier × price`：

~~~python
target = risk_manager.exposure(option_id, level="position_cash")
hedge_unit = risk_manager.exposure(hedge_id, quantity=1, level="contract_cash")
hedge_contracts = -target.delta_cash_1pct / hedge_unit.delta_cash_1pct
~~~

这要求两边 `factor_id` 与风险因子定义一致。若 ETF 的价格变化只近似跟踪指数，
应先在上游对 ETF/指数收益率回归得到 beta 或 inverse-beta 约定，再将该约定、
`factor_id` 与 `factor_price` 一起作为非期权风险记录输入；manager 不应猜测 beta。

### 8.2 Black-97 因子与 Greek 单位

Black-97 的 Delta/Gamma 是对 forward 的导数。因此期权回测应在创建 manager 时
明确 `option_factor_price="forward"`；若模型确实提供并使用现货 Greeks，才选择
`"underlying"`。不要把期权价格当作因子价格，否则 Gamma 项会按权利金的价格尺度
错误计算。

当前 cfutures Black-97 数据约定为：

- `delta`、`gamma`：对 forward 价格单位的导数；
- `vega`：对 IV 小数（例如 0.01）的导数；
- `theta`：对年化时间的导数；
- `rho`：对利率小数的导数；
- `vanna`、`vomma`、`charm`：也使用归因公式中的小数 IV / 年化时间口径。

因此 Analyzer 中的 `dIV` 和 `dt_year` 不再额外乘错 100 或 365。现金风险表中的
`*_cash_1vol`、`*_cash_1d` 只是为了风险展示再施加标准 shock；它们不是历史 PnL
归因的输入列。

### 8.3 历史归因、组合聚合与无效区间

Analyzer 对每个资产只比较它自己的相邻快照：

~~~text
(20:13, A) -> A 的一段归因
(20:15, A) -> A 的下一段归因
(20:15, B) -> B 的独立归因
~~~

Reporter 才按精确 end timestamp 和 `factor_id` 把上述独立区间聚合。它不会做
“最近快照”向前填充；如需日频组合曲线，应由调用方在报告层明确选择 as-of 对齐
规则。这样事件驱动的非等距资产时钟不会被 Analyzer 隐式扭曲。

一段归因若缺少期权起点 Delta、起终点 factor price、价格、乘数，或因子 ID
改变，则：

- `actual_pnl` 仍尽可能保留（只要价格、仓位和乘数足够）；
- `valid=False` 且 `missing` 写入确切原因；
- `approximate_pnl`、`residual_pnl` 为 None；
- Reporter 不把它的 Greek PnL/残差混入有效组合分解。

所以必须分开报告“全样本 actual PnL”和“可归因样本 actual/approximate/residual
PnL”。有效区间的推荐精度指标为：

~~~text
weighted absolute attribution error
= sum(abs(residual_pnl)) / sum(abs(actual_pnl))
~~~

不要用 `sum(residual) / sum(actual)` 作为唯一精度指标：当正负 PnL 相互抵消时，
该比例会被放大。逐合约日的相对误差也应排除 `actual_pnl=0`，并同时给出中位数、
分位数和按绝对实际 PnL 加权的结果。

### 8.4 回测接线的权威顺序

推荐按下列顺序组装；关键是 risk manager 先于策略读取面板更新，Analyzer 在 OMS
成交后读取持仓，并在每个估值点保留持仓快照：

~~~python
risk_manager = GreekRiskManager(engine.security_manager, engine.oms)
events.register(EVENT_SLICE, lambda event: risk_manager.on_slice(event.data))

analyzer = OptionBacktestAnalyzer(risk_manager)
analyzer.subscribe_trade_events(events)  # 必须在 OMS 已注册之后
gateway.option_analyzer = analyzer        # gateway.process_valuation 内 record(...)

engine.reporting = OptionBacktestReporting(
    recorder=gateway.recorder,
    analyzer=PerformanceAnalyzer(initial_cash=initial_cash, annual_days=252),
    oms=engine.oms,
    option_analyzer=analyzer,
)
~~~

网关的估值记录应包含所有当前持仓以及 Analyzer 已见过的资产，以便平仓后的零仓位
终点可形成最后一段归因。`subscribe_trade_events()` 提供成交后快照，估值快照提供
持仓期间的浮动 PnL；两者都需要。

### 8.5 全样本归因验证模块

`tests/integration/option_attribution_validation.py` 是一个可执行的集成验证模块，
不是交易策略。它只使用 `/Desktop/data/autotrade_rq` 中的
`optioninstrument_{asset}.pkl`、`optionprice_{asset}.pkl` 与
`calculatedoptionGreeks_{asset}.pkl`：

1. 每日只在一个时点决策；
2. 所有当日可交易、尚未持有且剩余到期日不少于 3 天的期权各买 1 张；
3. 持仓剩余到期日小于 3 天即平仓并永久退休；
4. 不因 Greek 缺失跳过合约，而是用 `valid/missing` 检验数据和归因覆盖率。

~~~bash
cd /home/buzheng/Desktop/autotrade
PYTHONPATH=src python tests/integration/option_attribution_validation.py \
  --asset MO --start 2023-01-03 --end 2023-02-10
~~~

不传 `--start/--end` 则覆盖数据全区间。输出目录默认是
`tests/integration/results_all_option_attribution/{asset}`，包含：

~~~text
validation_summary.csv
position_cash_greeks.parquet
instrument_greek_pnl.parquet
portfolio_cash_greeks.parquet
portfolio_greek_pnl.parquet
greek_pnl_analysis.csv
~~~

Parquet 保留 MultiIndex，适合全期权面板的大表；若需 Excel，使用
`OptionBacktestReporting.export_xlsx()` 输出同名 sheet。该验证模块覆盖大量深度
虚实值、陈旧价格、临近到期和缺失曲面记录，因此它的残差不能直接代表经流动性和
信号筛选后的真实策略；它的用途是定位口径错误、缺失率和归因尾部样本。
