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
- state.forward_price 来自最新风险记录；state.driver_price 优先取 forward，
  缺失时自然退回 state.price。
- state.unit_exposure("delta") 为单份合约的 delta × multiplier；
  state.unit_dollar_delta_1pct 再乘 driver_price × 1%。

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
        "delta": 1.0,              # 或策略定义的 beta 映射 delta
        "forward_price": 3.01,     # 可选；缺失时归因用该资产自身价格
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
print(state.price, state.delta, state.forward_price)
print(state.unit_delta_exposure)

asset = risk_manager.asset_exposure("510050.XSHG")
print(asset.quantity, asset.delta, asset.gamma)

portfolio = risk_manager.portfolio_exposure()
print(portfolio.delta, portfolio.vega, portfolio.missing)
~~~

asset_exposure 默认从 OMS 取得带符号仓位：Direction.SHORT 的正数量转为负数量。
每个 Greek 暴露为：

~~~
position_quantity × security.multiplier × greek
~~~

portfolio_exposure 对全部有仓位资产逐项相加；未提供的字段记录到 missing，
如 "510050.XSHG:vega"。它不会将缺失 Greek 默认为零或将线性资产 delta
默认为一，这是运行时风险展示的保守语义。

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

driver_price 优先为记录中的 forward_price，缺失时为资产自身 Security.price。
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
| `get_position_greeks_df()` | `(asof, instrument_id)` | 逐资产持仓、合约特征、仓位调整 Greeks、`delta_notional`、`delta_pnl_1pct` |
| `get_instrument_greek_pnl_df()` | `(end, instrument_id)` | 各资产相邻快照区间的实际 PnL、各 Greek PnL、近似 PnL、残差、有效性 |
| `get_portfolio_greeks_df()` | `date` | 只对同一事件时间的资产风险快照加总；不做最近值补齐 |
| `get_portfolio_greek_pnl_df()` | `date` | 按逐资产归因区间的 `end` 时间加总 |
| `get_portfolio_greek_pnl_analysis_df()` | `greek` | 风险暴露与 Greek PnL 的统计特征 |

风险统计表将每段 Greek PnL 配对到其**期初**风险暴露，输出平均/最大绝对暴露、
累计/平均 PnL、PnL 波动、正收益比例、单位平均绝对暴露 PnL；Delta 另输出平均
`delta_notional` 和 `delta_pnl_1pct`。归因无效的组合时点不进入这项统计。

期权 Excel 报告在基础四张表 `performance`、`account_daily`、`trade_log`、
`position_daily` 上额外输出：

~~~text
position_greeks
instrument_greek_pnl
portfolio_greeks
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
