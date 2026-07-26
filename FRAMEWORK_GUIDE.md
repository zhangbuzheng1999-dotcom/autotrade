# Autotrade 框架与使用指南

> 架构版本：v0.4.0
>
> 版本日期：2026-07-26
>
> 适用范围：当前 `src/autotrade` 的统一实盘/回测运行时

本文是当前框架的交接文档。它面向使用者、Gateway 开发者、策略开发者和后续维护代码的 AI。修改架构前，应先核对本文描述和测试；如果代码行为发生变化，应在同一个提交中更新本文。

## 1. 设计目标

Autotrade 当前遵循三个原则：

1. **模块通过事件通信**：策略、行情、OMS、Gateway、日志和可选插件不互相硬编码，可以独立安装和拆除。
2. **实盘与回测共用主干**：策略、数据对象、SecurityManager、OMS、OrderRouter、TimeSliceDriver 和日志系统保持一致。
3. **只隔离不可避免的差异**：实盘由真实行情和券商推动、异步处理；回测遍历历史 TimeSlice、同步处理，并用模拟 Gateway 代替券商。

框架不是通过在 `BacktestEngine` 中复制一套实盘逻辑来实现回测，而是使用相同的运行时组件图，只替换事件引擎、数据生产者和 Gateway。

## 2. 总体结构

```text
RuntimeEngine
└── RuntimeComponents
    ├── EventEngine
    ├── SecurityManager
    ├── OmsBase
    ├── OrderRouter
    ├── TimeSliceDriver
    ├── Gateway
    └── LogEngine

LiveEngine(RuntimeEngine)
├── EventEngine                  # 异步
├── LiveDataManager             # 实时数据 -> TimeSlice
└── Live Gateway                # 券商/交易所适配器

BacktestEngine(RuntimeEngine)
├── BacktestEventEngine          # 同步
├── DataManager                 # 历史数据 -> TimeSlice 流
├── BacktestGateway             # 模拟券商门面
│   ├── SimulatedOrderBook
│   ├── MatchingEngine
│   ├── AccountLedger
│   ├── CommissionModel
│   ├── MarginModel
│   └── BacktestEventPublisher
└── BacktestReporting
    ├── BacktestRecorder
    └── PerformanceAnalyzer
```

装配入口：

- 通用装配：`src/autotrade/engine/runtime_engine.py`
- 实盘装配：`src/autotrade/engine/live_engine.py`
- 回测装配：`src/autotrade/backtest/backtest_engine.py`

## 3. 核心数据流

### 3.1 TimeSlice

实盘和回测都必须把数据转换为同一种 `TimeSlice`：

```python
TimeSlice(
    time=when,
    slice=strategy_visible_slice,
    security_updates=(...),
    valuation_updates=(...),
    is_bootstrap=False,
)
```

三个数据区域职责不同：

- `slice`：策略可见的数据。可以同时包含 tick、1m、15m、自定义数据或
  `OptionAnalyticsData`，并用 `data_name` 区分。
- `security_updates`：用于更新 `SecurityManager` 的标的定义和最新市场状态。
- `valuation_updates`：专门触发账户盯市、保证金刷新和历史记录；为空时不进行估值和记录。

这意味着“策略周期”“标的最新状态周期”“估值周期”可以分别配置。例如：

- tick 同时更新策略和 SecurityManager；
- 1m 只提供给策略；
- 15m 同时提供给策略并触发估值。

`InstrumentStateData(time=None)` 是启动数据，只更新标的定义，不触发策略和模拟成交。

### 3.2 TimeSlice 的固定消费顺序

`TimeSliceDriver.process()` 是实盘和回测共享的时间片消费者。

回测顺序严格为：

```text
1. security_updates -> EVENT_DATA -> SecurityManager
2. market.before    -> BacktestGateway 激活旧订单并撮合
3. slice            -> EVENT_SLICE -> Strategy
4. market.after     -> 可选的当根收盘成交
5. account.valuation（仅有 valuation_updates 时）
                     -> 盯市、账户事件、Recorder 快照
```

这个顺序保证：

- 策略运行前，SecurityManager 已经是当前时刻的状态；
- 默认市价单不能偷看当前 bar，通常在下一根 bar 成交；
- 开启 `cheat_on_close` 后，当前策略产生的订单才允许在 `market.after` 撮合；
- 没有 `valuation_updates` 时不做无意义的盯市和历史记录，适合 tick/分钟级回测。

实盘使用同一个 Driver，但 `simulated_broker=False`，因此只执行：

```text
security_updates -> EVENT_DATA
slice -> EVENT_SLICE
```

成交和账户状态由真实 Gateway 异步返回。

## 4. 事件与消息协议

### 4.1 Event：广播状态

`Event(type, data)` 表示已发生的事实或状态更新，可有多个订阅者。

主要事件：

| 事件 | 内容 | 主要消费者 |
| --- | --- | --- |
| `EVENT_LIVE_DATA` | Gateway 原始实时数据 | `LiveDataManager` |
| `EVENT_DATA` | 标的定义或市场状态 | `SecurityManager` |
| `EVENT_SLICE` | 策略可见的标准 `Slice` | 策略 |
| `EVENT_ORDER` | 订单最新状态 | `OmsBase`、策略 |
| `EVENT_TRADE` | 已确认成交 | `OmsBase`、策略 |
| `EVENT_POSITION_SNAPSHOT` | 券商持仓快照 | `OmsBase` 对账入口 |
| `EVENT_POSITION` | OMS 推导后的统一持仓 | 策略及其他订阅者 |
| `EVENT_ACCOUNT` | 账户最新状态 | `OmsBase` |
| `EVENT_REQUEST_STATUS` | 请求接受、拒绝或失败 | 策略/调用方 |
| `EVENT_LOG` | 日志数据 | `LogEngine` |

规则：

- 行情和合约信息不进入 OMS，只进入 SecurityManager。
- `EVENT_POSITION_SNAPSHOT` 是 Gateway/券商输入；`EVENT_POSITION` 由 OMS 统一发布。
- `EVENT_TRADE` 是唯一成交事实，OMS 根据它投影持仓。

### 4.2 Message：有路由的命令

`Message` 用于“要求某个组件执行动作”，而不是广播状态。

```python
Message(
    kind=MessageKind.COMMAND,
    name=COMMAND_ORDER_SUBMIT,
    data=order_request,
    source="strategy.MyStrategy",
    target="order_router",
)
```

命令消费者通过 `(target, name)` 唯一确定：

```python
event_engine.register_command(
    "order_router",
    COMMAND_ORDER_SUBMIT,
    handler,
)
```

因此订阅者不是订阅笼统的 `COMMAND`，而是注册准确的目标和命令名。一个 `(target, name)` 只允许一个消费者，重复注册会抛出 `DuplicateHandlerError`。

下单路由：

```text
Strategy
  -- target=order_router, name=order.submit -->
OrderRouter
  -- target=execution, name=order.submit -->
Gateway
  -- EVENT_ORDER / EVENT_TRADE / EVENT_ACCOUNT -->
OMS、Strategy、其他插件
```

`OrderRouter` 是下单策略层与执行层之间的公共控制点，可用于静默标的、风控拦截、rollover 内部订单放行等。

### 4.3 异步与同步

`EventEngine`：

- 使用队列和工作线程；
- `put()` 只入队，处理异步发生；
- 用于实盘，避免行情/API 回调被业务处理阻塞。

`BacktestEventEngine(EventEngine)`：

- 复用完全相同的注册、路由和处理规则；
- 不启动线程；
- `put()` 在当前线程排空队列，所有派生事件处理完成后才返回；
- 用 deque 和 `_processing` 防止事件重入破坏顺序。

因此业务组件不应判断自己运行在实盘还是回测，也不应直接调用另一个组件的方法来“保证顺序”；顺序由 TimeSliceDriver 和事件引擎实现。

## 5. 状态所有权与模块边界

### 5.1 SecurityManager

`SecurityManager` 是标的定义和最新市场状态的唯一权威来源。

负责：

- `instrument_id -> Security` 映射；
- 合约乘数、保证金率、手续费率、交易状态等静态/动态属性；
- 最新价格、OHLC、盘口、成交量和持仓量；
- 将通用 `Security` 按状态数据升级为股票、期货或期权对象。

只订阅 `EVENT_DATA`。不维护订单、成交、持仓或账户。

实盘与回测必须使用同一个 `SecurityManager` 实现和同一个实例；`BacktestGateway` 的撮合、手续费、保证金和估值都读取该实例。

### 5.2 标的信息导入、生命周期与 Security 更新

#### 5.2.1 设计模型

`SecurityManager` 不主动读取数据库、CSV 或 DataFrame。运行时创建它时，
其内部 `securities` 字典为空；它绑定共享 `event_engine` 并只消费
`EVENT_DATA`。标的信息导入分为三层：

1. **原始信息表**：数据库或 DataFrame 中的合约定义和历史属性；
2. **状态事件**：Reader 将每行转换为带生效时间的
   `InstrumentStateData`；
3. **运行时对象**：`SecurityManager` 根据状态类型创建或更新唯一的
   `Security`。

具体类型映射为：

| 状态数据 | 运行时对象 |
| --- | --- |
| `InstrumentStateData` | `Security` |
| `EquityStateData` | `EquitySecurity` |
| `FutureStateData` | `FutureContract` |
| `OptionStateData` | `OptionContract` |

状态数据表示“从 `time` 开始生效的一份完整标的状态快照”，不是
`Security` 本身。`Security` 则是一个随时间持续更新、可被 Gateway、
策略和其他组件查询的运行时对象。

期货信息的完整导入链路为：

```text
期货信息表
-> _InstrumentFrameNormalizer.expand()
-> FutureStateReader
-> FutureStateData 流
-> DataManager 按 time 与行情同步
-> TimeSlice.security_updates
-> TimeSliceDriver 发布 EVENT_DATA
-> SecurityManager._apply_instrument_state()
-> 创建或更新 FutureContract
```

第一次收到某个 `instrument_id` 的 `FutureStateData` 时，
`SecurityManager` 创建 `FutureContract`；以后再收到同一
`instrument_id` 的状态时，对同一个对象调用 `apply_state()`，不会为每次
属性变化创建新对象。更新字段包括：

- `is_active`、`is_tradable` 和 `exchange`；
- `multiplier`、`margin_rate` 和各方向手续费率；
- `list_date`、`delist_date` 和 `attributes`；
- 期货的 `expiry`、`root_instrument_id`；
- 期权的标的、到期日、行权价、方向和行权方式。

行情也是通过 `EVENT_DATA` 更新同一个对象。`TradeBar`、`Tick` 或
`QuoteBar` 会更新最新价格、OHLC、盘口、成交量和持仓量，但不会建立第二份
合约缓存。

#### 5.2.2 信息表字段

通用状态字段为：

| 标准字段 | 含义 | 默认别名示例 |
| --- | --- | --- |
| `instrument_id` | 唯一合约标识 | `order_book_id`、`symbol`、`code` |
| `date` | 本行完整状态的生效时间 | `effective_date`、`effective_time` |
| `list_date` | 上市生效日期 | `listed_date`、`start_date` |
| `delist_date` | 失效日期 | `delisted_date`、`end_date` |
| `is_active` | 从 `date` 起是否有效 | `active` |
| `multiplier` | 合约乘数 | `contract_multiplier`、`size` |
| `margin_rate` | 保证金率 | `initial_margin_rate` |
| `commission_rate` | 通用手续费率 | `fee_rate` |
| `long_commission_rate` | 多方向手续费率 | `long_rate` |
| `short_commission_rate` | 空方向手续费率 | `short_rate` |
| `expiry` | 期货/期权到期时间 | `expiry_date`、`maturity_date` |
| `root_instrument_id` | 期货品种代码 | `root_symbol`、`product` |

无法由默认别名识别的列应显式传入 `schema`。例如原表使用
`underlying_symbol` 表示期货品种、使用 `delistdate` 表示退市日：

```python
reader = FutureStateReader(
    schema={
        "instrument_id": "instrument_id",
        "root_instrument_id": "underlying_symbol",
        "list_date": "list_date",
        "delist_date": "delistdate",
        "multiplier": "contract_multiplier",
        "margin_rate": "initial_margin_rate",
        "commission_rate": "fee_rate",
        "expiry": "expiry_date",
    }
)

states = reader.read(futures_df, exchange=Exchange.CFFEX)
```

未映射为标准字段的其他列会保存在 `state.attributes` 中，并在更新后复制到
`security.attributes`。低频使用的交易时段、品种名称等信息适合放在这里；
撮合、保证金或风控频繁使用的字段应定义为正式字段。

对期货而言，`root_instrument_id` 通常表示 `IF`、`IH` 这样的期货品种。
如果 `underlying_symbol` 表示 `000300.SH` 这样的真实现货标的，不应将它
映射成 `root_instrument_id`，而应作为独立附加字段保存在
`attributes`。

#### 5.2.3 信息表的四种形态

`_InstrumentFrameNormalizer` 将静态信息和历史状态统一展开为
`date + instrument_id + 完整状态` 的事件表。`date` 表示状态生效时间，
不是普通交易日期。输入可以分成四种情况：

| 原表有 `date` | 有 `list_date`/`delist_date` | 场景 | 处理结果 |
| --- | --- | --- | --- |
| 否 | 否 | 固定配置、连续或永久有效标的 | 保留 `date=NaT`，生成 bootstrap 状态 |
| 否 | 是 | 属性固定的普通到期合约 | 用生命周期日期生成上市/退市状态 |
| 是 | 否 | 属性会变化，但当前数据不管理生命周期 | 保留所有历史状态，默认持续有效 |
| 是 | 是 | 完整的合约状态历史 | 保留历史状态，并补齐上市/退市状态 |

**无 date、无生命周期**

```text
instrument_id  multiplier  margin_rate
HK.MHImain     10          0.10
```

会产生 `time=None` 的 `FutureStateData`。`DataManager` 将所有无时间状态
组成一个 `is_bootstrap=True` 的 `TimeSlice`，在第一根历史行情之前初始化
`SecurityManager`，但不调用策略、不撮合也不估值。

**无 date、有生命周期**

```text
instrument_id  list_date   delist_date  multiplier
IF2608         2026-06-22  2026-08-21   300
```

展开为：

```text
date        instrument_id  is_active  multiplier
2026-06-22  IF2608         True       300
2026-08-21  IF2608         False      300
```

当存在 `list_date` 时，合成的上市状态替代原来的无日期定义，不再作为
bootstrap 状态。如果只有 `delist_date`，无日期定义仍负责 bootstrap，
并在退市日另外产生失效状态；如果只有 `list_date`，则只产生上市状态，
不会自动失效。

**有 date、无生命周期**

```text
date        instrument_id  multiplier  margin_rate
2026-01-01  IF2608         300         0.12
2026-07-01  IF2608         300         0.15
```

两行均作为 `is_active=True` 的完整状态事件进入时间轴。框架会在
`2026-07-01` 更新同一个 `FutureContract` 的保证金率，但由于没有退市
信息，该对象之后仍保持有效。

**有 date、有生命周期**

原有 `date` 行全部保留；如果 `list_date` 没有对应状态，则从上市日或上市
日之前最近的完整状态补一条 `is_active=True` 记录；`delist_date` 总是从
该日期或之前最近的完整状态复制一条 `is_active=False` 记录。例如：

```text
date        active  multiplier  margin_rate
2026-06-22  True    300         0.12
2026-07-10  True    300         0.15
2026-08-21  False   300         0.15
```

框架不会用上市日之后的未来状态倒推上市时的属性。如果没有
`date <= list_date` 的状态，也没有可用的无日期基础定义，会明确报错。
同一标的存在多个冲突的 `list_date` 或 `delist_date` 也会报错。

#### 5.2.4 加入回测 TimeSlice

合约状态数据源必须路由到 `security_data_names`，不能作为估值源：

```python
data = DataManager(
    DataRoutingConfig(
        strategy_data_names={"1m"},
        security_data_names={"instruments", "1m"},
        valuation_data_names={"1m"},
    )
)

data.add_data(
    "instruments",
    FutureStateReader(schema={...}).read(
        futures_df,
        exchange=Exchange.CFFEX,
    ),
)
data.add_data(
    "1m",
    TradeBarReader().read(
        bars_df,
        interval=Interval.K_1M,
        exchange=Exchange.CFFEX,
    ),
)

engine.run(data.stream())
```

`DataManager` 将各数据源按 `time` 合并。相同时间的状态和行情进入同一个
`TimeSlice`，其中标的状态会排在行情更新之前。`TimeSliceDriver` 又会在
策略和模拟撮合之前同步发布全部 `security_updates`，因此回测中
`put()` 返回时更新已经完成：

```text
instrument state -> SecurityManager
market state     -> SecurityManager
market.before
strategy slice
market.after
valuation
```

例如退市状态进入时间片后，同一时刻的策略执行前就有：

```python
security = engine.security_manager["IF2608"]
assert security.is_active is False
assert security.is_tradable is False
```

输入到 `DataManager` 的各条流必须已经按 `(time, instrument_id)` 升序
排列，并且 `DataManager` 是单次消费对象。

#### 5.2.5 实盘初始化和更新

实盘应尽量走相同的事件边界。Gateway 或合约数据适配器把券商合约定义转换为
`FutureStateData`，再通过 `EVENT_DATA` 或 `LiveDataManager` 对应的
`TimeSlice.security_updates` 发布。不要直接修改
`security_manager.securities`。

启动时需要立即可用、但不应触发策略的合约，应在策略接收第一份行情前发布
`EVENT_DATA`。回测 `DataManager` 会把 `time=None` 解释为专门的 bootstrap
状态；实盘 `LiveDataManager.push()` 则会把缺失时间替换成当前时间，二者
不能混为一谈。测试或装配代码可以直接调用
`security_manager.on_data(state)`，但正式组件之间仍应使用事件协议。

实盘 `EventEngine` 异步处理并保持队列顺序；回测
`BacktestEventEngine` 同步排空。因此实盘调用方如果刚发布状态就立即在
当前回调线程读取 `SecurityManager`，不能假设异步事件已经处理完成；策略
通过后续 `EVENT_SLICE` 读取时，`TimeSliceDriver` 的发布顺序仍保证状态事件
先入队。

#### 5.2.6 数据约束和已知边界

1. **每个 dated 行必须是完整快照。** Reader 不把带 `date` 的行当作增量
   patch。缺失的乘数、保证金率或手续费率会使用字段默认值，而不会继承上一
   行。稀疏历史表应在导入前按 `instrument_id, date` 排序并向前填充。
2. **明确 `delist_date` 的业务含义。** 日期值会标准化为当天 `00:00`，
   `is_active=False` 从该时刻开始生效。如果数据源中的 `delist_date`
   表示“最后交易日”，这会使合约在最后交易日开盘前失效；此时应提供收盘后
   的精确失效时间，或转换为下一交易日的 `00:00`。长期应区分
   `last_trading_date` 和 `delist_effective_time`。
3. **`is_active` 不会从行情推断。** 没有显式字段时 dated 状态默认
   `True`；暂停、恢复等状态必须作为显式状态事件提供。
4. **同一时刻退市状态最终优先。** 如果 `delist_date` 当天已有一条有效
   状态，Normalizer 仍追加失效状态；同一 TimeSlice 中先应用有效状态、
   再应用失效状态，最终对象为失效状态。
5. **不要混淆到期和退市。** `expiry` 是合约到期属性，
   `delist_date`/失效状态决定运行时是否可交易；当前框架不会仅根据
   `expiry` 自动把合约设为失效。

核心回归测试位于：

- `tests/test_instrument_reader.py`：四种输入形态和生命周期展开；
- `tests/test_data_pipeline.py`：bootstrap、属性变更和
  `SecurityManager` 更新；
- `tests/test_routed_runtime.py`：状态先于策略消费的运行时顺序；
- `tests/test_state_ownership.py`：`SecurityManager` 的唯一状态所有权。

### 5.3 OmsBase

`OmsBase` 是已确认交易状态的运行时投影和查询入口。

负责：

- 最新订单及活动订单；
- 去重后的成交；
- 根据成交投影净持仓；
- 接受券商持仓快照进行启动/重连对账；
- 最新账户和报价状态；
- 向策略发布统一 `EVENT_POSITION`。

不负责：

- 行情、合约信息和最新价格；
- 模拟撮合；
- 回测现金、盈亏、保证金的权威计算；
- 向券商发送订单。

回测直接使用 `OmsBase`，不再维护一套 Backtest OMS。回测资金账本由 `BacktestGateway.AccountLedger` 负责，OMS 只消费 Gateway 发布的标准事件。

### 5.4 Gateway

Gateway 是执行系统边界。

实盘 Gateway：

- 连接券商/交易所；
- 把行情回调发布为 `EVENT_LIVE_DATA`；
- 消费 `execution` 目标的下单、撤单、改单命令；
- 把券商确认结果发布为标准订单、成交、账户和持仓快照事件；
- 不直接修改 OMS 或策略状态。

BacktestGateway：

- 对外表现为一个模拟券商；
- 消费与实盘 Gateway 相同的 `execution` 命令；
- 额外消费 `simulated_broker` 的 before/after/valuation 命令；
- 内部组合订单簿、撮合器、账户账本和发布器；
- 对外只发布与实盘相同的标准事件。

### 5.5 Reporting

`BacktestRecorder` 只复制权威状态，不重新计算账户。

`PerformanceAnalyzer` 只在回测结束后对记录的权益序列计算收益、回撤和 Sharpe。

`BacktestReporting` 是二者的门面，负责导出 DataFrame 和统计结果。

日志不属于 `BacktestEngine` 或 Gateway。`LogEngine` 在实盘和回测中都订阅 `EVENT_LOG`，并从共享 `RuntimeContext.current_time` 获取当前运行时刻。

## 6. 回测使用方法

完整入口参考 `example/macd_backtest.py`。

```python
from autotrade.backtest import BacktestEngine, BacktestEventEngine
from autotrade.backtest.data import (
    DataManager,
    DataRoutingConfig,
    FutureStateReader,
    TradeBarReader,
)
from autotrade.coreutils.constant import Exchange, Interval

event_engine = BacktestEventEngine()

strategy = MyStrategy(
    event_engine=event_engine,
    instrument_id="HK.MHImain",
)
strategy.initialize()

engine = BacktestEngine(
    event_engine=event_engine,
    initial_cash=50_000,
)

data = DataManager(
    DataRoutingConfig(
        strategy_data_names={"1h"},
        security_data_names={"instruments", "1h"},
        valuation_data_names={"1h"},
    )
)

data.add_data(
    "instruments",
    FutureStateReader().read(
        instrument_dataframe,
        exchange=Exchange.HKFE,
    ),
)
data.add_data(
    "1h",
    TradeBarReader().read(
        bar_dataframe,
        interval=Interval.K_1H,
        exchange=Exchange.HKFE,
    ),
)

result = engine.run(data.stream())
trades = engine.get_trade_log_df()
accounts = engine.get_account_daily_df()
```

注意：

- 所有输入流必须按 `(time, instrument_id)` 升序排列。
- `DataManager` 是单次消费对象，调用一次 `stream()` 后不可复用。
- 每个 `data_name` 必须至少路由到 strategy/security/valuation 之一。
- 标的状态数据应加入 `security_data_names`。
- `valuation_data_names` 决定记录频率，不要无条件把 tick 流加入估值。
- 多周期数据会按相同时间戳合并为一个 TimeSlice。

### 6.1 成交设置

```python
from autotrade.backtest.gateway import BacktestSettings

settings = BacktestSettings(
    cheat_on_close=False,
    market_fill_price="next_open",
    stop_limit_same_bar="conservative",
    execution_data_name="1h",
)
engine = BacktestEngine(settings=settings)
```

- `cheat_on_close=False`：策略当前 bar 下的市价单最早下一根执行。
- `cheat_on_close=True`：允许当前 bar 收盘执行。
- `execution_data_name`：指定哪一路 bar 用于撮合，避免多周期时错误选取。
- 自定义撮合行为应继承 `FillModel`，不要把撮合判断写进策略或 Engine。

## 7. 实盘使用方法

完整入口参考 `example/macd_realenv.py`。

```python
from autotrade.engine.live_engine import LiveEngine
from autotrade.gateway.gateway_futu import FutuGateway

engine = LiveEngine(
    gateway_factory=FutuGateway,
    engine_id="live-main",
)

strategy = MyStrategy(
    event_engine=engine.event_engine,
    security_manager=engine.security_manager,
)
strategy.initialize()
engine.install(strategy)

engine.start(
    {
        "symbols": ["HK.MHImain"],
        "intervals": [...],
    }
)

# 退出时
engine.stop()
```

实盘数据路径：

```text
券商回调
-> BaseGateway.on_tick/on_contract
-> EVENT_LIVE_DATA
-> LiveDataManager
-> TimeSlice
-> TimeSliceDriver
-> EVENT_DATA + EVENT_SLICE
```

如果多个实时源必须在同一时刻交给策略，应由上游聚合器调用 `LiveDataManager.push_batch()`，并明确：

- `named_data`：策略可见数据；
- `security_updates`：更新标的状态的数据；
- `valuation_data_names`：需要形成估值更新的数据源。

实盘 Gateway 必须：

- 接受构造参数 `event_engine`；
- 实现 `connect()`、`close()`、`send_order()`、`cancel_order()` 等接口；
- 调用 `bind_execution()` 注册执行命令；
- 通过 `on_*` 方法发布标准事件；
- 保证 API 回调本身不做耗时策略计算。

## 8. 策略编写约定

策略继承 `StrategyBase`，实盘与回测使用同一份策略代码。

策略主要回调：

- `on_data(slice_)`：标准入口，可自行读取多路数据；
- `on_bar(bar)`、`on_tick(tick)`：基类默认从 Slice 分发；
- `on_order(order)`；
- `on_trade(trade)`；
- `on_position(position)`；
- `on_request_status(status)`。

下单必须使用：

```python
self.push_order_request(order_request)
self.push_cancel_request(cancel_request)
self.push_modify_request(modify_request)
```

不要直接调用 Gateway，也不要直接修改 OMS、SecurityManager 或 AccountLedger。

策略中的持仓变量若只是策略内部决策状态，可以保留；系统权威持仓应从 `EVENT_POSITION` 或 `engine.oms` 获取。

### 8.1 期权策略与分析面板

期权合约的基础信息和最新行情与其他资产使用相同路径：

```text
OptionStateData / TradeBar / QuoteBar / Tick
-> TimeSlice.security_updates
-> SecurityManager
-> OptionContract
```

`OptionContract` 不保存 IV 或 Greeks。模型输出使用带版本信息的
`OptionAnalyticsData`，只作为策略数据进入
`Slice.option_analytics[data_name][instrument_id]`，不进入
`security_updates` 或 `valuation_updates`。

期权策略可继承 `OptionStrategy`：

```python
from autotrade.strategy import OptionPanelView, OptionStrategy


class MyOptionStrategy(OptionStrategy):
    def on_option_panel(
        self,
        panel: OptionPanelView,
        slice_,
    ) -> None:
        frame = panel.to_frame()
        # 按 underlying、expiry 或其他条件由策略自行分组和分析
```

初始化时指定分析数据源：

```python
strategy = MyOptionStrategy(
    event_engine=event_engine,
    security_manager=security_manager,
    option_analytics_data_name="mo_black76_v1",
)
```

`OptionStrategy.on_data()` 保留 `StrategyBase` 原有的 tick/bar 分发。只有当前
Slice 包含配置的数据源时，它才调用同一模块中的
`OptionPanelAssembler`。Assembler 以 Analytics 的 `instrument_id` 查询
`SecurityManager` 并生成策略私有的 `OptionPanelView`，不会扫描全部
Security，也不会按 underlying 或到期日分组。

`OptionPanelView.contracts` 是
`instrument_id -> OptionContractView` 映射，其中每个 View 同时提供：

- `view.security`：合约基础信息和 SecurityManager 当前行情；
- `view.analytics`：当前 Slice 的 IV、Greeks、模型及版本信息。

Panel 可以包含多个 underlying。对象视图只适用于当前策略回调；需要横截面
分析或保存快照时使用 `panel.to_frame()`。

## 9. 插件和扩展

运行时可安装日志之外的独立插件：

```python
plugin = engine.install(MyPlugin(engine.event_engine))
```

插件应：

- 只订阅自己需要的 Event/Message；
- 提供 `stop()` 或 `unregister()`；
- 不直接持有并修改其他组件内部字典；
- 通过事件发布输出；
- 卸载后不留下注册处理器或后台线程。

适合插件化的模块包括 App 通信、rollover、风控、监控、告警和数据落库。

扩展建议：

- 新券商：实现新的 live Gateway。
- 新成交规则：实现新的 `FillModel`。
- 新手续费/保证金规则：替换 `CommissionModel`/`MarginModel`。
- 新数据类型：扩展 `MarketData`、Reader 和 `Slice` 索引。
- 新绩效指标：扩展 `PerformanceAnalyzer`，不要侵入 Gateway。

## 10. 实盘与回测差异表

| 方面 | 实盘 | 回测 |
| --- | --- | --- |
| 装配入口 | `LiveEngine` | `BacktestEngine` |
| 事件引擎 | `EventEngine`，异步线程 | `BacktestEventEngine`，同步排空 |
| 时间来源 | 真实市场/API 回调 | 历史 TimeSlice 时间 |
| 数据生产 | `LiveDataManager` | `backtest.data.DataManager` |
| 执行边界 | 真实 Gateway | `BacktestGateway` |
| 成交 | 券商/交易所确认 | `MatchingEngine + FillModel` |
| 账户权威 | 券商 | `AccountLedger` |
| 历史记录 | 外部插件按需实现 | `BacktestRecorder` |
| 策略、OMS、Security | 共用 | 共用 |
| OrderRouter、Driver、日志 | 共用 | 共用 |

切换实盘/回测时，策略不应改动；只更换运行时装配和数据输入。

## 11. 维护约束

后续重构应守住以下不变量：

1. `RuntimeComponents` 的实盘和回测结构保持一致。
2. 所有组件必须使用同一个 `event_engine`。
3. Gateway 和 Runtime 必须使用同一个 `SecurityManager`。
4. `SecurityManager` 是标的/行情状态唯一权威。
5. `OmsBase` 是订单/成交/统一持仓/账户投影的查询入口。
6. 策略只接收 `EVENT_SLICE`，不直接消费原始 `EVENT_DATA`。
7. 回测 `put()` 返回时，当前事件及其派生事件必须全部处理完成。
8. 只有存在 `valuation_updates` 才盯市和记录。
9. Gateway 发布事实事件，不能直接回调策略或修改 OMS。
10. 新模块优先通过事件协议接入，不向 `LiveEngine`/`BacktestEngine` 堆积业务方法。

核心回归测试：

```bash
pytest -q tests
```

重点测试文件：

- `tests/test_routed_runtime.py`：组件结构、命令路由和 TimeSlice 顺序；
- `tests/test_state_ownership.py`：Security/OMS/Gateway 状态权威；
- `tests/test_data_pipeline.py`：历史数据同步与路由；
- `tests/test_option_analytics.py`：期权分析数据、策略侧 Panel 组装和旧接口清理；
- `tests/test_numeric_safety.py`：撮合、账本和绩效数值安全；
- `tests/test_instrument_reader.py`：标的状态读取。

## 12. 当前限制

当前版本仍有以下已知限制，使用者不应误判为完整能力：

- 默认 `BarFillModel` 是 bar 级保守撮合，不模拟盘口队列。
- `_apply_fill` 暂不支持部分成交。
- 实盘多数据源同步依赖上游正确调用 `push_batch()`。
- `performance_plot()` 尚未记录完整市场数据，因此会明确报错。
- `OmsBase` 当前采用单一净持仓投影，复杂的今昨仓、组合持仓需另行扩展。
- `EVENT_TICK`、`EVENT_BAR`、`EVENT_CONTRACT` 仅保留兼容常量；新代码统一走 `EVENT_LIVE_DATA`、`EVENT_DATA` 和 `EVENT_SLICE`。

遇到不确定行为时，以测试和当前代码为最终依据，并同步修正文档。
