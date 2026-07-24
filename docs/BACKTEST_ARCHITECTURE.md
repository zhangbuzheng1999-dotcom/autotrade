# autotrade TimeSlice 回测架构

## 责任边界

### Reader

Reader 只把外部表格解释成框架标准对象：

- `TradeBarReader`、`TickReader`、`CustomDataReader`
- `EquityStateReader`、`FutureStateReader`、`OptionStateReader`

Instrument Reader 输出完整状态快照，不创建运行时 Security。

### Instrument Reader 内部标准化

这是 Instrument Reader 的私有步骤，不属于公共 API。它保证信息表存在
`date`、`symbol`、`is_active`：

- 无日期且无生命周期：保留 `date=NaT`，作为 bootstrap。
- 有 `list_date`：生成生效事件。
- 有 `delist_date`：复制当时最新完整状态并生成失效事件。
- 已有日期的属性变化：作为定时完整状态保留。

### DataManager

DataManager 是数据层唯一门面：

1. 通过 `add_data(data_name, records)` 接收已经标准化的数据。
2. 隔离 bootstrap 状态。
3. 为定时数据建立有序 DataStream。
4. 合并所有时间流。
5. 根据 `DataRoutingConfig` 路由数据。
6. 输出完整 TimeSlice 流。

`data_name` 属于 DataManager 中的数据源身份，不属于 Bar、Tick 或 InstrumentState
本身。内部使用 `(data_name, record)` 命名信封传递，不修改标准数据对象。

### TimeSliceRouter

Router 是唯一的数据用途决策点：

```text
strategy_data_names  -> TimeSlice.slice
security_data_names  -> TimeSlice.security_updates
valuation_data_names -> TimeSlice.valuation_updates
```

Reader 不决定消费者，SecurityManager 和模拟券商账本也不扫描 Slice。

### SecurityManager

SecurityManager 维护每个标的的唯一最新运行时对象：

- `Security`
- `EquitySecurity`
- `FutureContract`
- `OptionContract`

这些对象与 Bar、Tick、InstrumentState 一样定义在 `coreutils/object.py`；
`autotrade/engine/security_manager.py` 只保留创建、升级和更新对象的
`SecurityManager`。期货和期权合约本身就是对应资产的运行时 Security；
Chain 直接引用这些 Contract，不再维护第二套轻量合约对象。

InstrumentStateData 更新合约属性和生命周期；Bar、Tick、Quote 更新高频市场状态。
其他模块只读取 Security，不维护合约参数副本。

### OmsBase 与 BacktestGateway

`OmsBase` 是实盘和回测完全共用的订单、成交、持仓、账户和报价状态中心。它消费
`EVENT_ORDER`、`EVENT_TRADE`、`EVENT_ACCOUNT`、`EVENT_QUOTE` 以及券商持仓快照；
成交更新持仓后由 OMS 发布 `EVENT_POSITION`。

`BacktestGateway` 是模拟券商门面，内部组合 OrderBook、MatchingEngine、
AccountLedger、CommissionModel、MarginModel 和 EventPublisher。它负责模拟订单、
成交、持仓、手续费、保证金和账户估值，并发布与实盘 Gateway 相同的订单、成交、
持仓快照和账户事件。合约参数始终直接读取共享 SecurityManager。

### Recorder 与 Analyzer

`BacktestRecorder` 是模拟券商的内部协作者。当且仅当 TimeSlice 包含
`valuation_updates` 时，AccountLedger 才完成盯市，Gateway 随后发布账户并立即记录
快照；Tick 或其他非估值数据不会产生账户历史。Recorder 不参与资金或持仓计算。
`PerformanceAnalyzer` 对 Recorder 的历史快照做纯统计，不持有运行时交易状态。

### Engine

Engine 只接受：

```python
engine.run(iterable_of_time_slices)
```

bootstrap TimeSlice 只交给 SecurityManager。普通 TimeSlice 的固定顺序是：

```text
SecurityManager
-> Gateway before data
-> Strategy
-> Gateway after data
-> BacktestGateway valuation + recorder snapshot
```

Engine 不读取 DataFrame、不持有 Reader 或数据源配置，也不决定数据频率。

TimeSlice 的阶段顺序由共享 `TimeSliceDriver` 明确推进。回测使用同步
`BacktestEventEngine`，所以 security update、market before、strategy Slice、
market after、valuation 中每个阶段及其派生事件处理完成后才进入下一阶段。
BacktestGateway 通过发送给 `simulated_broker` 的 Command 接收撮合和估值阶段，
BacktestEngine 不再直接调用其业务方法。

## 实盘与回测共享协议

策略、移仓和外部模块统一向 `order_router` 发送 `order.submit`、`order.cancel`、
`order.modify` Command；OrderRouter 完成开关和 mute 检查后转发给逻辑目标
`execution`。实盘 Gateway 与 BacktestGateway 绑定同一执行能力，订单、成交、
持仓和账户结果继续通过广播事件进入共享 OmsBase。

实盘 Gateway 将原始回调发布为 `EVENT_LIVE_DATA`，`LiveTimeSliceBuilder` 将 Tick、
Bar 和多数据源批次标准化为 TimeSlice，再交给同一个 TimeSliceDriver。策略只订阅
`EVENT_SLICE`。实盘 EventEngine 异步入队，BacktestEventEngine 同步排空；消息路由、
模块和数据对象保持一致。

## Instrument 生命周期

生命周期不是从行情缺失推断，而是由显式 `list_date`、`delist_date` 生成状态事件。
若输入只有日期而没有具体时间，Reader 将其解释为该日期零点；更精细的交易所规则
应在输入中提供完整有效时间。

退市不会删除 Security。Security 保留最后状态，并设置：

```python
security.is_active = False
security.is_tradable = False
```

这样历史持仓、结算和审计仍可访问该对象。
