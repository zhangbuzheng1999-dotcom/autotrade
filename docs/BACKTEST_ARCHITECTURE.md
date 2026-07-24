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

Reader 不决定消费者，SecurityManager 和 BacktestOms 也不扫描 Slice。

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

### OmsBase 与 BacktestOms

`OmsBase` 是实盘和回测共用的订单、成交、持仓、账户和报价状态中心。它消费
`EVENT_ORDER`、`EVENT_TRADE`、`EVENT_ACCOUNT`、`EVENT_QUOTE` 以及券商持仓快照；
成交更新持仓后由 OMS 发布 `EVENT_POSITION`。

`BacktestOms` 继承 `OmsBase`，只覆盖成交后的模拟账务 hook，并在 TimeSlice 末尾
消费 `valuation_updates` 完成盯市。合约乘数、保证金率和手续费率始终直接读取共享
的 SecurityManager，不保留第二份合约参数。

### Recorder 与 Analyzer

`BacktestRecorder` 是 `BacktestOms` 的内部协作者。当且仅当 TimeSlice 包含
`valuation_updates` 时，OMS 才完成盯市、发布账户并立即记录快照；Tick 或其他非估值
数据不会产生账户历史。Recorder 只读复制 OMS 和 SecurityManager 的权威状态，不参与
资金或持仓计算。`PerformanceAnalyzer` 对 Recorder 的历史快照做纯统计，不持有运行时
交易状态。

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
-> BacktestOms valuation + recorder snapshot
```

Engine 不读取 DataFrame、不持有 Reader 或数据源配置，也不决定数据频率。

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
