# Option 数据解耦重构计划

> 基线版本：Autotrade v0.3.0
>
> 计划日期：2026-07-26
>
> 目标版本：v0.4.0
>
> 状态：设计确认，尚未开始实现

本文记录 v0.3.0 之后的期权数据模型重构边界。实施过程中如设计发生变化，
应同步更新本文、`FRAMEWORK_GUIDE.md` 和相关测试。

## 1. 已确认的状态边界

`TimeSlice` 继续维持现有三分法，不增加期权专用区域：

- `security_updates`：更新 `SecurityManager` 中的合约定义和最新市场状态；
- `slice`：向策略提供当前时刻可见的数据；
- `valuation_updates`：触发账户盯市、保证金刷新和历史记录。

期权与股票、期货使用相同的 Security 更新链路：

```text
OptionStateData / TradeBar / QuoteBar / Tick
-> TimeSlice.security_updates
-> EVENT_DATA
-> SecurityManager
-> OptionContract
```

`SecurityManager` 只负责回答“合约是什么”和“市场当前状态是什么”。模型相关
的 IV、Greeks 和波动率曲面结果不属于它的权威状态。

## 2. 计划中的对象调整

### 2.1 精简 OptionContract

保留：

- `underlying_instrument_id`；
- `expiry`、`strike`、`right`、`style`；
- 从 `Security` 继承的交易属性、生命周期和最新行情。

计划移除：

- `iv`；
- `delta`、`gamma`、`vega`、`theta`。

这些字段是定价模型输出，可能因模型、曲面、输入和版本不同而同时存在多个
结果，不应直接写入唯一的 `OptionContract` 状态。

### 2.2 新增 OptionAnalyticsData

新增逐时间、逐合约、带模型版本的策略数据类型，计划包括：

- 市场 IV、曲面 IV；
- `delta`、`gamma`、`vega`、`theta`、`rho`；
- `vanna`、`vomma`、`charm` 等高阶指标；
- 计算时使用的标的价格、远期价格、利率和剩余期限；
- `model_id`、`model_version` 及必要的输入版本信息。

`OptionAnalyticsData` 只路由到 `strategy_data_names`，进入
`Slice`，不进入 `security_updates` 或 `valuation_updates`。

### 2.3 移除核心 OptionChain

当前 `coreutils.object.OptionChain` 没有独立 Reader 或运行时生产者，并且
持有可变的 `OptionContract` 对象。计划从核心市场数据模型中移除：

- `OptionChain`；
- `Slice.option_chains`；
- `Slice._index()` 中对应的索引分支。

期权链改为期权策略侧的临时组合视图，不作为原始 `MarketData`，也不写回
共享 `Slice`。

## 3. 策略侧组合

新增可复用但不侵入通用运行时的 `OptionChainAssembler`：

```text
SecurityManager
├── 期权基础信息
├── 最新期权行情
└── 最新标的行情

Slice
└── OptionAnalyticsData

以上两者
-> OptionChainAssembler
-> 策略私有 OptionChainView
```

Assembler 只读 `SecurityManager` 和当前 `Slice`，不修改共享状态。只有
需要期权分析数据的策略才安装和调用它；撮合、OMS、账户和通用 Engine 不依赖
OptionAnalyticsData。

## 4. 数据导入目标

历史期权宽表应拆成三个逻辑数据源：

1. **合约信息**
   - 由 `OptionStateReader` 转换为 `OptionStateData`；
   - 只路由到 `security_data_names`；
   - 静态定义在 bootstrap 时加载，生命周期变化按生效时间更新。
2. **期权和标的行情**
   - 由 `TradeBarReader`、`TickReader` 或 `QuoteBarReader` 转换；
   - 路由到 `security_data_names` 以更新最新状态；
   - 同时按策略和估值需求路由到 `strategy_data_names`、
     `valuation_data_names`。
3. **期权分析指标**
   - 由新的 `OptionAnalyticsReader` 转换为 `OptionAnalyticsData`；
   - 只路由到 `strategy_data_names`；
   - 按模型和版本独立保存、增量重建。

现有大宽表可以作为兼容输入或研究导出，但不再作为运行时唯一权威数据。

## 5. 计划实施顺序

1. 新增 `OptionAnalyticsData`、Reader 和 `Slice.option_analytics` 索引；
2. 增加 Reader、DataManager 路由和同时间数据对齐测试；
3. 增加策略侧 `OptionChainAssembler` 与 `OptionChainView`；
4. 使用短窗口 MO 数据验证合约、行情、分析指标三路导入；
5. 将已有期权策略迁移到新接口；
6. 标记旧 `OptionContract` Greeks 和核心 `OptionChain` 为 deprecated；
7. 确认无调用方后删除旧字段和索引；
8. 更新 `FRAMEWORK_GUIDE.md`，运行完整 `pytest -q tests`。

## 6. 必须守住的不变量

- 不改变 `TimeSlice` 的三类数据职责；
- `SecurityManager` 仍是合约定义和最新市场状态的唯一权威；
- 分析指标不能影响撮合、OMS 或账户权威状态；
- 策略处理 Slice 时，当前时刻的 Security 更新必须已经完成；
- 分析结果必须能够通过模型版本复现；
- 新增或重算某个指标时，不应重写合约定义和原始行情。
