# Autotrade 改动日志

本文记录 Autotrade 的架构和公开接口变化。版本号遵循语义化版本；带破坏性
接口调整的版本会明确列出迁移方式。

## [0.4.0] - 2026-07-26

### 架构主题

期权合约状态、市场行情和模型分析结果解耦。SecurityManager 将期权作为普通
Security 管理；IV、Greeks 和波动率曲面只作为带模型版本的策略数据存在。

### 新增

- 新增 `OptionAnalyticsData`：
  - 逐时间、逐期权保存定价输入、IV、Greeks 和高阶指标；
  - 强制提供 `model_id` 和 `model_version`；
  - 校验非有限数值、负 IV 和负剩余期限。
- 新增 `OptionAnalyticsReader`，支持原始列到标准 Analytics 字段的 schema
  映射。
- 新增 `Slice.option_analytics[data_name][instrument_id]` 索引。
- 新增 `strategy/option_strategy.py`：
  - `OptionStrategy`；
  - `OptionPanelAssembler`；
  - `OptionPanelView`；
  - `OptionContractView`。
- 新增 `OptionPanelView.to_frame()`，生成以 `instrument_id` 为索引的独立
  DataFrame 横截面。
- 新增 `tests/test_option_analytics.py`，覆盖 Reader、路由、组装、多
  underlying、策略回调、DataFrame 和旧接口移除。

### 修改

- 包版本由 `0.3.0` 更新为 `0.4.0`。
- `OptionStrategy.on_data()` 先保留 `StrategyBase` 的 tick/bar 分发，再仅
  在配置的 Analytics 数据源出现时组装 Panel 并调用
  `on_option_panel(panel, slice_)`。
- `OptionPanelAssembler` 以 Analytics 为成员集合，按 `instrument_id`
  查询 SecurityManager，不扫描全部 Security，不按 underlying 分组。
- 回测数据 Router 强制 `OptionAnalyticsData` 只进入
  `strategy_data_names`，禁止进入 Security 和 valuation。
- `FRAMEWORK_GUIDE.md` 增加完整期权架构、职责边界、数据导入、策略调用和
  迁移说明。

### 移除

以下为破坏性接口变化：

- 删除 `OptionContract.iv`；
- 删除 `OptionContract.delta`；
- 删除 `OptionContract.gamma`；
- 删除 `OptionContract.vega`；
- 删除 `OptionContract.theta`；
- 删除核心 `OptionChain`；
- 删除 `Slice.option_chains` 及其索引逻辑。

### 迁移

```text
旧：security.delta
新：option_contract_view.analytics.delta

旧：slice.option_chains
新：slice.option_analytics
    -> OptionPanelAssembler.build(security_manager, analytics_data)
    -> OptionPanelView
```

### 验证

- `tests/` 共 35 项通过；
- 使用本地 MO 首个交易日数据完成只读烟雾验证：
  - 150 张期权成功组装为 Panel；
  - DataFrame 为 150 行；
  - 148 行 Delta 有效。

### 后续文档维护

- 完整扩充 `FRAMEWORK_GUIDE.md` 的期权章节，说明：
  - SecurityManager 将期权作为普通 Security 管理，不持有 IV 或 Greeks；
  - 合约信息、行情和 Analytics 的独立数据来源及 TimeSlice 路由；
  - `Slice.option_analytics` 的数据结构和多周期行为；
  - `OptionPanelAssembler` 的职责、严格错误边界和多 underlying 支持；
  - `OptionStrategy` 的标准调用流程；
  - `OptionPanelView` 的对象访问、DataFrame 快照和生命周期；
  - 历史数据存储、模型版本管理和旧接口迁移方式。
- 新建本改动日志，后续公开接口或架构行为变化应与代码在同一提交中记录。

### 后续验证工具

- 新增 `tests/manual_validate_mo_pipeline.py`，对本地 MO 期权宽表执行只读
  集成验证：
  - 将宽表逻辑拆分为合约基础信息、日行情和 Analytics 三部分；
  - 统一混合的 `maturity_date` 日期表示后校验合约静态属性；
  - 分别通过 `OptionStateReader`、`TradeBarReader` 和
    `OptionAnalyticsReader` 建立三条数据流；
  - 使用 `DataManager` 合并 TimeSlice 并更新 `SecurityManager`；
  - 按日调用 `OptionPanelAssembler`，检查 Panel、DataFrame 和当前
    Security 行情时间的一致性；
  - 可用 `--max-dates` 做短窗口验证，用 `--output-dir` 选择性写出三份
    pickle，默认不写文件。
- 完整 MO 验证结果：
  - 原始记录 236,514 行；
  - 953 个交易日、3,052 张历史合约；
  - 成功生成 953 个 Panel，共组装 236,514 条合约分析记录；
  - 单日 Panel 包含 150–382 张合约；
  - `surface_iv` 和 Delta 各有 217,214 条有效值；
  - 数据范围为 2022-07-22 至 2026-06-30。

### 后续接口精简

- 删除 `OptionPanelView.time`。Panel 只保存按 `instrument_id` 合并后的
  `contracts` 并提供 `to_frame()`；当前策略时间统一从 `slice_.time` 获取。
- `OptionPanelAssembler` 删除 Analytics 时间收集和同时间校验，只负责：
  - 校验 Analytics 映射键；
  - 按 `instrument_id` 查询 `OptionContract`；
  - 创建 `OptionContractView`；
  - 返回 `OptionPanelView`。
- Dynamic Collar 每个 Slice 只调用一次 `panel.to_frame()`，并把生成的
  DataFrame 传给候选打分函数，避免重复展开同一 Panel。
- `OptionPanelView.to_frame()` 改为动态发现 Security 和 Analytics 的
  dataclass 字段及只读属性；新增行情字段或 Greek 指标无需维护固定列清单。
  字段重名时保留 Security 的当前状态。

## [0.3.0] - 2026-07-25

### 架构主题

统一实盘和回测运行时主干。

### 主要变化

- 引入共享 `RuntimeEngine`、`RuntimeComponents` 和 `TimeSliceDriver`；
- 实盘和回测共用 SecurityManager、OMS、OrderRouter、策略和日志；
- 使用异步 `EventEngine` 支撑实盘、同步 `BacktestEventEngine` 支撑回测；
- 将回测 Gateway 组合为订单簿、撮合、账户、手续费和保证金组件；
- 明确 Event 广播事实、Message 路由命令；
- 明确 SecurityManager、OMS、Gateway 和账户账本的状态所有权；
- 增加标的信息 Reader、生命周期展开、bootstrap 和多数据源路由。
