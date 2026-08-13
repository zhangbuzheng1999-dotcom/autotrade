# Autotrade 改动日志

本文记录 Autotrade 的架构和公开接口变化。版本号遵循语义化版本；带破坏性
接口调整的版本会明确列出迁移方式。

## [Unreleased]

### Option Greek risk / attribution architecture (implementation commit: `ca7d9aa`)

- `GreekRiskManager` 重构为 raw、contract、position、contract cash、position
  cash 五个显式层级；以 `delta_cash_1pct`、`gamma_cash_1pct`、
  `vega_cash_1vol`、`theta_cash_1d` 等字段表达标准化现金风险。
- 期权风险因子由 `option_factor_price="forward"|"underlying"` 明确选择；
  期权 forward/underlying 缺失时不再退回权利金，归因区间会安全标记为无效。
- `OptionBacktestAnalyzer` 的逐资产归因增加 `factor_id`，因子变化或输入缺失的
  实际 PnL 保留，但不再混入 approximate/residual Greek PnL 汇总。
- `OptionBacktestReporting` 输出 factor-aware 的现金 Greek、逐合约 Greek PnL、
  组合 Greek PnL 与风险收益分析；Excel 导出禁用 MultiIndex 单元格合并，保证
  可无损读回。
- 新增 `tests/integration/option_attribution_validation.py` 全期权滚动持有验证，
  并扩充 `frameworkguide.md`：记录四层单位、Black-97 因子、回测接线、有效
  区间误差指标与使用方式。

### 回测报告与期权归因报告（基线：0.10.0 / 9ad6c06）

- `PerformanceAnalyzer` 与 `BacktestEngine` 的默认交易期统一为 252；CAGR、
  Sharpe 与无风险收益日化使用同一交易期口径。
- `BacktestReporting` 新增 `get_position_daily_df()` 和
  `export_xlsx(path)`：基础回测报告可导出 `performance`、`account_daily`、
  `trade_log`、`position_daily` 四张 Excel 工作表。
- 新增 `OptionBacktestReporting`，在基础报告上追加
  `position_greeks`、`instrument_greek_pnl`、`portfolio_greeks`、
  `portfolio_greek_pnl` 与 `greek_pnl_analysis` 工作表。
- `OptionBacktestAnalyzer` 改为按资产保存不等距风险快照与相邻区间归因，新增
  `InstrumentGreekRiskSnapshot` / `InstrumentPnlAttribution`；支持显式资产
  快照和成交事件订阅。平仓后即使 OMS 已删除持仓，仍会记录零仓位终点快照。
- 期权报告新增逐资产与组合 Greek/PnL DataFrame、Delta 名义风险和 1% 冲击
  PnL，以及 Greek 暴露与收益的统计分析。
- 已使用桌面 MO 真实期权价格、合约信息和 Black97 Greeks 抽样验证归因公式。

### Option 风险状态与模块收敛

- 新增 `autotrade.option` 领域包，统一容纳期权 analytics、策略面板、
  `GreekRiskManager` 和回测 PnL 分析器；删除旧
  `autotrade.analytics.options` 与 `autotrade.strategy.option_strategy` 路径。
- 新增 `GreekRiskState` / `GreekRiskManager`：不修改通用 `Security`，按
  `instrument_id` 维护最新风险记录，并在读取时组合 `SecurityManager` 的最新
  价格与 multiplier；支持单资产及 OMS 组合 Greek 暴露聚合。
- 支持期权从 `Slice.option_analytics["option_analytics"]` 接收
  `OptionAnalyticsData`，线性资产从
  `Slice.custom_data["non_option_greek_risk"]` 接收同字段协议的
  `CustomData.payload`。
- 新增 `OptionBacktestAnalyzer`：冻结 Greek/价格/持仓快照，并以期初持仓与
  Greeks 做 delta、gamma、vega、theta、rho、vanna、vomma、charm 区间归因；
  非期权缺失 delta 时仅在归因中按线性 delta=1 回退，期权缺 delta 则标记归因
  无效。
- 新增 `frameworkguide.md`，记录运行时接线、输入协议、归因公式、缺失值语义和
  实盘/回测职责边界；新增风险管理场景测试。

## [0.10.0] - 2026-08-02

### Forward 统一为利率平价

- Calculated Greeks 不再判断期权标的是期货、ETF还是指数；
- 删除通过合约代码是否包含 `.` 判断资产类型的逻辑；
- 删除 Greeks 在线计算对 `FuturePriceService` 和标的价格 Service 的依赖；
- 所有期权统一使用完整 Call/Put 截面，通过利率平价反推 Forward；
- 保留成交量加权聚合和基于有效平价锚点的期限插值；
- 禁用期货收盘价和 Spot Carry 兜底；没有任何有效平价锚点时 Forward/Greeks
  保持 NULL。

### 模型版本

- Calculated Greeks 默认 `model_version` 更新为 `parity_v1`；
- IVX 已经使用利率平价 Forward，继续使用 `cfutures_v1`。

## [0.9.0] - 2026-07-31

### cfutures 算法完全兼容

- 将 cfutures 的 `opt_forward_curve.py` 和 `cal_opt_greek.py` 原样复制到
  `autotrade.analytics.options`；
- 将实际生成历史 `ivx_data` 的 `cal_ivx.py` 复制到同一目录，仅修改包内
  相对导入；
- 删除旧 `greeks.py`、`ivx.py` 和中间 `cfutures/` 子目录；
- DataSource 直接调用三个 cfutures 模块。

### Forward

- 期货期权继续使用实际对应期货合约收盘价；
- ETF/指数期权恢复 cfutures 的 Call/Put 平价 Forward；
- 使用 Call/Put 成交量均值作为权重并进行 `weighted_mean` 聚合；
- 恢复 Spot Carry 兜底、期限 log-linear 插值和平端外推；
- 非期货标的价格也严格使用 `SOURCE_ONLY, persist=False` 获取。

### IVX

- 使用 cfutures 的等权 Forward 候选均值；
- 恢复缺失期限插值和平端外推；
- 为保持完全一致，固定 `target_days=30`、`min_days=7`；
- cfutures 原函数不返回期限诊断，因此 `near_t_days`、
  `next_t_days`、`near_variance` 和 `next_variance` 保留为 NULL。

### 版本与验证

- Greeks 和 IVX 默认 `model_version` 升级为 `cfutures_v1`；
- ClickHouse 中旧 `autotrade_v1` 计算结果已删除；
- 2026-07-10 的 AU、HO、IO、MO 真实 SOURCE_ONLY 验证：
  - 合约集合和 Forward 完全一致；
  - 全部 Greeks 和 NULL 位置一致；
  - 四个 IVX 与历史 PKL 差值均为 0。

## [0.8.0] - 2026-07-31

### 新增

- `autotrade.analytics.options.calculate_ivx()` 纯计算入口；
- `CalculatedOptionIVXService`、DataSource、Repository 和 Spec；
- ClickHouse 表 `rq_option_data.calculated_option_ivx_1d`；
- 近月/次近月期限、方差及输入期权数量等诊断字段；
- `tests/test_calculated_option_ivx.py`。

### 数据访问约定

- `DB_ONLY` 按 `opt_symbol` 和日期直接读取 ClickHouse；
- `SOURCE_ONLY` 按 `opt_symbol` 获取完整期权截面并现场计算；
- `opt_symbol` 同时匹配 `underlying_symbol` 原值及去除交易所后缀后的值，
  再按上市日和到期日筛选查询区间内的有效合约；
- 内部合约信息和期权价格均使用 `SOURCE_ONLY, persist=False`；
- IVX 是品种级指标，不支持按单个 `order_book_id` 现场计算。

### 计算与验证

- Call/Put 平价构造各到期月份 Forward；
- OTM 期权积分计算模型无关方差，默认插值到 30 天；
- AU `2026-07-10` 使用 732 行完整期权截面得到
  `ivx=27.218222`；
- 已完成 `SOURCE_ONLY -> ClickHouse -> DB_ONLY` 真实闭环。

## [0.7.0] - 2026-07-30

### 架构主题

新增与 RiceQuant Service 调用方式一致的计算型期权 Greeks 资源。纯
Black97 计算与数据获取、Forward 构造、ClickHouse 持久化解耦，避免继续
依赖巨型期权宽表作为计算和存储的共同接口。

### 新增

- `autotrade.analytics.options.calculate_black97_greeks()`：
  - 只接受标准化的最小 Black97 输入；
  - 输出 IV、Delta、Gamma、Vega、Theta、Rho、Vanna、Vomma 和 Charm；
  - 不访问数据库或 RiceQuant。
- `CalculatedOptionGreeksService`：
  - `DB_ONLY` 直接查询 ClickHouse；
  - `SOURCE_ONLY` 可以按 `opt_symbol` 或 `order_book_ids` 请求；
  - 按合约请求时内部计算并持久化完整品种截面，最后裁剪返回结果。
- `CalculatedOptionGreeksSpec`、DataSource 和 Repository。
- ClickHouse 表
  `rq_option_data.calculated_option_greeks_1d`。
- `tests/test_calculated_option_greeks.py`，验证 SOURCE_ONLY 模式传播和
  “完整截面先持久化、请求范围后裁剪”。
- `src/autotrade/data/CALCULATED_OPTION_ANALYTICS.md`，记录架构、API、
  字段和验证结果。

### SOURCE_ONLY 规则

外层 `CalculatedOptionGreeksService` 使用 `SOURCE_ONLY` 时：

- 合约信息使用 `OptionInstrumentService(SOURCE_ONLY)`；
- 期权行情使用 `OptionPriceService(SOURCE_ONLY)`；
- 期货期权所需期货价格使用 `FuturePriceService(SOURCE_ONLY)`；
- 内部基础数据调用使用 `persist=False`；
- 外层 `persist=True` 时只持久化最终完整 Greeks 截面。

### Forward 与模型

- 期货期权使用实际对应期货合约收盘价；
- ETF、指数期权使用 Call/Put 平价候选 Forward 的中位数；
- 默认 `model_id="black97"`；
- 默认 `model_version="autotrade_v1"`；
- 新增运行依赖 `py-vollib==1.0.1`。

### 验证

- AU `2026-07-10` 真实 SOURCE_ONLY 计算 732 行，并完成
  `SOURCE_ONLY -> ClickHouse -> DB_ONLY` 闭环；
- 510050 `2026-07-10` 真实 SOURCE_ONLY 计算 96 行；
- 请求单个 AU 合约时，完整 732 行截面先落库，最终只返回请求合约；
- 新增文件通过 Python 编译和模式传播断言测试。

### IVX

v0.7.0 未包含 IVX Service。现有算法仍位于 cfutures
`opt_tools/cal_ivx.py`；计划复用本版本的完整品种截面、模式传播和持久化
约定，新增 `CalculatedOptionIVXService` 和
`calculated_option_ivx_1d`。

### 已知限制

- 当前只支持 `frequency="1d"` 和 `price_type="close"`；
- `DB_THEN_SOURCE` 尚未增加完整截面 coverage 判断；
- 计算表尚未保存 `underlying_price`；
- 全项目编译仍会被既有 `gateway_futu.py` 的 f-string 语法错误阻挡。

## [0.6.0] - 2026-07-29

### 架构主题

为回测数据增加可选的内存物化与持久化能力，同时保留默认的惰性流式执行。
`BacktestEngine.run()` 继续只依赖 `Iterable[TimeSlice]`，策略、Engine、
Reader 和路由协议没有破坏性变化。

### 新增

- `DataManager.materialize()`：
  - 完整消费内部惰性流；
  - 保留 `tuple[TimeSlice, ...]`；
  - 成功后释放 `_sources` 中的 Reader iterable；
  - 物化后的 `stream()` 可重复遍历。
- `DataManager.save()` 和 `DataManager.load()`：
  - 只允许保存已物化且不再持有 source 的 DataManager；
  - 加载后可以直接将 `stream()` 交给 `BacktestEngine.run()`；
  - 不保存原始 DataFrame 或 Reader generator。
- `DataManager.is_materialized` 和 `time_slice_count` 只读状态。
- Dynamic Collar 工作区新增物化缓存生成和运行示例：
  - `data_gerator.py`；
  - `run_mo_dynamic_collar_materialized.py`。

### 修改

- 包版本由 `0.5.0` 更新为 `0.6.0`。
- 原 `DataManager.stream()` 主体拆为 `_stream_once()`；公开 `stream()` 根据
  状态返回一次性惰性流或物化 tuple 的新 iterator。
- `_sources.clear()` 移入 `finally`，正常完成、异常和 generator 关闭时均
  尽力释放 Reader source。
- `FRAMEWORK_GUIDE.md` 将架构版本更新为 v0.6.0，详细记录两种模式的数据
  生命周期、保存/加载方式、Dynamic Collar 示例和性能取舍。

### 兼容性

- 现有 `engine.run(data.stream())` 调用无需修改，默认仍是低内存惰性模式。
- 惰性 `stream()` 仍为单次消费。
- `materialize()` 必须在惰性消费开始前调用。
- 物化后的 `stream()` 可重复调用，但每次回测仍应创建独立的运行时状态。
- pickle 缓存不是承诺长期兼容的跨版本格式；数据对象或路由协议变化后应重建。

### Dynamic Collar 验证

- 完整数据生成 961 个 TimeSlice，缓存文件 132,267,011 字节
  （126.14 MiB）。
- 默认惰性模式：26.84 秒，峰值 RSS 289.28 MiB。
- 物化缓存模式：22.61 秒，峰值 RSS 951.68 MiB。
- 首次缓存生成：12.32 秒，峰值 RSS 约 1.04 GiB。
- 两种模式绩效结果、决策日志和 639 条成交一致；仅随机订单 ID 不同。
- 42 项自动化回归测试通过。

### Git 信息

- 开发分支：`rollback-3648d01`。
- 基线提交：`5fa55c562330a4aefd4866ee9fb2e4eabf7d3306`
  （`docs: release option panel API as v0.5.0`）。
- 发布提交：`8c896f6fa5693613cce95eb5388e3cf79578917a`
  （`feat(backtest): add materialized data manager v0.6.0`）。
- Git 元数据补录使用独立文档提交，避免发布提交对自身哈希产生循环引用。

## [0.5.0] - 2026-07-28

### 架构主题

精简期权策略视图：`OptionPanelAssembler` 只组合 Security 与 Analytics，
Panel 保持对象结构；是否转换为 DataFrame 由具体策略按需决定。

### 修改

- 包版本由 `0.4.0` 更新为 `0.5.0`。
- 删除 `OptionPanelView.time`。当前策略时间统一从 `slice_.time` 获取。
- `OptionPanelAssembler` 删除 Analytics 时间收集和同时间校验，只负责：
  - 校验 Analytics 映射键与对象的 `instrument_id`；
  - 从 SecurityManager 查询相应 `OptionContract`；
  - 创建 `OptionContractView`；
  - 返回只包含 `contracts` 的 `OptionPanelView`。
- `OptionPanelView.to_frame()` 改为动态发现 Security 和 Analytics 的
  dataclass 字段及只读属性。新增行情字段或 Greek 指标无需维护固定列清单；
  字段重名时保留 Security 的当前状态。
- 字段结构按对象类型缓存，避免对面板中的每张期权重复扫描类定义。
- 明确 `to_frame()` 是策略主动调用的可选便利方法：
  - 对象逻辑可以直接访问 `panel.contracts`；
  - 需要分组、筛选、排序或向量化计算时再生成 DataFrame；
  - Assembler 和 `OptionStrategy` 不会自动创建 DataFrame。
- Dynamic Collar 每个 Slice 只调用一次 `panel.to_frame()`，并把生成的
  DataFrame 传给候选打分函数，避免重复展开同一 Panel。

### 兼容性与迁移

这是公开接口调整：

```text
旧：panel.time
新：slice_.time

对象访问：panel.contracts[instrument_id]
按需表格：frame = panel.to_frame()
```

### 验证

- 40 项自动化测试通过；
- 新增扩展 Analytics 测试，确认自定义 Greek 字段无需修改 `to_frame()`
  即可自动成为 DataFrame 列；
- Dynamic Collar 全量回测结果保持一致，并通过避免重复转换缩短运行时间。

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
