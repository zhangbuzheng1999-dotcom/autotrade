# Tradebook 交接文档

## 1. 目标

这个目录现在承载的是一套通用交易账本框架，用来处理：

- 交易记录标准化
- 标的识别与真实补全
- 持仓滚动
- 市值盯市
- 已实现 / 未实现 PnL
- 权益快照
- Mongo 落库

它已经从原先强耦合期权脚本的思路，拆成了多层结构。当前框架可以同时支持：

- 回测结果导入验证
- 实盘交易记录导入
- 日常增量刷新
- 历史全量重算

当前重点验证过的资产和场景：

- `MO + IM888`
- `AU + AU888`
- `db_option.trade_book.csv` 实盘交易表导入


## 2. 当前目录结构

```text
tradebook/
├── enrichment/
├── ledger/
├── market/
├── scripts/
├── service/
├── storage/
├── tests/
└── utils/
```

各层职责如下。

### `ledger/`

纯计算层，不负责数据库，不负责外部行情，不负责流程编排。

核心职责：

- 校验交易与价格表
- 交易滚动成持仓
- 根据价格盯市
- 生成日级 `positions/equity`
- 历史全量重放

核心文件：

- [ledger/schema.py](/home/buzheng/Desktop/tradebook/ledger/schema.py:1)
- [ledger/engine.py](/home/buzheng/Desktop/tradebook/ledger/engine.py:1)


### `market/`

市场数据层，只负责查真实标的信息和价格。

当前实现：

- `RQDataMarketGateway`

核心文件：

- [market/base.py](/home/buzheng/Desktop/tradebook/market/base.py:1)
- [market/in_memory.py](/home/buzheng/Desktop/tradebook/market/in_memory.py:1)
- [market/rqdata.py](/home/buzheng/Desktop/tradebook/market/rqdata.py:1)

当前约束：

- `rqdatac` 必须在 `conda` 环境 `rq_data` 下调用
- 实现里已经封装成 `conda run -n rq_data ...`


### `storage/`

存储层，负责 trade / position / equity / instrument 的读写。

当前实现：

- 内存实现
- Mongo 实现

核心文件：

- [storage/base.py](/home/buzheng/Desktop/tradebook/storage/base.py:1)
- [storage/in_memory.py](/home/buzheng/Desktop/tradebook/storage/in_memory.py:1)
- [storage/mongo.py](/home/buzheng/Desktop/tradebook/storage/mongo.py:1)
- [storage/schema.py](/home/buzheng/Desktop/tradebook/storage/schema.py:1)


### `service/`

业务编排层，负责把 `storage + market + ledger` 串起来。

它不算公式，不直接写 Mongo，也不直接调 `rq.get_price` 细节。

核心职责：

- 刷新某一天
- 全量重算
- 查询结果

核心文件：

- [service/refresh_service.py](/home/buzheng/Desktop/tradebook/service/refresh_service.py:1)
- [service/rebuild_service.py](/home/buzheng/Desktop/tradebook/service/rebuild_service.py:1)
- [service/query_service.py](/home/buzheng/Desktop/tradebook/service/query_service.py:1)


### `enrichment/`

交易补全层。

它负责把“原始交易输入”补成账本需要的标准交易记录。

核心职责：

- 用 `order_book_id` 查真实资产信息
- 识别 `asset_type`
- 补 `multiplier`
- 补 `fee`
- 补 `exchange / underlying_order_book_id / underlying_symbol`

核心文件：

- [enrichment/parser.py](/home/buzheng/Desktop/tradebook/enrichment/parser.py:1)
- [enrichment/contract_rules.py](/home/buzheng/Desktop/tradebook/enrichment/contract_rules.py:1)
- [enrichment/trade_enricher.py](/home/buzheng/Desktop/tradebook/enrichment/trade_enricher.py:1)


### `scripts/`

脚本层，放临时或可复用的工作流脚本。

当前有几类脚本：

- 回测 trade log 转标准 trade 表
- 回放并对比
- RQ 链路验证


## 3. 标准数据模型

### 3.1 Trade 表

账本标准交易字段定义在 [ledger/schema.py](/home/buzheng/Desktop/tradebook/ledger/schema.py:1)。

字段如下：

- `trade_id`
- `account`
- `book_name`
- `trade_date`
- `trade_time`
- `order_book_id`
- `asset_type`
- `side`
- `offset`
- `qty`
- `price`
- `multiplier`
- `fee`
- `currency`
- `remark`

约束：

- `qty` 始终为正数
- `side` 只允许 `buy/sell`
- `offset` 只允许 `open/close/roll`
- `order_book_id` 是统一标的键


### 3.2 Position 表

- `date`
- `account`
- `book_name`
- `order_book_id`
- `asset_type`
- `qty`
- `avg_cost`
- `cost_basis`
- `last_trade_date`
- `last_trade_time`
- `last_price`
- `market_value`
- `unrealized_pnl`


### 3.3 Equity 表

- `date`
- `account`
- `book_name`
- `cash`
- `realized_pnl_cum`
- `unrealized_pnl`
- `fee_cum`
- `market_value`
- `nav`
- `gross_exposure`
- `net_exposure`


## 4. 核心调用方式

### 4.1 纯内存全量重放

适合验证回测 CSV 或临时 DataFrame。

基本链路：

1. 准备标准 `trade_df`
2. 准备 `market gateway`
3. 用 `LedgerRebuildService` 重放

示意：

```python
from storage.in_memory import InMemoryLedgerStorage
from market.rqdata import RQDataMarketGateway
from service.rebuild_service import LedgerRebuildService

storage = InMemoryLedgerStorage(trade_df=trade_df)
market = RQDataMarketGateway()
service = LedgerRebuildService(storage=storage, market=market)
positions, equity = service.rebuild_history(
    account="opt",
    book_name="dynamic_collar",
    persist=False,
)
```


### 4.2 Mongo 读写

现在 `MongoLedgerStorage` 已支持：

- `save_trades(...)`
- `load_trades(...)`
- `save_positions(...)`
- `load_positions(...)`
- `save_equity(...)`
- `load_equity(...)`

示意：

```python
from storage.mongo import MongoLedgerStorage

storage = MongoLedgerStorage()
storage.save_trades(trade_df=trade_df)
storage.save_positions(date="2026-05-26", position_df=position_df)
storage.save_equity(date="2026-05-26", equity_df=equity_df)
```


### 4.3 补全后再重放

如果原始输入缺少 `asset_type/multiplier/fee`，先过 `enrichment`。

```python
from enrichment.trade_enricher import enrich_trade_records
from market.rqdata import RQDataMarketGateway

market = RQDataMarketGateway()
trade_df = enrich_trade_records(raw_trade_df, market=market)
```


## 5. 当前默认行为

### 5.1 `LedgerRebuildService` 默认跑到“当前日期”

这个行为已经改掉了旧逻辑。

旧逻辑：

- 默认只跑到最后一笔交易所在日期

现在：

- 如果不传 `end_date`
- 默认跑到当前日期
- 即便最后一天没有交易，只要还有持仓，也会继续取价并生成权益快照

位置：

- [service/rebuild_service.py](/home/buzheng/Desktop/tradebook/service/rebuild_service.py:1)

测试：

- [tests/test_service_layer.py](/home/buzheng/Desktop/tradebook/tests/test_service_layer.py:1)


### 5.2 期货连续合约口径

对于动态领口这类回测导出结果，已经验证：

- `MO` 这类股指期权对应的期货腿，喂给当前系统时应使用 `IM888`
- `AU` 这类黄金期权对应的期货腿，喂给当前系统时应使用 `AU888`

原因：

- 原始回测账本本质上用的是连续 futures 口径
- 如果映射成具体合约如 `IM2606`
- 会把连续仓位拆碎，导致 PnL 和持仓错位

这个结论已经被 `MO` 和 `AU` 两组数据验证。


## 6. 已验证结论

### 6.1 动态领口回测结果验证

已验证的数据：

- `final_dynamic_collar_MO_*`
- `final_dynamic_collar_AU_*`

验证结论：

- 当 futures 用 `IM888/AU888` 时
- 持仓滚动、已实现 PnL、未实现 PnL、组合 PnL 都能和原回测结果对上

其中：

- `MO` 用 `IM888`
- `AU` 用 `AU888`


### 6.2 `db_option.trade_book.csv`

文件：

- [db_option.trade_book.csv](/home/buzheng/Desktop/tradebook/tests/db_option.trade_book.csv)

当前测试过两条链：

1. 主链路
- 保留原始库表里的 `contract_multiplier` 和 `fee`
- 标准化成标准 trade 表
- 全量重放
- 结果能正确算出 `2026-05-21 ~ 2026-05-26`

2. 自动补全审计链
- 清空 `asset_type/multiplier/fee`
- 用系统真实补全
- 再检查补全结果

结论：

- `asset_type` 补全正确
- `multiplier` 补全正确
- `fee` 补全当前不可靠

也就是说：

- 功能主链路可用
- 手续费补全规则还不能直接信

审计文件：

- [db_option.dynamic_collar.enrichment_audit.json](/home/buzheng/Desktop/tradebook/tests/db_option.dynamic_collar.enrichment_audit.json)


## 7. 当前已知问题

### 7.1 真实 fee 补全不可靠

这是当前最明确的缺口。

在 `db_option.trade_book.csv` 这类实盘导出表里：

- `asset_type` 补对了
- `multiplier` 补对了
- `fee` 和真实库表不一致

典型表现：

- `MO` 被补成 `0.0`
- `AU` 会被补成规则口径放大的值
- 但库表真实 fee 并不是这套

受影响文件：

- [enrichment/contract_rules.py](/home/buzheng/Desktop/tradebook/enrichment/contract_rules.py:1)
- [enrichment/trade_enricher.py](/home/buzheng/Desktop/tradebook/enrichment/trade_enricher.py:1)

结论：

- 目前实盘交易表如果已经有真实 `fee`
- 应优先信原表
- 不要强行覆盖


### 7.2 Mongo 集合里仍有旧 schema 索引残留

现状：

- `db_option.tradebook_*` 集合早期建过旧版索引
- 老索引用的是：
  - `strategy`
  - `instrument_id`

当前代码为了兼容，没有删旧索引，而是把新索引版本化命名：

- `uniq_position_snapshot_v2`
- `acct_book_position_date_v2`
- `uniq_equity_snapshot_v2`
- `uniq_order_book_id_v2`

这能正常工作，但说明库里同时存在历史 schema 痕迹。


### 7.3 `save_trades()` 是新补的

Mongo 和内存实现现在都有 `save_trades()`，但这是后补的接口。

调用位置：

- [storage/base.py](/home/buzheng/Desktop/tradebook/storage/base.py:1)
- [storage/in_memory.py](/home/buzheng/Desktop/tradebook/storage/in_memory.py:1)
- [storage/mongo.py](/home/buzheng/Desktop/tradebook/storage/mongo.py:1)


## 8. 当前脚本清单

### [scripts/transform_dynamic_collar_trade_log.py](/home/buzheng/Desktop/tradebook/scripts/transform_dynamic_collar_trade_log.py:1)

用途：

- 把动态领口回测导出的 `trade_log.csv`
- 转成标准 trade 表

支持：

- `--asset MO`
- `--asset AU`


### [scripts/rebuild_and_compare_dynamic_collar.py](/home/buzheng/Desktop/tradebook/scripts/rebuild_and_compare_dynamic_collar.py:1)

用途：

- 重放动态领口 trade 表
- 与原始 `pos_log/account_log` 比较

支持：

- `--asset MO`
- `--asset AU`


### [scripts/verify_rqdata_flow.py](/home/buzheng/Desktop/tradebook/scripts/verify_rqdata_flow.py:1)

用途：

- 检查真实 `RQDataMarketGateway`
- 以及内存重放闭环是否可跑


## 9. Mongo 相关说明

数据库：

- `db_option`

新集合：

- `tradebook_trades`
- `tradebook_positions_daily`
- `tradebook_equity_daily`
- `tradebook_instruments`

旧集合没有被改动：

- `trade_book`
- `position_book`
- `equity_book`

约束：

- 当前设计是新旧并存
- 不直接覆盖旧集合


## 10. 推荐使用流程

### 场景 A：用 CSV 跑全量

1. 准备原始交易 CSV
2. 标准化字段
3. 如果缺字段，先过 `enrichment`
4. 用 `LedgerRebuildService.rebuild_history(...)`
5. 得到 `positions/equity`
6. 需要的话落 Mongo


### 场景 B：用数据库交易表跑全量

1. 从业务表读交易
2. 标准化为 trade schema
3. 如果业务表已有真实 `fee/multiplier`，优先信原表
4. 只用 `enrichment` 补 `asset_type` 等缺失信息
5. 重放
6. 写入 `tradebook_positions_daily` / `tradebook_equity_daily`


### 场景 C：验证某个策略回测导出结果

1. 先确认 futures 是否应该走 `XXX888`
2. 转换 trade log
3. 跑 compare 脚本
4. 看 `position/equity pnl diff` 是否接近 0


## 11. 后续优先级建议

### P0

- 修 `fee enrichment`
- 明确 `db_option.trade_book.csv` 这类实盘表的真实 fee 口径


### P1

- 把“从 CSV / DataFrame 标准化并入库再重放”的流程封成脚本
- 让 `db_option.trade_book.csv -> save_trades -> rebuild -> save_positions/save_equity` 一键可跑


### P2

- 补数据库层单测
- 增加对 `save_trades()` 的 Mongo 自动化测试脚本


### P3

- 视需求决定是否清理旧版 Mongo 索引
- 当前不建议直接清，因为容易影响历史数据兼容


## 12. 一句话总结

这套框架现在已经能稳定完成：

- 交易标准化
- 真实市场数据补全
- 持仓与权益重放
- Mongo 落库

而当前最值得继续修的点，不在账本核心，而在：

- `fee` 的真实补全规则
- 以及把实盘导入流程封装成固定入口。
