# RiceQuant `rq_data` 模块完整说明

## 1. 模块目标

`src/autotrade/data/ricequant/` 是项目内统一的 RiceQuant 数据接入与本地持久化模块。

它的目标不是简单包一层 `rqdatac`，而是提供一套稳定的数据层能力：

- 对外暴露统一 `Service.get(...)` 调用接口
- 把 RiceQuant API 语义和本地数据库语义明确分开
- 把不同资产类型拆成独立可维护链路
- 把元数据和时序数据分库存储
- 支持 `DB_ONLY / SOURCE_ONLY / DB_THEN_SOURCE`
- 支持从 API 拉取、标准化、落库、再从本地库查询的完整闭环

这套模块当前已经实测通过端到端验证：

- `trading_dates`
- `future / option / stock / etf / index` instrument
- `future / option / stock / etf / index` price
- `option_greeks`

并且已经真实跑通：

`API -> normalize -> persist -> DB_ONLY query`

## 计算型期权 Greeks

自定义 Black97 Greeks 与 RiceQuant 官方 `options.get_greeks` 分开维护：

- 官方数据：`OptionGreeksService` / `option_greeks_*`
- 本地计算：`CalculatedOptionGreeksService` /
  `calculated_option_greeks_1d`

调用示例：

```python
from autotrade.data.ricequant.base import FetchMode
from autotrade.data.ricequant.service.calculated_options import (
    CalculatedOptionGreeksService,
)

service = CalculatedOptionGreeksService()

# 已落库结果可直接按合约查询。
db_result = service.get(
    mode=FetchMode.DB_ONLY,
    order_book_ids=["AU2608C1000"],
    start_date="2026-07-10",
    end_date="2026-07-10",
)

# 现场计算时会解析该合约所属品种，使用 SOURCE_ONLY 获取完整合约
# 截面、期权价格和所需期货价格；完整截面落库后只返回请求合约。
source_result = service.get(
    mode=FetchMode.SOURCE_ONLY,
    order_book_ids=["AU2608C1000"],
    start_date="2026-07-10",
    end_date="2026-07-10",
    persist=True,
)
```

`SOURCE_ONLY` 的模式会传播到全部内部数据服务，不会混合 DB 数据。
期货期权使用对应期货合约收盘价作为 Forward；ETF/指数期权直接使用复制自
cfutures 的 Forward 引擎，以 Call/Put 成交量加权构造 Forward，并保留
期限插值和 Spot 兜底。纯 Black97 计算位于
`autotrade.analytics.options`，不负责数据库查询或合约拼接。

品种级 IVX 使用相同访问模式：

```python
from autotrade.data.ricequant.service.calculated_options import (
    CalculatedOptionIVXService,
)

ivx_service = CalculatedOptionIVXService()
ivx = ivx_service.get(
    mode=FetchMode.SOURCE_ONLY,
    opt_symbol="AU",
    start_date="2026-07-10",
    end_date="2026-07-10",
    persist=True,
)
```

IVX 必须按 `opt_symbol` 现场计算，内部获取完整期权截面；落库表为
`rq_option_data.calculated_option_ivx_1d`。

## 2. 当前架构总览

模块采用四层结构：

1. `service/`
   - 对外统一调用入口
   - 负责调度 DB / SOURCE 路径
   - 负责决定是否持久化

2. `spec/`
   - 这是核心规则层
   - 定义每个资源的 API 参数、DB 参数、默认值、字段标准化、分表分库规则、存储后端、写入模式

3. `datasource/`
   - 负责调用真实 RiceQuant API
   - 不直接处理数据库

4. `repository/`
   - 负责数据库读写
   - MySQL repository 负责 snapshot/upsert 风格数据
   - ClickHouse repository 负责 timeseries 数据

除此之外还有三个基础文件：

- `base.py`
  - 定义 `FetchMode`、`FetchResult`
  - 定义 `BaseRQSpec`、`BaseRQService`、`BaseRQRepository`、`BaseClickHouseRepository`
- `_clickhouse.py`
  - ClickHouse 客户端封装
- `init_rq_data.py`
  - 数据库、表的初始化和重建入口

## 3. 数据库存储设计

### 3.1 总体原则

当前模块明确区分两类数据：

- MySQL：instrument / metadata / 交易日历类表
- ClickHouse：price 时序表、`option_greeks`

这是当前真实实现，不是计划。

### 3.2 数据库清单

模块会创建以下数据库：

- `rq_data`
- `rq_stock_data`
- `rq_etf_data`
- `rq_future_data`
- `rq_option_data`
- `rq_index_data`

当前 `init_rq_data.py` 会同时创建：

- 同名 MySQL database
- 同名 ClickHouse database

### 3.3 各数据库职责

`rq_data`

- 公共表
- 当前至少包含 `trading_dates`

`rq_stock_data`

- MySQL：`cn_stock_instruments`
- ClickHouse：`stock_price_*`

`rq_etf_data`

- MySQL：`etf_instruments`
- ClickHouse：`etf_price_*`

`rq_future_data`

- MySQL：`future_instruments`
- ClickHouse：`future_price_*`

`rq_option_data`

- MySQL：`option_instruments`
- ClickHouse：`option_price_*`
- ClickHouse：`option_greeks_*`

`rq_index_data`

- MySQL：`index_instruments`
- ClickHouse：`index_price_*`

## 4. 表设计与主存储语义

### 4.1 Instrument 表

instrument 表统一放在 MySQL。

写入方式：

- `snapshot_upsert`

语义：

- 相同主键记录会做 upsert
- 当前主键基本是 `order_book_id`

这类表适合存：

- 合约基础信息
- 上市/退市日期
- 市场、板块、交易时间等属性

### 4.2 Price 表

price 表统一放在 ClickHouse。

支持频率：

- `1d`
- `1w`
- `1m`
- `5m`
- `15m`
- `30m`
- `60m`

当前 price 表引擎统一是：

- `ReplacingMergeTree(ingest_time)`

这意味着：

- 逻辑上支持重复回灌后的最终去重
- 物理上不是“插入瞬时唯一”
- 普通查询层已经统一加了 `FINAL`

### 4.3 `stock / etf` 的 `adjust_type`

股票和 ETF 的日线、周线、分钟线 price 目前已经支持三种复权模式并持久化：

- `none`：不复权
- `pre`：前复权
- `post`：后复权

默认值：

- `adjust_type = "none"`

当前只有 `stock` 和 `etf` 的 price 表显式存储 `adjust_type`。

其 ClickHouse 表结构已经包含：

- `adjust_type String`

并且排序键为：

- 日线/周线：`ORDER BY (date, order_book_id, adjust_type)`
- 分钟：`ORDER BY (datetime, order_book_id, adjust_type)`

这样可以保证在 `ReplacingMergeTree` 语义下：

- 同一日期/时间
- 同一代码
- 同一 `adjust_type`

才会互相替换。

不同复权模式会并存，不会互相覆盖。

### 4.4 `future / option / index` 的 price

`future / option / index` 目前 price 表不带 `adjust_type`。

排序键为：

- 日线/周线：`ORDER BY (date, order_book_id)`
- 分钟：`ORDER BY (datetime, order_book_id)`

但分钟线这里现在已经是“按资产拆分语义”，不能再一概而论：

- `index` minute price 仍然是纯 `datetime` 语义，不存 `trading_date`
- `future / option` minute price 已恢复 `trading_date Nullable(Date)`

当前真实实现是：

- `future / option`
  - minute 表同时存 `datetime` 和 `trading_date`
  - `DB_ONLY` 的 `start_date / end_date` 按 `trading_date` 过滤
  - 但 ClickHouse 物理排序字段仍然保持 `ORDER BY (datetime, order_book_id)`
- `stock / etf / index`
  - minute 表不存 `trading_date`
  - minute 过滤与落库继续按 `datetime` 处理

这样设计的原因是：

- `future / option` 的源端 minute bars 自带 `trading_date`
- 尤其 futures 夜盘会出现：
  - `datetime = 前一自然日夜盘时间`
  - `trading_date = 次一交易日`
- 但分钟 bars 的真实时间轴主字段仍然是 `datetime`
  - `trading_date` 是辅助查询字段
  - 不是物理排序主字段

### 4.5 `option_greeks`

`option_greeks` 当前也走 ClickHouse。

其日频和分钟频分表维护，核心时间语义分别是：

- 日频：`trading_date`
- 分钟：`datetime`

## 5. 模块结构与职责边界

### 5.1 `service/`

这是外部调用层。

每种资产各自维护自己的 service：

- `FuturePriceService`
- `FutureInstrumentService`
- `OptionPriceService`
- `OptionInstrumentService`
- `OptionGreeksService`
- `CNStockPriceService`
- `CNStockInstrumentService`
- `ETFPriceService`
- `ETFInstrumentService`
- `IndexPriceService`
- `IndexInstrumentService`
- `TradingDatesService`

`service` 负责：

- 接受调用参数
- 按 `FetchMode` 决定查 DB 还是查 API
- 在需要时触发持久化
- 返回统一 `FetchResult`

不负责：

- 手写 SQL 规则
- API 字段适配
- 表路由

这些都应该放在 `spec`。

### 5.2 `spec/`

这是当前模块最重要的一层。

每个 spec 至少负责：

- `API_PARAMS`
- `API_REQUIRED_FILTERS`
- `DB_QUERY_FIELDS`
- `DB_REQUIRED_FILTERS`
- `DEFAULT_FILTERS`
- `resolve_database(...)`
- `resolve_table(...)`
- `resolve_db_filter_specs(...)`
- `normalize_df(...)`
- `filter_df(...)`
- 必要时 `normalize_query_filters(...)`
- 必要时 `normalize_db_query_filters(...)`

其中：

- `normalize_query_filters(...)`
  - 处理外部调用参数语义
- `normalize_db_query_filters(...)`
  - 处理 DB 查询语义
  - 尤其是分钟线 `end_date` 的“整天扩展”

当前已经拆成“每个资产自己维护自己的 price/instrument spec”，不再用一个统一 common price spec 直接服务所有品种。

这意味着后续如果某个品种有特殊字段或特殊路由，应该直接改自己的 spec，而不是继续往“通用 spec”里堆分支。

### 5.3 `datasource/`

每个 datasource 只负责一件事：

- 调 RiceQuant API

例如：

- `CNStockPriceDataSource` 调 `rqdatac.get_price(...)`
- `OptionGreeksDataSource` 调 `rqdatac.options.get_greeks(...)`
- `TradingDatesDataSource` 调 `rqdatac.get_trading_dates(...)`

datasource 不应该：

- 写数据库
- 做复杂业务判断
- 决定落哪张表

### 5.4 `repository/`

repository 只处理数据库。

当前分三类：

- `BaseRQRepository`
  - MySQL snapshot / 普通查询
- `BaseClickHouseRepository`
  - ClickHouse timeseries 查询与插入
- `BackendRoutingRepository`
  - 针对同一资源按 spec 决定走 MySQL 还是 ClickHouse

目前 `option_greeks` 就使用了 backend routing。

## 6. 调用模式与行为规范

### 6.1 `FetchMode`

模块统一支持三种模式：

- `FetchMode.DB_ONLY`
- `FetchMode.SOURCE_ONLY`
- `FetchMode.DB_THEN_SOURCE`

### 6.2 三种模式的语义

`DB_ONLY`

- 只查数据库
- 过滤参数遵守 DB 语义
- 允许的字段由 spec 的 `DB_QUERY_FIELDS` 决定

`SOURCE_ONLY`

- 只查源端 API
- 参数必须符合 API 语义
- 默认 `persist=True`
- 也就是说，默认会在拉完源数据后立刻落库

`DB_THEN_SOURCE`

- 先查本地库
- 本地命中则直接返回
- 本地未命中则查源并按 `persist` 决定是否落库

### 6.3 `persist` 与 `refresh`

`persist`

- 默认值是 `True`
- 只要是走源端路径，默认都会持久化

`refresh`

- 仅对 `DB_THEN_SOURCE` 有意义
- `refresh=True` 时会跳过 DB 命中逻辑，直接拉源

### 6.4 返回值

所有 service 返回统一 `FetchResult`：

- `status`
- `data`
- `error`

成功：

- `status = FetchStatus.SUCCESS`
- `data` 是 `pd.DataFrame`

失败：

- `status = FetchStatus.FAILED`
- `error` 是异常对象

## 7. 典型调用方式

### 7.1 股票价格

```python
from autotrade.data.ricequant.service.cn_stock import CNStockPriceService
from autotrade.data.ricequant.base import FetchMode

service = CNStockPriceService()

res = service.get(
    order_book_ids=["000001.XSHE"],
    start_date="2024-01-01",
    end_date="2024-01-31",
    frequency="1d",
    adjust_type="pre",
    mode=FetchMode.SOURCE_ONLY,
    persist=True,
)
```

### 7.2 ETF 分钟线

```python
from autotrade.data.ricequant.service.etf import ETFPriceService

service = ETFPriceService()

res = service.get(
    order_book_ids=["159001.XSHE"],
    start_date="2024-01-10",
    end_date="2024-01-10",
    frequency="1m",
)
```

默认行为相当于：

- `mode=DB_THEN_SOURCE`
- `persist=True`
- `adjust_type="none"`

### 7.3 期权 Greeks

```python
from autotrade.data.ricequant.service.options import OptionGreeksService
from autotrade.data.ricequant.base import FetchMode

service = OptionGreeksService()

res = service.get(
    order_book_ids=["10005765"],
    start_date="2024-01-10",
    end_date="2024-01-10",
    frequency="1m",
    mode=FetchMode.SOURCE_ONLY,
)
```

### 7.4 只查本地库

```python
from autotrade.data.ricequant.base import FetchMode

res = service.get(
    order_book_ids=["000001.XSHE"],
    start_date="2024-01-01",
    end_date="2024-01-31",
    frequency="1d",
    mode=FetchMode.DB_ONLY,
)
```

## 8. 配置与环境

### 8.1 配置入口

当前统一配置入口是：

```python
from autotrade.coreutils.config import DatabaseInfo, ClickHouseInfo, load_env
```

这是当前稳定接口，应该保持兼容。

### 8.2 `.env` 读取行为

`load_env()` 的顺序是：

1. 显式 `env_path`
2. 从当前工作目录向上搜索项目根 `.env`
3. `APP_ENV_FILE`
4. 系统环境变量

因此模块运行时通常只需要保证项目根目录有 `.env`。

### 8.3 当前使用的环境变量

MySQL：

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`

ClickHouse：

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_HTTP_PORT`
- `CLICKHOUSE_TCP_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`

### 8.4 运行环境

调用 RiceQuant API 需要在能导入 `rqdatac` 的环境下执行。

当前项目实际验证环境是：

- `conda activate rq_data`

同时需要以下依赖可用：

- `rqdatac`
- `pymysql`
- `clickhouse-connect`

## 9. 初始化与重建

### 9.1 全量初始化入口

```bash
PYTHONPATH=/home/buzheng/Desktop/autotrade/src \
conda run -n rq_data python src/autotrade/data/ricequant/init_rq_data.py
```

这会创建：

- MySQL databases
- ClickHouse databases
- 所有 instrument / price / greeks / trading_dates 表

如果需要“删库后完整重建”，当前已经有显式入口：

```python
from autotrade.coreutils.config import load_env
from autotrade.data.ricequant.init_rq_data import rebuild_rq_databases

load_env()
rebuild_rq_databases()
```

这个入口会删除并重建以下 MySQL / ClickHouse database：

- `rq_data`
- `rq_stock_data`
- `rq_etf_data`
- `rq_future_data`
- `rq_option_data`
- `rq_index_data`

注意这不是单纯 drop table，而是整套 rq 数据库重建。

### 9.2 各资产独立建表入口

价格表目前都已经拆成“每个资产单独维护”：

- `create_future_price_tables()`
- `create_option_price_tables()`
- `create_index_price_tables()`
- `create_cn_stock_price_tables()`
- `create_etf_price_tables()`

各资产完整初始化入口：

- `create_rq_futures_data()`
- `create_rq_options_data()`
- `create_rq_index_data()`
- `create_rq_cn_stock_data()`
- `create_rq_etf_data()`
- `create_rq_data_common_tables()`

### 9.3 重建 ClickHouse 表

模块里保留了 ClickHouse 重建入口，适合表结构调整后使用。

如果修改了 ClickHouse DDL，必须注意：

- `CREATE TABLE IF NOT EXISTS` 不会修改已存在表结构
- 需要先 `DROP TABLE`
- 再重新执行建表函数

这点在 `stock/etf` 增加 `adjust_type` 时已经实际踩过。

最近一次结构调整里，future / option minute 的处理方式已经改成：

- 表里补回 `trading_date`
- 但 `ORDER BY` 仍然保持 `ORDER BY (datetime, order_book_id)`

也就是说：

- 允许按 `trading_date` 查交易日语义
- 但不把 `trading_date` 当成 minute 的物理排序主字段

## 10. ClickHouse 读写技术细节

### 10.1 客户端

当前使用：

- `clickhouse_connect`

封装位于 `_clickhouse.py`。

### 10.2 插入方式

当前 `insert_dataframe(...)` 会：

- 对 dataframe 做逐值规范化
- 处理 `NaN / NaT / Timestamp / numpy` 类型
- 使用 `async_insert=1`
- 使用 `wait_for_async_insert=1`

### 10.3 查询方式

当前所有走 `BaseClickHouseRepository` 的查询统一使用：

- `SELECT * FROM db.table FINAL ...`

这不是部分查询才加，而是统一加。

原因是 price / greeks 当前使用：

- `ReplacingMergeTree(ingest_time)`

如果不加 `FINAL`，在后台 merge 尚未完成时，查询可能看到重复行。

### 10.4 大 `IN` 条件分块

`BaseClickHouseRepository` 当前支持自动分块查询：

- `CHUNK_SIZE = 5000`

当 `order_book_ids` 太大时，会自动拆成多批查询再拼接结果。

### 10.5 `ReplacingMergeTree` 的真实语义

它不是 MySQL 式唯一键。

当前语义是：

- 同一排序键可以重复插入
- 后台 merge 时会保留新版本
- 查询层通过 `FINAL` 尽量读到最终视图

因此：

- 它是“最终去重”
- 不是“插入瞬时去重”

## 11. 时间字段与查询语义

### 11.1 price 的时间字段

日线/周线：

- `date`

分钟：

- `datetime`

### 11.2 `option_greeks` 的时间字段

日频：

- `trading_date`

分钟：

- `datetime`

### 11.3 分钟查询的 `end_date` 扩展

模块已经修复过一个关键 bug：

- 如果分钟线 `end_date="2024-01-10"` 直接按午夜处理，会把整天分钟数据过滤掉

当前 `normalize_db_query_filters(...)` 会把分钟 `end_date` 自动扩到当天结束时刻。

这套逻辑已经在：

- price
- option_greeks

上统一落实。

### 11.4 周线语义

周线不是“任意单日一定对应一条记录”的日频数据。

在做 `1w` 查询时，建议使用一个完整区间，而不是把某一天当成“必须命中一条周线”的精确日期。

## 12. 当前对外资源清单

### 12.1 Futures

- `FutureInstrumentService`
- `FuturePriceService`

### 12.2 Options

- `OptionInstrumentService`
- `OptionPriceService`
- `OptionGreeksService`

### 12.3 CN Stock

- `CNStockInstrumentService`
- `CNStockPriceService`

### 12.4 ETF

- `ETFInstrumentService`
- `ETFPriceService`

### 12.5 Index

- `IndexInstrumentService`
- `IndexPriceService`

### 12.6 Common

- `TradingDatesService`

## 13. 新增资源的维护规范

如果后续要新增一个新资源，不要只改 service。

标准流程应该是：

1. 在 `spec/` 新建资源 spec
2. 明确：
   - API 参数
   - DB 查询参数
   - 默认值
   - 字段标准化
   - 分库分表规则
   - 存储后端
   - 写入模式
3. 在 `datasource/` 新建 API 适配器
4. 在 `repository/` 新建数据库读写适配器
5. 在 `service/` 暴露统一入口
6. 在 `init_rq_data.py` 增加建表逻辑
7. 做端到端验证

不要把“某个资产的特殊逻辑”重新塞回一个通用 `common price` 实现里。

当前项目已经明确转向：

- 每个品种独立维护自己的 price / instrument 接口

这是后续维护的基本原则。

## 14. 数据健康检查

`healthy_check.py` 提供了按交易日检查缺失数据的能力。

当前已有：

- `fut_healthy_check(...)`
- `opt_healthy_check(...)`
- `index_healthy_check(...)`
- `etf_healthy_check(...)`

逻辑大致是：

- 先拿 instrument 基础信息
- 再拿交易日历
- 推导某天理论应存在的合约集合
- 与本地 `1d` 数据做比对

这是运维和补数时的重要辅助工具。

## 15. 已验证结果

当前已经真实验证通过的闭环包括：

- `trading_dates`
- `future` instrument + 全频率 price
- `option` instrument + 全频率 price + `option_greeks`
- `stock` instrument + 全频率 price
- `etf` instrument + 全频率 price
- `index` instrument + 全频率 price

另外 `stock/etf` 的 `adjust_type` 也已经实测通过：

- `none`
- `pre`
- `post`

并验证了：

- API 返回数据可落库
- 回查能按 `adjust_type` 精确过滤
- 三种复权模式会分别独立存储

### 15.1 本次 `trading_date` 恢复后的额外验证

本次又补了一轮更严格的确认，先直接看了源端 minute 返回，结论是：

- `stock`：没有 `trading_date`
- `etf`：没有 `trading_date`
- `index`：没有 `trading_date`
- `future`：有 `trading_date`
- `option`：有 `trading_date`

因此当前代码已经按资产拆分：

- `future / option`
  - minute 恢复 `trading_date`
  - `DB_ONLY` 的 `start_date / end_date` 按 `trading_date` 过滤
- `stock / etf / index`
  - minute 继续只按 `datetime` 处理

### 15.2 删库重建后的端到端验证

这次不是在旧表上补列后验证，而是：

- 先删掉全部 `rq_*` MySQL / ClickHouse 数据库
- 再使用新的 `init_rq_data.py` 完整重建
- 再重新跑 `SOURCE_ONLY -> persist -> DB_ONLY`

验证窗口：

- `2024-01-08` 到 `2024-01-12`

验证方式：

- 检查 source / DB 每天 bucket 分布
- 检查首尾时间
- 检查 source / DB 键集合是否一致
- 检查共享数值列是否逐行一致

本次验证结果：

- `stock`
  - `1d` 5 行
  - `1m` 1200 行
  - source / DB 键集合一致，值一致
- `etf`
  - `1d` 5 行
  - `1m` 1200 行
  - source / DB 键集合一致，值一致
- `index`
  - `1d` 5 行
  - `1m` 1200 行
  - source / DB 键集合一致，值一致
- `future A2405`
  - `1d` 5 行
  - `1m` 1725 行
  - source / DB 键集合一致，值一致
  - `trading_date` 分桶正确保留夜盘归属
  - 例如 `2024-01-10` 交易日对应：
    - `2024-01-09 21:01:00 -> 2024-01-10 15:00:00`
- `option 10005765`
  - `1d` 5 行
  - `1m` 1200 行
  - source / DB 键集合一致，值一致

结论：

- 当前重建后的库结构与代码逻辑一致
- `future / option` 的 minute `trading_date` 恢复方案已经真实跑通
- 同时仍保留 `datetime` 作为 minute 表的物理排序主字段

### 15.3 本次 minute healthy check 扩展

这次又把 `healthy_check.py` 从“只支持 `1d` 缺失检查”扩成了“支持 minute 完整性检查”。

#### 15.3.1 当前 minute healthy check 的实现口径

minute 检查不再像 `1d` 那样先推理论存续区间再逐日对账，而是：

1. 如果显式传了 `order_book_ids`
   - 不再先查基础信息判断“这天是否应存在”
   - 直接依赖 `rqdatac.get_trading_periods(...)`
   - 因为不存在 / 当天无交易时段的合约，米筐会直接不返回

2. 先对 `get_trading_periods(...)` 返回结果做展开
   - 得到：
     - `date`
     - `time`
     - `order_book_id`

3. 再查本地 DB minute 数据
   - `future / option`
     - 用 `trading_date + time + order_book_id` 对比
   - `stock / etf / index`
     - 用 `datetime.date + time + order_book_id` 对比

4. 只要缺任意一分钟 / 任意一个 bar
   - 就把整个 `(trade_date, order_book_id)` 标记为缺失
   - 默认不返回具体缺失的分钟点

#### 15.3.2 当前已补的 minute healthy check 函数

代码位置：

- `src/autotrade/data/ricequant/healthy_check.py`

当前已经有：

- `fut_1m_healthy_check(...)`
- `opt_1m_healthy_check(...)`
- `stock_1m_healthy_check(...)`
- `index_1m_healthy_check(...)`
- `etf_1m_healthy_check(...)`

虽然函数名保留了 `1m`，但现在都已经支持传：

- `1m`
- `5m`
- `15m`
- `30m`
- `60m`

#### 15.3.3 trading period 展开规则

现在的 `expand_trading_periods_to_time_rows(...)` 已支持 minute 频率展开：

- `1m`
- `5m`
- `15m`
- `30m`
- `60m`

其中：

- `1m`
  - 直接逐分钟展开
- `5m / 15m / 30m`
  - 按“bar 结束时刻”生成
  - 起点因为 trading period 是 `xx:01` / `xx:31` 这类 minute，所以第一根 bar 会落到：
    - `5m` -> `xx:05`
    - `15m` -> `xx:15`
    - `30m` -> `xx:30`
  - 必要时会补 segment end

#### 15.3.4 `future 60m` 的特殊规则

`future 60m` 不能直接沿用通用的 segment 展开规则。

实测发现：

- 通用规则会把 `09:01-10:15` 展开出 `10:15`
- 也会把 `13:31-15:00` 展开出 `14:30`

但米筐真实 `get_price(..., frequency='60m')` 返回并不是这样。

例如：

- `A2405 / 2024-01-10`
  - 真实 `60m`：
    - `22:00, 23:00, 10:00, 11:00, 11:30, 14:00, 15:00`
- `AU888 / 2026-02-12`
  - 真实 `60m`：
    - `22:00, 23:00, 00:00, 01:00, 02:00, 02:30, 10:00, 11:00, 11:30, 14:00, 15:00`

因此当前代码已经单独补了：

- `expand_future_60m_trading_periods_to_time_rows(...)`

并且只让 `future + 60m` 走这条特殊展开逻辑，其它资产和频率继续走通用规则。

#### 15.3.5 本次 minute healthy check 的实测验证

本次在 `rq_data` 环境下，按下面这组样本做了：

- `SOURCE_ONLY + persist=True`
- `DB_ONLY`
- `minute healthy check`

样本：

- `future`: `A2405`
- `option`: `10005765`
- `stock`: `000001.XSHE`
- `index`: `000300.XSHG`
- `etf`: `510050.XSHG`

频率：

- `1m`
- `5m`
- `15m`
- `30m`
- `60m`

最终结果：

- `future`
  - `1m / 5m / 15m / 30m / 60m` 全部返回 `{}` 
- `option`
  - `1m / 5m / 15m / 30m / 60m` 全部返回 `{}` 
- `stock`
  - `1m / 5m / 15m / 30m / 60m` 全部返回 `{}` 
- `index`
  - `1m / 5m / 15m / 30m / 60m` 全部返回 `{}` 
- `etf`
  - `1m / 5m / 15m / 30m / 60m` 全部返回 `{}`

结论：

- 当前 minute healthy check 已经覆盖：
  - `future / option / stock / index / etf`
- 当前 minute healthy check 已经实测覆盖：
  - `1m / 5m / 15m / 30m / 60m`
- 并且 `future 60m` 的特殊切桶规则也已对齐米筐真实返回

## 16. 当前已知约束

1. ClickHouse 的去重依赖 `ReplacingMergeTree + FINAL`
   - 不是 MySQL 唯一键语义

2. `SOURCE_ONLY` 默认 `persist=True`
   - 这意味着它不是“只拉源不落库”
   - 如果只想临时查看 API 数据，应显式传 `persist=False`

3. 修改 ClickHouse 表结构后，不能只重跑 `CREATE TABLE IF NOT EXISTS`
   - 需要手动 drop 后重建

4. 周线查询建议按时间区间使用
   - 不要把它当作“某单日一定唯一命中”的日频数据

## 17. 推荐维护动作

日常维护建议按这个顺序做：

1. 先确认 `spec` 是否表达了正确语义
2. 再确认 datasource API 参数是否一致
3. 再确认 repository 查询字段和表结构是否匹配
4. 变更 DDL 后立即重建目标表
5. 至少跑一次端到端验证：
   - `SOURCE_ONLY + persist=True`
   - `DB_ONLY`

## 18. 最后结论

当前这套 `rq_data` 已经不是“简单的 rqdatac 包装器”，而是一套：

- 有明确分层
- 有明确存储边界
- 有独立资产维护入口
- 有初始化脚本
- 有健康检查能力
- 有真实端到端验证结果

的本地数据访问层。

后续无论是新增品种、调表结构、改查询语义，还是加新的本地持久化策略，都应优先遵守这份文档里的职责边界和维护原则。
