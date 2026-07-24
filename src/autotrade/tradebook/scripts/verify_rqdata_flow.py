from __future__ import annotations

from pathlib import Path

import pandas as pd

from autotrade.tradebook.ledger.schema import TRADE_COLUMNS
from autotrade.tradebook.market.rqdata import RQDataMarketGateway
from autotrade.tradebook.service.rebuild_service import LedgerRebuildService
from autotrade.tradebook.storage.in_memory import InMemoryLedgerStorage

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def make_trade(
    trade_id: str,
    trade_date: str,
    trade_time: str,
    order_book_id: str,
    side: str,
    offset: str,
    qty: float,
    price: float,
    *,
    account: str = "ACC_RQ",
    book_name: str = "SIM_RQ",
    asset_type: str = "unknown",
    multiplier: float = 1.0,
    fee: float = 0.0,
) -> dict:
    return {
        "trade_id": trade_id,
        "account": account,
        "book_name": book_name,
        "trade_date": trade_date,
        "trade_time": trade_time,
        "order_book_id": order_book_id,
        "asset_type": asset_type,
        "side": side,
        "offset": offset,
        "qty": qty,
        "price": price,
        "multiplier": multiplier,
        "fee": fee,
        "currency": "CNY",
        "remark": "",
    }


def main() -> None:
    market = RQDataMarketGateway()
    order_book_ids = ["000001.XSHE", "510050.XSHG", "IF2606"]
    instruments = market.get_instruments(order_book_ids=order_book_ids)
    print("Instruments:")
    print(instruments.to_string(index=False))

    trades = pd.DataFrame(
        [
            make_trade("RQ1", "2026-05-20", "2026-05-20 09:35:00", "000001.XSHE", "buy", "open", 100, 10.86, asset_type="CS", fee=1.0),
            make_trade("RQ2", "2026-05-20", "2026-05-20 10:00:00", "510050.XSHG", "buy", "open", 200, 3.52, asset_type="ETF", fee=1.0),
            make_trade("RQ3", "2026-05-20", "2026-05-20 10:30:00", "IF2606", "sell", "open", 1, 3907.0, asset_type="Future", multiplier=300.0, fee=20.0),
            make_trade("RQ4", "2026-05-21", "2026-05-21 10:15:00", "000001.XSHE", "sell", "close", 40, 10.78, asset_type="CS", fee=1.0),
            make_trade("RQ5", "2026-05-21", "2026-05-21 14:00:00", "IF2606", "buy", "close", 1, 3836.8, asset_type="Future", multiplier=300.0, fee=20.0),
        ],
        columns=TRADE_COLUMNS,
    )

    storage = InMemoryLedgerStorage(trade_df=trades)
    rebuild = LedgerRebuildService(storage=storage, market=market)
    positions, equity = rebuild.rebuild_history(
        account="ACC_RQ",
        book_name="SIM_RQ",
        start_date="2026-05-20",
        end_date="2026-05-21",
        initial_cash=500000.0,
        persist=True,
    )

    print("\nPosition History:")
    print(positions.to_string(index=False))
    print("\nEquity History:")
    print(equity.to_string(index=False))


if __name__ == "__main__":
    main()
