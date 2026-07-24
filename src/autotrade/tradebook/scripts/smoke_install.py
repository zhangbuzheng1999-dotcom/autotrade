from __future__ import annotations

import argparse

import pandas as pd

from autotrade.tradebook.market.rqdata import RQDataMarketGateway
from autotrade.tradebook.service.query_service import LedgerQueryService
from autotrade.tradebook.service.rebuild_service import LedgerRebuildService
from autotrade.tradebook.storage.mongo import (
    MongoLedgerStorage,
    bootstrap_tradebook_collections,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test autotrade.tradebook install using Mongo + RQData")
    parser.add_argument("--account", default="db_option")
    parser.add_argument("--book-name", default="dynamic_collar_MO")
    parser.add_argument("--db-name", default="db_option")
    args = parser.parse_args()

    bootstrap_tradebook_collections(db_name=args.db_name)
    storage = MongoLedgerStorage(db_name=args.db_name)
    market = RQDataMarketGateway()
    query = LedgerQueryService(storage=storage)
    rebuild = LedgerRebuildService(storage=storage, market=market)

    trades = storage.load_trades(account=args.account, book_name=args.book_name)
    if trades.empty:
        raise ValueError(f"no trades found for account={args.account}, book_name={args.book_name}")

    order_book_ids = sorted(trades["order_book_id"].astype(str).dropna().unique().tolist())
    trade_start = pd.to_datetime(trades["trade_date"]).min().normalize()
    trade_end = pd.to_datetime(trades["trade_date"]).max().normalize()

    instruments = market.get_instruments(order_book_ids=order_book_ids)
    if instruments.empty:
        raise ValueError(f"rqdata returned empty instruments for book_name={args.book_name}")

    preview_positions, preview_equity = rebuild.rebuild_history(
        account=args.account,
        book_name=args.book_name,
        start_date=trade_start,
        end_date=trade_end,
        initial_cash=0.0,
        persist=False,
    )
    if preview_positions.empty or preview_equity.empty:
        raise ValueError(f"rebuild preview returned empty snapshots for book_name={args.book_name}")

    latest_positions = query.get_latest_positions(
        account=args.account,
        book_name=args.book_name,
        asof_date=trade_end,
    )
    equity_series = query.get_equity_series(
        account=args.account,
        book_name=args.book_name,
        start_date=trade_start,
        end_date=trade_end,
    )

    if latest_positions.empty:
        raise ValueError(f"latest positions empty for book_name={args.book_name}")
    if equity_series.empty:
        raise ValueError(f"equity series empty for book_name={args.book_name}")

    print("tradebook smoke test passed")
    print(f"db_name={args.db_name}")
    print(f"account={args.account}")
    print(f"book_name={args.book_name}")
    print(f"trade_rows={len(trades)}")
    print(f"instrument_rows={len(instruments)}")
    print(f"preview_position_rows={len(preview_positions)}")
    print(f"preview_equity_rows={len(preview_equity)}")
    print(f"latest_position_rows={len(latest_positions)}")
    print(f"equity_rows={len(equity_series)}")
    print(f"trade_range={trade_start.date()} -> {trade_end.date()}")


if __name__ == "__main__":
    main()
