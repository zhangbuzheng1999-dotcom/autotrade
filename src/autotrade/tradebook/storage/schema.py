from __future__ import annotations

from ..ledger.schema import EQUITY_COLUMNS, POSITION_COLUMNS, TRADE_COLUMNS

TRADEBOOK_DB_NAME = "db_option"

TRADE_COLLECTION = "tradebook_trades"
POSITION_COLLECTION = "tradebook_positions_daily"
EQUITY_COLLECTION = "tradebook_equity_daily"
INSTRUMENT_COLLECTION = "tradebook_instruments"

INSTRUMENT_COLUMNS = [
    "order_book_id",
    "symbol",
    "name",
    "asset_type",
    "exchange",
    "currency",
    "multiplier",
    "underlying_order_book_id",
    "expiry_date",
    "strike",
    "option_type",
    "is_active",
    "remark",
]

POSITION_BOOK_COLUMNS = ["date"] + POSITION_COLUMNS

COLLECTION_COLUMNS = {
    TRADE_COLLECTION: TRADE_COLUMNS,
    POSITION_COLLECTION: POSITION_BOOK_COLUMNS,
    EQUITY_COLLECTION: EQUITY_COLUMNS,
    INSTRUMENT_COLLECTION: INSTRUMENT_COLUMNS,
}
