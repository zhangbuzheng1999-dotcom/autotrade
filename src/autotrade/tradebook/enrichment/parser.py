from __future__ import annotations

import re

import pandas as pd


_LINE_PATTERN = re.compile(
    r"[（(]\s*([^，,]+)\s*[，,]\s*([-]?\d+(?:\.\d+)?)\s*[，,]\s*(买|卖)\s*[，,]\s*(开|平|移)\s*[，,]\s*([\d.]+)\s*[，,]\s*([^)）]+?)\s*[）)]"
)


def _normalize_side(side_text: str) -> str:
    return {"买": "buy", "卖": "sell"}[side_text]


def _normalize_offset(offset_text: str) -> str:
    return {"开": "open", "平": "close", "移": "roll"}[offset_text]


def parse_cn_trade_lines(
    trade_text: str,
    *,
    account: str,
    book_name: str,
    asset_type: str = "unknown",
    currency: str = "CNY",
) -> pd.DataFrame:
    rows: list[dict] = []
    for idx, match in enumerate(_LINE_PATTERN.finditer(trade_text), start=1):
        order_book_id, qty_text, side_text, offset_text, price_text, trade_time = match.groups()
        trade_ts = pd.to_datetime(trade_time)
        rows.append(
            {
                "trade_id": f"{book_name}_{trade_ts.strftime('%Y%m%d%H%M%S')}_{idx}",
                "account": account,
                "book_name": book_name,
                "trade_date": trade_ts.normalize(),
                "trade_time": trade_ts,
                "order_book_id": str(order_book_id).strip().upper(),
                "asset_type": asset_type,
                "side": _normalize_side(side_text),
                "offset": _normalize_offset(offset_text),
                "qty": abs(float(qty_text)),
                "price": float(price_text),
                "multiplier": pd.NA,
                "fee": pd.NA,
                "currency": currency,
                "remark": "",
            }
        )
    return pd.DataFrame(rows)
