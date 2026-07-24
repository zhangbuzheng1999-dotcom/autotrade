from __future__ import annotations

import pandas as pd

from ..ledger.schema import TRADE_COLUMNS
from ..market.base import MarketDataGateway

from .contract_rules import (
    calc_trade_fee_from_rule,
    get_contract_rule_by_key,
    get_margin_rate,
    get_multiplier,
    resolve_contract_rule_key,
)
from .parser import parse_cn_trade_lines


def enrich_trade_records(
    trade_df: pd.DataFrame,
    *,
    market: MarketDataGateway | None = None,
) -> pd.DataFrame:
    if trade_df is None or trade_df.empty:
        return pd.DataFrame(columns=TRADE_COLUMNS + ["margin_rate"])

    out = trade_df.copy()
    out["order_book_id"] = out["order_book_id"].astype(str).str.upper()
    out["qty"] = pd.to_numeric(out["qty"], errors="coerce").abs()
    out["price"] = pd.to_numeric(out["price"], errors="coerce")

    if "multiplier" not in out.columns:
        out["multiplier"] = pd.NA
    if "fee" not in out.columns:
        out["fee"] = pd.NA

    if market is not None:
        instruments = market.get_instruments(order_book_ids=sorted(out["order_book_id"].astype(str).unique().tolist()))
        if not instruments.empty:
            keep_cols = [
                c for c in [
                    "order_book_id",
                    "asset_type",
                    "exchange",
                    "underlying_order_book_id",
                    "underlying_symbol",
                    "multiplier",
                ] if c in instruments.columns
            ]
            out = out.merge(instruments[keep_cols], how="left", on="order_book_id", suffixes=("", "_inst"))
            if "asset_type_inst" in out.columns:
                out["asset_type"] = out["asset_type_inst"].where(out["asset_type"].isin([None, "", "unknown"]) | out["asset_type"].isna(), out["asset_type"])
                out = out.drop(columns=["asset_type_inst"])
            if "multiplier_inst" in out.columns:
                out["multiplier"] = out["multiplier"].where(out["multiplier"].notna(), out["multiplier_inst"])
                out = out.drop(columns=["multiplier_inst"])

    out["rule_key"] = out.apply(lambda row: resolve_contract_rule_key(row.to_dict()), axis=1)
    out["contract_rule"] = out["rule_key"].map(get_contract_rule_by_key)

    out["multiplier"] = out.apply(
        lambda row: float(row["multiplier"]) if pd.notna(row["multiplier"]) else (
            float(row["contract_rule"].get("contract_amt", 1.0)) if isinstance(row["contract_rule"], dict) else get_multiplier(row["order_book_id"])
        ),
        axis=1,
    )
    out["fee"] = out.apply(
        lambda row: float(row["fee"]) if pd.notna(row["fee"]) else calc_trade_fee_from_rule(
            rule=row["contract_rule"],
            qty=row["qty"],
            price=row["price"],
        ),
        axis=1,
    )
    out["margin_rate"] = out.apply(
        lambda row: float(row["contract_rule"].get("margin_rate", 0.0)) if isinstance(row["contract_rule"], dict) else 0.0,
        axis=1,
    )
    out = out.drop(columns=["contract_rule"])

    for col in TRADE_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    extra_cols = [c for c in ["margin_rate", "exchange", "underlying_order_book_id", "underlying_symbol", "rule_key"] if c in out.columns]
    return out[TRADE_COLUMNS + extra_cols].copy()


def enrich_trade_text(
    trade_text: str,
    *,
    account: str,
    book_name: str,
    market: MarketDataGateway | None = None,
) -> pd.DataFrame:
    parsed = parse_cn_trade_lines(
        trade_text,
        account=account,
        book_name=book_name,
    )
    return enrich_trade_records(parsed, market=market)
