from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from autotrade.tradebook.enrichment.trade_enricher import enrich_trade_records
from autotrade.tradebook.ledger.schema import TRADE_COLUMNS
from autotrade.tradebook.market.rqdata import RQDataMarketGateway

PROJECT_ROOT = Path(__file__).resolve().parents[1]


EPSILON = 1e-12


def _map_symbol_to_order_book_id(trade_df: pd.DataFrame) -> pd.DataFrame:
    out = trade_df.copy()
    out["trade_date"] = pd.to_datetime(out["date_time"]).dt.normalize()
    out["symbol_upper"] = out["symbol"].astype(str).str.upper()

    direct_mask = out["symbol_upper"].str.contains(r"\d")
    out["order_book_id"] = out["symbol_upper"].where(direct_mask, pd.NA)
    out["order_book_id"] = out["order_book_id"].where(out["order_book_id"].notna(), out["symbol_upper"] + "888")

    missing = out.loc[out["order_book_id"].isna(), ["date_time", "symbol"]]
    if not missing.empty:
        raise ValueError(f"failed to map order_book_id for symbols:\n{missing.to_string(index=False)}")
    return out


def _infer_offset(df: pd.DataFrame) -> pd.Series:
    current_pos: dict[str, float] = {}
    offsets: list[str] = []
    for _, row in df.iterrows():
        signed_qty = float(row["qty"]) if row["side"] == "buy" else -float(row["qty"])
        prev = float(current_pos.get(row["order_book_id"], 0.0))
        if abs(prev) <= EPSILON or prev * signed_qty > 0:
            offset = "open"
        elif prev * signed_qty < 0:
            if abs(signed_qty) > abs(prev):
                offset = "roll"
            else:
                offset = "close"
        else:
            offset = "open"
        current_pos[row["order_book_id"]] = prev + signed_qty
        offsets.append(offset)
    return pd.Series(offsets, index=df.index)


def _derive_option_contract_basis(pos_df: pd.DataFrame, market: RQDataMarketGateway) -> tuple[float, float]:
    option_rows = pos_df.loc[pos_df["asset_type"].astype(str).str.lower().eq("option")].copy()
    if option_rows.empty:
        raise ValueError("cannot derive option contract basis: no option rows found in pos log")

    option_fee = pd.to_numeric(option_rows["fee"], errors="coerce").dropna()
    if option_fee.empty:
        raise ValueError("cannot derive option fee from pos log")
    fee_per_lot = float(option_fee.iloc[0])

    sample_option = str(option_rows["order_book_id"].dropna().astype(str).iloc[0]).upper()
    instruments = market.get_instruments(order_book_ids=[sample_option])
    if instruments.empty:
        raise ValueError(f"cannot load RQ instrument for option {sample_option}")
    option_multiplier = float(instruments.iloc[0]["multiplier"])
    if option_multiplier <= 0:
        raise ValueError(f"invalid option multiplier {option_multiplier} for {sample_option}")

    return fee_per_lot, option_multiplier


def transform_trade_log(
    trade_log_path: str | Path,
    pos_log_path: str | Path,
    *,
    account: str = "opt",
    book_name: str = "dynamic_collar_MO",
) -> pd.DataFrame:
    trade_df = pd.read_csv(trade_log_path).copy()
    pos_df = pd.read_csv(pos_log_path).copy()
    market = RQDataMarketGateway()

    trade_df = _map_symbol_to_order_book_id(trade_df)

    trade_df["side"] = trade_df["direction"].map({"Long": "buy", "Short": "sell"})
    if trade_df["side"].isna().any():
        bad = trade_df.loc[trade_df["side"].isna(), "direction"].astype(str).unique().tolist()
        raise ValueError(f"unsupported direction values: {bad}")

    trade_df["qty"] = pd.to_numeric(trade_df["volume"], errors="coerce").abs()
    trade_df["price"] = pd.to_numeric(trade_df["price"], errors="coerce")
    trade_df["trade_date"] = pd.to_datetime(trade_df["date_time"]).dt.normalize()

    trade_df["trade_time"] = trade_df["trade_date"] + pd.to_timedelta(trade_df.groupby("trade_date").cumcount(), unit="s")
    trade_df["offset"] = _infer_offset(trade_df)
    trade_df["trade_id"] = trade_df["trade_date"].dt.strftime("%Y%m%d") + "_" + (trade_df.index + 1).astype(str).str.zfill(4)
    trade_df["account"] = account
    trade_df["book_name"] = book_name
    trade_df["asset_type"] = "unknown"
    trade_df["multiplier"] = pd.NA
    trade_df["fee"] = pd.NA
    trade_df["currency"] = "CNY"
    trade_df["remark"] = trade_df["reference"].astype(str).where(trade_df["reference"].notna(), "")

    base_df = trade_df[TRADE_COLUMNS].copy()
    enriched = enrich_trade_records(base_df, market=market)
    option_fee_per_lot, option_multiplier = _derive_option_contract_basis(pos_df, market)
    normalized_fee = option_fee_per_lot / option_multiplier
    deriv_mask = enriched["asset_type"].isin(["Option", "Future"])
    enriched.loc[deriv_mask, "multiplier"] = option_multiplier
    enriched.loc[deriv_mask, "fee"] = enriched.loc[deriv_mask, "qty"].astype(float) * normalized_fee
    return enriched[TRADE_COLUMNS].copy()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset", default="MO")
    args = parser.parse_args()

    asset = str(args.asset).upper()
    trade_log = PROJECT_ROOT / "tests" / f"final_dynamic_collar_{asset}_trade_log.csv"
    pos_log = PROJECT_ROOT / "tests" / f"final_dynamic_collar_{asset}_pos_log.csv"
    output_path = PROJECT_ROOT / "tests" / f"final_dynamic_collar_{asset}_tradebook_trades.csv"

    transformed = transform_trade_log(
        trade_log,
        pos_log,
        book_name=f"dynamic_collar_{asset}",
    )
    transformed.to_csv(output_path, index=False)
    print(output_path)
    print(transformed.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
