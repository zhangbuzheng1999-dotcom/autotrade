from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from autotrade.tradebook.market.rqdata import RQDataMarketGateway
from autotrade.tradebook.service.rebuild_service import LedgerRebuildService
from autotrade.tradebook.storage.in_memory import InMemoryLedgerStorage

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _safe_mean_abs(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.abs().mean())


def _safe_max_abs(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.abs().max())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset", default="MO")
    args = parser.parse_args()

    asset = str(args.asset).upper()
    base = PROJECT_ROOT / "tests"
    trade_path = base / f"final_dynamic_collar_{asset}_tradebook_trades.csv"
    pos_log_path = base / f"final_dynamic_collar_{asset}_pos_log.csv"
    account_log_path = base / f"final_dynamic_collar_{asset}_account_log.csv"

    position_out = base / f"final_dynamic_collar_{asset}_positions_daily.csv"
    equity_out = base / f"final_dynamic_collar_{asset}_equity_daily.csv"
    equity_cmp_out = base / f"final_dynamic_collar_{asset}_equity_compare.csv"
    position_cmp_out = base / f"final_dynamic_collar_{asset}_position_compare.csv"
    summary_out = base / f"final_dynamic_collar_{asset}_compare_summary.json"

    trade_df = pd.read_csv(trade_path)
    account_log = pd.read_csv(account_log_path).rename(columns={"Unnamed: 0": "date"})
    pos_log = pd.read_csv(pos_log_path)
    compare_multiplier = float(pd.to_numeric(trade_df["multiplier"], errors="coerce").dropna().iloc[0])

    storage = InMemoryLedgerStorage(trade_df=trade_df)
    service = LedgerRebuildService(storage=storage, market=RQDataMarketGateway())
    positions, equity = service.rebuild_history(
        account="opt",
        book_name=f"dynamic_collar_{asset}",
        initial_cash=0.0,
        persist=False,
    )

    positions = positions.copy()
    equity = equity.copy()
    positions["date"] = pd.to_datetime(positions["date"]).dt.normalize()
    equity["date"] = pd.to_datetime(equity["date"]).dt.normalize()

    positions.to_csv(position_out, index=False)
    equity.to_csv(equity_out, index=False)

    account_log["date"] = pd.to_datetime(account_log["date"]).dt.normalize()
    equity_cmp = equity.merge(
        account_log[["date", "cash", "realized_pnl", "unrealized_pnl", "equity", "margin", "cost"]],
        on="date",
        how="outer",
        suffixes=("_new", "_old"),
    )
    equity_cmp["realized_pnl_scaled"] = pd.to_numeric(equity_cmp["realized_pnl_cum"], errors="coerce") / compare_multiplier
    equity_cmp["unrealized_pnl_scaled"] = pd.to_numeric(equity_cmp["unrealized_pnl_new"], errors="coerce") / compare_multiplier
    equity_cmp["equity_pnl_scaled"] = equity_cmp["realized_pnl_scaled"] + equity_cmp["unrealized_pnl_scaled"]
    equity_cmp["equity_pnl_old"] = pd.to_numeric(equity_cmp["realized_pnl"], errors="coerce") + pd.to_numeric(equity_cmp["unrealized_pnl_old"], errors="coerce")
    equity_cmp["realized_pnl_diff"] = equity_cmp["realized_pnl_scaled"] - pd.to_numeric(equity_cmp["realized_pnl"], errors="coerce")
    equity_cmp["unrealized_pnl_diff"] = equity_cmp["unrealized_pnl_scaled"] - pd.to_numeric(equity_cmp["unrealized_pnl_old"], errors="coerce")
    equity_cmp["equity_pnl_diff"] = equity_cmp["equity_pnl_scaled"] - equity_cmp["equity_pnl_old"]
    equity_cmp["realized_pnl_scaled_diff1"] = equity_cmp["realized_pnl_scaled"].diff()
    equity_cmp["realized_pnl_old_diff1"] = pd.to_numeric(equity_cmp["realized_pnl"], errors="coerce").diff()
    equity_cmp["realized_pnl_diff1_diff"] = equity_cmp["realized_pnl_scaled_diff1"] - equity_cmp["realized_pnl_old_diff1"]
    equity_cmp["unrealized_pnl_scaled_diff1"] = equity_cmp["unrealized_pnl_scaled"].diff()
    equity_cmp["unrealized_pnl_old_diff1"] = pd.to_numeric(equity_cmp["unrealized_pnl_old"], errors="coerce").diff()
    equity_cmp["unrealized_pnl_diff1_diff"] = equity_cmp["unrealized_pnl_scaled_diff1"] - equity_cmp["unrealized_pnl_old_diff1"]
    equity_cmp["equity_pnl_scaled_diff1"] = equity_cmp["equity_pnl_scaled"].diff()
    equity_cmp["equity_pnl_old_diff1"] = equity_cmp["equity_pnl_old"].diff()
    equity_cmp["equity_pnl_diff1_diff"] = equity_cmp["equity_pnl_scaled_diff1"] - equity_cmp["equity_pnl_old_diff1"]
    equity_cmp.to_csv(equity_cmp_out, index=False)

    original_positions = pos_log.copy()
    original_positions["date"] = pd.to_datetime(original_positions["date"]).dt.normalize()
    original_positions["cmp_order_book_id"] = original_positions["order_book_id"]
    future_mask = original_positions["asset_type"].astype(str).str.lower().eq("underlying")
    original_positions["cmp_asset_type"] = original_positions["asset_type"].map({"underlying": "Future", "option": "Option"}).fillna(original_positions["asset_type"])
    original_cmp = pd.DataFrame(
        {
            "date": original_positions["date"],
            "order_book_id": original_positions["cmp_order_book_id"],
            "asset_type": original_positions["cmp_asset_type"],
            "qty_old": pd.to_numeric(original_positions["pos"], errors="coerce"),
            "avg_cost_old": pd.to_numeric(original_positions["hold_price"], errors="coerce"),
        }
    )
    positions_cmp = positions.copy()
    positions_cmp.loc[positions_cmp["asset_type"].astype(str).str.lower().eq("future"), "order_book_id"] = (
        positions_cmp.loc[positions_cmp["asset_type"].astype(str).str.lower().eq("future"), "order_book_id"]
        .astype(str)
        .str.replace(r"888$", "", regex=True)
    )
    position_cmp = positions_cmp.merge(
        original_cmp,
        on=["date", "order_book_id", "asset_type"],
        how="outer",
    )
    position_cmp["qty_diff"] = pd.to_numeric(position_cmp["qty"], errors="coerce") - pd.to_numeric(position_cmp["qty_old"], errors="coerce")
    position_cmp["avg_cost_diff"] = pd.to_numeric(position_cmp["avg_cost"], errors="coerce") - pd.to_numeric(position_cmp["avg_cost_old"], errors="coerce")
    position_cmp.to_csv(position_cmp_out, index=False)

    summary = {
        "trade_rows": int(len(trade_df)),
        "position_rows_new": int(len(positions)),
        "position_rows_old": int(len(pos_log)),
        "equity_rows_new": int(len(equity)),
        "equity_rows_old": int(len(account_log)),
        "equity_metrics": {
            "realized_pnl_mean_abs_diff": _safe_mean_abs(equity_cmp["realized_pnl_diff"]),
            "realized_pnl_max_abs_diff": _safe_max_abs(equity_cmp["realized_pnl_diff"]),
            "unrealized_pnl_mean_abs_diff": _safe_mean_abs(equity_cmp["unrealized_pnl_diff"]),
            "unrealized_pnl_max_abs_diff": _safe_max_abs(equity_cmp["unrealized_pnl_diff"]),
            "equity_pnl_mean_abs_diff": _safe_mean_abs(equity_cmp["equity_pnl_diff"]),
            "equity_pnl_max_abs_diff": _safe_max_abs(equity_cmp["equity_pnl_diff"]),
            "realized_pnl_diff1_mean_abs_diff": _safe_mean_abs(equity_cmp["realized_pnl_diff1_diff"]),
            "realized_pnl_diff1_max_abs_diff": _safe_max_abs(equity_cmp["realized_pnl_diff1_diff"]),
            "unrealized_pnl_diff1_mean_abs_diff": _safe_mean_abs(equity_cmp["unrealized_pnl_diff1_diff"]),
            "unrealized_pnl_diff1_max_abs_diff": _safe_max_abs(equity_cmp["unrealized_pnl_diff1_diff"]),
            "equity_pnl_diff1_mean_abs_diff": _safe_mean_abs(equity_cmp["equity_pnl_diff1_diff"]),
            "equity_pnl_diff1_max_abs_diff": _safe_max_abs(equity_cmp["equity_pnl_diff1_diff"]),
        },
        "position_metrics": {
            "matched_rows": int(position_cmp[["qty", "qty_old"]].notna().all(axis=1).sum()),
            "qty_mean_abs_diff": _safe_mean_abs(position_cmp["qty_diff"]),
            "qty_max_abs_diff": _safe_max_abs(position_cmp["qty_diff"]),
            "avg_cost_mean_abs_diff": _safe_mean_abs(position_cmp["avg_cost_diff"]),
            "avg_cost_max_abs_diff": _safe_max_abs(position_cmp["avg_cost_diff"]),
        },
        "notes": [
            f"equity comparison ignores cash and initial equity, and compares pnl levels/increments after dividing by multiplier={compare_multiplier}",
            "equity comparison uses realized_pnl + unrealized_pnl instead of cash/nav",
            "position comparison maps new future continuous ids like IM888 back to root ids like IM",
        ],
    }

    summary_out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(position_out)
    print(equity_out)
    print(equity_cmp_out)
    print(position_cmp_out)
    print(summary_out)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
