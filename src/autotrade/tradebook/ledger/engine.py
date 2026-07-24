from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .schema import EQUITY_COLUMNS, POSITION_COLUMNS, PRICE_COLUMNS, TRADE_COLUMNS

EPSILON = 1e-12


@dataclass
class RollResult:
    positions: pd.DataFrame
    cash_delta: float
    realized_pnl_delta: float
    fee_delta: float


def _ensure_columns(df: pd.DataFrame | None, columns: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)

    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = pd.NA
    return out[columns].copy()


def _empty_positions() -> pd.DataFrame:
    return pd.DataFrame(columns=POSITION_COLUMNS)


def validate_trades(trade_df: pd.DataFrame) -> list[str]:
    trades = _ensure_columns(trade_df, TRADE_COLUMNS)
    issues: list[str] = []

    if trades.empty:
        return issues

    if trades["trade_id"].isna().any():
        issues.append("trade_id contains null values")
    if trades["trade_id"].duplicated().any():
        dup_ids = trades.loc[trades["trade_id"].duplicated(), "trade_id"].astype(str).tolist()
        issues.append(f"duplicate trade_id found: {dup_ids}")

    for col in ["account", "book_name", "strategy", "opt_symbol", "order_book_id", "side", "offset"]:
        if trades[col].isna().any():
            issues.append(f"{col} contains null values")

    if not trades["side"].astype(str).str.lower().isin({"buy", "sell"}).all():
        bad = trades.loc[~trades["side"].astype(str).str.lower().isin({"buy", "sell"}), "side"].astype(str).unique()
        issues.append(f"unsupported side values: {sorted(bad.tolist())}")
    if not trades["offset"].astype(str).str.lower().isin({"open", "close", "roll"}).all():
        bad = trades.loc[
            ~trades["offset"].astype(str).str.lower().isin({"open", "close", "roll"}),
            "offset",
        ].astype(str).unique()
        issues.append(f"unsupported offset values: {sorted(bad.tolist())}")

    for col in ["qty", "price", "multiplier", "fee"]:
        numeric = pd.to_numeric(trades[col], errors="coerce")
        if numeric.isna().any():
            issues.append(f"{col} contains non-numeric values")

    if (pd.to_numeric(trades["qty"], errors="coerce") <= 0).any():
        issues.append("qty must be positive")
    if (pd.to_numeric(trades["price"], errors="coerce") < 0).any():
        issues.append("price must be non-negative")
    if (pd.to_numeric(trades["multiplier"], errors="coerce") <= 0).any():
        issues.append("multiplier must be positive")
    if (pd.to_numeric(trades["fee"], errors="coerce") < 0).any():
        issues.append("fee must be non-negative")

    return issues


def validate_prices(price_df: pd.DataFrame) -> list[str]:
    prices = _ensure_columns(price_df, PRICE_COLUMNS)
    issues: list[str] = []

    if prices.empty:
        return issues

    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    if prices["date"].isna().any():
        issues.append("price date contains invalid timestamps")
    if prices["order_book_id"].isna().any():
        issues.append("order_book_id contains null values")

    close_price = pd.to_numeric(prices["close_price"], errors="coerce")
    if close_price.isna().any():
        issues.append("close_price contains non-numeric values")
    if (close_price < 0).any():
        issues.append("close_price must be non-negative")

    duplicated = prices.duplicated(subset=["date", "order_book_id"])
    if duplicated.any():
        dup_keys = prices.loc[duplicated, ["date", "order_book_id"]].astype(str).agg("|".join, axis=1).tolist()
        issues.append(f"duplicate price snapshots found: {dup_keys}")

    return issues


def _standardize_positions(position_df: pd.DataFrame | None) -> pd.DataFrame:
    positions = _ensure_columns(position_df, POSITION_COLUMNS)
    if positions.empty:
        return positions

    for col in ["qty", "avg_cost", "cost_basis", "last_price", "market_value", "unrealized_pnl"]:
        positions[col] = pd.to_numeric(positions[col], errors="coerce")

    positions["last_trade_date"] = pd.to_datetime(positions["last_trade_date"], errors="coerce")
    positions["last_trade_time"] = pd.to_datetime(positions["last_trade_time"], errors="coerce")
    positions = positions.loc[positions["qty"].fillna(0.0).abs() > EPSILON].copy()
    return positions.reset_index(drop=True)


def _signed_qty(side: str, qty: float) -> float:
    side_lower = str(side).lower()
    if side_lower == "buy":
        return float(qty)
    if side_lower == "sell":
        return -float(qty)
    raise ValueError(f"unsupported side: {side}")


def roll_positions(
    trade_df: pd.DataFrame,
    pre_position_df: pd.DataFrame | None = None,
) -> RollResult:
    issues = validate_trades(trade_df)
    if issues:
        raise ValueError("; ".join(issues))

    trades = _ensure_columns(trade_df, TRADE_COLUMNS)
    if trades.empty:
        return RollResult(_standardize_positions(pre_position_df), 0.0, 0.0, 0.0)

    trades = trades.copy()
    trades["trade_date"] = pd.to_datetime(trades["trade_date"]).dt.normalize()
    trades["trade_time"] = pd.to_datetime(trades["trade_time"])
    trades["qty"] = pd.to_numeric(trades["qty"], errors="coerce")
    trades["price"] = pd.to_numeric(trades["price"], errors="coerce")
    trades["multiplier"] = pd.to_numeric(trades["multiplier"], errors="coerce")
    trades["fee"] = pd.to_numeric(trades["fee"], errors="coerce")
    trades = trades.sort_values(["trade_time", "trade_id"]).reset_index(drop=True)

    positions: dict[tuple[str, str, str], dict] = {}
    pre_positions = _standardize_positions(pre_position_df)
    for _, row in pre_positions.iterrows():
        key = (str(row["account"]), str(row["book_name"]), str(row["order_book_id"]))
        positions[key] = row.to_dict()

    cash_delta = 0.0
    realized_pnl_delta = 0.0
    fee_delta = float(trades["fee"].sum())

    for _, trade in trades.iterrows():
        key = (str(trade["account"]), str(trade["book_name"]), str(trade["order_book_id"]))
        signed_qty = _signed_qty(trade["side"], float(trade["qty"]))
        trade_price = float(trade["price"])
        multiplier = float(trade["multiplier"])
        fee = float(trade["fee"])
        cash_delta += -signed_qty * trade_price * multiplier - fee

        current = positions.get(key)
        if current is None:
            current = {
                "account": trade["account"],
                "book_name": trade["book_name"],
                "strategy": trade["strategy"],
                "opt_symbol": trade["opt_symbol"],
                "order_book_id": trade["order_book_id"],
                "asset_type": trade["asset_type"],
                "qty": 0.0,
                "avg_cost": 0.0,
                "cost_basis": 0.0,
                "last_trade_date": pd.NaT,
                "last_trade_time": pd.NaT,
                "last_price": pd.NA,
                "market_value": pd.NA,
                "unrealized_pnl": pd.NA,
            }

        old_qty = float(current["qty"])
        old_avg_cost = float(current["avg_cost"])

        if abs(old_qty) <= EPSILON:
            new_qty = signed_qty
            new_avg_cost = trade_price
        elif old_qty * signed_qty > 0:
            new_qty = old_qty + signed_qty
            new_avg_cost = (
                old_avg_cost * abs(old_qty) + trade_price * abs(signed_qty)
            ) / abs(new_qty)
        else:
            close_qty = min(abs(old_qty), abs(signed_qty))
            if old_qty > 0:
                realized = (trade_price - old_avg_cost) * close_qty * multiplier
            else:
                realized = (old_avg_cost - trade_price) * close_qty * multiplier
            realized_pnl_delta += realized

            new_qty = old_qty + signed_qty
            if abs(new_qty) <= EPSILON:
                positions.pop(key, None)
                continue
            if abs(signed_qty) < abs(old_qty):
                new_avg_cost = old_avg_cost
            else:
                new_avg_cost = trade_price

        current["qty"] = new_qty
        current["avg_cost"] = new_avg_cost
        current["cost_basis"] = new_avg_cost * new_qty * multiplier
        current["last_trade_date"] = trade["trade_date"]
        current["last_trade_time"] = trade["trade_time"]
        current["asset_type"] = trade["asset_type"]
        current["strategy"] = trade["strategy"]
        current["opt_symbol"] = trade["opt_symbol"]
        positions[key] = current

    position_df = pd.DataFrame(list(positions.values())) if positions else _empty_positions()
    position_df = _standardize_positions(position_df)
    if not position_df.empty:
        position_df = position_df.sort_values(["account", "book_name", "order_book_id"]).reset_index(drop=True)

    return RollResult(
        positions=position_df,
        cash_delta=float(cash_delta),
        realized_pnl_delta=float(realized_pnl_delta),
        fee_delta=float(fee_delta),
    )


def mark_positions_to_market(
    position_df: pd.DataFrame,
    price_df: pd.DataFrame,
    *,
    price_date: str | pd.Timestamp,
) -> pd.DataFrame:
    positions = _standardize_positions(position_df)
    if positions.empty:
        return positions

    price_issues = validate_prices(price_df)
    if price_issues:
        raise ValueError("; ".join(price_issues))

    prices = _ensure_columns(price_df, PRICE_COLUMNS)
    snapshot_date = pd.to_datetime(price_date).normalize()
    prices["date"] = pd.to_datetime(prices["date"]).dt.normalize()
    prices["close_price"] = pd.to_numeric(prices["close_price"], errors="coerce")
    prices = prices.loc[prices["date"] == snapshot_date, ["order_book_id", "close_price"]].copy()
    prices = prices.drop_duplicates(subset=["order_book_id"], keep="last")

    out = positions.merge(prices, how="left", on="order_book_id")
    out["last_price"] = out["close_price"]

    multiplier_lookup = (
        positions[["order_book_id", "cost_basis", "avg_cost", "qty"]]
        .assign(
            inferred_multiplier=lambda x: (
                pd.to_numeric(x["cost_basis"], errors="coerce").abs()
                / (
                    pd.to_numeric(x["avg_cost"], errors="coerce").abs()
                    * pd.to_numeric(x["qty"], errors="coerce").abs()
                )
            )
        )[["order_book_id", "inferred_multiplier"]]
    )
    out = out.merge(multiplier_lookup, how="left", on="order_book_id")

    out["market_value"] = out["qty"] * out["last_price"] * out["inferred_multiplier"]
    out["unrealized_pnl"] = (out["last_price"] - out["avg_cost"]) * out["qty"] * out["inferred_multiplier"]
    out = out.drop(columns=["close_price", "inferred_multiplier"])
    return out[POSITION_COLUMNS].copy()


def build_daily_snapshots(
    *,
    date: str | pd.Timestamp,
    trade_df: pd.DataFrame,
    pre_position_df: pd.DataFrame | None = None,
    price_df: pd.DataFrame | None = None,
    opening_cash: float = 0.0,
    opening_realized_pnl: float = 0.0,
    opening_fee: float = 0.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    roll = roll_positions(trade_df=trade_df, pre_position_df=pre_position_df)
    positions = roll.positions

    if price_df is not None and not positions.empty:
        positions = mark_positions_to_market(positions, price_df, price_date=date)

    snapshot_date = pd.to_datetime(date).normalize()
    cash = float(opening_cash) + roll.cash_delta
    realized_pnl = float(roll.realized_pnl_delta)
    realized_pnl_cum = float(opening_realized_pnl) + roll.realized_pnl_delta
    fee = float(roll.fee_delta)
    fee_cum = float(opening_fee) + roll.fee_delta

    if positions.empty:
        unrealized_pnl = 0.0
        market_value = 0.0
        gross_exposure = 0.0
        net_exposure = 0.0
    else:
        unrealized_pnl = float(pd.to_numeric(positions["unrealized_pnl"], errors="coerce").fillna(0.0).sum())
        market_value = float(pd.to_numeric(positions["market_value"], errors="coerce").fillna(0.0).sum())
        gross_exposure = float(pd.to_numeric(positions["market_value"], errors="coerce").abs().fillna(0.0).sum())
        net_exposure = market_value

    daily_pnl = realized_pnl + unrealized_pnl
    pnl_total_cum = realized_pnl_cum + unrealized_pnl

    if trade_df is not None and not trade_df.empty:
        account = str(trade_df.iloc[-1]["account"])
        book_name = str(trade_df.iloc[-1]["book_name"])
        strategy = str(trade_df.iloc[-1]["strategy"])
        opt_symbol = str(trade_df.iloc[-1]["opt_symbol"])
    elif positions is not None and not positions.empty:
        account = str(positions.iloc[-1]["account"])
        book_name = str(positions.iloc[-1]["book_name"])
        strategy = str(positions.iloc[-1]["strategy"])
        opt_symbol = str(positions.iloc[-1]["opt_symbol"])
    else:
        account = ""
        book_name = ""
        strategy = ""
        opt_symbol = ""

    equity_row = {
        "account": account,
        "book_name": book_name,
        "strategy": strategy,
        "opt_symbol": opt_symbol,
        "date": snapshot_date,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "daily_pnl": daily_pnl,
        "fee": fee,
        "realized_pnl_cum": realized_pnl_cum,
        "pnl_total_cum": pnl_total_cum,
        "fee_cum": fee_cum,
        "cash": cash,
        "market_value": market_value,
        "nav": cash + market_value,
        "gross_exposure": gross_exposure,
        "net_exposure": net_exposure,
    }
    equity_df = pd.DataFrame([equity_row], columns=EQUITY_COLUMNS)
    return positions, equity_df


def replay_ledger(
    trade_df: pd.DataFrame,
    price_df: pd.DataFrame,
    *,
    initial_cash: float = 0.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    trade_issues = validate_trades(trade_df)
    if trade_issues:
        raise ValueError("; ".join(trade_issues))
    price_issues = validate_prices(price_df)
    if price_issues:
        raise ValueError("; ".join(price_issues))

    trades = _ensure_columns(trade_df, TRADE_COLUMNS)
    trades["trade_date"] = pd.to_datetime(trades["trade_date"]).dt.normalize()
    prices = _ensure_columns(price_df, PRICE_COLUMNS)
    prices["date"] = pd.to_datetime(prices["date"]).dt.normalize()

    all_dates = sorted(set(trades["trade_date"].tolist()) | set(prices["date"].tolist()))
    positions = _empty_positions()
    equity_frames: list[pd.DataFrame] = []
    position_frames: list[pd.DataFrame] = []
    cash = float(initial_cash)
    realized_pnl_cum = 0.0
    fee_cum = 0.0

    for date in all_dates:
        day_trades = trades.loc[trades["trade_date"] == date].copy()
        day_prices = prices.loc[prices["date"] == date].copy()
        positions, equity = build_daily_snapshots(
            date=date,
            trade_df=day_trades,
            pre_position_df=positions,
            price_df=day_prices,
            opening_cash=cash,
            opening_realized_pnl=realized_pnl_cum,
            opening_fee=fee_cum,
        )
        cash = float(equity.iloc[0]["cash"])
        realized_pnl_cum = float(equity.iloc[0]["realized_pnl_cum"])
        fee_cum = float(equity.iloc[0]["fee_cum"])

        if not positions.empty:
            day_positions = positions.copy()
            day_positions.insert(0, "date", pd.to_datetime(date).normalize())
            position_frames.append(day_positions)
        equity_frames.append(equity)

    position_history = pd.concat(position_frames, ignore_index=True) if position_frames else pd.DataFrame()
    equity_history = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame(columns=EQUITY_COLUMNS)
    return position_history, equity_history
