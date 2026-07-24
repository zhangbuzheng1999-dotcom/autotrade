from __future__ import annotations

import pandas as pd

from ..ledger.schema import EQUITY_COLUMNS, POSITION_COLUMNS, TRADE_COLUMNS

from .base import LedgerStorage


def _with_equity_aliases(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    if "book_name" in out.columns and "strategy" not in out.columns:
        out["strategy"] = out["book_name"]
    if "fee_cum" in out.columns and "fee" not in out.columns:
        out["fee"] = pd.NA
    if "realized_pnl_cum" in out.columns and "realized_pnl" not in out.columns:
        out["realized_pnl"] = pd.NA
    if "daily_pnl" not in out.columns and "pnl_total" in out.columns:
        out["daily_pnl"] = out["pnl_total"]
    return out


def _filter_book(
    df: pd.DataFrame,
    *,
    account: str | None = None,
    book_name: str | None = None,
    start_date: str | pd.Timestamp | None = None,
    end_date: str | pd.Timestamp | None = None,
    date_col: str,
) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out

    if account is not None:
        out = out.loc[out["account"].astype(str) == str(account)]
    if book_name is not None:
        out = out.loc[out["book_name"].astype(str) == str(book_name)]

    if start_date is not None:
        start = pd.to_datetime(start_date).normalize()
        out = out.loc[pd.to_datetime(out[date_col]).dt.normalize() >= start]
    if end_date is not None:
        end = pd.to_datetime(end_date).normalize()
        out = out.loc[pd.to_datetime(out[date_col]).dt.normalize() <= end]
    return out.reset_index(drop=True)


class InMemoryLedgerStorage(LedgerStorage):
    def __init__(
        self,
        *,
        trade_df: pd.DataFrame | None = None,
        position_df: pd.DataFrame | None = None,
        equity_df: pd.DataFrame | None = None,
    ):
        self.trade_df = pd.DataFrame(columns=TRADE_COLUMNS) if trade_df is None else trade_df.copy()
        self.position_df = pd.DataFrame(columns=["date"] + POSITION_COLUMNS) if position_df is None else position_df.copy()
        self.equity_df = pd.DataFrame(columns=EQUITY_COLUMNS) if equity_df is None else equity_df.copy()

    def seed_trades(self, trade_df: pd.DataFrame) -> None:
        self.trade_df = trade_df.copy()

    def save_trades(
        self,
        *,
        trade_df: pd.DataFrame,
        overwrite: bool = False,
    ) -> None:
        payload = trade_df.copy()
        for col in TRADE_COLUMNS:
            if col not in payload.columns:
                payload[col] = pd.NA
        payload = payload[TRADE_COLUMNS].copy()

        if payload.empty:
            return

        if overwrite or self.trade_df.empty:
            self.trade_df = payload.reset_index(drop=True)
            return

        existing = self.trade_df.copy()
        if "trade_id" in existing.columns and "trade_id" in payload.columns:
            existing = existing.loc[~existing["trade_id"].astype(str).isin(payload["trade_id"].astype(str))].copy()
        self.trade_df = pd.concat([existing, payload], ignore_index=True)

    def load_trades(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        trades = self.trade_df.copy()
        if trades.empty:
            return pd.DataFrame(columns=TRADE_COLUMNS)
        for col in TRADE_COLUMNS:
            if col not in trades.columns:
                trades[col] = pd.NA
        trades = trades[TRADE_COLUMNS].copy()
        return _filter_book(
            trades,
            account=account,
            book_name=book_name,
            start_date=start_date,
            end_date=end_date,
            date_col="trade_date",
        )

    def load_latest_positions(
        self,
        *,
        account: str,
        book_name: str,
        before_date: str | pd.Timestamp,
    ) -> pd.DataFrame:
        positions = self.load_positions(account=account, book_name=book_name, end_date=before_date)
        if positions.empty:
            return pd.DataFrame(columns=POSITION_COLUMNS)

        cutoff = pd.to_datetime(before_date).normalize()
        positions["date"] = pd.to_datetime(positions["date"]).dt.normalize()
        positions = positions.loc[positions["date"] < cutoff]
        if positions.empty:
            return pd.DataFrame(columns=POSITION_COLUMNS)

        last_date = positions["date"].max()
        return positions.loc[positions["date"] == last_date, POSITION_COLUMNS].reset_index(drop=True)

    def load_positions(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        positions = self.position_df.copy()
        if positions.empty:
            return pd.DataFrame(columns=["date"] + POSITION_COLUMNS)
        cols = ["date"] + POSITION_COLUMNS
        for col in cols:
            if col not in positions.columns:
                positions[col] = pd.NA
        positions = positions[cols].copy()
        return _filter_book(
            positions,
            account=account,
            book_name=book_name,
            start_date=start_date,
            end_date=end_date,
            date_col="date",
        )

    def save_positions(
        self,
        *,
        date: str | pd.Timestamp,
        position_df: pd.DataFrame,
        overwrite: bool = True,
    ) -> None:
        snapshot_date = pd.to_datetime(date).normalize()
        payload = position_df.copy()
        if "date" not in payload.columns:
            payload.insert(0, "date", snapshot_date)
        else:
            payload["date"] = snapshot_date

        cols = ["date"] + POSITION_COLUMNS
        for col in cols:
            if col not in payload.columns:
                payload[col] = pd.NA
        payload = payload[cols].copy()

        if overwrite and not self.position_df.empty:
            existing = self.position_df.copy()
            existing["date"] = pd.to_datetime(existing["date"]).dt.normalize()
            mask = existing["date"] != snapshot_date
            self.position_df = pd.concat([existing.loc[mask], payload], ignore_index=True)
        else:
            self.position_df = pd.concat([self.position_df, payload], ignore_index=True)

    def load_equity(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        equity = self.equity_df.copy()
        if equity.empty:
            return pd.DataFrame(columns=EQUITY_COLUMNS)
        equity = _with_equity_aliases(equity)
        for col in EQUITY_COLUMNS:
            if col not in equity.columns:
                equity[col] = pd.NA
        equity = equity[EQUITY_COLUMNS].copy()
        return _filter_book(
            equity,
            account=account,
            book_name=book_name,
            start_date=start_date,
            end_date=end_date,
            date_col="date",
        )

    def save_equity(
        self,
        *,
        date: str | pd.Timestamp,
        equity_df: pd.DataFrame,
        overwrite: bool = True,
    ) -> None:
        snapshot_date = pd.to_datetime(date).normalize()
        payload = _with_equity_aliases(equity_df.copy())
        payload["date"] = snapshot_date
        for col in EQUITY_COLUMNS:
            if col not in payload.columns:
                payload[col] = pd.NA
        payload = payload[EQUITY_COLUMNS].copy()

        if overwrite and not self.equity_df.empty:
            existing = self.equity_df.copy()
            existing["date"] = pd.to_datetime(existing["date"]).dt.normalize()
            mask = existing["date"] != snapshot_date
            self.equity_df = pd.concat([existing.loc[mask], payload], ignore_index=True)
        else:
            self.equity_df = pd.concat([self.equity_df, payload], ignore_index=True)
