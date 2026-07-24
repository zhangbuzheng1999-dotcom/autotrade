from __future__ import annotations

import json
import os
import subprocess
from textwrap import dedent

import pandas as pd

from ..ledger.schema import PRICE_COLUMNS
from ..storage.schema import INSTRUMENT_COLUMNS

from .base import MarketDataGateway


_RQDATA_WORKER = dedent(
    """
    import json
    import os
    import pandas as pd
    import rqdatac as rq

    def _serialize_scalar(value):
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                pass
        return value

    def _serialize_records(df):
        records = []
        for row in df.to_dict(orient="records"):
            clean = {}
            for key, value in row.items():
                if pd.isna(value):
                    clean[key] = None
                else:
                    clean[key] = _serialize_scalar(value)
            records.append(clean)
        return records

    def _normalize_prices(raw, order_book_ids):
        if raw is None:
            return pd.DataFrame(columns=["date", "order_book_id", "close_price"])
        if isinstance(raw, pd.Series):
            raw = raw.to_frame(name="close")
        frame = raw.reset_index()
        if "order_book_id" not in frame.columns:
            if len(order_book_ids) != 1:
                raise ValueError("missing order_book_id in rq.get_price result")
            frame["order_book_id"] = order_book_ids[0]
        if "date" not in frame.columns and "datetime" in frame.columns:
            frame["date"] = pd.to_datetime(frame["datetime"]).dt.normalize()
        frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
        if "close" not in frame.columns:
            value_cols = [c for c in frame.columns if c not in {"date", "datetime", "order_book_id"}]
            if len(value_cols) != 1:
                raise ValueError(f"unexpected rq.get_price columns: {frame.columns.tolist()}")
            frame = frame.rename(columns={value_cols[0]: "close"})
        frame = frame[["date", "order_book_id", "close"]].rename(columns={"close": "close_price"})
        return frame.sort_values(["date", "order_book_id"]).reset_index(drop=True)

    def _normalize_instruments(inst_obj):
        if inst_obj is None:
            return pd.DataFrame(columns=[
                "order_book_id", "symbol", "name", "asset_type", "exchange",
                "currency", "multiplier", "underlying_order_book_id",
                "expiry_date", "strike", "option_type", "is_active", "remark",
            ])
        if isinstance(inst_obj, (list, tuple)):
            inst_list = list(inst_obj)
        else:
            inst_list = [inst_obj]

        rows = []
        for inst in inst_list:
            rows.append({
                "order_book_id": getattr(inst, "order_book_id", None),
                "symbol": getattr(inst, "symbol", None),
                "name": getattr(inst, "abbrev_symbol", None) or getattr(inst, "symbol", None),
                "asset_type": getattr(inst, "type", None),
                "exchange": str(getattr(inst, "exchange", None)) if getattr(inst, "exchange", None) is not None else None,
                "currency": getattr(inst, "trading_currency", None) or getattr(inst, "settlement_currency", None) or "CNY",
                "multiplier": getattr(inst, "contract_multiplier", None) or getattr(inst, "round_lot", None) or 1,
                "underlying_order_book_id": getattr(inst, "underlying_order_book_id", None),
                "expiry_date": getattr(inst, "maturity_date", None),
                "strike": getattr(inst, "strike_price", None),
                "option_type": getattr(inst, "option_type", None),
                "is_active": getattr(inst, "status", None) != "Delisted",
                "remark": None,
            })
        return pd.DataFrame(rows)

    payload = json.loads(os.environ["RQDATA_PAYLOAD"])
    rq.init()

    if payload["action"] == "get_prices":
        raw = rq.get_price(
            payload["order_book_ids"],
            start_date=payload["start_date"],
            end_date=payload["end_date"],
            fields=["close"],
        )
        out = _normalize_prices(raw, payload["order_book_ids"])
    elif payload["action"] == "get_instruments":
        raw = rq.instruments(payload["order_book_ids"])
        out = _normalize_instruments(raw)
    else:
        raise ValueError(f"unsupported action: {payload['action']}")

    print(json.dumps(_serialize_records(out), ensure_ascii=False))
    """
)


class RQDataMarketGateway(MarketDataGateway):
    def __init__(self, *, conda_env: str = "rq_data"):
        self.conda_env = conda_env

    def _run_worker(self, payload: dict) -> list[dict]:
        env = os.environ.copy()
        env["RQDATA_PAYLOAD"] = json.dumps(payload, ensure_ascii=False)
        proc = subprocess.run(
            ["conda", "run", "-n", self.conda_env, "python", "-c", _RQDATA_WORKER],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                "RQData worker failed\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}"
            )
        stdout = proc.stdout.strip()
        if not stdout:
            return []
        return json.loads(stdout)

    def get_prices(
        self,
        *,
        start_date: str | pd.Timestamp,
        end_date: str | pd.Timestamp,
        order_book_ids: list[str] | None = None,
    ) -> pd.DataFrame:
        if not order_book_ids:
            return pd.DataFrame(columns=PRICE_COLUMNS)
        records = self._run_worker(
            {
                "action": "get_prices",
                "order_book_ids": [str(x) for x in order_book_ids],
                "start_date": str(pd.to_datetime(start_date).date()),
                "end_date": str(pd.to_datetime(end_date).date()),
            }
        )
        prices = pd.DataFrame(records)
        if prices.empty:
            return pd.DataFrame(columns=PRICE_COLUMNS)
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.normalize()
        prices["close_price"] = pd.to_numeric(prices["close_price"], errors="coerce")
        for col in PRICE_COLUMNS:
            if col not in prices.columns:
                prices[col] = pd.NA
        return prices[PRICE_COLUMNS].sort_values(["date", "order_book_id"]).reset_index(drop=True)

    def get_instruments(
        self,
        *,
        order_book_ids: list[str],
    ) -> pd.DataFrame:
        if not order_book_ids:
            return pd.DataFrame(columns=INSTRUMENT_COLUMNS)
        records = self._run_worker(
            {
                "action": "get_instruments",
                "order_book_ids": [str(x) for x in order_book_ids],
            }
        )
        instruments = pd.DataFrame(records)
        if instruments.empty:
            return pd.DataFrame(columns=INSTRUMENT_COLUMNS)
        if "expiry_date" in instruments.columns:
            instruments["expiry_date"] = pd.to_datetime(instruments["expiry_date"], errors="coerce")
        for col in INSTRUMENT_COLUMNS:
            if col not in instruments.columns:
                instruments[col] = pd.NA
        return instruments[INSTRUMENT_COLUMNS].sort_values("order_book_id").reset_index(drop=True)
