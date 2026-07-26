"""Split and validate the local MO option panel with the Autotrade runtime.

This is a manual integration script, not a pytest test. It reads the source
pickle without modifying it, splits the wide panel into instrument, market and
analytics frames, routes the three streams through ``DataManager``, updates
``SecurityManager`` and assembles one ``OptionPanelView`` per analytics date.

Examples:

    PYTHONPATH=src python tests/manual_validate_mo_pipeline.py --max-dates 5
    PYTHONPATH=src python tests/manual_validate_mo_pipeline.py
    PYTHONPATH=src python tests/manual_validate_mo_pipeline.py \
        --output-dir /tmp/mo_split
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from autotrade.backtest.data import (
    DataManager,
    DataRoutingConfig,
    OptionAnalyticsReader,
    OptionStateReader,
    TradeBarReader,
)
from autotrade.coreutils.constant import Exchange, Interval
from autotrade.engine.security_manager import SecurityManager
from autotrade.strategy import OptionPanelAssembler


DEFAULT_MO_PATH = Path(
    "/home/buzheng/Desktop/cfutures/rq_data/opt_greek_underlying/MO.pkl"
)

INSTRUMENT_COLUMNS = (
    "order_book_id",
    "underlying_order_book_id",
    "maturity_date",
    "strike_price",
    "option_type",
    "exercise_type",
    "contract_multiplier",
)

MARKET_COLUMNS = (
    "order_book_id",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "total_turnover",
    "open_interest",
)

ANALYTICS_COLUMNS = (
    "order_book_id",
    "date",
    "underlying_order_book_id",
    "underlying_close",
    "forward_price",
    "r",
    "T_days",
    "iv",
    "delta",
    "gamma",
    "vega",
    "theta",
    "rho",
    "vanna",
    "vomma",
    "charm",
)


@dataclass(frozen=True, slots=True)
class SplitFrames:
    instruments: pd.DataFrame
    market: pd.DataFrame
    analytics: pd.DataFrame


@dataclass(frozen=True, slots=True)
class ValidationSummary:
    source_rows: int
    dates: int
    instrument_rows: int
    market_rows: int
    analytics_rows: int
    panels: int
    panel_contracts: int
    min_contracts_per_panel: int
    max_contracts_per_panel: int
    non_null_surface_iv: int
    non_null_delta: int
    first_date: str
    last_date: str


def load_mo_panel(
    path: Path,
    *,
    max_dates: int | None = None,
) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"MO panel does not exist: {path}")
    panel = pd.read_pickle(path)
    required = set(
        INSTRUMENT_COLUMNS + MARKET_COLUMNS + ANALYTICS_COLUMNS
    )
    missing = sorted(required - set(panel.columns))
    if missing:
        raise ValueError(f"MO panel is missing required columns: {missing!r}")
    panel = panel.copy()
    panel["date"] = pd.to_datetime(panel["date"])
    panel["maturity_date"] = pd.to_datetime(panel["maturity_date"])
    if max_dates is not None:
        if max_dates <= 0:
            raise ValueError("max_dates must be positive")
        selected = (
            panel["date"]
            .drop_duplicates()
            .sort_values()
            .head(max_dates)
        )
        panel = panel.loc[panel["date"].isin(selected)].copy()
    return panel.sort_values(["date", "order_book_id"]).reset_index(drop=True)


def split_mo_panel(panel: pd.DataFrame) -> SplitFrames:
    duplicate_keys = panel.duplicated(["date", "order_book_id"])
    if duplicate_keys.any():
        sample = panel.loc[
            duplicate_keys,
            ["date", "order_book_id"],
        ].head(5)
        raise ValueError(
            "MO panel contains duplicate (date, order_book_id) rows: "
            f"{sample.to_dict('records')!r}"
        )

    static_counts = (
        panel.groupby("order_book_id", sort=False)[
            list(INSTRUMENT_COLUMNS[1:])
        ]
        .nunique(dropna=False)
    )
    conflicts = static_counts.gt(1).any(axis=1)
    if conflicts.any():
        raise ValueError(
            "MO panel contains conflicting instrument definitions for "
            f"{int(conflicts.sum())} contracts"
        )

    instruments = (
        panel.loc[:, INSTRUMENT_COLUMNS]
        .drop_duplicates("order_book_id")
        .sort_values("order_book_id")
        .reset_index(drop=True)
    )
    market = (
        panel.loc[:, MARKET_COLUMNS]
        .sort_values(["date", "order_book_id"])
        .reset_index(drop=True)
    )
    analytics = (
        panel.loc[:, ANALYTICS_COLUMNS]
        .copy()
        .sort_values(["date", "order_book_id"])
        .reset_index(drop=True)
    )
    analytics["time_to_expiry"] = analytics["T_days"] / 365.0
    return SplitFrames(
        instruments=instruments,
        market=market,
        analytics=analytics,
    )


def build_data_manager(frames: SplitFrames) -> DataManager:
    manager = DataManager(
        DataRoutingConfig(
            strategy_data_names={
                "option_daily",
                "mo_black76_legacy_v1",
            },
            security_data_names={
                "option_instruments",
                "option_daily",
            },
            valuation_data_names={
                "option_daily",
            },
        )
    )
    manager.add_data(
        "option_instruments",
        OptionStateReader(
            schema={
                "instrument_id": "order_book_id",
                "underlying_instrument_id":
                    "underlying_order_book_id",
                "expiry": "maturity_date",
                "strike": "strike_price",
                "right": "option_type",
                "style": "exercise_type",
                "multiplier": "contract_multiplier",
            }
        ).read(
            frames.instruments,
            exchange=Exchange.CFFEX,
        ),
    )
    manager.add_data(
        "option_daily",
        TradeBarReader(
            schema={
                "instrument_id": "order_book_id",
                "time": "date",
                "turnover": "total_turnover",
            }
        ).read(
            frames.market,
            interval=Interval.K_DAY,
            exchange=Exchange.CFFEX,
        ),
    )
    manager.add_data(
        "mo_black76_legacy_v1",
        OptionAnalyticsReader(
            schema={
                "instrument_id": "order_book_id",
                "time": "date",
                "underlying_instrument_id":
                    "underlying_order_book_id",
                "underlying_price": "underlying_close",
                "risk_free_rate": "r",
                "surface_iv": "iv",
            }
        ).read(
            frames.analytics,
            model_id="black76_grid",
            model_version="legacy_v1",
            exchange=Exchange.CFFEX,
        ),
    )
    return manager


def validate_pipeline(
    source: pd.DataFrame,
    frames: SplitFrames,
) -> ValidationSummary:
    security_manager = SecurityManager()
    manager = build_data_manager(frames)
    panel_sizes = []
    panel_contracts = 0
    non_null_surface_iv = 0
    non_null_delta = 0

    for time_slice in manager.stream():
        security_manager.on_timeslice(time_slice)
        analytics = time_slice.slice.option_analytics.get(
            "mo_black76_legacy_v1"
        )
        if not analytics:
            continue

        panel = OptionPanelAssembler.build(
            security_manager,
            analytics,
        )
        if panel is None:
            raise AssertionError("non-empty analytics produced no panel")
        frame = panel.to_frame()
        expected = len(analytics)
        if len(panel.contracts) != expected or len(frame) != expected:
            raise AssertionError(
                f"{time_slice.time}: analytics={expected}, "
                f"contracts={len(panel.contracts)}, frame={len(frame)}"
            )
        if not all(
            view.security.time == time_slice.time
            for view in panel.contracts.values()
        ):
            raise AssertionError(
                f"{time_slice.time}: panel contains stale Security market state"
            )

        panel_sizes.append(expected)
        panel_contracts += expected
        non_null_surface_iv += int(frame["surface_iv"].notna().sum())
        non_null_delta += int(frame["delta"].notna().sum())

    expected_dates = int(source["date"].nunique())
    if len(panel_sizes) != expected_dates:
        raise AssertionError(
            f"expected {expected_dates} panels, got {len(panel_sizes)}"
        )
    if panel_contracts != len(frames.analytics):
        raise AssertionError(
            f"expected {len(frames.analytics)} assembled contracts, "
            f"got {panel_contracts}"
        )

    dates = source["date"]
    return ValidationSummary(
        source_rows=len(source),
        dates=expected_dates,
        instrument_rows=len(frames.instruments),
        market_rows=len(frames.market),
        analytics_rows=len(frames.analytics),
        panels=len(panel_sizes),
        panel_contracts=panel_contracts,
        min_contracts_per_panel=min(panel_sizes),
        max_contracts_per_panel=max(panel_sizes),
        non_null_surface_iv=non_null_surface_iv,
        non_null_delta=non_null_delta,
        first_date=str(dates.min().date()),
        last_date=str(dates.max().date()),
    )


def write_split_frames(
    frames: SplitFrames,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    frames.instruments.to_pickle(output_dir / "mo_instruments.pkl")
    frames.market.to_pickle(output_dir / "mo_market.pkl")
    frames.analytics.to_pickle(output_dir / "mo_analytics.pkl")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the Autotrade option pipeline with MO.pkl"
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_MO_PATH,
        help="source MO.pkl path",
    )
    parser.add_argument(
        "--max-dates",
        type=int,
        default=None,
        help="validate only the first N dates; default validates all dates",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="optionally write the three split DataFrames as pickle files",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = load_mo_panel(args.path, max_dates=args.max_dates)
    frames = split_mo_panel(source)
    if args.output_dir is not None:
        write_split_frames(frames, args.output_dir)
    summary = validate_pipeline(source, frames)
    print("MO option pipeline validation passed")
    for key, value in asdict(summary).items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
