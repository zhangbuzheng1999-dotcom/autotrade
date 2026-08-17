"""Generate Reader-compatible synthetic market data without external downloads.

Examples
--------
Generate 100 synthetic futures and one month of daily bars::

    python -m example.generate_synthetic_data \
        --kind futures --num-futures 100 --frequency 1d --periods 22

Generate one 100-strike call/put option chain, 60 one-minute observations,
and its synthetic Greeks::

    python -m example.generate_synthetic_data \
        --kind all --num-strikes 100 --maturities 1 \
        --frequency 1m --periods 60

The generated files use the canonical column names accepted by the current
Readers.  They are suitable for pipeline, strategy, reporting, and UI tests;
they are deliberately not a substitute for real-market calibration or Greek
validation.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd


DatasetKind = Literal["futures", "options", "greeks", "all"]
OutputFormat = Literal["xlsx", "pickle", "csv"]
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "data"

_PANDAS_FREQUENCIES = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "1d": "1D",
}


@dataclass(frozen=True, slots=True)
class SyntheticDataConfig:
    """Parameters controlling synthetic Reader-compatible data generation."""

    kind: DatasetKind = "all"
    start: str = "2025-01-02 09:30:00"
    frequency: str = "1m"
    periods: int = 60
    num_futures: int = 1
    num_underlyings: int = 1
    num_strikes: int = 10
    maturities: int = 1
    base_price: float = 5_000.0
    seed: int = 7
    exchange: str = "CFFEX"
    multiplier: float = 100.0
    margin_rate: float = 0.12
    risk_free_rate: float = 0.02

    def validate(self) -> None:
        if self.kind not in {"futures", "options", "greeks", "all"}:
            raise ValueError(f"unsupported kind: {self.kind!r}")
        if self.frequency not in _PANDAS_FREQUENCIES:
            raise ValueError(
                f"unsupported frequency {self.frequency!r}; "
                f"choose one of {sorted(_PANDAS_FREQUENCIES)!r}"
            )
        for name in ("periods", "num_futures", "num_underlyings", "num_strikes", "maturities"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.num_underlyings > self.num_futures:
            raise ValueError("num_underlyings cannot exceed num_futures")
        for name in ("base_price", "multiplier"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.margin_rate < 0 or self.risk_free_rate < 0:
            raise ValueError("margin_rate and risk_free_rate must be non-negative")


def generate_synthetic_frames(config: SyntheticDataConfig) -> dict[str, pd.DataFrame]:
    """Return standard DataFrames keyed by their intended data-source name.

    Returned frames are sorted by their Reader-relevant keys and can be passed
    directly to ``FutureStateReader``, ``OptionStateReader``,
    ``TradeBarReader``, and ``OptionAnalyticsReader``.
    """

    config.validate()
    times = pd.date_range(
        start=pd.Timestamp(config.start),
        periods=config.periods,
        freq=_PANDAS_FREQUENCIES[config.frequency],
    )
    rng = np.random.default_rng(config.seed)
    future_instruments, future_bars, future_prices = _generate_futures(config, times, rng)
    frames: dict[str, pd.DataFrame] = {
        "future_instruments": future_instruments,
        "future_bars": future_bars,
    }

    if config.kind == "futures":
        return frames

    option_instruments, option_bars, option_analytics = _generate_options(
        config,
        times,
        future_prices,
        rng,
    )
    frames["option_instruments"] = option_instruments
    if config.kind in {"options", "all"}:
        frames["option_bars"] = option_bars
    if config.kind in {"greeks", "all"}:
        frames["option_analytics"] = option_analytics
    return frames


def write_synthetic_dataset(
    config: SyntheticDataConfig,
    output_dir: str | Path | None = None,
    *,
    output_format: OutputFormat = "xlsx",
) -> dict[str, Path]:
    """Generate and write data files plus a reproducibility manifest.

    When ``output_dir`` is omitted, files are written to ``example/data`` so
    examples can create their own local, disposable input data on demand.
    """

    if output_format not in {"xlsx", "pickle", "csv"}:
        raise ValueError("output_format must be 'xlsx', 'pickle', or 'csv'")
    target = Path(output_dir) if output_dir is not None else DEFAULT_OUTPUT_DIR
    target.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for name, frame in generate_synthetic_frames(config).items():
        suffix = {"xlsx": "xlsx", "pickle": "pkl", "csv": "csv"}[output_format]
        path = target / f"{name}.{suffix}"
        if output_format == "pickle":
            frame.to_pickle(path)
        elif output_format == "xlsx":
            frame.to_excel(path, index=False)
        else:
            frame.to_csv(path, index=False)
        written[name] = path

    manifest = {
        "generator": "example.generate_synthetic_data",
        "config": asdict(config),
        "files": {name: path.name for name, path in written.items()},
    }
    manifest_path = target / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    written["manifest"] = manifest_path
    return written


def _generate_futures(
    config: SyntheticDataConfig,
    times: pd.DatetimeIndex,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, np.ndarray]]:
    start = times[0]
    end = times[-1]
    instruments = []
    bars = []
    paths: dict[str, np.ndarray] = {}

    for index in range(config.num_futures):
        instrument_id = f"SYNF{index + 1:03d}"
        root_id = f"SYN{index + 1:03d}"
        instruments.append(
            {
                "instrument_id": instrument_id,
                "list_date": start - pd.Timedelta(days=30),
                "delist_date": end + pd.Timedelta(days=365),
                "multiplier": config.multiplier,
                "margin_rate": config.margin_rate,
                "commission_rate": 0.00002,
                "expiry": end + pd.Timedelta(days=365),
                "root_instrument_id": root_id,
                "exchange": config.exchange,
                "synthetic": True,
            }
        )
        initial = config.base_price * (1.0 + index * 0.04)
        path = _price_path(initial, len(times), rng)
        paths[instrument_id] = path
        previous_close = initial
        for when, close in zip(times, path, strict=True):
            open_ = previous_close
            spread = max(close * abs(rng.normal(0.0, 0.0015)), 0.01)
            high = max(open_, close) + spread
            low = max(min(open_, close) - spread, 0.01)
            volume = int(rng.integers(100, 10_000))
            bars.append(
                {
                    "instrument_id": instrument_id,
                    "time": when,
                    "open": round(open_, 6),
                    "high": round(high, 6),
                    "low": round(low, 6),
                    "close": round(close, 6),
                    "volume": volume,
                    "turnover": round(volume * close * config.multiplier, 6),
                    "open_interest": int(rng.integers(500, 50_000)),
                    "exchange": config.exchange,
                }
            )
            previous_close = close

    return (
        pd.DataFrame(instruments).sort_values("instrument_id").reset_index(drop=True),
        pd.DataFrame(bars).sort_values(["time", "instrument_id"]).reset_index(drop=True),
        paths,
    )


def _generate_options(
    config: SyntheticDataConfig,
    times: pd.DatetimeIndex,
    future_prices: dict[str, np.ndarray],
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    instruments = []
    bars = []
    analytics = []
    first_time = times[0]
    last_time = times[-1]
    underlyings = list(future_prices)[: config.num_underlyings]

    for underlying_index, underlying_id in enumerate(underlyings):
        prices = future_prices[underlying_id]
        reference = float(prices[0])
        strike_offsets = np.linspace(-0.30, 0.30, config.num_strikes)
        strikes = np.round(reference * (1.0 + strike_offsets), 2)
        for maturity_index in range(config.maturities):
            expiry = last_time + pd.Timedelta(days=30 * (maturity_index + 1))
            for strike in strikes:
                for right in ("C", "P"):
                    instrument_id = (
                        f"SYNOPT{underlying_index + 1:02d}"
                        f"{expiry:%y%m}{right}{int(round(strike * 100)):07d}"
                    )
                    instruments.append(
                        {
                            "instrument_id": instrument_id,
                            "list_date": first_time - pd.Timedelta(days=7),
                            "delist_date": expiry,
                            "multiplier": config.multiplier,
                            "margin_rate": config.margin_rate,
                            "commission_rate": 0.00003,
                            "underlying_instrument_id": underlying_id,
                            "expiry": expiry,
                            "strike": float(strike),
                            "right": right,
                            "style": "E",
                            "exchange": config.exchange,
                            "synthetic": True,
                        }
                    )
                    previous_price = None
                    for when, forward in zip(times, prices, strict=True):
                        ttm = max((expiry - when).total_seconds() / (365.0 * 86_400.0), 1 / 3650)
                        smile = 0.18 + 0.20 * abs(math.log(float(strike) / float(forward)))
                        surface_iv = max(smile + rng.normal(0.0, 0.003), 0.05)
                        values = _black76_greeks(
                            forward=float(forward),
                            strike=float(strike),
                            time_to_expiry=ttm,
                            volatility=surface_iv,
                            risk_free_rate=config.risk_free_rate,
                            right=right,
                        )
                        close = max(values["price"] * (1.0 + rng.normal(0.0, 0.002)), 0.01)
                        open_ = close if previous_price is None else previous_price
                        spread = max(close * abs(rng.normal(0.0, 0.01)), 0.01)
                        volume = int(rng.integers(0, 800))
                        bars.append(
                            {
                                "instrument_id": instrument_id,
                                "time": when,
                                "open": round(open_, 6),
                                "high": round(max(open_, close) + spread, 6),
                                "low": round(max(min(open_, close) - spread, 0.001), 6),
                                "close": round(close, 6),
                                "volume": volume,
                                "turnover": round(volume * close * config.multiplier, 6),
                                "open_interest": int(rng.integers(0, 20_000)),
                                "exchange": config.exchange,
                            }
                        )
                        analytics.append(
                            {
                                "instrument_id": instrument_id,
                                "time": when,
                                "value": round(close, 6),
                                "underlying_instrument_id": underlying_id,
                                "underlying_price": round(float(forward), 6),
                                "forward_price": round(float(forward), 6),
                                "risk_free_rate": config.risk_free_rate,
                                "time_to_expiry": ttm,
                                "market_iv": surface_iv,
                                "surface_iv": surface_iv,
                                "delta": values["delta"],
                                "gamma": values["gamma"],
                                "vega": values["vega"],
                                "theta": values["theta"],
                                "rho": values["rho"],
                                "vanna": values["vanna"],
                                "vomma": values["vomma"],
                                "charm": values["charm"],
                                "model_id": "synthetic_black76",
                                "model_version": "v1",
                                "exchange": config.exchange,
                            }
                        )
                        previous_price = close

    return (
        pd.DataFrame(instruments).sort_values("instrument_id").reset_index(drop=True),
        pd.DataFrame(bars).sort_values(["time", "instrument_id"]).reset_index(drop=True),
        pd.DataFrame(analytics).sort_values(["time", "instrument_id"]).reset_index(drop=True),
    )


def _price_path(initial: float, periods: int, rng: np.random.Generator) -> np.ndarray:
    returns = rng.normal(loc=0.0, scale=0.004, size=periods)
    returns[0] = 0.0
    return initial * np.exp(np.cumsum(returns))


def _black76_greeks(
    *,
    forward: float,
    strike: float,
    time_to_expiry: float,
    volatility: float,
    risk_free_rate: float,
    right: str,
) -> dict[str, float]:
    """Return smooth, internally consistent Black-76-like synthetic Greeks."""

    sqrt_t = math.sqrt(time_to_expiry)
    sigma_sqrt_t = volatility * sqrt_t
    d1 = (math.log(forward / strike) + 0.5 * volatility * volatility * time_to_expiry) / sigma_sqrt_t
    d2 = d1 - sigma_sqrt_t
    discount = math.exp(-risk_free_rate * time_to_expiry)
    nd1 = _normal_cdf(d1)
    nd2 = _normal_cdf(d2)
    pdf_d1 = _normal_pdf(d1)

    if right == "C":
        price = discount * (forward * nd1 - strike * nd2)
        delta = discount * nd1
    elif right == "P":
        price = discount * (strike * _normal_cdf(-d2) - forward * _normal_cdf(-d1))
        delta = -discount * _normal_cdf(-d1)
    else:
        raise ValueError(f"unsupported option right: {right!r}")

    gamma = discount * pdf_d1 / (forward * sigma_sqrt_t)
    vega = discount * forward * pdf_d1 * sqrt_t
    theta = -discount * forward * pdf_d1 * volatility / (2.0 * sqrt_t) + risk_free_rate * price
    rho = -time_to_expiry * price
    vanna = -discount * pdf_d1 * d2 / volatility
    vomma = vega * d1 * d2 / volatility
    charm = -discount * pdf_d1 * (2 * risk_free_rate * time_to_expiry - d2 * sigma_sqrt_t) / (
        2 * time_to_expiry * sigma_sqrt_t
    )
    return {
        "price": price,
        "delta": delta,
        "gamma": gamma,
        "vega": vega,
        "theta": theta,
        "rho": rho,
        "vanna": vanna,
        "vomma": vomma,
        "charm": charm,
    }


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _normal_pdf(value: float) -> float:
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=("futures", "options", "greeks", "all"), default="all")
    parser.add_argument("--start", default="2025-01-02 09:30:00")
    parser.add_argument("--frequency", choices=tuple(_PANDAS_FREQUENCIES), default="1m")
    parser.add_argument("--periods", type=int, default=60)
    parser.add_argument("--num-futures", type=int, default=1)
    parser.add_argument("--num-underlyings", type=int, default=1)
    parser.add_argument("--num-strikes", type=int, default=10, help="strikes per underlying and maturity; both C/P are generated")
    parser.add_argument("--maturities", type=int, default=1)
    parser.add_argument("--base-price", type=float, default=5_000.0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--exchange", default="CFFEX")
    parser.add_argument("--multiplier", type=float, default=100.0)
    parser.add_argument("--margin-rate", type=float, default=0.12)
    parser.add_argument("--risk-free-rate", type=float, default=0.02)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="directory for generated files (default: example/data)",
    )
    parser.add_argument("--format", choices=("xlsx", "pickle", "csv"), default="xlsx")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = SyntheticDataConfig(
        kind=args.kind,
        start=args.start,
        frequency=args.frequency,
        periods=args.periods,
        num_futures=args.num_futures,
        num_underlyings=args.num_underlyings,
        num_strikes=args.num_strikes,
        maturities=args.maturities,
        base_price=args.base_price,
        seed=args.seed,
        exchange=args.exchange,
        multiplier=args.multiplier,
        margin_rate=args.margin_rate,
        risk_free_rate=args.risk_free_rate,
    )
    files = write_synthetic_dataset(config, args.output_dir, output_format=args.format)
    print(f"generated {len(files) - 1} data files in {args.output_dir}")
    for name, path in files.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
