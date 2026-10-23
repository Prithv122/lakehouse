"""Seeded synthetic event generator (vectorised; 5,000,000 rows in a few seconds).

Rows are sorted by `event_ts` (stable, so ties keep draw order) and `event_id` numbers them in
that order, so "ordered arrival" in the experiment means "in time order".
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from lakehouse import spec

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
_START_US = (spec.START - _EPOCH) // dt.timedelta(microseconds=1)
_US_PER_HOUR = 3_600 * 1_000_000
_US_PER_DAY = 24 * _US_PER_HOUR

ZIPF_EXPONENT = 1.1
AMOUNT_LOG_MEAN = 7.0
AMOUNT_LOG_SIGMA = 0.8


@dataclass(frozen=True)
class _Columns:
    """Unsorted draws for one batch of events, as numpy arrays of equal length."""

    ts_us: np.ndarray
    user_id: np.ndarray
    event_name: np.ndarray  # indices into spec.EVENT_NAMES
    channel: np.ndarray
    platform: np.ndarray
    amount: np.ndarray
    country: np.ndarray


def _probabilities(weights: tuple[float, ...] | tuple[int, ...]) -> np.ndarray:
    w = np.asarray(weights, dtype=np.float64)
    return w / w.sum()


def _choice(rng: np.random.Generator, weights: tuple, rows: int) -> np.ndarray:
    """Weighted draw of category indices via the inverse CDF."""
    cdf = np.cumsum(_probabilities(weights))
    cdf[-1] = 1.0
    return np.searchsorted(cdf, rng.random(rows), side="right").astype(np.int64)


def _timestamps_us(rng: np.random.Generator, rows: int) -> np.ndarray:
    day = rng.integers(0, spec.DAYS, size=rows, dtype=np.int64)
    hour = _choice(rng, spec.HOUR_WEIGHTS, rows)
    within = rng.integers(0, _US_PER_HOUR, size=rows, dtype=np.int64)
    return _START_US + day * _US_PER_DAY + hour * _US_PER_HOUR + within


def _user_ids(rng: np.random.Generator, rows: int, users: int) -> np.ndarray:
    ranks = np.arange(1, users + 1, dtype=np.float64)
    cdf = np.cumsum(ranks**-ZIPF_EXPONENT)
    cdf /= cdf[-1]
    cdf[-1] = 1.0
    return np.searchsorted(cdf, rng.random(rows), side="right").astype(np.int64) + 1


def _amounts(rng: np.random.Generator, rows: int, is_purchase: np.ndarray) -> np.ndarray:
    raw = rng.lognormal(AMOUNT_LOG_MEAN, AMOUNT_LOG_SIGMA, size=rows)
    amount = np.maximum(np.round(raw, 2), 0.01)
    return np.where(is_purchase, amount, np.nan)


def _draw(rows: int, seed: int) -> _Columns:
    if rows < 1:
        raise ValueError("rows must be >= 1")
    rng = np.random.default_rng(seed)
    event_name = _choice(rng, spec.EVENT_WEIGHTS, rows)
    purchase = spec.EVENT_NAMES.index("purchase")
    return _Columns(
        ts_us=_timestamps_us(rng, rows),
        user_id=_user_ids(rng, rows, spec.users_for(rows)),
        event_name=event_name,
        channel=_choice(rng, spec.CHANNEL_WEIGHTS, rows),
        platform=_choice(rng, spec.PLATFORM_WEIGHTS, rows),
        amount=_amounts(rng, rows, event_name == purchase),
        country=_choice(rng, spec.COUNTRY_WEIGHTS, rows),
    )


def _strings(names: tuple[str, ...], idx: np.ndarray) -> pa.Array:
    return pa.array(names, type=pa.string()).take(pa.array(idx))


def _to_table(cols: _Columns, first_event_id: int, schema: pa.Schema) -> pa.Table:
    order = np.argsort(cols.ts_us, kind="stable")
    rows = len(order)
    arrays = {
        "event_id": pa.array(np.arange(first_event_id, first_event_id + rows, dtype=np.int64)),
        "user_id": pa.array(cols.user_id[order]),
        "event_ts": pa.array(
            cols.ts_us[order].view("datetime64[us]"), type=pa.timestamp("us", tz="UTC")
        ),
        "event_name": _strings(spec.EVENT_NAMES, cols.event_name[order]),
        "channel": _strings(spec.CHANNELS, cols.channel[order]),
        "platform": _strings(spec.PLATFORMS, cols.platform[order]),
        "amount": pa.array(cols.amount[order], from_pandas=True),
        "country": _strings(spec.COUNTRIES, cols.country[order]),
    }
    return pa.Table.from_arrays([arrays[f.name] for f in schema], schema=schema)


def generate_events(rows: int, seed: int = spec.SEED) -> pa.Table:
    """`rows` events in schema v1, deterministic for (rows, seed), event_id = 0..rows-1."""
    return _to_table(_draw(rows, seed), 0, spec.ARROW_SCHEMA_V1)


def generate_v2_events(rows: int, seed: int, first_event_id: int) -> pa.Table:
    """Schema-v2 events (adds `country`) from an independent stream (`seed + 1`).

    Same distributions and the same 30-day window as v1; ids start at `first_event_id`.
    """
    return _to_table(_draw(rows, seed + 1), first_event_id, spec.ARROW_SCHEMA_V2)
