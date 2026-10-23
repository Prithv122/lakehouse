"""Shared definitions: schema, layouts, ingest regimes, the fixed workload, metric rows.

Every other module builds on these. Changing anything here after the pre-registration commit
changes the experiment, so it needs a dated amendment in PREREGISTRATION.md first.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass, field
from typing import Literal

import pyarrow as pa

# --- data -------------------------------------------------------------------------------------

SEED = 35
START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
DAYS = 30
POINT_USER = 137

EVENT_NAMES = ("page_view", "search", "add_to_cart", "checkout", "purchase", "signup")
EVENT_WEIGHTS = (0.62, 0.06, 0.15, 0.08, 0.05, 0.04)
CHANNELS = ("organic", "paid_search", "social", "email", "referral")
CHANNEL_WEIGHTS = (0.40, 0.25, 0.18, 0.10, 0.07)
PLATFORMS = ("web", "ios", "android")
PLATFORM_WEIGHTS = (0.50, 0.22, 0.28)
COUNTRIES = ("IN", "US", "GB", "DE", "SG", "AE")
COUNTRY_WEIGHTS = (0.45, 0.20, 0.10, 0.10, 0.08, 0.07)
# Share of events per hour of day (UTC), low overnight, peaks at lunch and evening.
HOUR_WEIGHTS = (
    1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 7,
    8, 8, 7, 6, 6, 7, 8, 9, 9, 7, 4, 2,
)  # fmt: skip


def users_for(rows: int) -> int:
    """Number of distinct user ids the generator draws from (ids 1..users)."""
    return max(200, rows // 20)


ARROW_SCHEMA_V1 = pa.schema(
    [
        pa.field("event_id", pa.int64(), nullable=False),
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("event_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("event_name", pa.string(), nullable=False),
        pa.field("channel", pa.string(), nullable=False),
        pa.field("platform", pa.string(), nullable=False),
        pa.field("amount", pa.float64(), nullable=True),
    ]
)
ARROW_SCHEMA_V2 = ARROW_SCHEMA_V1.append(pa.field("country", pa.string(), nullable=True))

# Engines differ in nullability flags, field metadata and chunking. Results are compared
# after casting to this schema (all nullable, no metadata) and sorting by event_id.
CANONICAL_SCHEMA = pa.schema([pa.field(f.name, f.type, nullable=True) for f in ARROW_SCHEMA_V1])

# --- layouts and regimes ----------------------------------------------------------------------

Layout = Literal["L0", "L1", "L2", "L2s"]
Regime = Literal["B1", "B2", "B2c"]
Arrival = Literal["ordered", "shuffled"]
Engine = Literal["iceberg", "delta"]


@dataclass(frozen=True)
class LayoutSpec:
    name: str
    partition_by_day: bool
    sort_by_user: bool
    # None = the engine default (Iceberg write.target-file-size-bytes = 512 MiB).
    target_file_size_bytes: int | None
    primary: bool  # False = pre-registered sensitivity arm, not part of H1-H3's main reading
    description: str


LAYOUTS: dict[str, LayoutSpec] = {
    "L0": LayoutSpec("L0", False, False, None, True, "unpartitioned, arrival order"),
    "L1": LayoutSpec("L1", True, False, None, True, "partitioned by day(event_ts)"),
    "L2": LayoutSpec("L2", True, True, None, True, "L1 + each batch sorted by user_id"),
    "L2s": LayoutSpec(
        "L2s", True, True, 1024 * 1024, False, "L2 with a 1 MiB target file size"
    ),
}
PRIMARY_LAYOUTS = tuple(k for k, v in LAYOUTS.items() if v.primary)

# Sort key used for L2/L2s writes and their compaction rewrite.
SORT_KEY = (("user_id", "ascending"), ("event_ts", "ascending"), ("event_id", "ascending"))

LARGE_BATCHES = 10  # regime B1
TINY_BATCHES = 500  # regime B2 (B2c = B2 after compaction)

# --- workload ---------------------------------------------------------------------------------

Q_DAY = START + dt.timedelta(days=10)

Op = Literal["==", ">=", "<"]


@dataclass(frozen=True)
class Pred:
    column: str
    op: Op
    value: int | str | dt.datetime


@dataclass(frozen=True)
class Query:
    name: str
    description: str
    preds: tuple[Pred, ...]
    kind: Literal["rows", "agg"] = "rows"


def _day_range(start: dt.datetime, days: int) -> tuple[Pred, Pred]:
    return (
        Pred("event_ts", ">=", start),
        Pred("event_ts", "<", start + dt.timedelta(days=days)),
    )


QUERIES: tuple[Query, ...] = (
    Query("Q1_point_user", f"all events of user_id = {POINT_USER}", (Pred("user_id", "==", POINT_USER),)),
    Query("Q2_one_day", "all events on one day (day 10)", _day_range(Q_DAY, 1)),
    Query("Q3_seven_days", "all events over seven days (days 10-16)", _day_range(Q_DAY, 7)),
    Query("Q4_purchases", "all purchase events", (Pred("event_name", "==", "purchase"),)),
    Query(
        "Q5_user_week",
        f"user_id = {POINT_USER} over days 10-16",
        (Pred("user_id", "==", POINT_USER), *_day_range(Q_DAY, 7)),
    ),
    Query("Q6_full_agg", "count and summed amount (in cents) per event_name", (), kind="agg"),
)
QUERY_BY_NAME = {q.name: q for q in QUERIES}

# Q6's output: one row per event_name present, sorted by event_name.
# amount_cents = sum(round(amount * 100)) over non-null amounts, as int64, so the sum is exact
# whatever order an engine adds rows in.
AGG_SCHEMA = pa.schema(
    [
        pa.field("event_name", pa.string(), nullable=True),
        pa.field("events", pa.int64(), nullable=True),
        pa.field("amount_cents", pa.int64(), nullable=True),
    ]
)

# --- metrics ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class PlanMetrics:
    """What a scan of one query must touch, from table metadata only (no data read)."""

    files_total: int
    bytes_total: int
    records_total: int
    files_planned: int
    bytes_planned: int
    records_planned: int
    # Metadata objects: Iceberg manifests in the snapshot; Delta = None.
    manifests_total: int | None = None
    # Exploratory (no hypothesis): Parquet row groups in planned files whose footer min/max
    # could match the predicates. None when not computed.
    rowgroups_planned_files: int | None = None
    rowgroups_matching: int | None = None


@dataclass(frozen=True)
class MetricRow:
    engine: Engine
    layout: str
    regime: str
    arrival: str
    rows: int
    seed: int
    query: str
    snapshot: int  # Iceberg snapshot id, or Delta version
    files_total: int
    bytes_total: int
    records_total: int
    files_planned: int
    bytes_planned: int
    records_planned: int
    result_rows: int
    result_sha256: str
    manifests_total: int | None = None
    rowgroups_planned_files: int | None = None
    rowgroups_matching: int | None = None
    extra: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        extra = d.pop("extra")
        d.update({f"x_{k}": v for k, v in extra.items()})
        return d


METRIC_COLUMNS = tuple(f for f in MetricRow.__dataclass_fields__ if f != "extra")
