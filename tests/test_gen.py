import datetime as dt
from collections import Counter

import pyarrow.compute as pc

from lakehouse import spec
from lakehouse.gen import generate_events, generate_v2_events

ROWS = 5_000


def test_exact_schema_and_row_count():
    t = generate_events(ROWS)
    assert t.schema.equals(spec.ARROW_SCHEMA_V1)
    assert t.num_rows == ROWS
    assert not t.schema.field("event_id").nullable


def test_deterministic_per_seed():
    a = generate_events(ROWS, seed=7)
    assert a.equals(generate_events(ROWS, seed=7))
    assert not a.equals(generate_events(ROWS, seed=8))


def test_default_seed_is_spec_seed():
    assert generate_events(300).equals(generate_events(300, seed=spec.SEED))


def test_event_id_follows_time_order():
    t = generate_events(ROWS)
    assert t["event_id"].to_pylist() == list(range(ROWS))
    ts = t["event_ts"].to_pylist()
    assert ts == sorted(ts)


def test_timestamps_inside_window():
    t = generate_events(ROWS)
    lo = pc.min(t["event_ts"]).as_py()
    hi = pc.max(t["event_ts"]).as_py()
    assert lo >= spec.START
    assert hi < spec.START + dt.timedelta(days=spec.DAYS)


def test_every_day_is_hit_and_hours_are_diurnal():
    t = generate_events(20_000)
    days = {x.day for x in t["event_ts"].to_pylist()}
    assert len(days) == spec.DAYS
    hours = Counter(x.hour for x in t["event_ts"].to_pylist())
    # the pre-registered weights make 19:00 nine times as busy as 03:00
    assert hours[19] > 3 * hours[3]


def test_amount_only_on_purchases_and_positive_two_decimals():
    t = generate_events(ROWS)
    names = t["event_name"].to_pylist()
    amounts = t["amount"].to_pylist()
    assert "purchase" in names
    for name, amount in zip(names, amounts, strict=True):
        if name == "purchase":
            assert amount is not None
            assert amount > 0
            assert round(amount, 2) == amount
        else:
            assert amount is None


def test_users_in_range_with_heavy_tail():
    t = generate_events(ROWS)
    users = t["user_id"].to_pylist()
    assert min(users) >= 1
    assert max(users) <= spec.users_for(ROWS)
    counts = Counter(users)
    assert counts.most_common(1)[0][0] == 1
    assert counts[1] > counts[spec.POINT_USER]
    assert counts[1] > ROWS * 0.05


def test_categories_come_from_spec():
    t = generate_events(ROWS)
    assert set(t["event_name"].to_pylist()) <= set(spec.EVENT_NAMES)
    assert set(t["channel"].to_pylist()) <= set(spec.CHANNELS)
    assert set(t["platform"].to_pylist()) <= set(spec.PLATFORMS)
    share = Counter(t["event_name"].to_pylist())["page_view"] / ROWS
    assert abs(share - spec.EVENT_WEIGHTS[0]) < 0.03


def test_v2_schema_country_and_ids():
    t = generate_v2_events(1_000, seed=spec.SEED, first_event_id=ROWS)
    assert t.schema.equals(spec.ARROW_SCHEMA_V2)
    assert t["event_id"].to_pylist() == list(range(ROWS, ROWS + 1_000))
    assert set(t["country"].to_pylist()) <= set(spec.COUNTRIES)
    assert t["country"].null_count == 0
    ts = t["event_ts"].to_pylist()
    assert ts == sorted(ts)
    assert ts[0] >= spec.START and ts[-1] < spec.START + dt.timedelta(days=spec.DAYS)


def test_v2_is_deterministic_and_uses_a_different_stream_than_v1():
    a = generate_v2_events(500, 3, 0)
    assert a.equals(generate_v2_events(500, 3, 0))
    # same seed, different stream: v1 at seed 3 draws other users than v2 at seed 3
    assert not generate_events(500, seed=3)["user_id"].equals(a["user_id"])
    # the stream is seed + 1, so v2's shared columns match v1 drawn at seed + 1
    shared = generate_events(500, seed=4)
    for name in spec.ARROW_SCHEMA_V1.names:
        if name != "event_id":
            assert shared[name].equals(a[name])
