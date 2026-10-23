"""Decision rules, rendering and CSV round trip of lakehouse.report, on hand-made rows."""

from __future__ import annotations

import dataclasses

import pytest

from lakehouse import report, spec
from lakehouse.report import Gate

ALL_PASS = [Gate(n, True, "ok") for n in ("G1", "G2", "G3", "G4")]
QNAMES = [q.name for q in spec.QUERIES]


def _planned(layout: str, query: str) -> tuple[int, int]:
    """(files_planned, bytes_planned) of the baseline grid: every hypothesis supported."""
    if layout == "L0":
        return 10, 10_000
    day_query = query in ("Q2_one_day", "Q3_seven_days")
    if layout == "L1":
        return (3, 3_000) if day_query else (10, 10_000)
    if layout == "L2":
        if query in ("Q1_point_user", "Q5_user_week"):
            return 4, 4_000
        return (3, 3_000) if day_query else (10, 10_000)
    # L2s
    if query in ("Q1_point_user", "Q5_user_week"):
        return 2, 2_000
    return (3, 3_000) if day_query else (10, 10_000)


def make_row(
    layout: str,
    regime: str,
    arrival: str,
    query: str,
    *,
    engine: str = "iceberg",
    files_total: int | None = None,
    **over,
) -> spec.MetricRow:
    fp, bp = _planned(layout, query)
    if files_total is None:
        files_total = {"B1": 10, "B2": 500, "B2c": 10}[regime]
    base = {
        "engine": engine,
        "layout": layout,
        "regime": regime,
        "arrival": arrival,
        "rows": 1000,
        "seed": 35,
        "query": query,
        "snapshot": 1,
        "files_total": files_total,
        "bytes_total": 100_000,
        "records_total": 1000,
        "files_planned": fp,
        "bytes_planned": bp,
        "records_planned": 100,
        "result_rows": 10,
        "result_sha256": "ab" * 32,
        "manifests_total": 2,
    }
    base.update(over)
    return spec.MetricRow(**base)


def make_grid(
    arrivals=report.ARRIVALS, layouts=("L0", "L1", "L2", "L2s"), engine="iceberg"
) -> list[spec.MetricRow]:
    return [
        make_row(lay, reg, arr, q, engine=engine)
        for arr in arrivals
        for reg in report.REGIMES
        for lay in layouts
        for q in QNAMES
    ]


def edit(rows, changes: dict, **where) -> list[spec.MetricRow]:
    out = []
    for r in rows:
        if all(getattr(r, k) == v for k, v in where.items()):
            r = dataclasses.replace(r, **changes)
        out.append(r)
    return out


def get(verdicts, hyp: str, scope: str) -> report.Verdict:
    (v,) = [v for v in verdicts if v.hypothesis == hyp and v.scope == scope]
    return v


# --- helpers ----------------------------------------------------------------------------------


def test_ratio_and_fmt_bytes():
    assert report.ratio(10, 4) == 2.5
    assert report.ratio(1, 0) is None
    assert report.fmt_bytes(0) == "0 B"
    assert report.fmt_bytes(1023) == "1023 B"
    assert report.fmt_bytes(1024) == "1.0 KiB"
    assert report.fmt_bytes(1536 * 1024) == "1.5 MiB"
    assert report.fmt_bytes(5 * 1024**3) == "5.0 GiB"
    assert report.fmt_bytes(None) == "n/a"


# --- H1 ---------------------------------------------------------------------------------------


def test_baseline_all_supported():
    v = report.evaluate(make_grid(), ALL_PASS)
    for arrival in report.ARRIVALS:
        assert get(v, "H1", arrival).status == "supported"
        assert get(v, "H2", arrival).status == "supported"
        assert get(v, "H2-L2s", arrival).status == "supported"
        for layout in spec.PRIMARY_LAYOUTS:
            assert get(v, "H3", f"{layout}/{arrival}").status == "supported"


def test_h1_not_supported_when_files_only_improve():
    rows = edit(make_grid(), {"bytes_planned": 10_000}, layout="L1", regime="B1")
    v = report.evaluate(rows, ALL_PASS)
    assert get(v, "H1", "ordered").status == "not supported"
    assert get(v, "H1", "shuffled").status == "not supported"


def test_h1_not_supported_when_bytes_only_improve():
    rows = edit(make_grid(), {"files_planned": 10}, layout="L1", regime="B1")
    assert get(report.evaluate(rows, ALL_PASS), "H1", "ordered").status == "not supported"


def test_h1_equal_counts_are_not_supported():
    rows = edit(
        make_grid(),
        {"files_planned": 10, "bytes_planned": 10_000},
        layout="L1",
        regime="B1",
        arrival="ordered",
    )
    v = report.evaluate(rows, ALL_PASS)
    assert get(v, "H1", "ordered").status == "not supported"
    assert get(v, "H1", "shuffled").status == "supported"


def test_h1_one_query_passes_other_fails():
    rows = edit(
        make_grid(),
        {"files_planned": 10, "bytes_planned": 10_000},
        layout="L1",
        regime="B1",
        arrival="shuffled",
        query="Q3_seven_days",
    )
    v = report.evaluate(rows, ALL_PASS)
    h1 = get(v, "H1", "shuffled")
    assert h1.status == "not supported"
    assert "Q2_one_day" in h1.evidence
    assert "Q3_seven_days" in h1.evidence
    assert get(v, "H1", "ordered").status == "supported"


# --- H2 ---------------------------------------------------------------------------------------


def test_h2_equal_files_not_supported_but_l2s_reported_separately():
    rows = edit(make_grid(), {"files_planned": 10}, layout="L2", regime="B1")
    v = report.evaluate(rows, ALL_PASS)
    assert get(v, "H2", "ordered").status == "not supported"
    assert get(v, "H2-L2s", "ordered").status == "supported"


def test_h2_one_query_fails():
    rows = edit(make_grid(), {"files_planned": 10}, layout="L2", regime="B1", query="Q5_user_week")
    v = report.evaluate(rows, ALL_PASS)
    h2 = get(v, "H2", "ordered")
    assert h2.status == "not supported"
    assert "Q1_point_user: files 4 vs 10 -> pass" in h2.evidence
    assert "Q5_user_week: files 10 vs 10 -> fail" in h2.evidence


def test_h2_ignores_bytes():
    rows = edit(make_grid(), {"bytes_planned": 99_999}, layout="L2", regime="B1")
    assert get(report.evaluate(rows, ALL_PASS), "H2", "ordered").status == "supported"


def test_h2_uses_b1_only():
    rows = edit(make_grid(), {"files_planned": 99}, layout="L2", regime="B2")
    assert get(report.evaluate(rows, ALL_PASS), "H2", "ordered").status == "supported"


# --- H3 ---------------------------------------------------------------------------------------


def _h3_rows(b2: int, b2c: int):
    rows = edit(make_grid(), {"files_total": b2}, layout="L1", regime="B2", arrival="ordered")
    return edit(rows, {"files_total": b2c}, layout="L1", regime="B2c", arrival="ordered")


def test_h3_boundary_exactly_ten_is_supported():
    v = report.evaluate(_h3_rows(1000, 100), ALL_PASS)
    assert get(v, "H3", "L1/ordered").status == "supported"
    assert "10.00x" in get(v, "H3", "L1/ordered").evidence


def test_h3_just_below_ten_is_not_supported():
    v = report.evaluate(_h3_rows(999, 100), ALL_PASS)
    assert get(v, "H3", "L1/ordered").status == "not supported"
    # Other cells are untouched.
    assert get(v, "H3", "L0/ordered").status == "supported"


def test_h3_g4_failed_is_invalid():
    gates = [Gate("G4", False, "dup rows"), *[Gate(n, True, "ok") for n in ("G1", "G2", "G3")]]
    v = report.evaluate(make_grid(), gates)
    assert get(v, "H3", "L1/ordered").status == "invalid"
    assert "G4" in get(v, "H3", "L1/ordered").evidence


def test_h3_inconsistent_files_total_is_invalid():
    rows = edit(
        make_grid(),
        {"files_total": 501},
        layout="L0",
        regime="B2",
        arrival="shuffled",
        query="Q4_purchases",
    )
    v = report.evaluate(rows, ALL_PASS)
    h3 = get(v, "H3", "L0/shuffled")
    assert h3.status == "invalid"
    assert "inconsistent files_total" in h3.evidence
    assert get(v, "H3", "L0/ordered").status == "supported"


def test_h3_zero_files_after_compaction_not_evaluated():
    rows = edit(make_grid(), {"files_total": 0}, layout="L0", regime="B2c", arrival="ordered")
    assert get(report.evaluate(rows, ALL_PASS), "H3", "L0/ordered").status == "not evaluated"


# --- gates ------------------------------------------------------------------------------------


@pytest.mark.parametrize("failing", ["G1", "G2", "G3", "G4"])
def test_any_failed_gate_invalidates_everything(failing):
    gates = [Gate(n, n != failing, "x") for n in ("G1", "G2", "G3", "G4")]
    v = report.evaluate(make_grid(), gates)
    assert v
    assert {x.status for x in v} == {"invalid"}
    assert all(failing in x.evidence for x in v)


def test_empty_or_incomplete_gate_list_is_invalid():
    for gates in ([], ALL_PASS[:3]):
        v = report.evaluate(make_grid(), gates)
        assert {x.status for x in v} == {"invalid"}
        assert "missing gates" in v[0].evidence
    assert "G4" in report.evaluate(make_grid(), ALL_PASS[:3])[0].evidence


# --- missing cells ----------------------------------------------------------------------------


def test_single_arrival_grid_marks_other_arrival_not_evaluated():
    v = report.evaluate(make_grid(arrivals=("ordered",)), ALL_PASS)
    assert get(v, "H1", "ordered").status == "supported"
    for hyp in ("H1", "H2", "H2-L2s"):
        x = get(v, hyp, "shuffled")
        assert x.status == "not evaluated"
        assert "missing" in x.evidence or "no rows" in x.evidence
    assert get(v, "H3", "L0/shuffled").status == "not evaluated"
    assert get(v, "H3", "L0/ordered").status == "supported"


def test_missing_l2s_only_affects_sensitivity():
    v = report.evaluate(make_grid(layouts=("L0", "L1", "L2")), ALL_PASS)
    assert get(v, "H2-L2s", "ordered").status == "not evaluated"
    assert get(v, "H2", "ordered").status == "supported"


def test_missing_one_query_row_is_not_evaluated():
    rows = [
        r
        for r in make_grid()
        if not (r.layout == "L2" and r.regime == "B1" and r.query == "Q5_user_week")
    ]
    v = report.evaluate(rows, ALL_PASS)
    assert get(v, "H2", "ordered").status == "not evaluated"
    assert "L2/B1/ordered/Q5_user_week" in get(v, "H2", "ordered").evidence


def test_no_rows_at_all_does_not_crash():
    v = report.evaluate([], ALL_PASS)
    assert {x.status for x in v} == {"not evaluated"}
    md = report.render_markdown([], v, ALL_PASS, {})
    assert "## Verdicts" in md


# --- Delta never decides ----------------------------------------------------------------------


def test_delta_rows_do_not_affect_verdicts():
    iceberg = make_grid()
    # Delta rows that would flip every rule if they were counted.
    delta = [
        dataclasses.replace(r, files_planned=1000, bytes_planned=10**9, files_total=1)
        for r in make_grid(engine="delta")
    ]
    assert report.evaluate(iceberg + delta, ALL_PASS) == report.evaluate(iceberg, ALL_PASS)
    assert report.evaluate(delta, ALL_PASS) == report.evaluate([], ALL_PASS)


# --- render -----------------------------------------------------------------------------------

HEADINGS = [
    "# Lakehouse layout experiment",
    "## Run metadata",
    "## Correctness gates",
    "## Verdicts",
    "## Secondary reading (Amendment 1, defined after a dry run)",
    "## Sensitivity (H2-L2s)",
    "## Iceberg pruning",
    "## Compaction (B2 vs B2c)",
    "## Delta (descriptive)",
    "## Exploratory: row groups",
]


def _render(rows=None, gates=ALL_PASS):
    rows = make_grid() + make_grid(engine="delta") if rows is None else rows
    return report.render_markdown(
        rows, report.evaluate(rows, gates), gates, {"rows": 1000, "seed": 35, "git sha": "abc123"}
    )


def test_render_has_sections_in_order_and_is_deterministic():
    md = _render()
    positions = [md.index(h) for h in HEADINGS]
    assert positions == sorted(positions)
    assert md == _render()
    assert md == _render(list(reversed(make_grid() + make_grid(engine="delta"))))
    assert md.endswith("\n")
    assert "| git sha | abc123 |" in md
    assert "| G4 | pass | ok |" in md
    assert "Delta has no hypothesis" in md
    assert "10/10 · 9.8 KiB (10.0%)" in md  # L0 cell: 10 files, 10_000 of 100_000 bytes
    assert "Not collected in this run." in md


def test_render_marks_failed_gate_and_invalid_verdicts():
    gates = [Gate("G1", False, "mismatch"), *ALL_PASS[1:]]
    md = _render(gates=gates)
    assert "| G1 | FAIL | mismatch |" in md
    assert "| invalid |" in md
    assert "| supported |" not in md


def test_render_sensitivity_is_separate_from_main_verdicts():
    md = _render()
    main = md[md.index("## Verdicts") : md.index("## Secondary reading")]
    secondary = md[md.index("## Secondary reading") : md.index("## Sensitivity")]
    sens = md[md.index("## Sensitivity") : md.index("## Iceberg pruning")]
    assert "H2-L2s" not in main
    assert "H2-L2s" in sens
    assert "H2b-L2s" in sens
    assert "H2b-L2s" not in secondary
    assert "H1b" in secondary
    assert "H1b" not in main


def test_render_compaction_and_rowgroups():
    rows = edit(
        make_grid(),
        {"rowgroups_planned_files": 3, "rowgroups_matching": 2},
        layout="L1",
        regime="B1",
        arrival="ordered",
        query="Q1_point_user",
    )
    md = _render(rows)
    compaction = md[md.index("## Compaction") : md.index("## Delta")]
    assert "| L0 | 500 | 10 | 50.00x |" in compaction
    assert "9.8 KiB → 9.8 KiB" in compaction
    assert "No rows." in md[md.index("## Delta") :]
    rg = md[md.index("## Exploratory") :]
    assert "| iceberg | ordered | B1 | L1 | Q1_point_user | 10 | 3 | 2 |" in rg


def test_render_pipe_in_evidence_is_escaped():
    md = report.render_markdown([], [report.Verdict("H1", "ordered", "invalid", "a|b")], [], {})
    assert "a\\|b" in md


# --- CSV and JSON -----------------------------------------------------------------------------


def test_csv_round_trip_with_none_and_extras(tmp_path):
    rows = make_grid(arrivals=("ordered",), layouts=("L0", "L1")) + make_grid(
        arrivals=("shuffled",), layouts=("L0",), engine="delta"
    )
    rows = edit(rows, {"manifests_total": None}, engine="delta")
    rows = edit(
        rows, {"extra": {"plan_ms": "12.5", "note": 'a,b "q"'}}, layout="L1", query="Q1_point_user"
    )
    rows = edit(rows, {"rowgroups_planned_files": 4, "rowgroups_matching": 0}, layout="L0")
    path = tmp_path / "nested" / "dir" / "pruning.csv"
    report.write_csv(rows, path)
    back = report.read_csv(path)
    assert sorted(back, key=report._sort_key) == sorted(rows, key=report._sort_key)
    assert back == report._sorted(rows)
    header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
    assert header[: len(spec.METRIC_COLUMNS)] == list(spec.METRIC_COLUMNS)
    assert header[len(spec.METRIC_COLUMNS) :] == ["x_note", "x_plan_ms"]
    assert all(isinstance(r.files_total, int) for r in back)
    assert any(r.manifests_total is None for r in back)


def test_csv_order_is_stable_regardless_of_input_order(tmp_path):
    rows = make_grid()
    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    report.write_csv(rows, a)
    report.write_csv(list(reversed(rows)), b)
    assert a.read_bytes() == b.read_bytes()
    first = report.read_csv(a)[0]
    assert (first.engine, first.arrival, first.regime, first.layout, first.query) == (
        "iceberg",
        "ordered",
        "B1",
        "L0",
        "Q1_point_user",
    )


def test_json_round_trip_of_gates(tmp_path):
    gates = [Gate("G1", True, "all 36 cells match"), Gate("G2", False, 'snap 3 "x"')]
    verdicts = report.evaluate(make_grid(), gates)
    path = tmp_path / "sub" / "verdicts.json"
    report.write_json_verdicts(verdicts, gates, path)
    assert report.read_gates(path) == gates
    import json

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert [v["status"] for v in payload["verdicts"]] == [v.status for v in verdicts]
    assert set(payload["verdicts"][0]) == {"hypothesis", "scope", "status", "evidence"}


# --- amendment 1: secondary bytes-only readings, D1, cross-check ------------------------------

D1_PASS = Gate("D1", True, "all Delta cells agree")
D1_FAIL = Gate("D1", False, "3 cells disagree")


def _dry_run_rows():
    """The B1 dry run of Amendment 1: L0 10/10 files 73.9 MiB vs L1 10/300 files 2.6 MiB."""
    mib = 1024 * 1024
    rows = edit(
        make_grid(),
        {"files_planned": 10, "bytes_planned": int(73.9 * mib)},
        layout="L0",
        regime="B1",
        arrival="shuffled",
    )
    rows = edit(
        rows,
        {"files_planned": 10, "bytes_planned": int(2.6 * mib)},
        layout="L1",
        regime="B1",
        arrival="shuffled",
        query="Q2_one_day",
    )
    return edit(
        rows,
        {"files_planned": 70, "bytes_planned": int(18 * mib)},
        layout="L1",
        regime="B1",
        arrival="shuffled",
        query="Q3_seven_days",
    )


def test_h1b_supported_when_bytes_lower_but_files_not():
    v = report.evaluate(_dry_run_rows(), ALL_PASS)
    assert get(v, "H1", "shuffled").status == "not supported"
    h1b = get(v, "H1b", "shuffled")
    assert h1b.status == "supported"
    assert "Q2_one_day: bytes 2.6 MiB vs 73.9 MiB" in h1b.evidence
    assert "files 10 vs 10" in h1b.evidence
    # The ordered arrival was left at the baseline, so both readings agree there.
    assert get(v, "H1", "ordered").status == "supported"
    assert get(v, "H1b", "ordered").status == "supported"


def test_bytes_only_readings_need_strictly_fewer_bytes_for_both_queries():
    rows = edit(
        make_grid(), {"bytes_planned": 10_000}, layout="L1", regime="B1", query="Q3_seven_days"
    )
    v = report.evaluate(rows, ALL_PASS)
    assert get(v, "H1b", "ordered").status == "not supported"
    # Files still improve, bytes do not: H2b fails while the files-only H2 holds.
    rows = edit(make_grid(), {"bytes_planned": 10_000}, layout="L2", regime="B1")
    v = report.evaluate(rows, ALL_PASS)
    assert get(v, "H2", "ordered").status == "supported"
    assert get(v, "H2b", "ordered").status == "not supported"
    assert get(v, "H2b-L2s", "ordered").status == "supported"


def test_bytes_only_readings_ignore_files():
    rows = edit(make_grid(), {"files_planned": 10}, layout="L2", regime="B1")
    v = report.evaluate(rows, ALL_PASS)
    assert get(v, "H2", "ordered").status == "not supported"
    assert get(v, "H2b", "ordered").status == "supported"


def test_bytes_only_readings_not_evaluated_when_cells_missing():
    v = report.evaluate(make_grid(layouts=("L0", "L1", "L2")), ALL_PASS)
    assert get(v, "H2b-L2s", "ordered").status == "not evaluated"
    assert get(v, "H2b", "ordered").status == "supported"
    v = report.evaluate(make_grid(arrivals=("ordered",)), ALL_PASS)
    assert get(v, "H1b", "shuffled").status == "not evaluated"


def test_h2_evidence_shows_bytes_but_decides_on_files():
    v = report.evaluate(make_grid(), ALL_PASS)
    ev = get(v, "H2", "ordered").evidence
    assert "bytes" in ev
    assert "Q1_point_user: files 4 vs 10 -> pass" in ev
    assert "bytes 3.9 KiB vs 9.8 KiB" in ev


def test_d1_failure_does_not_invalidate_iceberg_verdicts():
    rows = make_grid() + make_grid(engine="delta")
    v = report.evaluate(rows, [*ALL_PASS, D1_FAIL])
    assert {x.status for x in v} == {"supported"}
    assert {x.hypothesis for x in v} >= {"H1", "H1b", "H2", "H2b", "H2-L2s", "H2b-L2s", "H3"}
    assert report.evaluate(rows, [*ALL_PASS, D1_PASS]) == v
    assert report.evaluate(rows, ALL_PASS) == v  # missing D1 is fine too


def test_g1_failure_still_invalidates_secondary_verdicts_even_with_d1_passing():
    gates = [Gate("G1", False, "mismatch"), *ALL_PASS[1:], D1_PASS]
    v = report.evaluate(make_grid(), gates)
    for hyp in ("H1b", "H2b", "H2b-L2s"):
        assert get(v, hyp, "ordered").status == "invalid"
        assert "G1" in get(v, hyp, "ordered").evidence
    assert {x.status for x in v} == {"invalid"}


def test_gates_table_lists_g1_to_g4_first_then_others_in_given_order():
    gates = [D1_FAIL, Gate("X9", True, "extra"), ALL_PASS[3], ALL_PASS[1], ALL_PASS[0], ALL_PASS[2]]
    md = _render(gates=gates)
    section = md[md.index("## Correctness gates") : md.index("## Verdicts")]
    order = [section.index(f"| {n} |") for n in ("G1", "G2", "G3", "G4", "D1", "X9")]
    assert order == sorted(order)
    assert "| D1 | FAIL | 3 cells disagree |" in section


def test_secondary_section_text_and_content():
    md = _render()
    sec = md[md.index("## Secondary reading") : md.index("## Sensitivity")]
    assert "defined after seeing a B1 dry run" in sec
    assert "do not replace H1 and H2" in sec
    assert "| H1b | ordered | supported |" in sec
    assert "| H2b | shuffled | supported |" in sec


def _delta_grid_with_extras(disagree: bool = False):
    rows = make_grid(engine="delta")
    rows = [
        dataclasses.replace(
            r, extra={"native_files": str(r.files_planned), "own_eval_files": str(r.files_planned)}
        )
        for r in rows
    ]
    if disagree:
        rows = edit(
            rows,
            {"extra": {"native_files": "4", "own_eval_files": "3"}},
            layout="L1",
            regime="B1",
            arrival="ordered",
            query="Q2_one_day",
        )
    return rows


D1_LINE = "**D1 failed: Delta planned counts disagree; treat this section as untrusted.**"


def test_delta_cross_check_table_renders_with_marker_for_disagreement():
    rows = make_grid() + _delta_grid_with_extras(disagree=True)
    md = _render(rows, gates=[*ALL_PASS, D1_FAIL])
    delta = md[md.index("## Delta") : md.index("## Exploratory")]
    assert delta.index(D1_LINE) < delta.index("Delta has no hypothesis")
    assert "`fragments/native/own`" in delta
    assert "| Q2_one_day | 10/10/10 | 3/4/3 ✗ |" in delta
    assert "| Q1_point_user | 10/10/10 | 4/4/4 | 4/4/4 |" not in delta  # L0 cell is 10/10/10
    assert "| Q1_point_user | 10/10/10 | 10/10/10 | 4/4/4 | 2/2/2 |" in delta
    # Once per regime table of the ordered arrival's B1 cell, plus the legend lines.
    assert delta.count("3/4/3 ✗") == 1
    # The Iceberg part never carries the cross-check or the D1 line.
    assert "D1 failed" not in md[: md.index("## Delta")]
    assert "fragments/native/own" not in md[: md.index("## Delta")]


def test_delta_without_extras_or_with_passing_d1_has_no_cross_check_or_warning():
    md = _render(gates=[*ALL_PASS, D1_PASS])
    assert "D1 failed" not in md
    assert "fragments/native/own" not in md
    md = _render(make_grid() + _delta_grid_with_extras(), gates=[*ALL_PASS, D1_PASS])
    assert "fragments/native/own" in md
    assert " ✗ |" not in md
    assert "D1 failed" not in md


def test_cross_check_extras_survive_csv_round_trip(tmp_path):
    rows = make_grid() + _delta_grid_with_extras(disagree=True)
    path = tmp_path / "pruning.csv"
    report.write_csv(rows, path)
    back = report.read_csv(path)
    gates = [*ALL_PASS, D1_FAIL]
    assert _render(back, gates) == _render(rows, gates)
    assert "3/4/3 ✗" in _render(back, gates)


def test_iceberg_bytes_ratio_lines_under_each_regime_table():
    md = _render()
    iceberg = md[md.index("## Iceberg pruning") : md.index("## Compaction")]
    # Baseline grid: L0 10_000 vs L1 3_000 for day queries; L1 10_000 vs L2 4_000 / L2s 2_000.
    assert "- Q2_one_day: L0/L1 bytes ratio 3.33x" in iceberg
    assert "- Q3_seven_days: L0/L1 bytes ratio 3.33x" in iceberg
    assert "- Q1_point_user: L1/L2 bytes ratio 2.50x; L1/L2s bytes ratio 5.00x" in iceberg
    assert "- Q5_user_week: L1/L2 bytes ratio 2.50x; L1/L2s bytes ratio 5.00x" in iceberg
    assert "Q4_purchases:" not in iceberg
    assert iceberg.count("Planned-bytes ratios") == 6  # 2 arrivals x 3 regimes
    # Delta tables get cross-check handling, never the ratio lines.
    assert "Planned-bytes ratios" not in md[md.index("## Delta") :]


def test_bytes_ratio_lines_skip_missing_layouts():
    md = _render(make_grid(layouts=("L0", "L1")))
    iceberg = md[md.index("## Iceberg pruning") : md.index("## Compaction")]
    assert "L0/L1 bytes ratio" in iceberg
    assert "L1/L2" not in iceberg


def test_render_with_new_sections_is_deterministic():
    rows = make_grid() + _delta_grid_with_extras(disagree=True)
    gates = [*ALL_PASS, D1_FAIL]
    assert _render(rows, gates) == _render(list(reversed(rows)), gates)
