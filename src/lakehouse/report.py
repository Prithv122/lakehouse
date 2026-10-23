"""Verdicts, REPORT.md and the metrics CSV.

Verdicts are computed mechanically from the decision rules in PREREGISTRATION.md, for Iceberg rows
only. Delta rows are rendered as descriptive tables and never enter a decision.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from lakehouse import spec

Status = Literal["supported", "not supported", "invalid", "not evaluated"]

GATE_NAMES = ("G1", "G2", "G3", "G4")
ARRIVALS = ("ordered", "shuffled")
REGIMES = ("B1", "B2", "B2c")
ENGINES = ("iceberg", "delta")
H3_THRESHOLD = 10

Q_DAY_QUERIES = ("Q2_one_day", "Q3_seven_days")
Q_USER_QUERIES = ("Q1_point_user", "Q5_user_week")


@dataclass(frozen=True)
class Gate:
    name: str  # "G1".."G4"
    passed: bool
    detail: str


@dataclass(frozen=True)
class Verdict:
    hypothesis: str  # "H1" | "H2" | "H3" | "H2-L2s"
    scope: str  # e.g. "ordered", "L1/shuffled"
    status: Status
    evidence: str


# --- small helpers ----------------------------------------------------------------------------


def ratio(a: float, b: float) -> float | None:
    """a / b, or None when b is zero."""
    if b == 0:
        return None
    return a / b


def fmt_ratio(r: float | None) -> str:
    return "n/a" if r is None else f"{r:.2f}x"


def fmt_bytes(n: int | float | None) -> str:
    if n is None:
        return "n/a"
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(value) < 1024 or unit == "GiB":
            return f"{int(value)} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GiB"  # pragma: no cover


def _order(value: str, order: Iterable[str]) -> tuple[int, str]:
    order = list(order)
    return (order.index(value), value) if value in order else (len(order), value)


def _sort_key(r: spec.MetricRow) -> tuple:
    return (
        _order(r.engine, ENGINES),
        _order(r.arrival, ARRIVALS),
        _order(r.regime, REGIMES),
        _order(r.layout, spec.LAYOUTS),
        _order(r.query, [q.name for q in spec.QUERIES]),
    )


def _sorted(rows: Iterable[spec.MetricRow]) -> list[spec.MetricRow]:
    return sorted(rows, key=_sort_key)


Index = dict[tuple[str, str, str, str, str], spec.MetricRow]


def _index(rows: Iterable[spec.MetricRow], engine: str) -> Index:
    return {
        (r.engine, r.layout, r.regime, r.arrival, r.query): r for r in rows if r.engine == engine
    }


def _cell(idx: Index, engine: str, layout: str, regime: str, arrival: str, query: str):
    return idx.get((engine, layout, regime, arrival, query))


# --- decision rules ---------------------------------------------------------------------------


def _missing(idx: Index, keys: list[tuple[str, str, str, str, str]]) -> list[str]:
    return ["/".join(k[1:]) for k in keys if k not in idx]


def _strictly_fewer(
    idx: Index,
    arrival: str,
    fewer_layout: str,
    more_layout: str,
    queries: tuple[str, ...],
    check_bytes: bool,
) -> tuple[Status, str]:
    """True iff `fewer_layout` plans strictly fewer files (and bytes) than `more_layout`."""
    needed = [
        ("iceberg", lay, "B1", arrival, q) for q in queries for lay in (fewer_layout, more_layout)
    ]
    missing = _missing(idx, needed)
    if missing:
        return "not evaluated", "missing cells (layout/regime/arrival/query): " + ", ".join(missing)
    ok_all = True
    parts: list[str] = []
    for q in queries:
        lo = idx[("iceberg", fewer_layout, "B1", arrival, q)]
        hi = idx[("iceberg", more_layout, "B1", arrival, q)]
        ok = lo.files_planned < hi.files_planned
        text = f"{q}: files {lo.files_planned} vs {hi.files_planned}"
        if check_bytes:
            ok = ok and lo.bytes_planned < hi.bytes_planned
            text += (
                f", bytes {fmt_bytes(lo.bytes_planned)} vs {fmt_bytes(hi.bytes_planned)}"
                f" (bytes ratio {fmt_ratio(ratio(hi.bytes_planned, lo.bytes_planned))})"
            )
        text += " -> " + ("pass" if ok else "fail")
        ok_all = ok_all and ok
        parts.append(text)
    head = f"{fewer_layout} vs {more_layout}; "
    return ("supported" if ok_all else "not supported"), head + "; ".join(parts)


def _h3(idx: Index, layout: str, arrival: str, g4_ok: bool) -> tuple[Status, str]:
    totals: dict[str, set[int]] = {}
    for regime in ("B2", "B2c"):
        cells = [
            r
            for (eng, lay, reg, arr, _q), r in idx.items()
            if eng == "iceberg" and lay == layout and reg == regime and arr == arrival
        ]
        if not cells:
            return "not evaluated", f"no rows for {layout}/{regime}/{arrival}"
        totals[regime] = {r.files_total for r in cells}
    for regime, values in totals.items():
        if len(values) > 1:
            return "invalid", f"inconsistent files_total in {regime}: {sorted(values)}"
    before = next(iter(totals["B2"]))
    after = next(iter(totals["B2c"]))
    r = ratio(before, after)
    if r is None:
        return "not evaluated", f"B2c files_total is {after}; ratio undefined"
    ok = before >= H3_THRESHOLD * after and g4_ok
    evidence = (
        f"files_total B2 {before} vs B2c {after} = {r:.2f}x (needs >= {H3_THRESHOLD}x); "
        f"G4 {'passed' if g4_ok else 'not passed'}"
    )
    return ("supported" if ok else "not supported"), evidence


def _gate_problem(gates: list[Gate]) -> str | None:
    """Why the run is invalid, or None when G1-G4 are all present and passed."""
    by_name: dict[str, list[Gate]] = {}
    for g in gates:
        by_name.setdefault(g.name, []).append(g)
    missing = [n for n in GATE_NAMES if n not in by_name]
    failed = [n for n in GATE_NAMES if n in by_name and not all(g.passed for g in by_name[n])]
    if not missing and not failed:
        return None
    bits = []
    if failed:
        bits.append("failed gates: " + ", ".join(failed))
    if missing:
        bits.append("missing gates: " + ", ".join(missing))
    return "; ".join(bits) + ". An invalid run reports no verdict."


def evaluate(rows: list[spec.MetricRow], gates: list[Gate]) -> list[Verdict]:
    """Apply the pre-registered decision rules to the Iceberg rows."""
    idx = _index(rows, "iceberg")
    g4_gates = [g for g in gates if g.name == "G4"]
    g4_ok = bool(g4_gates) and all(g.passed for g in g4_gates)

    tasks: list[tuple[str, str, Callable[[], tuple[Status, str]]]] = []
    for arrival in ARRIVALS:
        tasks.append(
            (
                "H1",
                arrival,
                lambda a=arrival: _strictly_fewer(idx, a, "L1", "L0", Q_DAY_QUERIES, True),
            )
        )
    for arrival in ARRIVALS:
        tasks.append(
            (
                "H2",
                arrival,
                lambda a=arrival: _strictly_fewer(idx, a, "L2", "L1", Q_USER_QUERIES, False),
            )
        )
    for layout in spec.PRIMARY_LAYOUTS:
        for arrival in ARRIVALS:
            tasks.append(
                (
                    "H3",
                    f"{layout}/{arrival}",
                    lambda lay=layout, a=arrival: _h3(idx, lay, a, g4_ok),
                )
            )
    for arrival in ARRIVALS:
        tasks.append(
            (
                "H2-L2s",
                arrival,
                lambda a=arrival: _strictly_fewer(idx, a, "L2s", "L1", Q_USER_QUERIES, False),
            )
        )

    problem = _gate_problem(gates)
    if problem is not None:
        return [Verdict(h, s, "invalid", problem) for h, s, _ in tasks]
    out = []
    for h, s, fn in tasks:
        status, evidence = fn()
        out.append(Verdict(h, s, status, evidence))
    return out


# --- CSV --------------------------------------------------------------------------------------

_INT_COLUMNS = frozenset(
    {
        "rows",
        "seed",
        "snapshot",
        "files_total",
        "bytes_total",
        "records_total",
        "files_planned",
        "bytes_planned",
        "records_planned",
        "result_rows",
        "manifests_total",
        "rowgroups_planned_files",
        "rowgroups_matching",
    }
)
_OPTIONAL_INT = frozenset({"manifests_total", "rowgroups_planned_files", "rowgroups_matching"})


def write_csv(rows: list[spec.MetricRow], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = _sorted(rows)
    extra_keys = sorted({k for r in ordered for k in r.extra})
    header = [*spec.METRIC_COLUMNS, *(f"x_{k}" for k in extra_keys)]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(header)
        for r in ordered:
            d = r.to_dict()
            writer.writerow(["" if d.get(c) is None else d[c] for c in header])


def read_csv(path: str | Path) -> list[spec.MetricRow]:
    out: list[spec.MetricRow] = []
    with Path(path).open(newline="", encoding="utf-8") as fh:
        for rec in csv.DictReader(fh):
            kwargs: dict[str, Any] = {}
            extra: dict[str, str] = {}
            for col, val in rec.items():
                if col.startswith("x_"):
                    if val != "":
                        extra[col[2:]] = val
                elif col in _INT_COLUMNS:
                    if val == "":
                        if col not in _OPTIONAL_INT:
                            raise ValueError(f"empty value for required column {col!r}")
                        kwargs[col] = None
                    else:
                        kwargs[col] = int(val)
                else:
                    kwargs[col] = val
            out.append(spec.MetricRow(**kwargs, extra=extra))
    return out


def write_json_verdicts(verdicts: list[Verdict], gates: list[Gate], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "gates": [asdict(g) for g in gates],
        "verdicts": [asdict(v) for v in verdicts],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def read_gates(path: str | Path) -> list[Gate]:
    """Read back the gates written by `write_json_verdicts`."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return [
        Gate(name=g["name"], passed=bool(g["passed"]), detail=g["detail"])
        for g in payload.get("gates", [])
    ]


# --- markdown ---------------------------------------------------------------------------------


def _esc(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _table(header: list[str], body: list[list[object]]) -> list[str]:
    lines = ["| " + " | ".join(_esc(h) for h in header) + " |"]
    lines.append("|" + "|".join("---" for _ in header) + "|")
    lines.extend("| " + " | ".join(_esc(c) for c in row) + " |" for row in body)
    return lines


def _pruning_cell(r: spec.MetricRow | None) -> str:
    if r is None:
        return "-"
    pct = 100.0 * r.bytes_planned / r.bytes_total if r.bytes_total else 0.0
    return f"{r.files_planned}/{r.files_total} · {fmt_bytes(r.bytes_planned)} ({pct:.1f}%)"


def _layouts_present(rows: list[spec.MetricRow]) -> list[str]:
    present = {r.layout for r in rows}
    return sorted(present, key=lambda x: _order(x, spec.LAYOUTS))


def _pruning_tables(rows: list[spec.MetricRow], engine: str, level: str) -> list[str]:
    sub = [r for r in rows if r.engine == engine]
    idx = _index(sub, engine)
    layouts = _layouts_present(sub)
    lines: list[str] = []
    if not sub:
        return ["No rows.", ""]
    inner = "#" * (len(level) + 1)
    for arrival in ARRIVALS:
        arr_rows = [r for r in sub if r.arrival == arrival]
        if not arr_rows:
            continue
        lines += [f"{level} {arrival}", ""]
        for regime in REGIMES:
            reg_rows = [r for r in arr_rows if r.regime == regime]
            if not reg_rows:
                continue
            lines += [f"{inner} {regime}", ""]
            body = []
            for q in spec.QUERIES:
                body.append(
                    [q.name]
                    + [
                        _pruning_cell(_cell(idx, engine, lay, regime, arrival, q.name))
                        for lay in layouts
                    ]
                )
            lines += _table(["query", *layouts], body)
            lines.append("")
    return lines


def _files_total(idx: Index, engine: str, layout: str, regime: str, arrival: str):
    for q in spec.QUERIES:
        r = _cell(idx, engine, layout, regime, arrival, q.name)
        if r is not None:
            return r
    return None


def _compaction_tables(rows: list[spec.MetricRow], engine: str) -> list[str]:
    sub = [r for r in rows if r.engine == engine]
    idx = _index(sub, engine)
    lines: list[str] = []
    for arrival in ARRIVALS:
        body = []
        for layout in _layouts_present(sub):
            before = _files_total(idx, engine, layout, "B2", arrival)
            after = _files_total(idx, engine, layout, "B2c", arrival)
            if before is None or after is None:
                continue
            row: list[object] = [
                layout,
                before.files_total,
                after.files_total,
                fmt_ratio(ratio(before.files_total, after.files_total)),
                "-" if before.manifests_total is None else before.manifests_total,
                "-" if after.manifests_total is None else after.manifests_total,
            ]
            for q in spec.QUERIES:
                b = _cell(idx, engine, layout, "B2", arrival, q.name)
                a = _cell(idx, engine, layout, "B2c", arrival, q.name)
                row.append(
                    "-"
                    if b is None or a is None
                    else f"{fmt_bytes(b.bytes_planned)} → {fmt_bytes(a.bytes_planned)}"
                )
            body.append(row)
        if not body:
            continue
        header = [
            "layout",
            "files_total B2",
            "files_total B2c",
            "ratio",
            "manifests B2",
            "manifests B2c",
            *(f"{q.name} bytes_planned" for q in spec.QUERIES),
        ]
        lines += [f"### {arrival}", "", *_table(header, body), ""]
    if not lines:
        return ["No B2/B2c rows.", ""]
    return lines


def _rowgroup_table(rows: list[spec.MetricRow]) -> list[str]:
    sel = [r for r in _sorted(rows) if r.rowgroups_matching is not None]
    if not sel:
        return ["Not collected in this run.", ""]
    body = [
        [
            r.engine,
            r.arrival,
            r.regime,
            r.layout,
            r.query,
            r.files_planned,
            r.rowgroups_planned_files if r.rowgroups_planned_files is not None else "-",
            r.rowgroups_matching,
        ]
        for r in sel
    ]
    header = [
        "engine",
        "arrival",
        "regime",
        "layout",
        "query",
        "files_planned",
        "row groups in planned files",
        "row groups matching",
    ]
    return [*_table(header, body), ""]


def render_markdown(
    rows: list[spec.MetricRow],
    verdicts: list[Verdict],
    gates: list[Gate],
    meta: dict,
) -> str:
    L: list[str] = []
    L += [
        "# Lakehouse layout experiment — report",
        "",
        "Verdicts below are computed by code from the decision rules in `PREREGISTRATION.md`, "
        "applied to the Iceberg rows only; nothing in this file is edited by hand.",
        "",
        "## Run metadata",
        "",
    ]
    L += _table(["key", "value"], [[k, v] for k, v in meta.items()]) if meta else ["None recorded."]
    L.append("")

    L += ["## Correctness gates", ""]
    if gates:
        L += _table(
            ["gate", "result", "detail"],
            [[g.name, "pass" if g.passed else "FAIL", g.detail] for g in gates],
        )
    else:
        L.append("No gate results recorded: the run is treated as invalid.")
    L.append("")

    L += ["## Verdicts", ""]
    main = [v for v in verdicts if v.hypothesis in ("H1", "H2", "H3")]
    L += _table(
        ["hypothesis", "scope", "status", "evidence"],
        [[v.hypothesis, v.scope, v.status, v.evidence] for v in main],
    )
    L.append("")

    L += [
        "## Sensitivity (H2-L2s)",
        "",
        "Same rule as H2 with the 1 MiB target-file-size arm (L2s) in place of L2. "
        "It is reported for information and does not decide H2.",
        "",
    ]
    sens = [v for v in verdicts if v.hypothesis == "H2-L2s"]
    L += _table(
        ["hypothesis", "scope", "status", "evidence"],
        [[v.hypothesis, v.scope, v.status, v.evidence] for v in sens],
    )
    L.append("")

    L += [
        "## Iceberg pruning",
        "",
        "Each cell is `files_planned/files_total · bytes_planned (share of bytes_total)`.",
        "",
    ]
    L += _pruning_tables(rows, "iceberg", "###")

    L += ["## Compaction (B2 vs B2c)", ""]
    L += _compaction_tables(rows, "iceberg")

    L += [
        "## Delta (descriptive)",
        "",
        "Delta has no hypothesis and takes no part in any verdict.",
        "",
    ]
    L += _pruning_tables(rows, "delta", "###")

    L += ["## Exploratory: row groups", ""]
    L += _rowgroup_table(rows)

    return "\n".join(L).rstrip("\n") + "\n"
