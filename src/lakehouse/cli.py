"""Command line: build and inspect the canonical table, and run the pre-registered grid."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from lakehouse import report, tables
from lakehouse.catalog import ensure_namespace, get_catalog, warehouse_location
from lakehouse.spec import LARGE_BATCHES, LAYOUTS, SEED, TINY_BATCHES

CANONICAL = "lake.events"
ARRIVALS = ("ordered", "shuffled")


def _default_warehouse() -> str:
    return os.environ.get("LAKEHOUSE_WAREHOUSE", "warehouse")


def _open(args):
    return get_catalog(args.warehouse).load_table(CANONICAL)


def cmd_init(args) -> int:
    catalog = get_catalog(args.warehouse)
    for ns in ("lake", "bench"):
        ensure_namespace(catalog, ns)
    print(f"catalog ready: warehouse {warehouse_location(args.warehouse)}, namespaces lake, bench")
    return 0


def cmd_ingest(args) -> int:
    from lakehouse.gen import generate_events

    catalog = get_catalog(args.warehouse)
    ensure_namespace(catalog, "lake")
    data = generate_events(args.rows, args.seed)
    batches = tables.split_batches(data, args.batches, args.arrival, args.seed)
    table = tables.create_events_table(catalog, CANONICAL, args.layout)
    ids = tables.append_batches(table, batches, args.layout)
    files = len(tables.data_file_paths(table))
    print(
        f"{CANONICAL}: {args.rows:,} rows in {len(ids)} appends, "
        f"layout {args.layout}, {files} files"
    )
    return 0


def cmd_evolve(args) -> int:
    table = _open(args)
    before = tables.data_file_paths(table)
    tables.evolve_add_country(table)
    after = tables.data_file_paths(table)
    print(
        f"added column country; data files before {len(before)}, after {len(after)}, "
        f"rewritten {len(before ^ after)}"
    )
    return 0 if before == after else 1


def cmd_time_travel(args) -> int:
    table = _open(args)
    snaps = tables.snapshots(table)
    if args.snapshot is None:
        for sid, at, records in snaps:
            print(f"{sid}  {at.isoformat(timespec='seconds')}  {records:>12,} records")
        return 0
    if args.snapshot not in {s[0] for s in snaps}:
        print(f"unknown snapshot {args.snapshot}", file=sys.stderr)
        return 2
    t = tables.time_travel(table, args.snapshot)
    print(f"snapshot {args.snapshot}: {t.num_rows:,} rows")
    for record in t.slice(0, args.head).to_pylist():
        print(record)
    return 0


def cmd_compact(args) -> int:
    table = _open(args)
    layout = args.layout or tables.layout_of(table)
    note = "" if args.layout else ", inferred"
    res = tables.compact(table, layout)
    print(
        f"compacted ({layout}{note}): files {res.files_before} -> {res.files_after}, "
        f"rows {res.rows_after:,}, snapshot {res.snapshot_before} -> {res.snapshot_after}"
    )
    return 0


def _print_rows(rows) -> None:
    head = (
        f"{'engine':8} {'layout':6} {'regime':6} {'arrival':9} {'query':15} "
        f"{'files':>13} {'bytes planned':>14} {'of total':>8} {'rows':>9}"
    )
    print(head)
    print("-" * len(head))
    for r in rows:
        pct = 100 * r.bytes_planned / r.bytes_total if r.bytes_total else 0.0
        print(
            f"{r.engine:8} {r.layout:6} {r.regime:6} {r.arrival:9} {r.query:15} "
            f"{f'{r.files_planned}/{r.files_total}':>13} {r.bytes_planned:>14,} {pct:>7.1f}% "
            f"{r.result_rows:>9,}"
        )


def _write_outputs(out: Path, rows, gates, meta) -> list:
    out.mkdir(parents=True, exist_ok=True)
    verdicts = report.evaluate(rows, gates)
    report.write_csv(rows, out / "pruning.csv")
    report.write_json_verdicts(verdicts, gates, out / "verdicts.json")
    (out / "meta.json").write_text(json.dumps(meta, indent=2) + "\n", "utf-8")
    md = report.render_markdown(rows, verdicts, gates, meta)
    (out / "REPORT.md").write_text(md, "utf-8")
    return verdicts


def cmd_workload_report(args) -> int:
    from lakehouse.experiment import GridConfig, run_grid

    cfg = GridConfig(
        warehouse=args.warehouse,
        rows=args.rows,
        seed=args.seed,
        layouts=tuple(args.layouts),
        arrivals=ARRIVALS if args.arrival == "both" else (args.arrival,),
        large_batches=args.large_batches,
        tiny_batches=args.tiny_batches,
        delta=not args.no_delta,
        rowgroups=args.rowgroups,
        timing=args.timing,
        note=args.note,
    )
    log = (lambda _m: None) if args.quiet else (lambda m: print(m, file=sys.stderr))
    result = run_grid(cfg, log=log, jobs=args.jobs, verbose=not args.quiet)
    _print_rows(result.rows)
    verdicts = _write_outputs(Path(args.out), result.rows, result.gates, result.meta)
    print()
    for g in result.gates:
        print(f"{g.name}: {'pass' if g.passed else 'FAIL'}  {g.detail}")
    for v in verdicts:
        print(f"{v.hypothesis} [{v.scope}]: {v.status}  {v.evidence}")
    print(f"\nwrote {args.out}/pruning.csv, REPORT.md, verdicts.json, meta.json")
    return 0 if all(g.passed for g in result.gates) else 1


def cmd_render(args) -> int:
    out = Path(args.out)
    rows = report.read_csv(out / "pruning.csv")
    gates = report.read_gates(out / "verdicts.json")
    meta = json.loads((out / "meta.json").read_text("utf-8"))
    _write_outputs(out, rows, gates, meta)
    print(f"re-rendered {out}/REPORT.md from pruning.csv")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="lakehouse", description=__doc__)
    p.add_argument(
        "--warehouse",
        default=_default_warehouse(),
        help="local directory or s3://bucket/path (default: $LAKEHOUSE_WAREHOUSE or ./warehouse)",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="create the catalog and namespaces").set_defaults(fn=cmd_init)

    s = sub.add_parser("ingest", help="build lake.events from generated data")
    s.add_argument("--rows", type=int, default=50_000)
    s.add_argument("--batches", type=int, default=LARGE_BATCHES)
    s.add_argument("--layout", choices=list(LAYOUTS), default="L1")
    s.add_argument("--arrival", choices=ARRIVALS, default="ordered")
    s.add_argument("--seed", type=int, default=SEED)
    s.set_defaults(fn=cmd_ingest)

    sub.add_parser("evolve-schema", help="add the country column to lake.events").set_defaults(
        fn=cmd_evolve
    )

    s = sub.add_parser("time-travel", help="list snapshots, or read one")
    s.add_argument("--snapshot", type=int, default=None)
    s.add_argument("--head", type=int, default=5, help="rows to print (0 = none)")
    s.set_defaults(fn=cmd_time_travel)

    s = sub.add_parser("compact", help="rewrite lake.events into few large files")
    s.add_argument(
        "--layout",
        choices=list(LAYOUTS),
        default=None,
        help="write rule for the rewrite (L2/L2s sort by user); default: inferred from the table",
    )
    s.set_defaults(fn=cmd_compact)

    s = sub.add_parser("workload-report", help="run the pre-registered grid and write results/")
    s.add_argument("--rows", type=int, default=50_000)
    s.add_argument("--seed", type=int, default=SEED)
    s.add_argument("--layouts", nargs="+", choices=list(LAYOUTS), default=list(LAYOUTS))
    s.add_argument("--arrival", choices=[*ARRIVALS, "both"], default="both")
    s.add_argument("--large-batches", type=int, default=LARGE_BATCHES)
    s.add_argument("--tiny-batches", type=int, default=TINY_BATCHES)
    s.add_argument("--no-delta", action="store_true", help="skip the Delta arm")
    s.add_argument("--rowgroups", action="store_true", help="also count matching row groups")
    s.add_argument("--timing", action="store_true", help="median wall-clock of 5 runs (local only)")
    s.add_argument("--jobs", type=int, default=1, help="cells run in this many processes")
    s.add_argument("--note", default="", help="what else was running, recorded in the report")
    s.add_argument("--out", default="results")
    s.add_argument("--quiet", action="store_true")
    s.set_defaults(fn=cmd_workload_report)

    s = sub.add_parser("render", help="re-render REPORT.md from an existing results directory")
    s.add_argument("--out", default="results")
    s.set_defaults(fn=cmd_render)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
