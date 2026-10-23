"""CLI commands end to end on a small warehouse."""

from __future__ import annotations

import json

import pyarrow.parquet as pq

from lakehouse import spec, tables
from lakehouse.catalog import get_catalog
from lakehouse.cli import main


def test_init_ingest_evolve_time_travel_compact(tmp_path, capsys):
    wh = str(tmp_path / "wh")
    assert main(["--warehouse", wh, "init"]) == 0
    assert main(["--warehouse", wh, "ingest", "--rows", "2000", "--batches", "4"]) == 0
    assert "2,000 rows in 4 appends" in capsys.readouterr().out

    assert main(["--warehouse", wh, "time-travel"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 4
    first = int(lines[0].split()[0])

    assert main(["--warehouse", wh, "time-travel", "--snapshot", str(first), "--head", "2"]) == 0
    assert "snapshot" in capsys.readouterr().out
    assert main(["--warehouse", wh, "time-travel", "--snapshot", "1"]) == 2

    assert main(["--warehouse", wh, "evolve-schema"]) == 0
    assert "rewritten 0" in capsys.readouterr().out

    assert main(["--warehouse", wh, "compact"]) == 0
    out = capsys.readouterr().out
    assert "rows 2,000" in out
    assert "compacted (L1, inferred)" in out  # ingest default layout is L1


def test_compact_infers_layout_from_the_table_and_honours_an_explicit_one(tmp_path, capsys):
    wh = str(tmp_path / "wh")
    assert main(["--warehouse", wh, "init"]) == 0
    ingest = ["--warehouse", wh, "ingest", "--rows", "1500", "--batches", "3", "--layout", "L2"]
    assert main(ingest) == 0
    capsys.readouterr()

    assert main(["--warehouse", wh, "compact"]) == 0
    assert "compacted (L2, inferred)" in capsys.readouterr().out
    table = get_catalog(wh).load_table("lake.events")
    for path in tables.data_file_paths(table):  # sorted by the L2 key, not by event_id
        f = pq.read_table(path, columns=[c for c, _ in spec.SORT_KEY])
        assert f.equals(f.sort_by(list(spec.SORT_KEY)))

    assert main(["--warehouse", wh, "compact", "--layout", "L1"]) == 0
    assert "compacted (L1):" in capsys.readouterr().out


def test_workload_report_writes_results_and_rerenders(tmp_path, capsys):
    wh, out = str(tmp_path / "wh"), tmp_path / "results"
    argv = [
        "--warehouse", wh, "workload-report", "--rows", "600", "--large-batches", "2",
        "--tiny-batches", "6", "--arrival", "ordered", "--layouts", "L0", "L1", "--no-delta",
        "--out", str(out), "--quiet",
    ]  # fmt: skip
    assert main(argv) == 0
    printed = capsys.readouterr().out
    assert "Q1_point_user" in printed and "G1: pass" in printed
    for name in ("pruning.csv", "REPORT.md", "verdicts.json", "meta.json"):
        assert (out / name).exists()
    assert json.loads((out / "meta.json").read_text("utf-8"))["rows"] == 600

    before = (out / "REPORT.md").read_text("utf-8")
    assert main(["render", "--out", str(out)]) == 0
    assert (out / "REPORT.md").read_text("utf-8") == before
