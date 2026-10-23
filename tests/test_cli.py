"""CLI commands end to end on a small warehouse."""

from __future__ import annotations

import json

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
    assert "rows 2,000" in capsys.readouterr().out


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
