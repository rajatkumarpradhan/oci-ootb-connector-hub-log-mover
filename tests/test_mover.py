"""Connector config validation and crash-safe mover semantics."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from oci_connector_hub_mover import ConfigError, ConnectorConfig, run, read_target


def cfg(**kw):
    base = {"name": "c", "source": "logging", "task": None, "target": "objectstorage"}
    base.update(kw)
    return ConnectorConfig.from_dict(base)


def export(path, n=10, severities=("INFO", "WARN", "ERROR")):
    with open(path, "w", encoding="utf-8") as fh:
        for i in range(1, n + 1):
            fh.write(json.dumps({"id": f"r{i}", "severity": severities[i % len(severities)]}) + "\n")
    return str(path)


def test_compatible_pairs_accepted():
    cfg(source="logging", target="streaming")
    cfg(source="monitoring", target="monitoring", severities=())
    cfg(task="logfilter", severities=("ERROR",))


def test_incompatible_target_rejected():
    with pytest.raises(ConfigError, match="cannot write"):
        cfg(source="logging", target="audit")
    with pytest.raises(ConfigError, match="unsupported source"):
        cfg(source="queues")


def test_task_rules():
    with pytest.raises(ConfigError, match="logging source only"):
        cfg(source="monitoring", task="logfilter", target="monitoring", severities=("ERROR",))
    with pytest.raises(ConfigError, match="non-empty severities"):
        cfg(task="logfilter")
    with pytest.raises(ConfigError, match="severities"):
        cfg(source="monitoring", target="monitoring", severities=("ERROR",))


def test_logfilter_task(tmp_path):
    c = cfg(task="logfilter", severities=("WARN", "ERROR"))
    target = tmp_path / "out"
    stats = run(c, export(tmp_path / "in.jsonl", 9), target, batch_size=2, progress=lambda *a: None)
    assert [r["severity"] for r in read_target(target)] == ["WARN", "ERROR"] * 3
    assert stats["filtered_out"] == 3 and stats["written"] == 6


def test_crash_resume_exactly_once(tmp_path):
    src = export(tmp_path / "in.jsonl", 20)
    c = cfg()
    target = tmp_path / "out"
    crashed = run(c, src, target, batch_size=4, crash_after_batch=2, progress=lambda *a: None)
    assert crashed["crashed"] and crashed["written"] == 8
    resumed = run(c, src, target, batch_size=4, progress=lambda *a: None)
    assert resumed["resumed_from"] == 8 and resumed["written"] == 12
    ids = [r["id"] for r in read_target(target)]
    assert len(ids) == 20 and len(set(ids)) == 20


def test_quarantine_and_checkpoint(tmp_path):
    src = Path(export(tmp_path / "in.jsonl", 5))
    with src.open("a") as fh:
        fh.write("{broken\n")
    target = tmp_path / "out"
    stats = run(cfg(), str(src), target, batch_size=2, progress=lambda *a: None)
    assert stats["quarantined"] == 1 and stats["written"] == 5
    assert json.loads((target / "_checkpoint.json").read_text())["offset"] == 6
    assert len((target / "_quarantine.jsonl").read_text().splitlines()) == 1


def test_second_full_run_is_idempotent(tmp_path):
    src = export(tmp_path / "in.jsonl", 6)
    target = tmp_path / "out"
    run(cfg(), src, target, batch_size=2, progress=lambda *a: None)
    again = run(cfg(), src, target, batch_size=2, progress=lambda *a: None)
    assert again["written"] == 0 and again["resumed_from"] == 6
    assert len(read_target(target)) == 6
