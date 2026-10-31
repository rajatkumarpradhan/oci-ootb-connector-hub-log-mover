"""Visible end-to-end demo: crash-safe connector run over a synthetic log export."""
import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from oci_connector_hub_mover import ConnectorConfig, load_config, run, read_target


def make_export(path: Path, n: int = 200) -> Path:
    levels = ["INFO", "INFO", "INFO", "DEBUG", "WARN", "ERROR"]
    with path.open("w", encoding="utf-8") as fh:
        for i in range(1, n + 1):
            fh.write(json.dumps({"id": f"rec-{i:04d}", "severity": levels[i % len(levels)],
                                 "message": f"request {i} handled",
                                 "source": "app-server-1"}) + "\n")
        fh.write('{"broken": ')  # truncated record -> quarantine
    return path


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="connector-hub-demo-"))
    export = make_export(work / "app.log.jsonl")
    target = work / "bucket"
    cfg = load_config(Path(__file__).resolve().parents[1] / "examples" / "connector.json")
    print(f"connector {cfg.name!r}: source={cfg.source} task={cfg.task} target={cfg.target} "
          f"severities={list(cfg.severities)}")
    print(f"source export: {export} (200 records + 1 truncated line)\n")

    print("-- first run (crash simulated after batch 2) --")
    crashed = run(cfg, export, target, batch_size=25, crash_after_batch=2)
    print(json.dumps({k: crashed[k] for k in ("read", "written", "filtered_out", "batches", "crashed")}, indent=2))

    print("\n-- resumed run --")
    done = run(cfg, export, target, batch_size=25)
    print(json.dumps({k: done[k] for k in ("read", "written", "filtered_out", "quarantined", "batches", "resumed_from", "crashed")}, indent=2))

    records = read_target(target)
    ids = [r["id"] for r in records]
    expect = sum(1 for i in range(1, 201) if ["INFO","INFO","INFO","DEBUG","WARN","ERROR"][i % 6] in ("WARN", "ERROR"))
    print(f"\ntarget records: {len(records)} (expected {expect})")
    print(f"unique ids: {len(set(ids))} (duplicates: {len(ids) - len(set(ids))})")
    print(f"batches in target: {sorted(p.name for p in target.glob('batch-*.jsonl'))}")
    shutil.rmtree(work)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
