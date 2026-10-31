"""Demo assertion gate for CI: crash-safe resume must deliver every filtered record exactly once."""
import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from oci_connector_hub_mover import ConfigError, ConnectorConfig, run, read_target
from demo import make_export

failed = 0
def check(name, cond):
    global failed
    print(f"{'PASS' if cond else 'FAIL'} {name}")
    failed += 0 if cond else 1

work = Path(tempfile.mkdtemp(prefix="connector-hub-validate-"))
export = make_export(work / "app.log.jsonl", 200)
target = work / "bucket"
cfg = ConnectorConfig.from_dict(json.loads((Path(__file__).resolve().parents[1] / "examples" / "connector.json").read_text()))

quiet = lambda *a, **k: None
run(cfg, export, target, batch_size=25, crash_after_batch=2, progress=quiet)
mid = read_target(target)
done = run(cfg, export, target, batch_size=25, progress=quiet)
records = read_target(target)
ids = [r["id"] for r in records]
expect = [f"rec-{i:04d}" for i in range(1, 201) if ["INFO","INFO","INFO","DEBUG","WARN","ERROR"][i % 6] in ("WARN", "ERROR")]

check("no record lost after crash + resume", sorted(ids) == sorted(expect))
check("no duplicate record after resume", len(ids) == len(set(ids)))
check("crash run committed 2 batches", mid and len(list(target.glob('batch-*.jsonl'))) > 2)
check("checkpoint exists with final offset", json.loads((target / "_checkpoint.json").read_text())["offset"] == 201)
check("truncated line quarantined, not moved", (target / "_quarantine.jsonl").exists()
      and len((target / "_quarantine.jsonl").read_text().splitlines()) == 1)
try:
    ConnectorConfig.from_dict({"name": "bad", "source": "monitoring", "target": "logging"})
    check("incompatible source->target rejected", False)
except ConfigError as e:
    check("incompatible source->target rejected", "cannot write" in str(e))
shutil.rmtree(work)
print(f"validate: {'all checks passed' if not failed else str(failed) + ' check(s) failed'}")
sys.exit(1 if failed else 0)
