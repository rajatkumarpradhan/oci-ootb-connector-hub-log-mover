# oci-ootb-connector-hub-log-mover

An **offline model** of Oracle Cloud Infrastructure (OCI) **Connector Hub** (formerly Service Connector Hub), an out-of-the-box OCI service that moves data between OCI services. A connector has one **source** (e.g. Logging), an optional **task** (a log filter), and one **target** (e.g. Object Storage, Streaming, Monitoring, Notifications, Functions). This repo implements the connector's two load-bearing behaviors locally, with real runnable code: configuration validation against a documented source-to-target compatibility matrix, and a **cursor-based batch mover** that survives a crash mid-run and resumes without losing or duplicating a single record.

> **No OCI tenancy or credentials are needed or contacted.** The "source" is a JSONL log export on disk; the "target bucket" is a local directory of batch files. Everything is synthetic and offline; the value is in the connector semantics, not in cloud access.

## Why this models the real service

- Connector Hub is itself **free**: per [Oracle's Connector Hub pricing page](https://www.oracle.com/application-development/connector-hub/pricing/), "There is no charge for using Connector Hub. Customers pay at standard rates for source and target services, such as Logging, Object Storage" (checked 2026-11-01). Sources and targets are ordinary Always Free-eligible services.
- Source/task/target shape and the compatible pairings are per the [Connector Hub overview](https://docs.oracle.com/en-us/iaas/Content/connector-hub/overview.htm) (checked 2026-11-01): a connector reads from exactly one source and writes to exactly one target, with an optional task in between.
- The mover mirrors connector-run behavior that matters in production: batch delivery, a **checkpoint** so an interrupted run resumes where it committed, and **exactly-once delivery across a crash** via atomic batch files and idempotent re-execution.

## Quickstart

```bash
python -m pip install -e .
python scripts/demo.py        # visible run: crash after batch 2, then resume
python scripts/validate.py    # assertion gate used in CI
python -m pytest -q
```

The demo moves a 200-record synthetic app-log export through `examples/connector.json` (source `logging`, task `logfilter` keeping WARN/ERROR, target `objectstorage`) in batches of 25, **simulates a crash after batch 2**, then resumes from the checkpoint and prints the reconciliation: records written, filtered out, quarantined (one truncated line), unique IDs, and the batch files in the target.

## What the code guarantees (and proves in CI)

- `connector.py` rejects a configuration the service would also reject (e.g. a `logging` source cannot target `logging`; a `logfilter` task needs a logging source and a non-empty severity list), with the compatible targets named in the error.
- `mover.py` commits each batch atomically (temp file + rename) and only then advances `_checkpoint.json`. A crash leaves either the whole batch committed or none of it; resume starts at the checkpoint, so **every accepted record lands exactly once** - `scripts/validate.py` and the unit tests assert sorted-ID equality with the expected filtered set, zero duplicates, quarantine of a truncated line, and idempotency of a second full run.

## Honest limits

- The compatibility matrix covers the documented `logging` and `monitoring` sources only; adding a source means extending `COMPATIBLE_TARGETS` from the Oracle docs, not guessing.
- A real connector is Oracle-managed and continuous; this model moves one export in batches and stops. It does not create OCI resources, call any OCI API, or validate IAM policy for a real connector.

## References

- Oracle, [Connector Hub pricing](https://www.oracle.com/application-development/connector-hub/pricing/): no charge for Connector Hub itself; standard rates for source/target services.
- Oracle, [Connector Hub overview](https://docs.oracle.com/en-us/iaas/Content/connector-hub/overview.htm): connector source/task/target model.
