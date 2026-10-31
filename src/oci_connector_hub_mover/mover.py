"""Cursor-based batch mover mirroring a connector run, with crash-safe resume.

A connector run streams source records to the target. This offline
implementation moves records from a JSONL export to a target directory of
batch files, keeping a checkpoint of the last committed offset so a crash
mid-run resumes without duplicates or loss (at-least-once delivery inside a
batch, with batch-level idempotent re-execution).
"""
from __future__ import annotations

import json
from pathlib import Path

from .connector import ConnectorConfig


class Quarantine:
    def __init__(self, path: Path):
        self.path = path
        self.count = 0

    def add(self, line: str, reason: str):
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"reason": reason, "line": line}) + "\n")
        self.count += 1


def _write_batch_atomic(path: Path, records: list[str]):
    tmp = path.with_suffix(".tmp")
    tmp.write_text("".join(r + "\n" for r in records), encoding="utf-8")
    tmp.replace(path)


def run(config: ConnectorConfig, source_path, target_dir, batch_size: int = 25,
        crash_after_batch: int | None = None, progress=print) -> dict:
    """Move records; returns run stats. crash_after_batch simulates a mid-run crash."""
    source = Path(source_path)
    target = Path(target_dir)
    target.mkdir(parents=True, exist_ok=True)
    ckpt_file = target / "_checkpoint.json"
    quarantine = Quarantine(target / "_quarantine.jsonl")

    start_offset, start_batch = 0, 0
    if ckpt_file.exists():
        ckpt = json.loads(ckpt_file.read_text())
        start_offset, start_batch = ckpt["offset"], ckpt["batch"]
        progress(f"resuming from checkpoint offset {start_offset} (batch {start_batch} committed)")

    stats = {"read": 0, "written": 0, "filtered_out": 0, "quarantined": 0,
             "batches": 0, "resumed_from": start_offset, "crashed": False}
    offset = 0
    batch, batch_index = [], start_batch + 1

    def flush():
        nonlocal batch, batch_index, offset
        if not batch:
            return
        _write_batch_atomic(target / f"batch-{batch_index:04d}.jsonl", batch)
        stats["written"] += len(batch)
        stats["batches"] += 1
        ckpt_file.write_text(json.dumps({"offset": offset, "batch": batch_index}))
        progress(f"batch {batch_index:04d}: wrote {len(batch)} record(s), checkpoint offset {offset}")
        batch, batch_index = [], batch_index + 1

    with source.open(encoding="utf-8") as fh:
        for line in fh:
            offset += 1
            if offset <= start_offset:
                continue  # already committed before the crash
            line = line.strip()
            if not line:
                continue
            stats["read"] += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                quarantine.add(line, "not JSON")
                stats["quarantined"] += 1
                continue
            if not config.passes_task(record):
                stats["filtered_out"] += 1
                continue
            batch.append(line)
            if len(batch) >= batch_size:
                flush()
                if crash_after_batch is not None and stats["batches"] >= crash_after_batch:
                    stats["crashed"] = True
                    stats["quarantined"] = quarantine.count
                    progress("simulated crash after batch "
                             f"{stats['batches']} (checkpoint at offset {offset})")
                    return stats
    flush()
    stats["quarantined"] = quarantine.count
    return stats


def read_target(target_dir) -> list[dict]:
    records = []
    for f in sorted(Path(target_dir).glob("batch-*.jsonl")):
        records += [json.loads(l) for l in f.read_text(encoding="utf-8").splitlines() if l.strip()]
    return records
