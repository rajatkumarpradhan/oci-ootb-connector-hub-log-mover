"""Offline model of OCI Connector Hub's source -> task -> target pipeline.

OCI Connector Hub (formerly Service Connector Hub) is an Oracle-managed
service that moves data between OCI services: a connector has one source, an
optional task, and one target. This module validates connector configurations
and applies the task stage to records. It never talks to OCI.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

# Compatibility matrix documented from the Connector Hub overview: a connector
# reads from one source and writes to one target, with an optional task in
# between. https://docs.oracle.com/en-us/iaas/Content/connector-hub/overview.htm
COMPATIBLE_TARGETS = {
    "logging": {"objectstorage", "streaming", "monitoring", "notifications", "functions"},
    "monitoring": {"monitoring", "objectstorage", "streaming", "notifications"},
}
TASKS = {None, "logfilter"}


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ConnectorConfig:
    name: str
    source: str          # "logging" | "monitoring"
    task: str | None     # None | "logfilter"
    target: str          # e.g. "objectstorage"
    severities: tuple = ()  # logfilter task: keep records whose severity is listed

    @staticmethod
    def from_dict(raw: dict) -> "ConnectorConfig":
        try:
            cfg = ConnectorConfig(
                name=str(raw["name"]),
                source=str(raw["source"]).lower(),
                task=(str(raw["task"]).lower() if raw.get("task") else None),
                target=str(raw["target"]).lower(),
                severities=tuple(str(s).upper() for s in raw.get("severities", [])),
            )
        except KeyError as exc:
            raise ConfigError(f"missing field {exc}") from exc
        if cfg.source not in COMPATIBLE_TARGETS:
            raise ConfigError(f"unsupported source {cfg.source!r}; supported: {sorted(COMPATIBLE_TARGETS)}")
        if cfg.target not in COMPATIBLE_TARGETS[cfg.source]:
            raise ConfigError(
                f"source {cfg.source!r} cannot write to target {cfg.target!r}; "
                f"compatible targets: {sorted(COMPATIBLE_TARGETS[cfg.source])}")
        if cfg.task not in TASKS:
            raise ConfigError(f"unsupported task {cfg.task!r}; supported: logfilter")
        if cfg.task == "logfilter" and cfg.source != "logging":
            raise ConfigError("task logfilter applies to a logging source only")
        if cfg.task == "logfilter" and not cfg.severities:
            raise ConfigError("task logfilter needs a non-empty severities list")
        if cfg.source == "monitoring" and cfg.severities:
            raise ConfigError("severities filter applies to a logging source only")
        return cfg

    def passes_task(self, record: dict) -> bool:
        if self.task != "logfilter":
            return True
        return str(record.get("severity", "")).upper() in self.severities


def load_config(path) -> ConnectorConfig:
    try:
        return ConnectorConfig.from_dict(json.loads(open(path, encoding="utf-8").read()))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot load connector config {path}: {exc}") from exc
