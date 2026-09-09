from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

AIRFLOW_PREFLIGHT_SOURCE = "dpone gitops airflow preflight"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPreflightReport:
    artifact_dir: str
    artifact_index_path: str
    runner_policy: str
    artifact_index: Any
    pod_doctor: dict[str, Any]
    checks: tuple[Any, ...]
    next_actions: tuple[str, ...] = ()
    warnings: tuple[Any, ...] = ()
    blockers: tuple[Any, ...] = ()
    kind: str = "gitops.airflow_preflight"
    schema_version: str = "1"
    producer: str = AIRFLOW_PREFLIGHT_SOURCE

    @property
    def passed(self) -> bool:
        return not self.blockers and all(check.passed or check.severity == "warning" for check in self.checks)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "artifact_dir": self.artifact_dir,
            "artifact_index_path": self.artifact_index_path,
            "runner_policy": self.runner_policy,
            "artifact_index": self.artifact_index.to_jsonable(),
            "pod_doctor": self.pod_doctor,
            "checks": [check.to_jsonable() for check in self.checks],
            "next_actions": list(self.next_actions),
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


__all__ = ["AIRFLOW_PREFLIGHT_SOURCE", "GitOpsAirflowPreflightReport"]
