from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

WORKLOAD_CATALOG_SOURCE = "dpone gitops workloads"


@dataclass(frozen=True, slots=True)
class GitOpsWorkloadCatalogIssue:
    code: str
    message: str
    path: str
    source: str = WORKLOAD_CATALOG_SOURCE

    def to_jsonable(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "path": self.path, "source": self.source}


@dataclass(frozen=True, slots=True)
class GitOpsConfigProvenance:
    scope: str
    path: str
    key: str

    def to_jsonable(self) -> dict[str, str]:
        return {"scope": self.scope, "path": self.path, "key": self.key}


@dataclass(frozen=True, slots=True)
class GitOpsWorkloadDefinition:
    workload_id: str
    manifest: str
    domain: str | None
    catalog_path: str
    effective_config: dict[str, Any]
    provenance: dict[str, GitOpsConfigProvenance]

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "workload_id": self.workload_id,
            "manifest": self.manifest,
            "catalog_path": self.catalog_path,
            "effective_config": _jsonable_mapping(self.effective_config),
            "provenance": {key: value.to_jsonable() for key, value in sorted(self.provenance.items())},
        }
        if self.domain is not None:
            payload["domain"] = self.domain
        return payload


@dataclass(frozen=True, slots=True)
class GitOpsWorkloadCatalogReport:
    workload_set: str
    env: str
    workloads: tuple[GitOpsWorkloadDefinition, ...]
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    kind: str = "gitops.workloads"
    schema_version: str = "1"
    producer: str = WORKLOAD_CATALOG_SOURCE

    @property
    def passed(self) -> bool:
        return not self.blockers

    def by_id(self, workload_id: str) -> GitOpsWorkloadDefinition:
        for workload in self.workloads:
            if workload.workload_id == workload_id:
                return workload
        raise KeyError(workload_id)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "workload_set": self.workload_set,
            "env": self.env,
            "workloads": [workload.to_jsonable() for workload in self.workloads],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class GitOpsWorkloadImpactReason:
    changed_path: str
    matched_path: str
    reason: str

    def to_jsonable(self) -> dict[str, str]:
        return {"changed_path": self.changed_path, "matched_path": self.matched_path, "reason": self.reason}


@dataclass(frozen=True, slots=True)
class GitOpsAffectedWorkload:
    workload_id: str
    manifest: str
    reasons: tuple[GitOpsWorkloadImpactReason, ...]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "workload_id": self.workload_id,
            "manifest": self.manifest,
            "reasons": [reason.to_jsonable() for reason in self.reasons],
        }


@dataclass(frozen=True, slots=True)
class GitOpsAffectedWorkloadReport:
    changed_files: tuple[str, ...]
    affected_workloads: tuple[GitOpsAffectedWorkload, ...]
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    kind: str = "gitops.affected_workloads"
    schema_version: str = "1"
    producer: str = "dpone gitops workloads affected"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "changed_files": list(self.changed_files),
            "affected_workloads": [item.to_jsonable() for item in self.affected_workloads],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


def issue(*, code: str, message: str, path: str, source: str = WORKLOAD_CATALOG_SOURCE) -> GitOpsWorkloadCatalogIssue:
    return GitOpsWorkloadCatalogIssue(code=code, message=message, path=path, source=source)


def _jsonable_mapping(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))


__all__ = [
    "GitOpsAffectedWorkload",
    "GitOpsAffectedWorkloadReport",
    "GitOpsConfigProvenance",
    "GitOpsWorkloadCatalogIssue",
    "GitOpsWorkloadCatalogReport",
    "GitOpsWorkloadDefinition",
    "GitOpsWorkloadImpactReason",
    "WORKLOAD_CATALOG_SOURCE",
    "issue",
]
