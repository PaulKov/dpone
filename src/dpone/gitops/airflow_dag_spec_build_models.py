from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_dag_spec import DAG_SPEC_ARTIFACT_DIR, DAG_SPEC_PRODUCER, GitOpsAirflowDagSpec
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue


@dataclass(frozen=True, slots=True)
class GitOpsAirflowDagSpecBuildReport:
    """Build outcome for every ``dags:`` entry of a workload-set."""

    workload_set: str
    env: str
    specs: tuple[GitOpsAirflowDagSpec, ...]
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    kind: str = "gitops.airflow_dag_spec_build"
    schema_version: str = "1"
    producer: str = DAG_SPEC_PRODUCER
    artifact_dir: str = DAG_SPEC_ARTIFACT_DIR

    @property
    def passed(self) -> bool:
        return not self.blockers

    def by_dag_id(self, dag_id: str) -> GitOpsAirflowDagSpec:
        for spec in self.specs:
            if spec.dag_id == dag_id:
                return spec
        raise KeyError(dag_id)

    @property
    def output_paths(self) -> tuple[str, ...]:
        root = self.artifact_dir.rstrip("/")
        return tuple(f"{root}/{spec.dag_id}.dag-spec.json" for spec in self.specs)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "workload_set": self.workload_set,
            "env": self.env,
            "dag_specs": dict(zip((spec.dag_id for spec in self.specs), self.output_paths, strict=True)),
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


__all__ = ["GitOpsAirflowDagSpecBuildReport"]
