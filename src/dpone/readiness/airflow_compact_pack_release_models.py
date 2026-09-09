"""Result and error models for compact Airflow release materialization."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


class CompactPackReleaseError(ValueError):
    """Fail-closed promotion error without secret values."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class CompactPackReleaseReport:
    """Public report for ``dpone gitops airflow release-materialize``."""

    release_id: str
    release_dir: str
    dag_ids: tuple[str, ...]
    workload_ids: tuple[str, ...]
    pack_fingerprints: Mapping[str, str]
    connection_projection_mode: str
    xcom_sidecar_image: str
    strict_init_fetch_rewrite: bool = True
    kind: str = "gitops.airflow_compact_pack_release"
    schema_version: str = "1"
    producer: str = "dpone gitops airflow release-materialize"
    blockers: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return not self.blockers and bool(self.release_id)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "passed": self.passed,
            "release_id": self.release_id,
            "release_dir": self.release_dir,
            "dag_ids": list(self.dag_ids),
            "workload_ids": list(self.workload_ids),
            "pack_fingerprints": dict(self.pack_fingerprints),
            "connection_projection_mode": self.connection_projection_mode,
            "xcom_sidecar_image": self.xcom_sidecar_image,
            "strict_init_fetch_rewrite": self.strict_init_fetch_rewrite,
            "blockers": list(self.blockers),
        }


__all__ = ["CompactPackReleaseError", "CompactPackReleaseReport"]
