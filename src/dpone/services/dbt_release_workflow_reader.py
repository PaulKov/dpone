"""Confined workload-pack and DAG readers shared by dbt evidence/source checks."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from dpone_airflow_pack.pack_identity import PackIdentityError, parse_pack_json, verify_pack_fingerprint

from dpone.contracts.dbt_release_workload_binding import (
    DbtDevEvidenceReleaseError as DbtDevEvidenceReleaseError,
)
from dpone.contracts.dbt_release_workload_binding import (
    ExpectedWorkflowDag as ExpectedWorkflowDag,
)
from dpone.contracts.dbt_release_workload_binding import (
    dbt_execution_from_pack,
    dbt_release_artifact_read_bound,
    decode_dbt_workflow_dag,
    require_dbt_release_artifact_bytes,
)
from dpone.contracts.dbt_release_workload_binding import (
    release_digest as release_digest,
)
from dpone.contracts.dbt_release_workload_binding import (
    release_mapping as release_mapping,
)
from dpone.contracts.dbt_release_workload_binding import (
    release_object as release_object,
)
from dpone.contracts.dbt_release_workload_binding import (
    release_text as release_text,
)
from dpone.contracts.dbt_release_workload_binding import (
    require_dbt_workflow_dag as require_dbt_workflow_dag,
)
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1
from dpone.manifest.confined_files import read_confined_file
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader

if TYPE_CHECKING:
    from dpone.contracts.dbt_execution_pack import DbtExecutionPack


def read_dbt_execution_pack(
    root: Path,
    descriptor: Mapping[str, object],
    workload_id: str,
    *,
    expected_runtime_payload_ids: tuple[str, ...] | None = None,
    wire_contract: str = DBT_RUNTIME_WIRE_V1,
    read_file: ConfinedReleaseFileReader = read_confined_file,
) -> DbtExecutionPack:
    pack = read_dbt_workload_pack(root, descriptor, workload_id, read_file=read_file)
    return dbt_execution_from_pack(
        pack, expected_runtime_payload_ids=expected_runtime_payload_ids, wire_contract=wire_contract
    )


def read_dbt_workload_pack(
    root: Path,
    descriptor: Mapping[str, object],
    workload_id: str,
    *,
    read_file: ConfinedReleaseFileReader = read_confined_file,
) -> Mapping[str, object]:
    """Verify an execution or transfer pack before trusting its declared role."""

    relative = release_text(descriptor.get("path"), "workload pack path")
    pack_bytes = read_file(
        root,
        relative,
        max_bytes=dbt_release_artifact_read_bound("workload pack"),
    )
    require_dbt_release_artifact_bytes(descriptor, pack_bytes, kind="workload pack")
    try:
        pack = parse_pack_json(pack_bytes)
        fingerprint = verify_pack_fingerprint(pack)
    except PackIdentityError as exc:
        raise DbtDevEvidenceReleaseError("workload pack identity is invalid") from exc
    workload = pack.get("workload")
    if (
        fingerprint != descriptor.get("pack_fingerprint")
        or not isinstance(workload, Mapping)
        or workload.get("workload_id") != workload_id
    ):
        raise DbtDevEvidenceReleaseError("workload pack identity differs from the release descriptor")
    return pack


def read_dbt_workflow_dags(
    root: Path,
    dag_specs: Mapping[str, Mapping[str, object]],
    workload_packs: Mapping[str, Mapping[str, object]],
    *,
    read_file: ConfinedReleaseFileReader = read_confined_file,
) -> dict[str, ExpectedWorkflowDag]:
    result: dict[str, ExpectedWorkflowDag] = {}
    for dag_id, descriptor in dag_specs.items():
        relative = release_text(descriptor.get("path"), "DAG spec path")
        payload_bytes = read_file(
            root,
            relative,
            max_bytes=dbt_release_artifact_read_bound("DAG spec"),
        )
        require_dbt_release_artifact_bytes(descriptor, payload_bytes, kind="DAG spec")
        workflow, dag = decode_dbt_workflow_dag(payload_bytes, dag_id=dag_id, workload_packs=workload_packs)
        if workflow in result:
            raise DbtDevEvidenceReleaseError("DAG spec workflow identity is invalid")
        result[workflow] = dag
    return result


__all__ = [
    "ConfinedReleaseFileReader",
    "DbtDevEvidenceReleaseError",
    "ExpectedWorkflowDag",
    "read_dbt_execution_pack",
    "read_dbt_workflow_dags",
    "read_dbt_workload_pack",
    "require_dbt_workflow_dag",
    "release_object",
    "release_mapping",
    "release_text",
    "release_digest",
]
