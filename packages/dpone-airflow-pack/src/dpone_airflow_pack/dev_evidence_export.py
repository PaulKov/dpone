"""Execution-time orchestration for native dbt publishing evidence export."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone_airflow_pack.deployment_identity import deployment_identity_from_context
from dpone_airflow_pack.dev_evidence_authority import (
    require_persisted_campaign_authority,
)
from dpone_airflow_pack.dev_evidence_export_contract import (
    ATTEMPT_EVIDENCE_SCHEMA,
    DEV_EVIDENCE_AUTHORITY_CONF_KEY,
    WORKFLOW_EVIDENCE_SCHEMA,
    DevEvidenceExportError,
    airflow_attempt,
    attempt_evidence,
    canonical_bytes,
    digest,
    evidence_authority,
    inventory_item,
    read_dbt_evidence,
    validated_authority,
    xcom_summary,
)
from dpone_airflow_pack.dev_evidence_export_contract import (
    run_identity_context as validate_run_identity_context,
)
from dpone_airflow_pack.dev_evidence_export_contract import (
    workflow_outcome as validate_workflow_outcome,
)
from dpone_airflow_pack.dev_evidence_export_contract import (
    workload_tasks as validate_workload_tasks,
)
from dpone_airflow_pack.dev_evidence_store import (
    DevEvidenceConfinedStore,
    DevEvidenceStoreError,
    evidence_set_directory_parts,
    logical_evidence_filename,
)

DEV_EVIDENCE_EXPORT_ROOT_ENV = "DPONE_DBT_EVIDENCE_EXPORT_ROOT"
_REPORT_SCHEMA = "dpone.dbt-dev-evidence-export-report.v1"


def export_dev_evidence_if_requested(
    *,
    workflow_id: str,
    workflow_outcome: Mapping[str, Any],
    workload_tasks: Sequence[Mapping[str, str]],
    run_identity_context: Mapping[str, Any] | None,
    ti: Any,
    dag_run: Any,
    evidence_root: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Export one workflow when an evidence-set identity is present in run conf."""

    authority = evidence_authority(dag_run)
    if authority is None:
        return {"schema": _REPORT_SCHEMA, "status": "not_requested"}
    environment = os.environ if environ is None else environ
    raw_root = evidence_root or environment.get(DEV_EVIDENCE_EXPORT_ROOT_ENV)
    try:
        store = DevEvidenceConfinedStore(Path(str(raw_root or "")))
        context = validate_run_identity_context(run_identity_context)
        release_id = digest(context.get("release_id"), "release")
        deployment_id = digest(context.get("deployment_id"), "deployment")
        evidence_set_id = validated_authority(
            authority,
            workflow_id=workflow_id,
            release_id=release_id,
            deployment_id=deployment_id,
            dag_run=dag_run,
        )
        tasks = validate_workload_tasks(workload_tasks)
        deployment_identity = deployment_identity_from_context(context)
        if deployment_identity is None:
            raise DevEvidenceExportError("exact provider evidence requires a cache activation identity")
        outcome = validate_workflow_outcome(
            workflow_outcome,
            workflow_id=workflow_id,
            release_id=release_id,
            deployment_id=deployment_id,
            run_id=str(getattr(dag_run, "run_id", "") or ""),
            evidence_set_id=evidence_set_id,
            expected_deployment_identity=deployment_identity,
        )
        base = evidence_set_directory_parts(
            release_id,
            deployment_id,
            evidence_set_id,
        )
        persisted_evidence_set_id = require_persisted_campaign_authority(
            store,
            base=base,
            authority=authority,
            workflow_id=workflow_id,
            release_id=release_id,
            deployment_id=deployment_id,
            dag_id=str(getattr(dag_run, "dag_id", "") or ""),
            dag_run_id=str(getattr(dag_run, "run_id", "") or ""),
        )
        if persisted_evidence_set_id != evidence_set_id:
            raise DevEvidenceExportError("persisted campaign identity differs")
        no_ops: list[bool] = []
        artifact_inventory: list[dict[str, object]] = []
        attempt_artifacts: list[tuple[str, bytes]] = []
        dbt_refs: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
        for workload_id, runtime_task_id in tasks:
            summary = xcom_summary(ti, runtime_task_id)
            attempt = airflow_attempt(dag_run, runtime_task_id)
            evidence = attempt_evidence(
                summary,
                context=context,
                workload_id=workload_id,
                attempt=attempt,
                evidence_set_id=evidence_set_id,
            )
            attempt_payload = canonical_bytes(evidence)
            attempt_artifacts.append((workload_id, attempt_payload))
            artifact_inventory.append(
                inventory_item(
                    category="airflow",
                    logical_id=workload_id,
                    payload=attempt_payload,
                )
            )
            raw_dbt_ref = summary.get("dbt_execution_evidence_ref")
            if isinstance(raw_dbt_ref, Mapping):
                dbt_refs.append((raw_dbt_ref, attempt))
        if len(dbt_refs) != 1:
            raise DevEvidenceExportError("requested workflow must contain exactly one dbt evidence descriptor")
        dbt_payload = read_dbt_evidence(
            store,
            base=base,
            raw_ref=dbt_refs[0][0],
            workflow_id=workflow_id,
            attempt=dbt_refs[0][1],
        )
        for workload_id, attempt_payload in attempt_artifacts:
            no_ops.append(
                store.install(
                    directory_parts=(*base, "airflow"),
                    filename=logical_evidence_filename(workload_id),
                    payload=attempt_payload,
                )
            )
        no_ops.append(
            store.install(
                directory_parts=(*base, "dbt"),
                filename=logical_evidence_filename(workflow_id),
                payload=dbt_payload,
            )
        )
        artifact_inventory.append(
            inventory_item(
                category="dbt",
                logical_id=workflow_id,
                payload=dbt_payload,
            )
        )
        outcome["artifacts"] = sorted(
            artifact_inventory,
            key=lambda item: (str(item["category"]), str(item["logical_id"])),
        )
        outcome_payload = canonical_bytes(outcome)
        no_ops.append(
            store.install(
                directory_parts=(*base, "outcomes"),
                filename=logical_evidence_filename(workflow_id),
                payload=outcome_payload,
            )
        )
    except DevEvidenceExportError:
        raise
    except (DevEvidenceStoreError, OSError, TypeError, ValueError):
        raise DevEvidenceExportError("requested dev evidence could not be exported safely") from None
    evidence_root = store.root.joinpath(*base)
    return {
        "schema": _REPORT_SCHEMA,
        "status": "exported",
        "release_id": release_id,
        "deployment_id": deployment_id,
        "evidence_set_id": evidence_set_id,
        "workflow_id": workflow_id,
        "evidence_root": evidence_root.as_posix(),
        "file_count": len(no_ops),
        "no_op": all(no_ops),
    }


__all__ = [
    "ATTEMPT_EVIDENCE_SCHEMA",
    "DEV_EVIDENCE_EXPORT_ROOT_ENV",
    "DEV_EVIDENCE_AUTHORITY_CONF_KEY",
    "DevEvidenceExportError",
    "WORKFLOW_EVIDENCE_SCHEMA",
    "export_dev_evidence_if_requested",
]
