from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from dpone_airflow_pack.dev_evidence_export import (
    DEV_EVIDENCE_EXPORT_ROOT_ENV,
    DevEvidenceExportError,
    export_dev_evidence_if_requested,
)
from dpone_airflow_pack.dev_evidence_export_contract import attempt_evidence
from dpone_airflow_pack.dev_evidence_store import (
    DevEvidenceConfinedStore,
    evidence_set_directory_parts,
    logical_evidence_filename,
)
from dpone_airflow_pack.workflow_outcome import evaluate_workflow_outcome

from dpone.contracts.dbt_publishing import canonical_dbt_execution_evidence_bytes
from dpone.services.dbt_dev_evidence_contracts import (
    validate_dbt_airflow_attempt_evidence,
    validate_workflow_outcome_contract,
)

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64
DAG_SPEC_SHA = "sha256:" + "d" * 64
DBT_PACK_SHA = "sha256:" + "e" * 64
TRANSFER_PACK_SHA = "sha256:" + "f" * 64
RUNTIME_IMAGE_SHA = "sha256:" + "1" * 64
ACTIVATION_ID = "3f60628e-ef48-48b0-84c3-a9e27a82a7f2"


def _campaign_identity() -> dict[str, object]:
    return {
        "schema": "dpone.dbt-dev-evidence-request.v1",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "producer_repository": "PaulKov/airflow-dev",
        "producer_workflow": "dbt-self-service-dev-evidence.yml",
        "source_commit": "a" * 40,
        "orchestration_run_id": "12345",
        "orchestration_run_attempt": 1,
        "workflows": [
            {
                "workflow_id": "daily_marts",
                "dag_id": "DAG__daily_marts",
            }
        ],
    }


def _fingerprint(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


EVIDENCE_SET_ID = _fingerprint(_campaign_identity())
RUN_ID = (
    f"dpone_evidence__{EVIDENCE_SET_ID.removeprefix('sha256:')[:20]}__{hashlib.sha256(b'daily_marts').hexdigest()[:12]}"
)


def _campaign_request() -> dict[str, object]:
    identity = _campaign_identity()
    return {
        **identity,
        "evidence_set_id": EVIDENCE_SET_ID,
        "workflows": [
            {
                **identity["workflows"][0],
                "dag_run_id": RUN_ID,
            }
        ],
    }


class _TaskInstance:
    def __init__(self, values: dict[str, object]) -> None:
        self._values = values

    def xcom_pull(self, *, task_ids: str, key: str | None = None) -> object:
        assert key in (None, "return_value")
        return self._values[task_ids]


class _WorkflowTaskInstance:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    def xcom_push(self, *, key: str, value: object) -> None:
        self.values[key] = value


def _run() -> SimpleNamespace:
    return SimpleNamespace(
        dag_id="DAG__daily_marts",
        run_id=RUN_ID,
        conf={
            "dpone_evidence_authority": {
                "schema": "dpone.dbt-dev-evidence-authority.v1",
                "request_id": EVIDENCE_SET_ID,
                "evidence_set_id": EVIDENCE_SET_ID,
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "workflow_id": "daily_marts",
                "dag_id": "DAG__daily_marts",
                "dag_run_id": RUN_ID,
            }
        },
        get_task_instances=lambda: [
            SimpleNamespace(
                task_id="dbt__daily_marts__dpone_runtime",
                state="success",
                try_number=1,
                map_index=-1,
            ),
            SimpleNamespace(
                task_id="publish_orders__dpone_runtime",
                state="success",
                try_number=1,
                map_index=-1,
            ),
        ],
    )


def _context() -> dict[str, object]:
    return {
        "schema": "dpone.airflow-run-identity.v1",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "dag_spec": {"id": "DAG__daily_marts", "sha256": DAG_SPEC_SHA},
        "runtime_image_digest": RUNTIME_IMAGE_SHA,
        "binding_set_ref": "sha256:" + "2" * 64,
        "connection_registry_ref": "sha256:" + "3" * 64,
        "credential_runtime_ref": "sha256:" + "4" * 64,
        "airflow_bundle": {
            "backend": "git",
            "ref": "git:" + "5" * 40,
            "versioned": True,
            "version": "5" * 40,
            "snapshot_ref": None,
        },
        "_workload_pack_sha256": {
            "dbt__daily_marts": DBT_PACK_SHA,
            "publish_orders": TRANSFER_PACK_SHA,
        },
        "_activation_id": ACTIVATION_ID,
    }


def _deployment_identity() -> dict[str, str]:
    return {
        "schema": "dpone.airflow-deployment-identity.v1",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "activation_id": ACTIVATION_ID,
    }


def _identity(workload_id: str, pack_sha256: str) -> dict[str, object]:
    return {
        **{key: value for key, value in _context().items() if not key.startswith("_")},
        "workload_pack": {"id": workload_id, "sha256": pack_sha256},
    }


def _dbt_evidence() -> dict[str, object]:
    return {
        "schema": "dpone.dbt-execution-evidence.v1",
        "status": "passed",
        "code": "DPONE_DBT_EXECUTION_PASSED",
        "workflow_id": "daily_marts",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "workload_pack_sha256": DBT_PACK_SHA,
        "project_bundle_sha256": "sha256:" + "6" * 64,
        "manifest_sha256": "sha256:" + "7" * 64,
        "selection_sha256": "sha256:" + "8" * 64,
        "toolchain_sha256": "sha256:" + "9" * 64,
        "dbt_exit_code": 0,
        "dbt_warning_policy": "fail",
        "dbt_warning_count": 0,
        "dbt_schema_version": "v6",
        "dbt_version": "1.12.3",
        "invocation_id": "invocation",
        "started_at": "2026-07-27T00:00:00+00:00",
        "finished_at": "2026-07-27T00:01:00+00:00",
        "airflow": {
            "dag_id": "DAG__daily_marts",
            "task_id": "dbt__daily_marts__dpone_runtime",
            "run_id": RUN_ID,
            "try_number": 1,
            "map_index": -1,
        },
        "credential_versions": [],
        "nodes": [
            {
                "unique_id": "model.analytics.orders",
                "status": "success",
                "execution_time": 1.0,
            }
        ],
    }


def _summary(
    workload_id: str,
    pack_sha256: str,
    *,
    dbt_evidence_ref: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "gitops.airflow_xcom_summary",
        "schema_version": "1",
        "producer": "dpone gitops airflow run-spec-exec",
        "status": "passed",
        "runtime_profile_path": "runtime-profile.json",
        "run_spec_path": "run-spec.json",
        "runtime_evidence_path": "runtime-evidence.json",
        "runtime_evidence_sha256": "sha256:" + "0" * 64,
        "runtime_evidence": {
            "schema_version": "dpone.airflow.inline_runtime_evidence.v1",
            "status": "passed",
            "metrics": {"duration_seconds": 1.0, "step_count": 1},
            "step_timeline": [],
            "warnings": [],
        },
        "failed_step": None,
        "warnings": [],
        "blockers": [],
        "run_identity": _identity(workload_id, pack_sha256),
        "deployment_identity": _deployment_identity(),
    }
    if dbt_evidence_ref is not None:
        payload["dbt_execution_evidence_ref"] = dbt_evidence_ref
    return payload


def _outcome() -> dict[str, object]:
    return {
        "schema": "dpone.dbt-workflow-outcome.v2",
        "status": "passed",
        "code": "DPONE_DBT_WORKFLOW_PASSED",
        "workflow_id": "daily_marts",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "dag_run_id": RUN_ID,
        "tasks": [
            {
                "task_id": "publish_orders__outcome_gate",
                "state": "success",
                "mapped_instances": 1,
            }
        ],
        "deployment_identity": _deployment_identity(),
    }


def _export(
    tmp_path: Path,
    *,
    dbt_ref_updates: dict[str, object] | None = None,
    secret_like_warning: str | None = None,
) -> dict[str, object]:
    dbt_payload = canonical_dbt_execution_evidence_bytes(_dbt_evidence())
    store = DevEvidenceConfinedStore(tmp_path)
    base = evidence_set_directory_parts(
        RELEASE_ID,
        DEPLOYMENT_ID,
        EVIDENCE_SET_ID,
    )
    store.install(
        directory_parts=base,
        filename="campaign-request.json",
        payload=(
            json.dumps(
                _campaign_request(),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8"),
    )
    store.install(
        directory_parts=(
            "dbt-spool",
            *base,
            "dbt",
        ),
        filename=logical_evidence_filename("daily_marts"),
        payload=dbt_payload,
    )
    dbt_ref = {
        "schema": "dpone.dbt-execution-evidence-ref.v1",
        "workflow_id": "daily_marts",
        "sha256": "sha256:" + hashlib.sha256(dbt_payload).hexdigest(),
        "bytes": len(dbt_payload),
        "storage_scope": "dbt_spool",
    }
    if dbt_ref_updates:
        dbt_ref.update(dbt_ref_updates)
    values = {
        "dbt__daily_marts__dpone_runtime": _summary(
            "dbt__daily_marts",
            DBT_PACK_SHA,
            dbt_evidence_ref=dbt_ref,
        ),
        "publish_orders__dpone_runtime": _summary(
            "publish_orders",
            TRANSFER_PACK_SHA,
        ),
    }
    if secret_like_warning is not None:
        transfer_summary = values["publish_orders__dpone_runtime"]
        transfer_summary["warnings"] = [{"message": secret_like_warning}]
    return export_dev_evidence_if_requested(
        workflow_id="daily_marts",
        workflow_outcome=_outcome(),
        workload_tasks=(
            {
                "workload_id": "dbt__daily_marts",
                "runtime_task_id": "dbt__daily_marts__dpone_runtime",
            },
            {
                "workload_id": "publish_orders",
                "runtime_task_id": "publish_orders__dpone_runtime",
            },
        ),
        run_identity_context=_context(),
        ti=_TaskInstance(values),
        dag_run=_run(),
        environ={DEV_EVIDENCE_EXPORT_ROOT_ENV: str(tmp_path)},
    )


def test_provider_exports_release_bound_raw_evidence_atomically(tmp_path: Path) -> None:
    report = _export(tmp_path)

    assert report["status"] == "exported"
    root = Path(str(report["evidence_root"]))
    assert root == (
        tmp_path
        / f"releases/sha256-{'a' * 64}"
        / f"deployments/sha256-{'b' * 64}"
        / ("sets/" + EVIDENCE_SET_ID.replace(":", "-", 1))
    )
    assert len(tuple((root / "airflow").glob("*.json"))) == 2
    assert len(tuple((root / "dbt").glob("*.json"))) == 1
    assert len(tuple((root / "outcomes").glob("*.json"))) == 1
    attempt = json.loads(next((root / "airflow").glob("*.json")).read_text(encoding="utf-8"))
    assert attempt["schema"] == "dpone.dbt-airflow-attempt-evidence.v2"
    assert attempt["status"] == "passed"
    assert attempt["evidence_set_id"] == EVIDENCE_SET_ID
    outcome = json.loads(next((root / "outcomes").glob("*.json")).read_text(encoding="utf-8"))
    assert outcome["schema"] == "dpone.dbt-workflow-evidence-outcome.v2"
    assert outcome["evidence_set_id"] == EVIDENCE_SET_ID
    assert len(outcome["artifacts"]) == 3


def test_attempt_evidence_binds_exact_deployment_identity() -> None:
    context = {**_context(), "_activation_id": ACTIVATION_ID}
    deployment_identity = {
        "schema": "dpone.airflow-deployment-identity.v1",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "activation_id": ACTIVATION_ID,
    }
    summary = {
        **_summary("publish_orders", TRANSFER_PACK_SHA),
        "deployment_identity": deployment_identity,
    }
    envelope = attempt_evidence(
        summary,
        context=context,
        workload_id="publish_orders",
        attempt={
            "dag_id": "DAG__daily_marts",
            "task_id": "publish_orders__dpone_runtime",
            "run_id": RUN_ID,
            "try_number": 1,
            "map_index": -1,
        },
        evidence_set_id=EVIDENCE_SET_ID,
    )

    assert envelope["deployment_identity"] == deployment_identity
    _, _, evidence_set_id = validate_dbt_airflow_attempt_evidence(envelope)
    assert evidence_set_id == EVIDENCE_SET_ID

    mismatched = dict(summary)
    mismatched["deployment_identity"] = {
        **deployment_identity,
        "activation_id": "4f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    }
    with pytest.raises(DevEvidenceExportError, match="deployment identity"):
        attempt_evidence(
            mismatched,
            context=context,
            workload_id="publish_orders",
            attempt=envelope["attempt"],
            evidence_set_id=EVIDENCE_SET_ID,
        )


def test_attempt_evidence_rejects_missing_activation_identity() -> None:
    context = dict(_context())
    context.pop("_activation_id")
    summary = _summary("publish_orders", TRANSFER_PACK_SHA)
    summary.pop("deployment_identity")

    with pytest.raises(DevEvidenceExportError, match="activation identity"):
        attempt_evidence(
            summary,
            context=context,
            workload_id="publish_orders",
            attempt={
                "dag_id": "DAG__daily_marts",
                "task_id": "publish_orders__dpone_runtime",
                "run_id": RUN_ID,
                "try_number": 1,
                "map_index": -1,
            },
            evidence_set_id=EVIDENCE_SET_ID,
        )


@pytest.mark.parametrize("activation_id", [ACTIVATION_ID, None])
def test_workflow_outcome_is_accepted_by_downstream_contract(
    activation_id: str | None,
) -> None:
    outcome = evaluate_workflow_outcome(
        workflow_id="daily_marts",
        expected_task_ids=("publish_orders",),
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        activation_id=activation_id,
        ti=_WorkflowTaskInstance(),
        dag_run=SimpleNamespace(
            run_id=RUN_ID,
            get_task_instances=lambda: [SimpleNamespace(task_id="publish_orders", state="success")],
        ),
    )

    assert validate_workflow_outcome_contract(outcome) is None


def test_provider_export_is_byte_idempotent_and_conflicts_fail_closed(
    tmp_path: Path,
) -> None:
    first = _export(tmp_path)
    second = _export(tmp_path)
    assert first["evidence_root"] == second["evidence_root"]
    assert second["no_op"] is True

    root = Path(str(first["evidence_root"]))
    path = next((root / "airflow").glob("*.json"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["status"] = "failed"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DevEvidenceExportError, match="safely"):
        _export(tmp_path)


def test_provider_validates_all_inputs_before_writing_attempt_files(
    tmp_path: Path,
) -> None:
    with pytest.raises(DevEvidenceExportError):
        _export(
            tmp_path,
            dbt_ref_updates={"storage_scope": "wrong_scope"},
        )

    base = tmp_path.joinpath(
        *evidence_set_directory_parts(
            RELEASE_ID,
            DEPLOYMENT_ID,
            EVIDENCE_SET_ID,
        )
    )
    assert not (base / "airflow").exists()
    assert not (base / "dbt").exists()
    assert not (base / "outcomes").exists()


def test_provider_rejects_secret_like_values_before_writing_attempt_files(
    tmp_path: Path,
) -> None:
    with pytest.raises(DevEvidenceExportError, match="sensitive value"):
        _export(
            tmp_path,
            secret_like_warning="password=must-not-be-persisted",
        )

    base = tmp_path.joinpath(
        *evidence_set_directory_parts(
            RELEASE_ID,
            DEPLOYMENT_ID,
            EVIDENCE_SET_ID,
        )
    )
    assert not (base / "airflow").exists()
    assert not (base / "dbt").exists()
    assert not (base / "outcomes").exists()


def test_provider_export_is_not_requested_for_normal_scheduled_run(
    tmp_path: Path,
) -> None:
    run = _run()
    run.conf = {}
    report = export_dev_evidence_if_requested(
        workflow_id="daily_marts",
        workflow_outcome=_outcome(),
        workload_tasks=(),
        run_identity_context=_context(),
        ti=_TaskInstance({}),
        dag_run=run,
        environ={},
    )

    assert report == {
        "schema": "dpone.dbt-dev-evidence-export-report.v1",
        "status": "not_requested",
    }
    assert not tuple(tmp_path.iterdir())


def test_requested_export_requires_safe_platform_root(tmp_path: Path) -> None:
    target = tmp_path / "actual"
    target.mkdir()
    link = tmp_path / "evidence"
    link.symlink_to(target, target_is_directory=True)

    with pytest.raises(DevEvidenceExportError, match="safely"):
        export_dev_evidence_if_requested(
            workflow_id="daily_marts",
            workflow_outcome=_outcome(),
            workload_tasks=(),
            run_identity_context=_context(),
            ti=_TaskInstance({}),
            dag_run=_run(),
            environ={DEV_EVIDENCE_EXPORT_ROOT_ENV: str(link)},
        )
