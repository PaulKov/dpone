"""The runtime's real execution-pack check binds versioned payload byte IDs."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError, canonical_fingerprint, sha256_bytes
from dpone.contracts.dbt_runtime import validate_dbt_runtime_release_identity
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2, dbt_runtime_payload_trio
from tests.test_airflow_runtime_init_fetch_cli import _dbt_runtime_fixture


def _arguments(tmp_path: Path, wire: str) -> dict[str, object]:
    project = tmp_path / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: analytics\n", encoding="utf-8")
    fixture = _dbt_runtime_fixture(project, execution_pack_v2=wire == DBT_RUNTIME_WIRE_V2)
    project_sha, manifest_sha = sha256_bytes(fixture.project_archive), sha256_bytes(fixture.manifest)
    ids = dbt_runtime_payload_trio(
        workflow_id=fixture.workflow_id,
        project_sha256=project_sha,
        manifest_sha256=manifest_sha,
        selection_lock_payload=fixture.selection_lock,
        wire_contract=wire,
    )
    return {
        "execution_pack_payload": fixture.execution_pack,
        "selection_lock_payload": fixture.selection_lock,
        "project_bundle_sha256": project_sha,
        "manifest_sha256": manifest_sha,
        "workload_id": "dbt__" + fixture.workflow_id,
        "airflow_runtime_payload_ids": list(ids),
        "runtime_payload_ids": ids,
        "wire_contract": wire,
    }


@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_runtime_accepts_exact_execution_pack_and_versioned_source_trio(tmp_path: Path, wire: str) -> None:
    validate_dbt_runtime_release_identity(**_arguments(tmp_path, wire))


def test_legacy_default_does_not_implicitly_accept_v2(tmp_path: Path) -> None:
    arguments = _arguments(tmp_path, DBT_RUNTIME_WIRE_V2)
    arguments.pop("wire_contract")
    with pytest.raises(DbtPublishingError):
        validate_dbt_runtime_release_identity(**arguments)


def test_v2_runtime_rejects_fully_rehashed_v1_execution_pack(tmp_path: Path) -> None:
    arguments = _arguments(tmp_path, DBT_RUNTIME_WIRE_V2)
    pack = json.loads(arguments["execution_pack_payload"])
    pack["schema"] = "dpone.dbt-execution-pack.v1"
    pack.pop("invocation_target")
    pack["pack_sha256"] = canonical_fingerprint({key: value for key, value in pack.items() if key != "pack_sha256"})
    arguments["execution_pack_payload"] = json.dumps(pack).encode()
    with pytest.raises(DbtPublishingError):
        validate_dbt_runtime_release_identity(**arguments)


@pytest.mark.parametrize("field", ["project_bundle_sha256", "manifest_sha256", "workload_id"])
def test_v2_runtime_rejects_cross_project_identity(tmp_path: Path, field: str) -> None:
    arguments = _arguments(tmp_path, DBT_RUNTIME_WIRE_V2)
    arguments[field] = "dbt__other" if field == "workload_id" else sha256_bytes(b"other")
    with pytest.raises(DbtPublishingError):
        validate_dbt_runtime_release_identity(**arguments)


def test_v2_runtime_binds_exact_selection_bytes_not_only_semantic_lock(tmp_path: Path) -> None:
    arguments = _arguments(tmp_path, DBT_RUNTIME_WIRE_V2)
    arguments["selection_lock_payload"] += b"\n"
    with pytest.raises(DbtPublishingError):
        validate_dbt_runtime_release_identity(**arguments)


def test_v2_runtime_rejects_reordered_trio_and_unknown_wire(tmp_path: Path) -> None:
    arguments = _arguments(tmp_path, DBT_RUNTIME_WIRE_V2)
    reordered = tuple(reversed(arguments["runtime_payload_ids"]))
    with pytest.raises(DbtPublishingError):
        validate_dbt_runtime_release_identity(**{**arguments, "runtime_payload_ids": reordered})
    with pytest.raises(DbtPublishingError):
        validate_dbt_runtime_release_identity(**{**arguments, "wire_contract": "unsupported"})


@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_plan_order_compatibility_preserves_strict_identity_validation(tmp_path: Path, wire: str) -> None:
    from dpone.contracts.dbt_runtime import dbt_runtime_plan_payload_order

    arguments = _arguments(tmp_path, wire)
    original = arguments["runtime_payload_ids"]
    reordered = tuple(reversed(original))
    # The public identity validator itself remains strict, including legacy v1.
    with pytest.raises(DbtPublishingError):
        validate_dbt_runtime_release_identity(**{**arguments, "runtime_payload_ids": reordered})
    repaired = dbt_runtime_plan_payload_order(
        airflow_runtime_payload_ids=arguments["airflow_runtime_payload_ids"],
        runtime_payload_ids=reordered,
        wire_contract=wire,
    )
    if wire == DBT_RUNTIME_WIRE_V1:
        assert repaired == original
        validate_dbt_runtime_release_identity(**{**arguments, "runtime_payload_ids": repaired})
    else:
        assert repaired == reordered
        with pytest.raises(DbtPublishingError):
            validate_dbt_runtime_release_identity(**{**arguments, "runtime_payload_ids": repaired})


@pytest.mark.parametrize("defect", ["missing", "extra", "duplicate", "not_list"])
def test_order_compatibility_does_not_authorize_invalid_membership(tmp_path: Path, defect: str) -> None:
    from dpone.contracts.dbt_runtime import dbt_runtime_plan_payload_order

    arguments = _arguments(tmp_path, DBT_RUNTIME_WIRE_V1)
    declared = list(arguments["airflow_runtime_payload_ids"])
    declared = {
        "missing": declared[:-1],
        "extra": [*declared, "dbt_selection_other"],
        "duplicate": [*declared, declared[0]],
        "not_list": tuple(declared),
    }[defect]
    planned = dbt_runtime_plan_payload_order(
        airflow_runtime_payload_ids=declared,
        runtime_payload_ids=arguments["runtime_payload_ids"],
        wire_contract=DBT_RUNTIME_WIRE_V1,
    )
    with pytest.raises(DbtPublishingError):
        validate_dbt_runtime_release_identity(
            **{**arguments, "runtime_payload_ids": planned, "airflow_runtime_payload_ids": declared}
        )
