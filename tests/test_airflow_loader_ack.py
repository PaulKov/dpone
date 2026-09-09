from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import dpone_airflow_pack.cache_activation_contract as activation_contract
import dpone_airflow_pack.dag_loader as dag_loader_module
import pytest
from dpone_airflow_pack.dag_loader import load_dpone_dags
from dpone_airflow_pack.deployment_index import LoadReport
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError
from dpone_airflow_pack.loader_ack import (
    LoaderAcknowledgementError,
    parse_loader_ack_json,
    write_dpone_loader_ack,
)

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64
INDEX_SHA256 = "sha256:" + "c" * 64
ACTIVATION_ID = "12345678-1234-4234-9234-123456789abc"


@pytest.mark.parametrize(
    ("loaded", "skipped", "message"),
    [
        (["DAG__z", "DAG__a"], [], "canonical sorted order"),
        (["DAG__same"], ["DAG__same"], "must be disjoint"),
    ],
)
def test_loader_ack_parser_rejects_noncanonical_dag_sets(
    loaded: list[str],
    skipped: list[str],
    message: str,
) -> None:
    payload = {
        "schema": "dpone.airflow_loader_ack.v2",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "airflow_index_sha256": INDEX_SHA256,
        "activation_id": ACTIVATION_ID,
        "loaded_dag_ids": loaded,
        "skipped_dag_ids": skipped,
        "error_codes": [],
        "fatal": False,
        "acknowledged_at": "2026-08-03T00:00:00+00:00",
    }

    with pytest.raises(LoaderAcknowledgementError, match=message):
        parse_loader_ack_json(json.dumps(payload).encode("utf-8"))


def test_loader_ack_is_atomic_bounded_and_bound_to_index(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    index = cache_root / "current" / "airflow-index.json"
    index.parent.mkdir(parents=True)
    index.write_text('{"deployment_id":"current"}\n', encoding="utf-8")
    acknowledgement = write_dpone_loader_ack(
        LoadReport(
            loaded=("DAG__sales__orders", "DAG__crm__wide"),
            skipped=({"dag_id": "DAG__legacy__same", "reason": "duplicate"},),
            errors=(
                {
                    "dag_id": "DAG__broken__spec",
                    "code": "DPONE_AIRFLOW_DAG_SPEC_INVALID",
                },
            ),
            release_id=RELEASE_ID,
            deployment_id=DEPLOYMENT_ID,
            airflow_index_sha256=INDEX_SHA256,
            cache_root=cache_root.resolve().as_posix(),
            activation_id=ACTIVATION_ID,
        ),
        index_path=index,
        ack_path=cache_root / "status" / "loader-ack.json",
    )

    payload = json.loads((cache_root / "status" / "loader-ack.json").read_text(encoding="utf-8"))
    assert acknowledgement.deployment_id == DEPLOYMENT_ID
    assert payload["loaded_dag_ids"] == [
        "DAG__crm__wide",
        "DAG__sales__orders",
    ]
    assert payload["error_codes"] == ["DPONE_AIRFLOW_DAG_SPEC_INVALID"]
    assert payload["airflow_index_sha256"] == INDEX_SHA256
    assert not list((cache_root / "status").glob("*.tmp"))


def test_loader_ack_rejects_unconfined_destination(tmp_path: Path) -> None:
    index = tmp_path / "cache" / "current" / "airflow-index.json"
    index.parent.mkdir(parents=True)
    index.write_text("{}\n", encoding="utf-8")

    with pytest.raises(
        LoaderAcknowledgementError,
        match="cache status directory",
    ):
        write_dpone_loader_ack(
            LoadReport(
                release_id=RELEASE_ID,
                deployment_id=DEPLOYMENT_ID,
                airflow_index_sha256=INDEX_SHA256,
                cache_root=(tmp_path / "cache").resolve().as_posix(),
                activation_id=ACTIVATION_ID,
            ),
            index_path=index,
            ack_path=tmp_path / "outside.json",
        )


def test_loader_ack_supports_external_writable_channel_with_read_only_cache(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / "cache"
    index = cache_root / "current" / "airflow-index.json"
    index.parent.mkdir(parents=True)
    index.write_text("{}\n", encoding="utf-8")
    ack_root = tmp_path / "ack"
    ack_root.mkdir()
    cache_root.chmod(0o555)
    try:
        acknowledgement = write_dpone_loader_ack(
            LoadReport(
                release_id=RELEASE_ID,
                deployment_id=DEPLOYMENT_ID,
                airflow_index_sha256=INDEX_SHA256,
                cache_root=cache_root.resolve().as_posix(),
                activation_id=ACTIVATION_ID,
            ),
            index_path=index,
            ack_path=ack_root / "loader-ack.json",
            ack_root=ack_root,
        )
    finally:
        cache_root.chmod(0o755)

    assert acknowledgement.activation_id == ACTIVATION_ID
    assert json.loads((ack_root / "loader-ack.json").read_text(encoding="utf-8"))["deployment_id"] == DEPLOYMENT_ID
    assert not (cache_root / "status").exists()


def test_loader_ack_rejects_destination_outside_explicit_ack_root(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / "cache"
    index = cache_root / "current" / "airflow-index.json"
    index.parent.mkdir(parents=True)
    index.write_text("{}\n", encoding="utf-8")
    ack_root = tmp_path / "ack"
    ack_root.mkdir()

    with pytest.raises(
        LoaderAcknowledgementError,
        match="configured acknowledgement root",
    ):
        write_dpone_loader_ack(
            LoadReport(
                release_id=RELEASE_ID,
                deployment_id=DEPLOYMENT_ID,
                airflow_index_sha256=INDEX_SHA256,
                cache_root=cache_root.resolve().as_posix(),
                activation_id=ACTIVATION_ID,
            ),
            index_path=index,
            ack_path=tmp_path / "outside" / "loader-ack.json",
            ack_root=ack_root,
        )


def test_loader_ack_rejects_report_without_loaded_index_digest(tmp_path: Path) -> None:
    index = tmp_path / "cache" / "current" / "airflow-index.json"
    index.parent.mkdir(parents=True)
    index.write_text("{}\n", encoding="utf-8")

    with pytest.raises(LoaderAcknowledgementError, match="airflow_index_sha256"):
        write_dpone_loader_ack(
            LoadReport(
                release_id=RELEASE_ID,
                deployment_id=DEPLOYMENT_ID,
            ),
            index_path=index,
            ack_path=tmp_path / "cache" / "status" / "loader-ack.json",
        )


def test_loader_ack_uses_digest_captured_by_parse_not_mutated_index(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    activation = cache_root / "activations" / "dev" / DEPLOYMENT_ID.replace(":", "-")
    activation.mkdir(parents=True)
    physical_index = activation / "airflow-index.json"
    physical_index.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "environment": "dev",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache_root / "current").symlink_to(
        Path("activations") / "dev" / DEPLOYMENT_ID.replace(":", "-"),
        target_is_directory=True,
    )
    _write_current_pointer(cache_root, activation_id=ACTIVATION_ID)
    index = cache_root / "current" / "airflow-index.json"
    expected_digest = "sha256:" + hashlib.sha256(physical_index.read_bytes()).hexdigest()
    report = load_dpone_dags({}, index_path=index)

    physical_index.write_text('{"mutated":true}\n', encoding="utf-8")
    acknowledgement = write_dpone_loader_ack(
        report,
        index_path=index,
        ack_path=cache_root / "status" / "loader-ack.json",
    )

    assert acknowledgement.airflow_index_sha256 == expected_digest


def test_reactivation_of_same_deployment_gets_a_new_persisted_activation_id(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / "cache"
    activation = cache_root / "activations" / "dev" / DEPLOYMENT_ID.replace(":", "-")
    activation.mkdir(parents=True)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "environment": "dev",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    target = Path("activations") / "dev" / DEPLOYMENT_ID.replace(":", "-")
    current = cache_root / "current"
    current.symlink_to(target, target_is_directory=True)
    _write_current_pointer(cache_root, activation_id=ACTIVATION_ID)
    first = load_dpone_dags({}, index_path=current / "airflow-index.json")

    current.unlink()
    current.symlink_to(target, target_is_directory=True)
    second_activation_id = "87654321-4321-4321-8321-cba987654321"
    _write_current_pointer(cache_root, activation_id=second_activation_id)
    second = load_dpone_dags({}, index_path=current / "airflow-index.json")

    assert first.activation_id == ACTIVATION_ID
    assert second.activation_id == second_activation_id


def test_loader_reports_current_removed_during_identity_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root = tmp_path / "cache"
    activation = cache_root / "activations" / "dev" / DEPLOYMENT_ID.replace(":", "-")
    activation.mkdir(parents=True)
    (cache_root / ".promotion.lock").touch(mode=0o644)
    (activation / "airflow-index.json").write_text("{}\n", encoding="utf-8")
    (cache_root / "current").symlink_to(
        Path("activations") / "dev" / DEPLOYMENT_ID.replace(":", "-"),
        target_is_directory=True,
    )

    def missing_current(*args: object, **kwargs: object) -> str:
        del args, kwargs
        raise FileNotFoundError("current changed")

    monkeypatch.setattr(activation_contract.os, "readlink", missing_current)

    report = load_dpone_dags(
        {},
        index_path=cache_root / "current" / "airflow-index.json",
    )

    assert report.fatal is True
    assert report.errors[0]["code"] == "DPONE_AIRFLOW_INDEX_READ_FAILED"


def test_indexed_parse_holds_shared_cache_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fcntl = pytest.importorskip("fcntl")
    cache_root = tmp_path / "cache"
    activation = cache_root / "activations" / "dev" / DEPLOYMENT_ID.replace(":", "-")
    activation.mkdir(parents=True)
    (cache_root / ".promotion.lock").touch(mode=0o644)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "environment": "dev",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache_root / "current").symlink_to(
        Path("activations") / "dev" / DEPLOYMENT_ID.replace(":", "-"),
        target_is_directory=True,
    )
    original = dag_loader_module._load_airflow_deployment_index_descriptor

    def guarded_descriptor(*args: object, **kwargs: object):
        descriptor = os.open(cache_root / ".promotion.lock", os.O_RDWR)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)
        return original(*args, **kwargs)

    monkeypatch.setattr(
        dag_loader_module,
        "_load_airflow_deployment_index_descriptor",
        guarded_descriptor,
    )

    report = load_dpone_dags({}, index_path=cache_root / "current" / "airflow-index.json")

    assert not report.fatal


def test_indexed_parse_and_ack_holds_one_shared_cache_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fcntl = pytest.importorskip("fcntl")
    cache_root = tmp_path / "cache"
    activation = cache_root / "activations" / "dev" / DEPLOYMENT_ID.replace(":", "-")
    activation.mkdir(parents=True)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "environment": "dev",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache_root / "current").symlink_to(
        Path("activations") / "dev" / DEPLOYMENT_ID.replace(":", "-"),
        target_is_directory=True,
    )
    _write_current_pointer(cache_root, activation_id=ACTIVATION_ID)
    original_write = dag_loader_module.write_dpone_loader_ack

    def guarded_write(*args: object, **kwargs: object):
        descriptor = os.open(cache_root / ".promotion.lock", os.O_RDWR)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)
        return original_write(*args, **kwargs)

    monkeypatch.setattr(dag_loader_module, "write_dpone_loader_ack", guarded_write)

    acknowledged_load = dag_loader_module.load_and_acknowledge_dpone_dags(
        {},
        index_path=cache_root / "current" / "airflow-index.json",
        ack_path=cache_root / "status" / "loader-ack.json",
    )

    assert not acknowledged_load.report.fatal
    assert acknowledged_load.acknowledgement.activation_id == ACTIVATION_ID
    assert json.loads((cache_root / "status" / "loader-ack.json").read_text())["activation_id"] == ACTIVATION_ID


def test_fatal_index_report_surfaces_original_error_not_ack_validation(
    tmp_path: Path,
) -> None:
    """A fatal (unparsed) report must raise the real index error, not a
    misleading 'loader report release_id is invalid' ack validation error."""

    cache_root = tmp_path / "cache"
    (cache_root / "current").mkdir(parents=True)
    (cache_root / ".promotion.lock").touch(mode=0o644)
    index_path = cache_root / "current" / "airflow-index.json"

    report = load_dpone_dags({}, index_path=index_path)
    assert report.fatal
    recorded_code = report.errors[0]["code"]

    with pytest.raises(AirflowDeploymentIndexError) as excinfo:
        dag_loader_module.load_and_acknowledge_dpone_dags(
            {},
            index_path=index_path,
            ack_path=cache_root / "status" / "loader-ack.json",
        )

    assert excinfo.value.code == recorded_code


def test_fatal_report_error_uses_last_recorded_error(tmp_path: Path) -> None:
    """Fatal report builders append the aborting error last; earlier
    skip_and_report entries must not shadow the actual cause."""

    report = LoadReport(
        errors=(
            {
                "dag_id": "DAG__broken__spec",
                "code": "DPONE_AIRFLOW_DAG_SPEC_INVALID",
                "message": "one skipped spec",
            },
            {
                "code": "DPONE_AIRFLOW_INIT_FETCH_PROVIDER_FAILED",
                "message": "provider rejected the artifact",
                "path": "/cache/current/pack.json",
            },
        ),
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        fatal=True,
    )

    error = dag_loader_module._fatal_report_error(
        tmp_path / "current" / "airflow-index.json",
        report,
    )

    assert error.code == "DPONE_AIRFLOW_INIT_FETCH_PROVIDER_FAILED"
    assert "provider rejected the artifact" in str(error)


def test_fatal_report_with_identity_still_persists_fatal_ack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identity-bearing fatal reports keep their durable fatal=True ack; only
    identity-less fatal reports raise instead of hitting ack validation."""

    cache_root = tmp_path / "cache"
    index = cache_root / "current" / "airflow-index.json"
    index.parent.mkdir(parents=True)
    (cache_root / ".promotion.lock").touch(mode=0o644)
    index.write_text('{"deployment_id":"current"}\n', encoding="utf-8")
    fatal_with_identity = LoadReport(
        errors=({"code": "DPONE_AIRFLOW_INIT_FETCH_PROVIDER_FAILED"},),
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        airflow_index_sha256=INDEX_SHA256,
        cache_root=cache_root.resolve().as_posix(),
        activation_id=ACTIVATION_ID,
        fatal=True,
    )
    monkeypatch.setattr(
        dag_loader_module,
        "_load_indexed_dags_unleased",
        lambda *args, **kwargs: fatal_with_identity,
    )

    acknowledged = dag_loader_module.load_and_acknowledge_dpone_dags(
        {},
        index_path=index,
        ack_path=cache_root / "status" / "loader-ack.json",
    )

    assert acknowledged.acknowledgement.fatal is True
    payload = json.loads((cache_root / "status" / "loader-ack.json").read_text(encoding="utf-8"))
    assert payload["fatal"] is True
    assert payload["release_id"] == RELEASE_ID


def test_loader_ack_rejects_cross_root_report(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    for root in (left, right):
        (root / "current").mkdir(parents=True)
        (root / "current" / "airflow-index.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(LoaderAcknowledgementError, match="different cache root"):
        write_dpone_loader_ack(
            LoadReport(
                release_id=RELEASE_ID,
                deployment_id=DEPLOYMENT_ID,
                airflow_index_sha256=INDEX_SHA256,
                cache_root=left.resolve().as_posix(),
                activation_id=ACTIVATION_ID,
            ),
            index_path=right / "current" / "airflow-index.json",
            ack_path=right / "status" / "loader-ack.json",
        )


def test_loader_ack_rejects_symlink_status_directory(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    outside = tmp_path / "outside"
    (cache_root / "current").mkdir(parents=True)
    outside.mkdir()
    (cache_root / "status").symlink_to(outside, target_is_directory=True)
    index = cache_root / "current" / "airflow-index.json"
    index.write_text("{}\n", encoding="utf-8")

    with pytest.raises(LoaderAcknowledgementError, match="could not be persisted"):
        write_dpone_loader_ack(
            LoadReport(
                release_id=RELEASE_ID,
                deployment_id=DEPLOYMENT_ID,
                airflow_index_sha256=INDEX_SHA256,
                cache_root=cache_root.resolve().as_posix(),
                activation_id=ACTIVATION_ID,
            ),
            index_path=index,
            ack_path=cache_root / "status" / "loader-ack.json",
        )

    assert not (outside / "loader-ack.json").exists()


def test_loader_ack_rejects_missing_canonical_identity(tmp_path: Path) -> None:
    with pytest.raises(LoaderAcknowledgementError, match="release_id"):
        write_dpone_loader_ack(
            LoadReport(),
            index_path=tmp_path / "current" / "airflow-index.json",
            ack_path=tmp_path / "status" / "loader-ack.json",
        )


def test_loader_ack_without_activation_id_fails_closed(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / "cache"
    index = cache_root / "current" / "airflow-index.json"
    index.parent.mkdir(parents=True)
    index.write_text("{}\n", encoding="utf-8")

    with pytest.raises(LoaderAcknowledgementError, match="activation_id is required"):
        write_dpone_loader_ack(
            LoadReport(
                release_id=RELEASE_ID,
                deployment_id=DEPLOYMENT_ID,
                airflow_index_sha256=INDEX_SHA256,
                cache_root=cache_root.resolve().as_posix(),
            ),
            index_path=index,
            ack_path=cache_root / "status" / "loader-ack.json",
        )
    assert not (cache_root / "status" / "loader-ack.json").exists()


def _write_current_pointer(cache_root: Path, *, activation_id: str) -> None:
    (cache_root / ".promotion.lock").touch(mode=0o644, exist_ok=True)
    (cache_root / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "activation_id": activation_id,
                "environment": "dev",
                "deployment_id": DEPLOYMENT_ID,
                "release_id": RELEASE_ID,
                "promoted_by": "test://loader",
                "promoted_at": "2026-07-27T09:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
