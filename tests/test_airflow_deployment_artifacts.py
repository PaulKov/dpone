from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import queue
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
import yaml
from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)

from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.manifest import confined_files
from dpone.manifest.confined_files import read_confined_file as read_snapshot
from dpone.readiness import airflow_deployment_artifacts as artifacts_module
from dpone.readiness import airflow_deployment_projection as projection_module
from dpone.readiness import airflow_deployment_projection_io as projection_io
from dpone.readiness.airflow_deployment_errors import deployment_projection_error, deployment_projection_exit_code
from dpone.readiness.airflow_deployment_projection import (
    AirflowDeploymentProjectionError,
    AirflowDeploymentProjectionService,
)
from dpone.readiness.airflow_deployment_projection_policy import require_runtime_payload_authority
from dpone.readiness.airflow_release_artifact_index import ReleaseArtifactRules, index_release_artifacts
from dpone.readiness.airflow_semantic_refresh_projection import (
    SemanticRefreshDeploymentSidecar,
)

_RUNTIME_IMAGE_DIGEST = "sha256:" + "d" * 64
_AIRFLOW_BUNDLE_REF = "git:" + "7" * 40
_PACK_PAYLOAD = {
    "id": "load_orders",
    "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
    "workload": {"workload_id": "load_orders"},
    "airflow": {"execution": {}},
    "connection_projection": {},
    "provider_execution": {
        "schema": "dpone.airflow-provider-execution.v1",
        "kpo_kwargs": {
            "task_id": "load_orders__dpone_runtime",
            "name": "dpone-load-orders",
            "labels": {"dpone.dev/workload-id": "load_orders"},
            "env_vars": {},
        },
        "pod_spec": {"spec": {"containers": [{"name": "base"}]}},
    },
    "xcom": {
        "sidecar_image": "registry.example/airflow/xcom@sha256:" + "a" * 64,
    },
}
_PACK_FINGERPRINT = compute_pack_fingerprint(_PACK_PAYLOAD)
_PACK_BYTES = json.dumps(
    {**_PACK_PAYLOAD, "pack_fingerprint": _PACK_FINGERPRINT},
    sort_keys=True,
).encode("utf-8")


def test_strict_runtime_payload_rejects_declared_byte_mismatch(tmp_path: Path) -> None:
    release_id = "sha256:" + "a" * 64
    payload = b"{}\n"
    relative = f"releases/{release_id.replace(':', '-')}/runtime/dbt/manifest.json"
    target = tmp_path / relative
    target.parent.mkdir(parents=True)
    target.write_bytes(payload)
    release = {
        "artifacts": {
            "runtime_payloads": [
                {
                    "id": "dbt_manifest",
                    "kind": "dbt_manifest",
                    "path": "runtime/dbt/manifest.json",
                    "sha256": _sha256(payload),
                    "bytes": 1,
                    "media_type": "application/json",
                }
            ]
        }
    }

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        index_release_artifacts(
            release,
            release_id=release_id,
            cache_root=tmp_path,
            section="runtime_payloads",
            rules=ReleaseArtifactRules(True, True, True),
        )

    assert exc.value.code == "DPONE_RELEASE_ARTIFACT_BYTES_MISMATCH"


def test_production_runtime_payloads_require_release_set_v2() -> None:
    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        require_runtime_payload_authority("dpone.release-set.v1", True, "production")
    assert exc.value.code == "DPONE_DBT_PRODUCTION_RELEASE_SCHEMA_REQUIRED"
    assert deployment_projection_exit_code(exc.value.code) == 2
    error = deployment_projection_error(exc.value, release_id="sha256:" + "a" * 64, environment="prod")
    assert error["fixes"] == [
        {
            "id": "build_release_set_v2",
            "safety": "manual",
            "command": (
                "dbt parse --project-dir path/to/dbt-project && "
                "dpone dbt compile path/to/dbt-project --cache-root .dpone-cache "
                "--output-dir .dpone/gitops/airflow-v2"
            ),
        }
    ]
    require_runtime_payload_authority("dpone.release-set.v1", True, "non_production")


def test_projection_rejects_stale_claimed_and_requested_release_id(tmp_path: Path) -> None:
    fake_release_id = "sha256:" + "a" * 64
    computed_release_id, _, _ = _write_release(
        tmp_path,
        requested_release_id=fake_release_id,
    )
    _write_environment(tmp_path)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, fake_release_id)

    assert computed_release_id != fake_release_id
    assert exc.value.code == "DPONE_RELEASE_FINGERPRINT_MISMATCH"
    assert not (tmp_path / ".dpone-cache" / "deployments").exists()


def test_projection_rejects_duplicate_release_set_key(tmp_path: Path) -> None:
    release_id, release_path, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    raw = release_path.read_bytes()
    release_path.write_bytes(
        raw.replace(
            b'"schema": "dpone.release-set.v1"',
            b'"schema": "dpone.release-set.v1", "schema": "dpone.release-set.v1"',
            1,
        )
    )

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RELEASE_SET_INVALID"


def test_projection_rejects_oversized_release_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, release_path, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    monkeypatch.setattr(
        artifacts_module,
        "MAX_RELEASE_SET_BYTES",
        release_path.stat().st_size - 1,
        raising=False,
    )

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RELEASE_SET_TOO_LARGE"


def test_projection_rejects_symlinked_release_set(tmp_path: Path) -> None:
    release_id, release_path, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    outside = tmp_path / "outside-release-set.json"
    outside.write_bytes(release_path.read_bytes())
    release_path.unlink()
    release_path.symlink_to(outside)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RELEASE_INPUT_UNSAFE"


def test_projection_rejects_duplicate_artifact_key(tmp_path: Path) -> None:
    duplicate_pack = _PACK_BYTES.replace(
        b'"pack_fingerprint":',
        b'"pack_fingerprint": "' + _PACK_FINGERPRINT.encode() + b'", "pack_fingerprint":',
        1,
    )
    release_id, _, _ = _write_release(tmp_path, pack_bytes=duplicate_pack)
    _write_environment(tmp_path)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RELEASE_ARTIFACT_INVALID"


@pytest.mark.parametrize(
    ("section", "artifact_relative_path"),
    [
        ("dag_specs", "dags/orders_daily.dag-spec.json"),
        ("workload_packs", "packs/load_orders.airflow-pack.json"),
    ],
)
def test_projection_rejects_duplicate_logical_ids_before_artifact_reads(
    tmp_path: Path,
    section: str,
    artifact_relative_path: str,
) -> None:
    release_id, release_path, _ = _write_release(
        tmp_path,
        dag_bytes=b'{"dag_id":"orders_daily"}',
        duplicate_section=section,
    )
    _write_environment(tmp_path)
    (release_path.parent / artifact_relative_path).unlink()

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RELEASE_ARTIFACT_ID_DUPLICATE"
    assert not (tmp_path / ".dpone-cache" / "deployments").exists()


def test_projection_rejects_non_object_json_artifact(tmp_path: Path) -> None:
    release_id, _, _ = _write_release(tmp_path, dag_bytes=b"[]")
    _write_environment(tmp_path)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RELEASE_ARTIFACT_INVALID"


def test_projection_rejects_oversized_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, _, pack_path = _write_release(tmp_path)
    _write_environment(tmp_path)
    monkeypatch.setattr(
        artifacts_module,
        "MAX_RELEASE_ARTIFACT_BYTES",
        pack_path.stat().st_size - 1,
        raising=False,
    )

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RELEASE_ARTIFACT_TOO_LARGE"


def test_projection_rejects_symlinked_artifact_escape(tmp_path: Path) -> None:
    release_id, _, pack_path = _write_release(tmp_path)
    _write_environment(tmp_path)
    outside = tmp_path / "outside-pack.json"
    outside.write_bytes(pack_path.read_bytes())
    pack_path.unlink()
    pack_path.symlink_to(outside)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RELEASE_ARTIFACT_UNSAFE"


def test_projection_uses_each_captured_source_snapshot_exactly_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag_bytes = b'{"dag_id":"orders_daily"}'
    release_id, _, pack_path = _write_release(tmp_path, dag_bytes=dag_bytes)
    _write_environment(tmp_path)
    mutated_payload = {
        **_PACK_PAYLOAD,
        "id": "mutated_load_orders",
    }
    mutated_fingerprint = compute_pack_fingerprint(mutated_payload)
    mutated_pack = json.dumps(
        {**mutated_payload, "pack_fingerprint": mutated_fingerprint},
        sort_keys=True,
    ).encode("utf-8")
    reads: Counter[str] = Counter()
    mutated = False

    def capture(root: Path, relative_path: str, *, max_bytes: int) -> bytes:
        nonlocal mutated
        reads[relative_path] += 1
        payload = read_snapshot(root, relative_path, max_bytes=max_bytes)
        if relative_path.endswith("/packs/load_orders.airflow-pack.json"):
            pack_path.write_bytes(mutated_pack)
            mutated = True
        return payload

    monkeypatch.setattr(
        artifacts_module,
        "read_confined_file",
        capture,
        raising=False,
    )

    result = _materialize(tmp_path, release_id)

    release_prefix = f"releases/{release_id.replace(':', '-')}"
    assert mutated is True
    assert reads == Counter(
        {
            f"{release_prefix}/release-set.json": 1,
            f"{release_prefix}/dags/orders_daily.dag-spec.json": 1,
            f"{release_prefix}/packs/load_orders.airflow-pack.json": 1,
            "environments/prod/binding-set.yaml": 1,
            "environments/prod/credential-runtime.yaml": 1,
            "platform/connection-registries/prod.yaml": 1,
        }
    )
    assert result.airflow_index["workload_packs"][0]["sha256"] == _sha256(_PACK_BYTES)
    assert result.airflow_index["workload_packs"][0]["pack_fingerprint"] == _PACK_FINGERPRINT
    assert result.airflow_index["release"]["sha256"] == _sha256(
        (tmp_path / ".dpone-cache" / release_prefix / "release-set.json").read_bytes()
    )


def test_concurrent_identical_projection_build_is_idempotent(
    tmp_path: Path,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    service = AirflowDeploymentProjectionService(root=tmp_path)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(_materialize_with_service, service, release_id) for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]

    assert results[0].deployment_dir == results[1].deployment_dir
    assert (results[0].deployment_dir / "_SUCCESS").read_text(encoding="utf-8") == "ok\n"


def test_projection_initializes_writer_lease_before_unlocked_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    state = {"held": False, "entries": 0}
    real_publish = projection_module.publish_immutable_projection

    @contextmanager
    def record_lease(_cache_root: Path):
        state["held"] = True
        state["entries"] += 1
        try:
            yield
        finally:
            state["held"] = False

    def publish_outside_lease(**kwargs: Any) -> Path:
        assert state["held"] is False
        return real_publish(**kwargs)

    monkeypatch.setattr(projection_module, "promotion_lock", record_lease)
    monkeypatch.setattr(projection_module, "publish_immutable_projection", publish_outside_lease)

    result = _materialize(tmp_path, release_id)

    assert state["entries"] == 1
    assert (result.deployment_dir / "_SUCCESS").read_text(encoding="utf-8") == "ok\n"


def test_projection_translates_writer_lease_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_common import DeploymentCacheError

    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)

    @contextmanager
    def fail_lease(_cache_root: Path):
        raise DeploymentCacheError("DPONE_CACHE_PROMOTION_LOCK_FAILED", "private lock detail")
        yield  # pragma: no cover

    monkeypatch.setattr(projection_module, "promotion_lock", fail_lease)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_LOCK_FAILED"
    assert "private lock detail" not in str(exc.value)


def test_self_service_projection_reports_cache_lock_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import airflow_self_service_deployment as self_service

    class FailingProjectionService:
        def __init__(self, *, root: Path) -> None:
            del root

        def materialize(self, **_kwargs: Any) -> None:
            raise AirflowDeploymentProjectionError(
                "DPONE_DEPLOYMENT_CACHE_LOCK_FAILED",
                "deployment cache writer lease could not be initialized",
            )

    monkeypatch.setattr(self_service, "AirflowDeploymentProjectionService", FailingProjectionService)

    result = self_service.build_deployment_result(
        root=tmp_path,
        release_id="sha256:" + "a" * 64,
        environment="prod",
        trust_tier="production",
        runtime_image_ref=f"registry.example/dpone-runtime@{_RUNTIME_IMAGE_DIGEST}",
        runtime_image_digest=_RUNTIME_IMAGE_DIGEST,
        artifact_registry_ref="dpone-prod-artifacts",
        registry_config_ref=_config_map_ref("registry"),
        trust_policy_ref=_config_map_ref("policy"),
        airflow_bundle_ref=_AIRFLOW_BUNDLE_REF,
    )

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_DEPLOYMENT_CACHE_LOCK_FAILED"
    assert result.errors[0]["fixes"][0]["id"] == "check_cache_root_permissions"


def test_concurrent_different_projection_build_returns_structured_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, release_path, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    base_release = json.loads(release_path.read_text(encoding="utf-8"))
    variants = {
        "one": _json_bytes({**base_release, "created_at": "2026-07-19T00:00:00Z"}),
        "two": _json_bytes({**base_release, "created_at": "2026-07-19T00:00:01Z"}),
    }
    assert {compute_release_id(json.loads(raw)) for raw in variants.values()} == {release_id}
    selected = threading.local()

    def capture(root: Path, relative_path: str, *, max_bytes: int) -> bytes:
        if relative_path.endswith("/release-set.json"):
            return variants[selected.name]
        return read_snapshot(root, relative_path, max_bytes=max_bytes)

    monkeypatch.setattr(
        artifacts_module,
        "read_confined_file",
        capture,
        raising=False,
    )
    service = AirflowDeploymentProjectionService(root=tmp_path)

    def build(name: str) -> str:
        selected.name = name
        try:
            _materialize_with_service(service, release_id)
        except AirflowDeploymentProjectionError as exc:
            return exc.code
        return "ok"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(build, ("one", "two")))

    assert sorted(results) == ["DPONE_DEPLOYMENT_ALREADY_EXISTS", "ok"]


@pytest.mark.parametrize(
    "artifact_name",
    [
        "deployment.json",
        "airflow-index.json",
        "binding-set.json",
        "connection-registry.ref",
        "credential-runtime.ref",
    ],
)
def test_existing_projection_requires_exact_published_bytes(
    tmp_path: Path,
    artifact_name: str,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    result = _materialize(tmp_path, release_id)
    artifact_path = result.deployment_dir / artifact_name
    published = artifact_path.read_bytes()
    if artifact_name.endswith(".json"):
        reformatted = json.dumps(
            json.loads(published),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    else:
        reformatted = b" " + published
    assert reformatted != published
    artifact_path.write_bytes(reformatted)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_DEPLOYMENT_ALREADY_EXISTS"


def test_existing_projection_rejects_unexpected_directory_entry(tmp_path: Path) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    result = _materialize(tmp_path, release_id)
    (result.deployment_dir / "unexpected.txt").write_text("not part of the projection\n", encoding="utf-8")

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_DEPLOYMENT_ALREADY_EXISTS"


def test_projection_publication_rejects_symlinked_cache_descendant(tmp_path: Path) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    service = AirflowDeploymentProjectionService(root=tmp_path)
    outside = tmp_path / "outside-cache"
    outside.mkdir()
    deployments = tmp_path / ".dpone-cache" / "deployments"
    deployments.symlink_to(outside, target_is_directory=True)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize_with_service(service, release_id)

    assert exc.value.code == "DPONE_DEPLOYMENT_WRITE_FAILED"
    assert not any(outside.iterdir())


def test_existing_projection_rejects_symlink_swap_at_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    result = _materialize(tmp_path, release_id)
    deployment_path = result.deployment_dir / "deployment.json"
    outside = tmp_path / "outside-deployment.json"
    outside.write_bytes(deployment_path.read_bytes())
    real_open = confined_files.os.open
    swapped = False

    def racing_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        if path == "deployment.json" and dir_fd is not None and not swapped:
            deployment_path.unlink()
            deployment_path.symlink_to(outside)
            swapped = True
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(confined_files.os, "open", racing_open)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert swapped is True
    assert exc.value.code == "DPONE_DEPLOYMENT_ALREADY_EXISTS"


def test_oversized_existing_projection_is_rejected_before_path_read_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    result = _materialize(tmp_path, release_id)
    deployment_path = result.deployment_dir / "deployment.json"
    deployment_path.write_bytes(deployment_path.read_bytes() + b" ")
    real_read_bytes = Path.read_bytes

    def forbid_unbounded_read(path: Path) -> bytes:
        if path == deployment_path:
            raise AssertionError("existing projection used an unbounded pathname read")
        return real_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", forbid_unbounded_read)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_DEPLOYMENT_ALREADY_EXISTS"


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO contract is POSIX-only")
def test_existing_projection_fifo_is_rejected_without_blocking(tmp_path: Path) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    result = _materialize(tmp_path, release_id)
    success_path = result.deployment_dir / "_SUCCESS"
    success_path.unlink()
    os.mkfifo(success_path)
    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(
        target=_materialize_error_code,
        args=(tmp_path.as_posix(), release_id, result_queue),
    )
    process.start()
    process.join(timeout=10)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("existing projection read blocked on a FIFO")
    try:
        error_code = result_queue.get(timeout=1)
    except queue.Empty:
        pytest.fail(f"projection process exited without a result (exit={process.exitcode})")
    finally:
        result_queue.close()
        result_queue.join_thread()

    assert error_code == "DPONE_DEPLOYMENT_ALREADY_EXISTS"


@pytest.mark.parametrize(
    ("target", "expected_code"),
    [
        ("binding-set", "DPONE_BINDING_SET_UNSAFE"),
        ("connection-registry", "DPONE_CONNECTION_REGISTRY_UNSAFE"),
        ("credential-runtime", "DPONE_CREDENTIAL_RUNTIME_UNSAFE"),
    ],
)
def test_environment_projection_inputs_reject_symlink_escape(
    tmp_path: Path,
    target: str,
    expected_code: str,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    paths = _write_environment(tmp_path)
    target_path = paths[target]
    outside = tmp_path / f"outside-{target}.yaml"
    outside.write_bytes(target_path.read_bytes())
    target_path.unlink()
    target_path.symlink_to(outside)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == expected_code
    assert not (tmp_path / ".dpone-cache" / "deployments").exists()


def test_environment_projection_input_size_is_checked_before_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    paths = _write_environment(tmp_path)
    monkeypatch.setattr(
        artifacts_module,
        "MAX_ENVIRONMENT_INPUT_BYTES",
        paths["binding-set"].stat().st_size - 1,
        raising=False,
    )

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_BINDING_SET_TOO_LARGE"


def test_strict_v2_requires_pack_fingerprint(tmp_path: Path) -> None:
    release_id, _, _ = _write_release(
        tmp_path,
        pack_bytes=b'{"id":"load_orders"}\n',
    )
    _write_environment(tmp_path)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED"


def test_strict_v2_rejects_self_declared_pack_fingerprint_drift(tmp_path: Path) -> None:
    stale = json.loads(_PACK_BYTES)
    stale["id"] = "changed-without-recomputing-identity"
    release_id, _, _ = _write_release(
        tmp_path,
        pack_bytes=json.dumps(stale, sort_keys=True).encode("utf-8"),
    )
    _write_environment(tmp_path)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RELEASE_ARTIFACT_INVALID"


def test_strict_v2_rejects_identity_valid_pack_without_closed_provider_contract(
    tmp_path: Path,
) -> None:
    incomplete = dict(_PACK_PAYLOAD)
    incomplete.pop("provider_execution")
    incomplete["pack_fingerprint"] = compute_pack_fingerprint(incomplete)
    release_id, _, _ = _write_release(
        tmp_path,
        pack_bytes=json.dumps(incomplete, sort_keys=True).encode("utf-8"),
    )
    _write_environment(tmp_path)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED"


def test_projection_write_failure_is_structured_and_cleans_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)

    def failing_write(_descriptor: int, _payload: bytes) -> None:
        raise OSError("private staging path")

    monkeypatch.setattr(projection_io, "_write_all", failing_write)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_DEPLOYMENT_WRITE_FAILED"
    assert exc.value.operation == "write"
    assert exc.value.cleanup_required is False
    assert "private staging path" not in str(exc.value)
    deployment_parent = tmp_path / ".dpone-cache" / "deployments" / "prod"
    assert not any(".tmp." in path.name for path in deployment_parent.iterdir())


def test_projection_fsync_failure_is_structured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)

    def failing_fsync(_descriptor: int) -> None:
        raise OSError("private fsync detail")

    monkeypatch.setattr(os, "fsync", failing_fsync)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_DEPLOYMENT_WRITE_FAILED"
    assert exc.value.operation == "fsync"
    assert exc.value.cleanup_required is False
    assert "private fsync detail" not in str(exc.value)


def test_projection_rename_failure_is_structured_and_cleans_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)

    def failing_rename(_parent_fd: int, _source: str, _target: str) -> None:
        raise OSError("private rename detail")

    monkeypatch.setattr(projection_io, "_rename_stage", failing_rename)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_DEPLOYMENT_WRITE_FAILED"
    assert exc.value.operation == "rename"
    assert exc.value.publication_state == "unknown"
    assert exc.value.cleanup_required is False
    assert "private rename detail" not in str(exc.value)
    deployment_parent = tmp_path / ".dpone-cache" / "deployments" / "prod"
    assert not any(".tmp." in path.name for path in deployment_parent.iterdir())


def test_projection_parent_fsync_failure_reports_published_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    real_fsync = os.fsync
    fsync_calls = 0

    def fail_after_rename(descriptor: int) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == 8:
            raise OSError("private parent fsync detail")
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_after_rename)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert fsync_calls == 8
    assert exc.value.code == "DPONE_DEPLOYMENT_WRITE_FAILED"
    assert exc.value.operation == "fsync"
    assert exc.value.publication_state == "published"
    assert exc.value.cleanup_required is False
    assert "private parent fsync detail" not in str(exc.value)
    deployment_parent = tmp_path / ".dpone-cache" / "deployments" / "prod"
    final_dirs = [path for path in deployment_parent.iterdir() if not path.name.startswith(".")]
    assert len(final_dirs) == 1
    assert (final_dirs[0] / "_SUCCESS").read_bytes() == b"ok\n"


def test_cleanup_failure_does_not_replace_primary_projection_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)

    def failing_write(_descriptor: int, _payload: bytes) -> None:
        raise OSError("private primary detail")

    def failing_cleanup(_parent_fd: int, _stage_name: str) -> None:
        raise OSError("private cleanup detail")

    monkeypatch.setattr(projection_io, "_write_all", failing_write)
    monkeypatch.setattr(projection_io, "_remove_staging_directory", failing_cleanup)

    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        _materialize(tmp_path, release_id)

    assert exc.value.code == "DPONE_DEPLOYMENT_WRITE_FAILED"
    assert exc.value.operation == "write"
    assert exc.value.cleanup_required is True
    assert exc.value.cleanup_code == "DPONE_DEPLOYMENT_TEMP_CLEANUP_FAILED"
    assert exc.value.cleanup_artifact == "temporary_projection"
    assert "private primary detail" not in str(exc.value)
    assert "private cleanup detail" not in str(exc.value)
    deployment_parent = tmp_path / ".dpone-cache" / "deployments" / "prod"
    residues = [path for path in deployment_parent.iterdir() if ".tmp." in path.name]
    assert len(residues) == 1
    residues[0].rmdir()


def test_projection_io_publishes_only_digest_named_semantic_refresh_sidecars(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "deployments" / "prod"
    digest = "a" * 64
    sidecar_name = f"semantic-refresh-{digest}.dag-projection.json"
    base_files = {
        "deployment.json": b"{}\n",
        "airflow-index.json": b"{}\n",
        "binding-set.json": b"{}\n",
        "connection-registry.ref": b"{}\n",
        "credential-runtime.ref": b"{}\n",
        "_SUCCESS": b"ok\n",
    }

    published = projection_io.publish_immutable_projection(
        root=tmp_path,
        parent=parent,
        final_name="sha256-" + "b" * 64,
        expected_files={**base_files, sidecar_name: b'{"schema":"projection"}\n'},
    )

    assert (published / sidecar_name).read_bytes() == b'{"schema":"projection"}\n'
    assert (published / "_SUCCESS").read_bytes() == b"ok\n"
    with pytest.raises(ValueError, match="projection file set"):
        projection_io.publish_immutable_projection(
            root=tmp_path,
            parent=parent,
            final_name="sha256-" + "c" * 64,
            expected_files={**base_files, "semantic-refresh-current.dag-projection.json": b"{}\n"},
        )


def test_projection_service_binds_semantic_sidecar_after_deployment_identity(
    tmp_path: Path,
) -> None:
    release_id, _, _ = _write_release(tmp_path)
    _write_environment(tmp_path)
    factory = _SemanticSidecarFactory()

    result = _materialize_with_service(
        AirflowDeploymentProjectionService(root=tmp_path),
        release_id,
        sidecars=factory,
    )

    assert factory.observed == (release_id, result.deployment["deployment_id"])
    descriptors = result.airflow_index["semantic_refresh_dag_projections"]
    assert len(descriptors) == 1
    filename = "semantic-refresh-" + "1" * 64 + ".dag-projection.json"
    assert (result.deployment_dir / filename).read_bytes() == factory.content
    assert descriptors[0]["artifact_ref"].endswith("/" + filename)


class _SemanticSidecarFactory:
    content = b'{"schema":"test.semantic-refresh-sidecar"}\n'

    def __init__(self) -> None:
        self.observed: tuple[str, str] | None = None

    def build(
        self,
        *,
        release_id: str,
        deployment_id: str,
    ) -> tuple[SemanticRefreshDeploymentSidecar, ...]:
        self.observed = release_id, deployment_id
        identity = {
            "dag_projection_sha256": "sha256:" + "1" * 64,
            "deployment_id": deployment_id,
            "package_artifacts_sha256": "sha256:" + "2" * 64,
            "plan_bundle_sha256": "sha256:" + "3" * 64,
            "pre_release_bundle_sha256": "sha256:" + "4" * 64,
            "release_id": release_id,
            "schema": "dpone.semantic-refresh-v2-dag-projection-authority.v1",
            "template_pack_fingerprint": "sha256:" + "5" * 64,
            "topology_sha256": "sha256:" + "6" * 64,
            "workflow_plan_sha256": "sha256:" + "7" * 64,
        }
        authority = {
            **identity,
            "authority_record_sha256": _sha256(
                json.dumps(
                    identity,
                    allow_nan=False,
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
            ),
        }
        return (
            SemanticRefreshDeploymentSidecar(
                content=self.content,
                descriptor={
                    "artifact_bytes": len(self.content),
                    "artifact_sha256": _sha256(self.content),
                    "dag_id": "semantic_refresh_daily_events",
                    "dag_projection_sha256": identity["dag_projection_sha256"],
                    "projection_id": ("semantic_refresh_v2::semantic_refresh_daily_events"),
                    "workflow_name": "daily_events",
                },
                authority=authority,
            ),
        )


def _materialize(root: Path, release_id: str):
    return _materialize_with_service(
        AirflowDeploymentProjectionService(root=root),
        release_id,
    )


def _materialize_with_service(
    service: AirflowDeploymentProjectionService,
    release_id: str,
    *,
    sidecars: Any = None,
):
    return service.materialize(
        release_id=release_id,
        environment="prod",
        trust_tier="production",
        runtime_image_digest=_RUNTIME_IMAGE_DIGEST,
        runtime_image_ref=f"registry.example/dpone-runtime@{_RUNTIME_IMAGE_DIGEST}",
        artifact_registry_ref="dpone-prod-artifacts",
        registry_config_ref=_config_map_ref("registry"),
        trust_policy_ref=_config_map_ref("policy"),
        airflow_bundle_ref=_AIRFLOW_BUNDLE_REF,
        semantic_refresh_sidecars=sidecars,
    )


def _materialize_error_code(
    root: str,
    release_id: str,
    result_queue: Any,
) -> None:
    try:
        _materialize(Path(root), release_id)
    except AirflowDeploymentProjectionError as exc:
        result_queue.put(exc.code)
    except Exception as exc:  # pragma: no cover - subprocess diagnostic boundary
        result_queue.put(type(exc).__name__)
    else:
        result_queue.put("ok")


def _write_release(
    root: Path,
    *,
    pack_bytes: bytes = _PACK_BYTES,
    dag_bytes: bytes | None = None,
    requested_release_id: str | None = None,
    duplicate_section: str | None = None,
) -> tuple[str, Path, Path]:
    dag_specs: list[dict[str, Any]] = []
    if dag_bytes is not None:
        dag_specs.append(
            {
                "id": "orders_daily",
                "path": "dags/orders_daily.dag-spec.json",
                "sha256": _sha256(dag_bytes),
            }
        )
    workload_packs = [
        {
            "id": "load_orders",
            "path": "packs/load_orders.airflow-pack.json",
            "sha256": _sha256(pack_bytes),
        }
    ]
    if duplicate_section == "dag_specs":
        dag_specs.append(dict(dag_specs[0]))
    elif duplicate_section == "workload_packs":
        workload_packs.append(dict(workload_packs[0]))
    release: dict[str, Any] = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": dag_specs,
            "workload_packs": workload_packs,
            "canonical_schemas": [],
        },
    }
    computed_release_id = compute_release_id(release)
    stored_release_id = requested_release_id or computed_release_id
    release["release_id"] = stored_release_id
    release_dir = root / ".dpone-cache" / "releases" / stored_release_id.replace(":", "-")
    (release_dir / "packs").mkdir(parents=True)
    pack_path = release_dir / "packs" / "load_orders.airflow-pack.json"
    pack_path.write_bytes(pack_bytes)
    if dag_bytes is not None:
        (release_dir / "dags").mkdir()
        (release_dir / "dags" / "orders_daily.dag-spec.json").write_bytes(dag_bytes)
    release_path = release_dir / "release-set.json"
    release_path.write_bytes(_json_bytes(release))
    return computed_release_id, release_path, pack_path


def _write_environment(root: Path) -> dict[str, Path]:
    environment_dir = root / "environments" / "prod"
    environment_dir.mkdir(parents=True)
    binding_set_path = environment_dir / "binding-set.yaml"
    credential_runtime_path = environment_dir / "credential-runtime.yaml"
    binding_set_path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "prod",
                "bindings": {},
                "runtime": {
                    "kubernetes_namespace": "airflow-example",
                    "service_account": "dpone-runtime",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    credential_runtime_path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.credential-runtime.v1",
                "environment": "prod",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    registry_dir = root / "platform" / "connection-registries"
    registry_dir.mkdir(parents=True)
    connection_registry_path = registry_dir / "prod.yaml"
    connection_registry_path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return {
        "binding-set": binding_set_path,
        "connection-registry": connection_registry_path,
        "credential-runtime": credential_runtime_path,
    }


def _config_map_ref(label: str) -> dict[str, str]:
    return {
        "kind": "kubernetes_config_map",
        "name": f"dpone-artifact-{label}-4f3a",
        "key": f"{label}.json",
        "sha256": "sha256:" + ("1" if label == "registry" else "2") * 64,
    }


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()
