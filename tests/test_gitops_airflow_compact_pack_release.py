"""Compact pack → release-set promotion for v2 RuntimeConnectionContext."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint
from dpone_airflow_pack.pack_identity import compute_pack_fingerprint

import dpone.readiness.airflow_compact_pack_runtime_payloads as runtime_payloads
from dpone.cli import main as cli_main
from dpone.readiness.airflow_compact_pack_release import (
    CompactPackReleaseError,
    closed_connection_projection,
    materialize_compact_pack_release,
    rewrite_strict_init_fetch_dag_spec,
    rewrite_strict_init_fetch_pack,
)
from dpone.readiness.airflow_deployment_projection import compute_release_id
from dpone.readiness.airflow_release_schema_validation import (
    validate_release_set_schema,
)

XCOM = "registry.example/airflow/xcom@sha256:" + "ab" * 32


def _run_cli(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, dict[str, object], str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    payload = json.loads(captured.out) if captured.out else {}
    return int(exc.value.code or 0), payload, captured.err


def _strict_airflow_build_args() -> list[str]:
    runtime_digest = "sha256:" + "b" * 64
    return [
        "--trust-tier",
        "non_production",
        "--runtime-image-ref",
        f"registry.example/dpone-runtime@{runtime_digest}",
        "--runtime-image-digest",
        runtime_digest,
        "--artifact-registry-ref",
        "dpone-dev-artifacts",
        "--registry-config-map-name",
        "dpone-artifact-registry",
        "--registry-config-map-key",
        "registry.json",
        "--registry-config-sha256",
        "sha256:" + "c" * 64,
        "--airflow-bundle-ref",
        "git:" + "7" * 40,
    ]


def _projection(**extra: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-airflow-connection-bridge",
        "mount_path": "/run/secrets/dpone/airflow-connections",
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "cleanup_policy": "after_execute",
        "connections": [
            {
                "connection_id": "warehouse",
                "connection_ref": "warehouse",
                "registry_connection_ref": "warehouse",
                "secret_key": "AIRFLOW_CONN_WAREHOUSE",
                "mount_path": "/run/secrets/dpone/airflow-connections/warehouse",
                "fields": {"uri": "uri"},
            }
        ],
    }
    payload.update(extra)
    return payload


def _pack(*, projection: dict[str, object] | None = None) -> dict[str, object]:
    workload_id = "orders"
    task_id = f"{workload_id}__dpone_runtime"
    payload: dict[str, object] = {
        "kind": "dpone.airflow_compact_pack",
        "schema_version": "1",
        "pack_identity": {"schema": "dpone.airflow-pack-identity.v1"},
        "workload": {"workload_id": workload_id},
        "airflow": {"execution": {"outlets": ["asset://demo"], "extra": "drop-me"}},
        "connection_projection": projection or _projection(),
        "xcom": {},
        "provider_execution": {
            "schema": "dpone.airflow-provider-execution.v1",
            "kpo_kwargs": {
                "task_id": task_id,
                "name": "dpone-orders",
                "env_vars": {"KEEP": "1"},
                "labels": {
                    "app": "dpone",
                    "dpone.dev/workload-id": workload_id,
                },
            },
            "pod_spec": {
                "spec": {
                    "containers": [
                        {
                            "name": "base",
                        }
                    ]
                }
            },
        },
        "runtime_bootstrap": {"schema": "dpone.airflow-runtime-bootstrap.v1", "commands": {}},
    }
    payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
    return payload


def _dag_spec(*, dag_id: str, workload_ids: tuple[str, ...]) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": "dpone.airflow-dag-spec.v1",
        "dag_id": dag_id,
        "domain": "demo",
        "operator_overrides": {
            "image": "drop-me",
            "in_cluster": True,
            "pool": "dpone_source",
            "retries": 0,
        },
        "nodes": [
            {
                "workload_id": workload_id,
                "pack_ref": {"path": f"{workload_id}/airflow-pack.json"},
            }
            for workload_id in workload_ids
        ],
    }
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    return payload


def _write_pack_root(root: Path, *, packs: dict[str, dict[str, object]], specs: dict[str, dict[str, object]]) -> None:
    for workload_id, pack in packs.items():
        path = root / workload_id / "airflow-pack.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(pack, sort_keys=True) + "\n", encoding="utf-8")
    dag_dir = root / "_dags"
    dag_dir.mkdir(parents=True, exist_ok=True)
    for dag_id, spec in specs.items():
        (dag_dir / f"{dag_id}.dag-spec.json").write_text(
            json.dumps(spec, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def test_closed_projection_preserves_uri_overrides() -> None:
    closed = closed_connection_projection(
        _projection(
            query_overrides={"warehouse": {"trust_server_certificate": "yes"}},
            scheme_overrides={"ClickHouse": "clickhouse"},
            database_overrides={"ClickHouse": "DWH_OLAP"},
        )
    )
    assert closed["payload_format"] == "airflow_connection_uri"
    assert closed["secret_values"] is False
    assert closed["query_overrides"]["warehouse"]["trust_server_certificate"] == "yes"
    assert closed["scheme_overrides"]["ClickHouse"] == "clickhouse"
    assert closed["database_overrides"]["ClickHouse"] == "DWH_OLAP"


def test_closed_projection_rejects_unsafe_airflow_env() -> None:
    from dpone.readiness.airflow_compact_pack_release import CompactPackReleaseError

    with pytest.raises(CompactPackReleaseError) as exc:
        closed_connection_projection({"mode": "unsafe_airflow_env", "connections": []})
    assert exc.value.code == "DPONE_COMPACT_PACK_RELEASE_PROJECTION_INVALID"


def test_closed_projection_scrubs_extra_connection_fields() -> None:
    closed = closed_connection_projection(
        _projection(
            connections=[
                {
                    "connection_id": "warehouse",
                    "connection_ref": "warehouse",
                    "registry_connection_ref": "warehouse",
                    "secret_key": "AIRFLOW_CONN_WAREHOUSE",
                    "mount_path": "/run/secrets/dpone/airflow-connections/warehouse",
                    "fields": {"uri": "uri"},
                    "uri": "postgresql://user:pass@host/db",
                    "password": "should-not-persist",
                }
            ]
        )
    )
    assert closed["connections"] == [
        {
            "connection_ref": "warehouse",
            "registry_connection_ref": "warehouse",
            "connection_id": "warehouse",
            "secret_key": "AIRFLOW_CONN_WAREHOUSE",
            "mount_path": "/run/secrets/dpone/airflow-connections/warehouse",
            "fields": {"uri": "uri"},
        }
    ]


def test_rewrite_rejects_tag_only_xcom_sidecar() -> None:
    from dpone.readiness.airflow_compact_pack_release import CompactPackReleaseError

    with pytest.raises(CompactPackReleaseError) as exc:
        rewrite_strict_init_fetch_pack(_pack(), xcom_sidecar_image="registry.example/xcom:latest")
    assert exc.value.code == "DPONE_COMPACT_PACK_RELEASE_XCOM_SIDECAR_INVALID"


def test_rewrite_strict_pack_recomputes_fingerprint_and_pins_xcom() -> None:
    rewritten = rewrite_strict_init_fetch_pack(_pack(), xcom_sidecar_image=XCOM)
    assert rewritten["xcom"]["sidecar_image"] == XCOM
    assert "extra" not in rewritten["airflow"]["execution"]
    assert rewritten["provider_execution"]["kpo_kwargs"]["env_vars"]["KEEP"] == "1"
    assert "AWS_ACCESS_KEY_ID" in rewritten["provider_execution"]["kpo_kwargs"]["env_vars"]
    assert rewritten["pack_fingerprint"] == compute_pack_fingerprint(rewritten)


def test_rewrite_strict_pack_keeps_provider_executor_while_stripping_body_policy() -> None:
    """The strict rewrite is exactly where the executor policy used to be
    dropped: airflow.execution loses task_executor, so the projected
    provider_execution.kpo_kwargs.executor must survive unchanged."""

    pack = _pack()
    execution = pack["airflow"]["execution"]  # type: ignore[index]
    execution["task_executor"] = "KubernetesExecutor"
    kpo_kwargs = pack["provider_execution"]["kpo_kwargs"]  # type: ignore[index]
    kpo_kwargs["executor"] = "KubernetesExecutor"
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)

    rewritten = rewrite_strict_init_fetch_pack(pack, xcom_sidecar_image=XCOM)

    assert "task_executor" not in rewritten["airflow"]["execution"]
    assert rewritten["provider_execution"]["kpo_kwargs"]["executor"] == "KubernetesExecutor"
    assert rewritten["pack_fingerprint"] == compute_pack_fingerprint(rewritten)


def test_rewrite_strict_dag_preserves_only_certified_scheduler_overrides() -> None:
    spec = _dag_spec(
        dag_id="DAG__demo__orders__sync",
        workload_ids=("orders",),
    )

    rewritten = rewrite_strict_init_fetch_dag_spec(
        spec,
        workload_ids=("orders",),
    )

    assert rewritten["operator_overrides"] == {
        "pool": "dpone_source",
        "retries": 0,
    }
    assert rewritten["spec_fingerprint"] == compute_dag_spec_fingerprint(rewritten)


@pytest.mark.parametrize("pool", ["", " padded", "padded ", "x" * 257])
def test_rewrite_strict_dag_rejects_invalid_certified_pool(pool: str) -> None:
    spec = _dag_spec(
        dag_id="DAG__demo__orders__sync",
        workload_ids=("orders",),
    )
    spec["operator_overrides"] = {"pool": pool, "retries": 0}

    with pytest.raises(CompactPackReleaseError) as exc_info:
        rewrite_strict_init_fetch_dag_spec(spec, workload_ids=("orders",))

    assert exc_info.value.code == ("DPONE_COMPACT_PACK_RELEASE_OPERATOR_OVERRIDES_INVALID")


def test_materialize_compact_release_fails_closed_for_invalid_pool(
    tmp_path: Path,
) -> None:
    pack_root = tmp_path / "airflow"
    cache_root = tmp_path / "cache"
    spec = _dag_spec(
        dag_id="DAG__demo__orders__sync",
        workload_ids=("orders",),
    )
    spec["operator_overrides"] = {"pool": "", "retries": 0}
    _write_pack_root(
        pack_root,
        packs={"orders": _pack()},
        specs={"DAG__demo__orders__sync": spec},
    )

    report = materialize_compact_pack_release(
        pack_root=pack_root,
        cache_root=cache_root,
        xcom_sidecar_image=XCOM,
    )

    assert report.passed is False
    assert report.blockers[0].startswith("DPONE_COMPACT_PACK_RELEASE_OPERATOR_OVERRIDES_INVALID:")
    assert not cache_root.exists()


def test_materialize_compact_pack_release_writes_immutable_release(tmp_path: Path) -> None:
    pack_root = tmp_path / "airflow"
    cache_root = tmp_path / "cache"
    packs = {"orders": _pack()}
    specs = {"DAG__demo__orders__sync": _dag_spec(dag_id="DAG__demo__orders__sync", workload_ids=("orders",))}
    _write_pack_root(pack_root, packs=packs, specs=specs)

    first = materialize_compact_pack_release(
        pack_root=pack_root,
        cache_root=cache_root,
        xcom_sidecar_image=XCOM,
        provenance={
            "repo_root": str(tmp_path / "repo"),
            "pack_root": str(pack_root),
            "wrapper": "materialize_smoke_v2_release.py",
            "label": "unit",
        },
    )
    assert first.passed
    assert first.release_id.startswith("sha256:")
    assert first.dag_ids == ("DAG__demo__orders__sync",)
    assert first.workload_ids == ("orders",)
    assert first.connection_projection_mode == "kubernetes_secret_volume"
    release_set = json.loads(Path(first.release_dir, "release-set.json").read_text(encoding="utf-8"))
    assert release_set["release_id"] == first.release_id
    validate_release_set_schema(
        release_set,
        path=Path(first.release_dir, "release-set.json"),
    )
    assert release_set["provenance"]["strict_init_fetch_rewrite"] is True
    assert release_set["provenance"]["promotion_profile"] == "compact_v2_runtime_connection_context"
    assert release_set["provenance"]["label"] == "unit"
    assert "repo_root" not in release_set["provenance"]
    assert "pack_root" not in release_set["provenance"]
    assert "wrapper" not in release_set["provenance"]
    assert release_set["promotion"]["profile"] == "compact_v2_runtime_connection_context"
    release_without_variant = dict(release_set)
    release_without_variant.pop("promotion")
    assert compute_release_id(release_without_variant) != first.release_id
    pack_bytes = Path(first.release_dir, "packs/orders.airflow-pack.json").read_bytes()
    dag_bytes = Path(first.release_dir, "dags/DAG__demo__orders__sync.dag-spec.json").read_bytes()
    released_dag = json.loads(dag_bytes)
    assert released_dag["operator_overrides"] == {
        "pool": "dpone_source",
        "retries": 0,
    }
    assert b"drop-me" not in dag_bytes
    assert b"in_cluster" not in dag_bytes
    assert b"cached://workloads/orders" in dag_bytes

    second = materialize_compact_pack_release(
        pack_root=pack_root,
        cache_root=cache_root,
        xcom_sidecar_image=XCOM,
        provenance={"repo_root": "/other/runner/path", "label": "unit"},
    )
    assert second.passed
    assert second.release_id == first.release_id
    assert Path(second.release_dir, "packs/orders.airflow-pack.json").read_bytes() == pack_bytes
    second_release = json.loads(Path(second.release_dir, "release-set.json").read_text(encoding="utf-8"))
    assert second_release == release_set


def test_compact_release_cli_handoff_builds_and_activates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    pack_root = tmp_path / ".dpone" / "gitops" / "airflow"
    _write_pack_root(
        pack_root,
        packs={"orders": _pack()},
        specs={
            "DAG__demo__orders__sync": _dag_spec(
                dag_id="DAG__demo__orders__sync",
                workload_ids=("orders",),
            )
        },
    )

    code, materialized, stderr = _run_cli(
        [
            "gitops",
            "airflow",
            "release-materialize",
            "--pack-root",
            str(pack_root),
            "--cache-root",
            ".dpone-cache",
            "--xcom-sidecar-image",
            XCOM,
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    code, built, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            str(materialized["release_id"]),
            "--environment",
            "dev",
            *_strict_airflow_build_args(),
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    code, activated, stderr = _run_cli(
        [
            "airflow",
            "cache-sync",
            "--cache-root",
            ".dpone-cache",
            "--deployment-dir",
            str(built["deployment_dir"]),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://tests",
            "--allowed-promoter",
            "ci://tests",
            "--expect-current-absent",
            "--confirm-promote",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert activated["release_id"] == materialized["release_id"]
    assert activated["deployment_id"] == built["deployment"]["deployment_id"]

    code, replayed, stderr = _run_cli(
        [
            "airflow",
            "cache-sync",
            "--cache-root",
            ".dpone-cache",
            "--deployment-dir",
            str(built["deployment_dir"]),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://tests",
            "--allowed-promoter",
            "ci://tests",
            "--expected-current-deployment-id",
            str(activated["deployment_id"]),
            "--confirm-promote",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert replayed["deployment_id"] == activated["deployment_id"]


def test_materialize_filters_dag_ids_and_fails_for_missing_pack(tmp_path: Path) -> None:
    pack_root = tmp_path / "airflow"
    cache_root = tmp_path / "cache"
    packs = {"orders": _pack()}
    specs = {
        "DAG__demo__orders__sync": _dag_spec(dag_id="DAG__demo__orders__sync", workload_ids=("orders",)),
        "DAG__demo__sales__sync": _dag_spec(dag_id="DAG__demo__sales__sync", workload_ids=("sales",)),
    }
    _write_pack_root(pack_root, packs=packs, specs=specs)

    selected = materialize_compact_pack_release(
        pack_root=pack_root,
        cache_root=cache_root,
        xcom_sidecar_image=XCOM,
        dag_ids=("DAG__demo__orders__sync",),
    )
    assert selected.passed
    assert selected.dag_ids == ("DAG__demo__orders__sync",)

    missing = materialize_compact_pack_release(
        pack_root=pack_root,
        cache_root=cache_root,
        xcom_sidecar_image=XCOM,
        dag_ids=("DAG__demo__sales__sync",),
    )
    assert not missing.passed
    assert any("PACK_MISSING" in item for item in missing.blockers)


def test_materialize_fails_closed_on_dangling_runtime_payload_ids(tmp_path: Path) -> None:
    pack_root = tmp_path / "airflow"
    cache_root = tmp_path / "cache"
    pack = _pack()
    pack["runtime_payload_ids"] = ["dbt_project", "dbt_manifest", "dbt_selection_demo"]
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    _write_pack_root(
        pack_root,
        packs={"orders": pack},
        specs={"DAG__demo__orders__sync": _dag_spec(dag_id="DAG__demo__orders__sync", workload_ids=("orders",))},
    )

    report = materialize_compact_pack_release(
        pack_root=pack_root,
        cache_root=cache_root,
        xcom_sidecar_image=XCOM,
    )
    assert not report.passed
    assert any("RUNTIME_PAYLOAD_MISSING" in item for item in report.blockers)


def test_materialize_includes_runtime_payloads_for_dbt_refs(tmp_path: Path) -> None:
    pack_root = tmp_path / "airflow"
    cache_root = tmp_path / "cache"
    pack = _pack()
    # Canonical dbt order — must be preserved on the workload pack descriptor
    # (init-fetch plans select in this sequence; sorting breaks identity).
    pack["runtime_payload_ids"] = ["dbt_project", "dbt_manifest", "dbt_selection_demo"]
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    _write_pack_root(
        pack_root,
        packs={"orders": pack},
        specs={"DAG__demo__orders__sync": _dag_spec(dag_id="DAG__demo__orders__sync", workload_ids=("orders",))},
    )
    runtime = pack_root / "runtime" / "dbt"
    runtime.mkdir(parents=True)
    (runtime / "project.tar.gz").write_bytes(b"project-bytes")
    (runtime / "manifest.json").write_text('{"manifest": true}\n', encoding="utf-8")
    (runtime / "demo.selection-lock.json").write_text('{"selection": true}\n', encoding="utf-8")

    report = materialize_compact_pack_release(
        pack_root=pack_root,
        cache_root=cache_root,
        xcom_sidecar_image=XCOM,
    )
    assert report.passed, report.blockers
    release_set = json.loads(Path(report.release_dir, "release-set.json").read_text(encoding="utf-8"))
    validate_release_set_schema(
        release_set,
        path=Path(report.release_dir, "release-set.json"),
    )
    payloads = release_set["artifacts"]["runtime_payloads"]
    # Release inventory may stay sorted for CAS stability...
    assert [item["id"] for item in payloads] == [
        "dbt_manifest",
        "dbt_project",
        "dbt_selection_demo",
    ]
    assert (Path(report.release_dir) / "runtime/dbt/project.tar.gz").read_bytes() == b"project-bytes"
    pack_desc = next(item for item in release_set["artifacts"]["workload_packs"] if item["id"] == "orders")
    # ...but pack descriptor order must stay canonical (not alphabetically sorted).
    assert pack_desc["runtime_payload_ids"] == [
        "dbt_project",
        "dbt_manifest",
        "dbt_selection_demo",
    ]


def test_materialize_fails_closed_on_sorted_dbt_runtime_payload_order(tmp_path: Path) -> None:
    pack_root = tmp_path / "airflow"
    cache_root = tmp_path / "cache"
    pack = _pack()
    pack["runtime_payload_ids"] = ["dbt_manifest", "dbt_project", "dbt_selection_demo"]
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    _write_pack_root(
        pack_root,
        packs={"orders": pack},
        specs={"DAG__demo__orders__sync": _dag_spec(dag_id="DAG__demo__orders__sync", workload_ids=("orders",))},
    )
    runtime = pack_root / "runtime" / "dbt"
    runtime.mkdir(parents=True)
    (runtime / "project.tar.gz").write_bytes(b"project-bytes")
    (runtime / "manifest.json").write_text('{"manifest": true}\n', encoding="utf-8")
    (runtime / "demo.selection-lock.json").write_text('{"selection": true}\n', encoding="utf-8")

    report = materialize_compact_pack_release(
        pack_root=pack_root,
        cache_root=cache_root,
        xcom_sidecar_image=XCOM,
    )
    assert not report.passed
    assert any("RUNTIME_PAYLOAD_ORDER_INVALID" in item for item in report.blockers)


def test_materialize_rejects_selection_id_that_cannot_form_schema_safe_path(tmp_path: Path) -> None:
    pack_root = tmp_path / "airflow"
    pack = _pack()
    pack["runtime_payload_ids"] = ["dbt_project", "dbt_manifest", "dbt_selection_team:prod"]
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    _write_pack_root(
        pack_root,
        packs={"orders": pack},
        specs={"DAG__demo__orders__sync": _dag_spec(dag_id="DAG__demo__orders__sync", workload_ids=("orders",))},
    )

    report = materialize_compact_pack_release(
        pack_root=pack_root,
        cache_root=tmp_path / "cache",
        xcom_sidecar_image=XCOM,
    )

    assert not report.passed
    assert any("RUNTIME_PAYLOAD_REFS_INVALID" in item for item in report.blockers)


def test_runtime_payload_selection_leaf_respects_portable_name_limit() -> None:
    def error_factory(code: str, message: str) -> Exception:
        return RuntimeError(f"{code}: {message}")

    accepted = f"dbt_selection_{'a' * 235}"
    ids = runtime_payloads._runtime_payload_ids_from_pack(  # noqa: SLF001 - focused policy contract
        {"runtime_payload_ids": ["dbt_project", "dbt_manifest", accepted]},
        error_factory=error_factory,
    )
    relative, _kind, _media_type = runtime_payloads._runtime_payload_locator(  # noqa: SLF001
        ids[-1], error_factory=error_factory
    )
    assert len(Path(relative).name.encode("ascii")) == 255

    rejected = f"dbt_selection_{'a' * 236}"
    with pytest.raises(RuntimeError, match="RUNTIME_PAYLOAD_REFS_INVALID"):
        runtime_payloads._runtime_payload_ids_from_pack(  # noqa: SLF001
            {"runtime_payload_ids": ["dbt_project", "dbt_manifest", rejected]},
            error_factory=error_factory,
        )


def test_runtime_payload_materialization_enforces_count_and_size_bounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def error_factory(code: str, message: str) -> Exception:
        return RuntimeError(f"{code}: {message}")

    too_many = [
        {"runtime_payload_ids": ["dbt_project", "dbt_manifest", f"dbt_selection_workflow_{index}"]}
        for index in range(65)
    ]
    with pytest.raises(RuntimeError, match="RUNTIME_PAYLOAD_REFS_INVALID"):
        runtime_payloads._materialize_runtime_payloads(  # noqa: SLF001 - focused policy contract
            pack_root=tmp_path,
            pack_artifacts=too_many,
            error_factory=error_factory,
        )

    runtime = tmp_path / "runtime/dbt"
    runtime.mkdir(parents=True)
    (runtime / "project.tar.gz").write_bytes(b"12345")
    monkeypatch.setattr(runtime_payloads, "_MAX_RUNTIME_PAYLOAD_BYTES", 4)
    with pytest.raises(RuntimeError, match="RUNTIME_PAYLOAD_OVERSIZED"):
        runtime_payloads._materialize_runtime_payloads(  # noqa: SLF001 - focused policy contract
            pack_root=tmp_path,
            pack_artifacts=[{"runtime_payload_ids": ["dbt_project"]}],
            error_factory=error_factory,
        )


def test_runtime_payload_materialization_rejects_symlinked_parent(tmp_path: Path) -> None:
    outside = tmp_path / "outside/dbt"
    outside.mkdir(parents=True)
    (outside / "project.tar.gz").write_bytes(b"candidate")
    (tmp_path / "runtime").symlink_to(outside.parent, target_is_directory=True)

    def error_factory(code: str, message: str) -> Exception:
        return RuntimeError(f"{code}: {message}")

    with pytest.raises(RuntimeError, match="RUNTIME_PAYLOAD_UNSAFE"):
        runtime_payloads._materialize_runtime_payloads(  # noqa: SLF001 - focused policy contract
            pack_root=tmp_path,
            pack_artifacts=[{"runtime_payload_ids": ["dbt_project"]}],
            error_factory=error_factory,
        )


def test_runtime_payload_materialization_rejects_leaf_symlink(tmp_path: Path) -> None:
    outside = tmp_path / "outside.tar.gz"
    outside.write_bytes(b"outside")
    runtime = tmp_path / "runtime/dbt"
    runtime.mkdir(parents=True)
    (runtime / "project.tar.gz").symlink_to(outside)

    def error_factory(code: str, message: str) -> Exception:
        return RuntimeError(f"{code}: {message}")

    with pytest.raises(RuntimeError, match="RUNTIME_PAYLOAD_UNSAFE"):
        runtime_payloads._materialize_runtime_payloads(  # noqa: SLF001 - focused policy contract
            pack_root=tmp_path,
            pack_artifacts=[{"runtime_payload_ids": ["dbt_project"]}],
            error_factory=error_factory,
        )


def test_runtime_payload_materialization_maps_torn_source_to_unsafe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def error_factory(code: str, message: str) -> Exception:
        return RuntimeError(f"{code}: {message}")

    def fail_read(
        _root: Path,
        _relative: str,
        *,
        max_bytes: int,
        root_identity: runtime_payloads.ProjectRootIdentity,
    ) -> bytes:
        assert max_bytes > 0
        assert root_identity.path == tmp_path
        raise runtime_payloads.ConfinedFileError("source_changed", "source changed")

    monkeypatch.setattr(runtime_payloads, "read_confined_file", fail_read)
    with pytest.raises(RuntimeError, match="RUNTIME_PAYLOAD_UNSAFE"):
        runtime_payloads._materialize_runtime_payloads(  # noqa: SLF001 - focused policy contract
            pack_root=tmp_path,
            pack_artifacts=[{"runtime_payload_ids": ["dbt_project"]}],
            error_factory=error_factory,
        )


def test_runtime_payload_materialization_rejects_root_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pack_root = tmp_path / "pack"
    original_runtime = pack_root / "runtime/dbt"
    original_runtime.mkdir(parents=True)
    (original_runtime / "project.tar.gz").write_bytes(b"expected")
    replacement = tmp_path / "replacement"
    replacement_runtime = replacement / "runtime/dbt"
    replacement_runtime.mkdir(parents=True)
    (replacement_runtime / "project.tar.gz").write_bytes(b"replacement")
    displaced = tmp_path / "displaced"
    real_read = runtime_payloads.read_confined_file
    swapped = False

    def swap_root_then_read(*args: object, **kwargs: object) -> bytes:
        nonlocal swapped
        if not swapped:
            swapped = True
            pack_root.rename(displaced)
            replacement.rename(pack_root)
        return real_read(*args, **kwargs)

    monkeypatch.setattr(runtime_payloads, "read_confined_file", swap_root_then_read)

    def error_factory(code: str, message: str) -> Exception:
        return RuntimeError(f"{code}: {message}")

    with pytest.raises(RuntimeError, match="RUNTIME_PAYLOAD_UNSAFE"):
        runtime_payloads._materialize_runtime_payloads(  # noqa: SLF001 - focused policy contract
            pack_root=pack_root,
            pack_artifacts=[{"runtime_payload_ids": ["dbt_project"]}],
            error_factory=error_factory,
        )
