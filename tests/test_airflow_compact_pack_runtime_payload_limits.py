"""Aggregate resource limits for compact-v1 runtime payload materialization."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import dpone.readiness.airflow_compact_pack_runtime_payloads as runtime_payloads
from dpone.cli import main as cli_main

_XCOM = "registry.example/airflow/xcom@sha256:" + "ab" * 32


def _error(code: str, message: str) -> Exception:
    return RuntimeError(f"{code}: {message}")


def _runtime_payload_fixture(root: Path) -> list[dict[str, list[str]]]:
    runtime = root / "runtime/dbt"
    runtime.mkdir(parents=True)
    (runtime / "project.tar.gz").write_bytes(b"12")
    (runtime / "manifest.json").write_bytes(b"34")
    (runtime / "workflow.selection-lock.json").write_bytes(b"56")
    return [
        {
            "runtime_payload_ids": [
                "dbt_project",
                "dbt_manifest",
                "dbt_selection_workflow",
            ]
        }
    ]


def _public_pack_fixture(root: Path) -> None:
    pack = {
        "kind": "dpone.airflow_compact_pack",
        "schema_version": "1",
        "pack_identity": {"schema": "dpone.airflow-pack-identity.v1"},
        "workload": {"workload_id": "orders"},
        "airflow": {"execution": {"outlets": ["asset://demo"]}},
        "connection_projection": {
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
        },
        "xcom": {},
        "provider_execution": {"kpo_kwargs": {"env_vars": {}}},
        "runtime_bootstrap": {
            "schema": "dpone.airflow-runtime-bootstrap.v1",
            "commands": {},
        },
        "runtime_payload_ids": [
            "dbt_project",
            "dbt_manifest",
            "dbt_selection_workflow",
        ],
    }
    pack_path = root / "orders/airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(json.dumps(pack, sort_keys=True) + "\n", encoding="utf-8")
    dag_dir = root / "_dags"
    dag_dir.mkdir(parents=True)
    dag_spec = {
        "schema": "dpone.airflow-dag-spec.v1",
        "dag_id": "DAG__demo__orders__sync",
        "domain": "demo",
        "nodes": [
            {
                "workload_id": "orders",
                "pack_ref": {"path": "orders/airflow-pack.json"},
            }
        ],
    }
    (dag_dir / "DAG__demo__orders__sync.dag-spec.json").write_text(
        json.dumps(dag_spec, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, dict[str, object], str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    payload = json.loads(captured.out) if captured.out else {}
    return int(exc.value.code or 0), payload, captured.err


def test_runtime_payload_materialization_accepts_exact_aggregate_byte_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts = _runtime_payload_fixture(tmp_path)
    monkeypatch.setattr(runtime_payloads, "_MAX_RUNTIME_PAYLOAD_BYTES", 4)
    monkeypatch.setattr(runtime_payloads, "_MAX_RUNTIME_PAYLOAD_TOTAL_BYTES", 6)

    files, descriptors = runtime_payloads._materialize_runtime_payloads(  # noqa: SLF001
        pack_root=tmp_path,
        pack_artifacts=artifacts,
        error_factory=_error,
    )

    assert sum(len(payload) for payload in files.values()) == 6
    assert sum(int(item["bytes"]) for item in descriptors) == 6


def test_runtime_payload_materialization_rejects_overflow_during_confined_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts = _runtime_payload_fixture(tmp_path)
    observed_limits: list[int] = []
    real_read = runtime_payloads.read_confined_file

    def recording_read(
        root: Path,
        relative_path: str,
        *,
        max_bytes: int,
        root_identity: runtime_payloads.ProjectRootIdentity,
    ) -> bytes:
        observed_limits.append(max_bytes)
        return real_read(root, relative_path, max_bytes=max_bytes, root_identity=root_identity)

    monkeypatch.setattr(runtime_payloads, "_MAX_RUNTIME_PAYLOAD_BYTES", 4)
    monkeypatch.setattr(runtime_payloads, "_MAX_RUNTIME_PAYLOAD_TOTAL_BYTES", 5)
    monkeypatch.setattr(runtime_payloads, "read_confined_file", recording_read)

    with pytest.raises(RuntimeError, match="RUNTIME_PAYLOAD_TOTAL_LIMIT_EXCEEDED"):
        runtime_payloads._materialize_runtime_payloads(  # noqa: SLF001
            pack_root=tmp_path,
            pack_artifacts=artifacts,
            error_factory=_error,
        )

    assert observed_limits == [4, 3, 1]


def test_release_materialize_cli_rejects_aggregate_overflow_without_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pack_root = tmp_path / ".dpone/gitops/airflow"
    cache_root = tmp_path / ".dpone-cache"
    _public_pack_fixture(pack_root)
    _runtime_payload_fixture(pack_root)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(runtime_payloads, "_MAX_RUNTIME_PAYLOAD_BYTES", 4)
    monkeypatch.setattr(runtime_payloads, "_MAX_RUNTIME_PAYLOAD_TOTAL_BYTES", 5)

    code, payload, stderr = _run_cli(
        [
            "gitops",
            "airflow",
            "release-materialize",
            "--pack-root",
            str(pack_root),
            "--cache-root",
            str(cache_root),
            "--xcom-sidecar-image",
            _XCOM,
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2
    assert stderr == ""
    assert payload["passed"] is False
    assert payload["release_id"] == ""
    assert payload["release_dir"] == ""
    assert payload["blockers"] == [
        "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_TOTAL_LIMIT_EXCEEDED: "
        "runtime payload 'dbt_selection_workflow' cannot be read safely"
    ]
    assert not cache_root.exists()
