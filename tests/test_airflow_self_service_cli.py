from __future__ import annotations

import base64
import builtins
import hashlib
import json
import multiprocessing
import os
import shlex
import shutil
import types
from pathlib import Path
from typing import Any

import pytest
import yaml
from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)

from dpone.cli import main as cli_main
from dpone.commands import airflow_artifact_attestation_cmd
from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.gitops.airflow_compact_pack_bootstrap import inline_workload_archive
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.manifest.confined_mutations import ConfinedReplaceOutcome
from dpone.readiness import airflow_loader_migration as airflow_loader_migration_module
from dpone.readiness.airflow_live_preflight import LivePreflightContext
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.readiness.airflow_self_service_models import SelfServiceResult
from dpone.readiness.airflow_self_service_templates import airflow_loader_template
from dpone.security_redaction import REDACTION_TOKEN

_REPO_ROOT = Path(__file__).resolve().parents[1]
_LEGACY_AIRFLOW_LOADER_V0731 = (
    "from airflow.providers.dpone import load_dpone_dags\n\n"
    "load_report = load_dpone_dags(\n"
    "    globals(),\n"
    "    index_path='/opt/airflow/dags/.dpone-cache/current/airflow-index.json',\n"
    ")\n"
)
_PREVIOUS_CANONICAL_AIRFLOW_LOADER = (
    "from airflow.providers.dpone import load_dpone_dags\n\n"
    "load_report = load_dpone_dags(\n"
    "    globals(),\n"
    "    index_path='/opt/airflow/.dpone-cache/current/airflow-index.json',\n"
    ")\n"
    "if load_report.fatal:\n"
    "    error_code = (\n"
    "        load_report.errors[0].get('code', 'DPONE_AIRFLOW_INDEX_INVALID')\n"
    "        if load_report.errors\n"
    "        else 'DPONE_AIRFLOW_INDEX_INVALID'\n"
    "    )\n"
    "    raise RuntimeError(f'{error_code}: dpone Airflow deployment index could not be loaded')\n"
)
_LEGACY_AIRFLOW_LOADER_PRE_SEMANTIC_REFRESH = (
    "from airflow.providers.dpone import load_dpone_dags\n\n"
    "load_report = load_dpone_dags(\n"
    "    globals(),\n"
    "    index_path='/opt/airflow/dags/.dpone-cache/current/airflow-index.json',\n"
    ")\n"
    "if load_report.fatal:\n"
    "    error_code = (\n"
    "        load_report.errors[0].get('code', 'DPONE_AIRFLOW_INDEX_INVALID')\n"
    "        if load_report.errors\n"
    "        else 'DPONE_AIRFLOW_INDEX_INVALID'\n"
    "    )\n"
    "    raise RuntimeError(f'{error_code}: dpone Airflow deployment index could not be loaded')\n"
)


class _PassingLivePreflightRunner:
    def run(self, context: LivePreflightContext) -> dict[str, object]:
        return {
            "probes": [
                {
                    "connection_ref": connection_ref,
                    "probe": probe,
                    "status": "passed",
                    "runner": "configured",
                    "network": True,
                    "secrets": True,
                    "source_queries": probe != "credential_resolution",
                }
                for connection_ref in context.resolved_connection_refs
                for probe in ("credential_resolution", "bounded_source_probe", "bounded_sink_probe")
            ],
            "errors": [],
        }


class _FailingLivePreflightRunner:
    def run(self, context: LivePreflightContext) -> dict[str, object]:
        del context
        raise RuntimeError("runner failed with password=must-not-leak token=also-must-not-leak")


class _ReturnedSecretErrorLivePreflightRunner:
    def run(self, context: LivePreflightContext) -> dict[str, object]:
        return {
            "probes": [
                {
                    "connection_ref": context.resolved_connection_refs[0],
                    "probe": "credential_resolution",
                    "status": "failed",
                    "runner": "configured",
                    "network": True,
                    "secrets": True,
                    "source_queries": False,
                }
            ],
            "errors": [
                {
                    "schema": "dpone.error.v1",
                    "code": "DPONE_LIVE_CHECK_PROBE_FAILED",
                    "stage": "check_live",
                    "severity": "error",
                    "message": "probe failed with password=must-not-leak token=also-must-not-leak",
                    "details": {
                        "stderr": "vault_token=super-secret",
                        "attempts": ["api_key=hidden-value"],
                    },
                    "fixes": [],
                }
            ],
        }


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def _strict_airflow_build_args(runtime_image_digest: str) -> list[str]:
    return [
        "--trust-tier",
        "non_production",
        "--runtime-image-ref",
        f"registry.example/dpone-runtime@{runtime_image_digest}",
        "--runtime-image-digest",
        runtime_image_digest,
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


def _composition_supervisor_args() -> list[str]:
    return [
        "--composition-supervisor-pvc",
        "dpone-composition-supervisor",
        "--composition-child-uid-start",
        "1000000000",
        "--composition-child-gid-start",
        "1000000000",
        "--composition-child-identity-count",
        "1000000",
    ]


def _safe_sample_cache_root(root: Path) -> Path:
    return root / ".dpone-cache" / "safe-sample-deployments"


def _write_airflow_build_release(root: Path) -> str:
    dag_bytes = b'{"dag_id":"orders_daily"}\n'
    pack_bytes = (json.dumps(_strict_workload_pack("load_orders"), sort_keys=True) + "\n").encode()
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [
                {
                    "id": "orders_daily",
                    "path": "dags/orders_daily.dag-spec.json",
                    "sha256": "sha256:" + hashlib.sha256(dag_bytes).hexdigest(),
                }
            ],
            "workload_packs": [
                {
                    "id": "load_orders",
                    "path": "packs/load_orders.airflow-pack.json",
                    "sha256": "sha256:" + hashlib.sha256(pack_bytes).hexdigest(),
                }
            ],
            "canonical_schemas": [],
        },
    }
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    release_dir = root / ".dpone-cache" / "releases" / release_id.replace(":", "-")
    (release_dir / "dags").mkdir(parents=True)
    (release_dir / "packs").mkdir()
    (release_dir / "dags" / "orders_daily.dag-spec.json").write_bytes(dag_bytes)
    (release_dir / "packs" / "load_orders.airflow-pack.json").write_bytes(pack_bytes)
    (release_dir / "release-set.json").write_text(
        json.dumps(release),
        encoding="utf-8",
    )
    return release_id


def _strict_workload_pack(workload_id: str) -> dict[str, object]:
    task_id = f"{workload_id}__dpone_runtime"
    payload: dict[str, object] = {
        "id": workload_id,
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "workload": {"workload_id": workload_id},
        "airflow": {"execution": {}},
        "connection_projection": {},
        "kpo_kwargs": {
            "task_id": task_id,
            "name": f"dpone-{workload_id.replace('_', '-')}",
        },
        "provider_execution": {
            "schema": "dpone.airflow-provider-execution.v1",
            "kpo_kwargs": {
                "task_id": task_id,
                "name": f"dpone-{workload_id.replace('_', '-')}",
                "labels": {"dpone.dev/workload-id": workload_id},
                "env_vars": {},
            },
            "pod_spec": {"spec": {"containers": [{"name": "base"}]}},
        },
        "xcom": {
            "sidecar_image": "registry.example/airflow/xcom@sha256:" + "d" * 64,
        },
    }
    payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
    return payload


def _process_safe_sample_fingerprints(root: str, results: Any) -> None:
    from dpone.readiness.airflow_local_safe_sample_deployment import ensure_local_safe_sample_deployment

    root_path = Path(root)
    source = "pipelines/orders_daily/pipeline.yaml"
    try:
        archive = inline_workload_archive(repo_root=root_path, paths=(source,))
        prepared = ensure_local_safe_sample_deployment(
            root=root_path,
            pipeline_source_path=source,
            policy_environment="development",
        )
        cache_root = _safe_sample_cache_root(root_path)
        index = json.loads((cache_root / "current" / "airflow-index.json").read_text())
        pack_ref = str(index["workload_packs"][0]["artifact_ref"])
        pack = (cache_root / pack_ref.removeprefix("cache://")).read_bytes()
        results.put(("ok", archive, "sha256:" + hashlib.sha256(pack).hexdigest(), prepared.release_id))
    except Exception as exc:  # noqa: BLE001 - spawned test worker must report its own failure.
        results.put(("error", repr(exc), None, None))


def _write_prod_airflow_connection_bridge(tmp_path: Path) -> None:
    prod_env = tmp_path / "environments" / "prod"
    prod_env.mkdir(parents=True)
    (prod_env / "binding-set.yaml").write_text(
        (tmp_path / "environments" / "dev" / "binding-set.yaml")
        .read_text(encoding="utf-8")
        .replace("environment: dev", "environment: prod"),
        encoding="utf-8",
    )
    (prod_env / "credential-runtime.yaml").write_text(
        "schema: dpone.credential-runtime.v1\nenvironment: prod\n",
        encoding="utf-8",
    )
    registry_dir = tmp_path / "platform" / "connection-registries"
    (registry_dir / "prod.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "mssql_dev": {
                        "type": "mssql",
                        "connection": {},
                        "credentials": {
                            "resolver": "airflow_connection",
                            "connection_id": "mssql_prod",
                            "execution_mode": "operator_bridge",
                        },
                    },
                    "clickhouse_dev": {
                        "type": "clickhouse",
                        "connection": {},
                        "credentials": {
                            "resolver": "airflow_connection",
                            "connection_id": "clickhouse_prod",
                            "execution_mode": "operator_bridge",
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def test_beginner_init_text_output_is_concise_and_actionable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init", "project", "--airflow"], capsys)

    assert code == 0, stderr
    assert "dpone init project" in stdout
    assert "- status: passed" in stdout
    assert "- changes: 5 create" in stdout
    assert "- next: dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental" in stdout
    assert "- files:" not in stdout
    assert "- dpone.yaml" not in stdout
    assert "--format json" not in stdout
    assert "--- /dev/null" not in stdout
    assert "rollback_journal" not in stdout
    assert "diff" not in stdout

    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone init pipeline" in stdout
    assert "- status: passed" in stdout
    assert "- changes: 4 create" in stdout
    assert "- next: dpone check orders_daily" in stdout
    assert "- files:" not in stdout
    assert "- pipelines/orders_daily/pipeline.yaml" not in stdout
    assert "--- /dev/null" not in stdout
    assert "rollback_journal" not in stdout
    assert "diff" not in stdout


def test_init_project_loader_fails_closed_for_fatal_deployment_index_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 0, stderr
    loader_path = tmp_path / "dags" / "dpone.py"
    loader = loader_path.read_text(encoding="utf-8")
    compile(loader, loader_path.as_posix(), "exec")
    assert "if loaded.report.fatal:" in loader
    assert "ack_root='/opt/airflow/.dpone-ack'" in loader
    assert "DPONE_AIRFLOW_INDEX_INVALID" in loader
    assert "raise RuntimeError" in loader


@pytest.mark.parametrize(
    "legacy_loader",
    [_LEGACY_AIRFLOW_LOADER_V0731, _LEGACY_AIRFLOW_LOADER_PRE_SEMANTIC_REFRESH],
)
def test_init_project_upgrades_known_generated_loader_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    legacy_loader: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    loader_path = tmp_path / "dags" / "dpone.py"
    fresh = loader_path.read_text(encoding="utf-8")
    loader_path.write_text(legacy_loader, encoding="utf-8")

    code, stdout, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 0, stderr
    payload = json.loads(stdout)
    loader_change = next(change for change in payload["changes"] if change["path"] == "dags/dpone.py")
    assert loader_change["action"] == "update"
    upgraded = loader_path.read_text(encoding="utf-8")
    assert upgraded == fresh
    assert "if loaded.report.fatal:" in upgraded
    assert payload["rollback_journal"]["entries"] == []

    code, stdout, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 0, stderr
    rerun = json.loads(stdout)
    assert {change["action"] for change in rerun["changes"]} == {"no_op"}
    assert loader_path.read_text(encoding="utf-8") == upgraded


def test_init_project_upgrades_previous_canonical_cache_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    loader_path = tmp_path / "dags" / "dpone.py"
    loader_path.write_text(_PREVIOUS_CANONICAL_AIRFLOW_LOADER, encoding="utf-8")

    code, stdout, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 0, stderr
    payload = json.loads(stdout)
    loader_change = next(change for change in payload["changes"] if change["path"] == "dags/dpone.py")
    assert loader_change["action"] == "update"
    assert loader_path.read_text(encoding="utf-8") == airflow_loader_template()


@pytest.mark.parametrize(
    "legacy_loader",
    [_LEGACY_AIRFLOW_LOADER_V0731, _LEGACY_AIRFLOW_LOADER_PRE_SEMANTIC_REFRESH],
)
def test_init_project_does_not_upgrade_user_modified_generated_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    legacy_loader: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    loader_path = tmp_path / "dags" / "dpone.py"
    modified = legacy_loader + "# user customization\n"
    loader_path.write_text(modified, encoding="utf-8")

    code, stdout, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 1, stderr
    payload = json.loads(stdout)
    loader_change = next(change for change in payload["changes"] if change["path"] == "dags/dpone.py")
    assert loader_change["action"] == "conflict"
    assert loader_path.read_text(encoding="utf-8") == modified
    assert payload["rollback_journal"]["entries"] == []


def test_init_project_preserves_loader_modified_during_upgrade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    loader_path = tmp_path / "dags" / "dpone.py"
    loader_path.write_text(_LEGACY_AIRFLOW_LOADER_V0731, encoding="utf-8")
    concurrent_bytes = b"# user edit that won the migration race\n"
    replace_file_if_digest = airflow_loader_migration_module.replace_file_if_digest

    def replace_after_user_edit(
        parent_fd: int,
        name: str,
        replacement_name: str,
        **kwargs: Any,
    ) -> None:
        descriptor = os.open(name, os.O_WRONLY | os.O_TRUNC, dir_fd=parent_fd)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(concurrent_bytes)
            stream.flush()
            os.fsync(descriptor)
        replace_file_if_digest(parent_fd, name, replacement_name, **kwargs)

    monkeypatch.setattr(
        airflow_loader_migration_module,
        "replace_file_if_digest",
        replace_after_user_edit,
    )

    code, stdout, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 1, stderr
    payload = json.loads(stdout)
    loader_change = next(change for change in payload["changes"] if change["path"] == "dags/dpone.py")
    assert loader_change["action"] == "conflict"
    assert loader_path.read_bytes() == concurrent_bytes
    assert not tuple(loader_path.parent.glob(".dpone.py.dpone-*"))


def test_init_project_reports_loader_cleanup_recovery_instead_of_false_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    loader_path = tmp_path / "dags" / "dpone.py"
    loader_path.write_text(_LEGACY_AIRFLOW_LOADER_V0731, encoding="utf-8")

    def commit_without_cleanup(
        parent_fd: int,
        name: str,
        replacement_name: str,
        **_: object,
    ) -> ConfinedReplaceOutcome:
        displaced = f".{name}.displaced"
        os.rename(name, displaced, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        os.rename(
            replacement_name,
            name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        os.rename(
            displaced,
            replacement_name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        return ConfinedReplaceOutcome(
            committed=True,
            cleanup_required=True,
            recovery_name=replacement_name,
        )

    monkeypatch.setattr(
        airflow_loader_migration_module,
        "replace_file_if_digest",
        commit_without_cleanup,
    )

    code, stdout, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 4, stderr
    payload = json.loads(stdout)
    assert payload["errors"][0]["code"] == "DPONE_SCAFFOLD_APPLY_FAILED"
    assert payload["recovery_required"] is True
    assert len(payload["recovery_artifacts"]) == 1
    assert (tmp_path / payload["recovery_artifacts"][0]).read_text(encoding="utf-8") == _LEGACY_AIRFLOW_LOADER_V0731


@pytest.mark.parametrize(
    "error_code",
    ["DPONE_AIRFLOW_INDEX_NOT_FOUND", "DPONE_AIRFLOW_INDEX_JSON_INVALID"],
)
def test_upgraded_loader_raises_during_import_for_fatal_index_report(
    error_code: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    loader_path = tmp_path / "dags" / "dpone.py"
    loader_path.write_text(_LEGACY_AIRFLOW_LOADER_V0731, encoding="utf-8")
    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr

    provider = types.SimpleNamespace(
        load_and_acknowledge_dpone_dags=lambda *_args, **_kwargs: types.SimpleNamespace(
            report=types.SimpleNamespace(
                fatal=True,
                errors=({"code": error_code},),
            )
        )
    )
    real_import = __import__

    def import_module(name: str, *args: object, **kwargs: object) -> object:
        if name == "airflow.providers.dpone":
            return provider
        return real_import(name, *args, **kwargs)

    loader_globals = {"__builtins__": {**vars(builtins), "__import__": import_module}}
    with pytest.raises(RuntimeError, match=error_code):
        exec(compile(loader_path.read_text(encoding="utf-8"), loader_path.as_posix(), "exec"), loader_globals)


def test_beginner_check_and_preview_text_output_is_actionable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )

    code, stdout, stderr = _run_cli(["check", "pipelines/orders_daily"], capsys)

    assert code == 0, stderr
    assert "dpone check: OK" in stdout
    assert "- pipeline: orders_daily" in stdout
    assert "- next: dpone airflow preview orders_daily" in stdout
    assert "- mode: static" not in stdout
    assert "- secrets: no" not in stdout

    code, stdout, stderr = _run_cli(["airflow", "preview", "orders_daily"], capsys)

    assert code == 0, stderr
    assert "dpone airflow preview: OK" in stdout
    assert "- pipeline: orders_daily" in stdout
    assert "- deployment: preview (not runnable)" in stdout
    assert "- airflow index: .dpone-cache/current/airflow-index.json" in stdout
    assert "- next: dpone test orders_daily" in stdout
    assert "- deployment type:" not in stdout
    assert "- node " not in stdout
    assert "- visible tasks:" not in stdout


def test_check_help_marks_selection_args_as_advanced(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(["check", "--help"], capsys)
    help_text = " ".join(f"{stdout}\n{stderr}".split())

    assert code == 0
    assert "Advanced selection:" in help_text
    assert "Advanced: filter workloads (see Workload selectors docs)" in help_text
    assert "Advanced: exclude workloads after selection expansion" in help_text
    assert "Advanced: local dpone.selection-state.v1 baseline for state:* selectors" in help_text
    assert "Advanced: named selector file (default: selectors.yaml)" in help_text
    assert "Advanced: hard selected workload limit (default: 1000)" in help_text


def test_beginner_explain_text_output_is_actionable_and_secret_topology_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    _run_cli(["airflow", "preview", "orders_daily", "--format", "json"], capsys)

    code, stdout, stderr = _run_cli(["airflow", "explain", "pipelines/orders_daily"], capsys)

    assert code == 0, stderr
    assert "dpone airflow explain: OK" in stdout
    assert "- pipeline: orders_daily" in stdout
    assert "- manifest: source" in stdout
    assert "- dag spec: materialized" in stdout
    assert "- airflow pack: materialized" in stdout
    assert "- operator diagnostics: materialized" in stdout
    assert "- operator pinning: pinned" in stdout
    assert "- runtime delivery: local_preview" in stdout
    assert "- parse side effects: none" in stdout
    assert "- next: dpone test orders_daily" in stdout
    assert "secret_name" not in stdout
    assert "secret_key" not in stdout
    assert "mount_path" not in stdout
    assert "/run/secrets" not in stdout
    assert "Traceback" not in stdout


def test_beginner_explain_fails_for_missing_pipeline_even_when_another_preview_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(["init", "pipeline", "orders_daily", "--airflow", "--format", "json"], capsys)
    _run_cli(["airflow", "preview", "orders_daily", "--format", "json"], capsys)

    code, stdout, _ = _run_cli(["airflow", "explain", "missing"], capsys)

    assert code == 1
    assert "dpone airflow explain: FAILED" in stdout
    assert "- manifest: missing" in stdout
    assert "DPONE_PIPELINE_SOURCE_NOT_FOUND" in stdout
    assert "dpone run pipelines/missing" not in stdout
    assert "dpone airflow preview missing" not in stdout


def test_beginner_explain_does_not_reuse_another_pipeline_preview(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(["init", "pipeline", "orders_daily", "--airflow", "--format", "json"], capsys)
    _run_cli(["airflow", "preview", "orders_daily", "--format", "json"], capsys)
    source = Path("pipelines/orders_daily/pipeline.yaml")
    customer_source = Path("pipelines/customers/pipeline.yaml")
    customer_source.parent.mkdir(parents=True)
    customer_source.write_text(
        source.read_text(encoding="utf-8").replace("orders_daily", "customers"),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(["airflow", "explain", "customers"], capsys)

    assert code == 0, stderr
    assert "dpone airflow explain: OK" in stdout
    assert "- manifest: source" in stdout
    assert "- dag spec: planned" in stdout
    assert "- airflow pack: planned" in stdout
    assert "- next: dpone airflow preview customers" in stdout
    assert "dpone run pipelines/customers" not in stdout


def test_beginner_explain_marks_preview_stale_after_semantic_source_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(["init", "pipeline", "orders_daily", "--airflow", "--format", "json"], capsys)
    _run_cli(["airflow", "preview", "orders_daily", "--format", "json"], capsys)
    source = Path("pipelines/orders_daily/pipeline.yaml")
    source.write_text(
        source.read_text(encoding="utf-8").replace("name: orders\n", "name: orders_v2\n"),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(["airflow", "explain", "orders_daily"], capsys)

    assert code == 0, stderr
    assert "dpone airflow explain: NEEDS_ATTENTION" in stdout
    assert "- dag spec: stale" in stdout
    assert "- airflow pack: stale" in stdout
    assert "- next: dpone airflow preview orders_daily" in stdout
    assert "dpone run pipelines/orders_daily" not in stdout


def test_beginner_explain_text_output_routes_planned_state_to_preview(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )

    code, stdout, stderr = _run_cli(["airflow", "explain", "orders_daily"], capsys)

    assert code == 0, stderr
    assert "dpone airflow explain: OK" in stdout
    assert "- dag spec: planned" in stdout
    assert "- operator diagnostics: planned" in stdout
    assert "- next: dpone airflow preview orders_daily" in stdout
    assert "dpone run pipelines/orders_daily" not in stdout


def test_beginner_explain_text_output_flags_invalid_operator_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    current = Path(".dpone-cache/current")
    current.mkdir(parents=True)
    (current / "airflow-index.json").write_text("{not-json", encoding="utf-8")

    code, stdout, stderr = _run_cli(["airflow", "explain", "orders_daily"], capsys)

    assert code == 1, stderr
    assert "dpone airflow explain: FAILED" in stdout
    assert "- operator diagnostics: invalid" in stdout
    assert "- action: Regenerate the local Airflow preview/deployment index" in stdout
    assert "dpone run pipelines/orders_daily" not in stdout
    assert "secret_name" not in stdout
    assert "mount_path" not in stdout
    assert "Traceback" not in stdout


def test_beginner_airflow_facade_scaffolds_checks_and_previews(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 0, stderr
    project = json.loads(stdout)
    assert project["passed"] is True
    jsonschema = pytest.importorskip("jsonschema")
    journal_schema = json.loads(
        (_REPO_ROOT / "docs/schemas/gitops/scaffold-rollback-journal.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.validate(project["rollback_journal"], journal_schema)
    assert {change["action"] for change in project["changes"]} == {"create"}
    assert project["rollback_journal"]["schema"] == "dpone.scaffold-rollback-journal.v1"
    assert {"action": "delete", "path": "dpone.yaml"} in project["rollback_journal"]["entries"]
    assert all(change["diff"].startswith("--- /dev/null") for change in project["changes"])
    assert (tmp_path / "dpone.yaml").exists()
    assert (tmp_path / "dags" / "dpone.py").exists()
    assert (tmp_path / "environments" / "dev" / "binding-set.yaml").exists()
    assert (tmp_path / "environments" / "dev" / "credential-runtime.yaml").exists()
    registry_path = tmp_path / "platform" / "connection-registries" / "dev.yaml"
    assert registry_path.exists()
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    assert registry["connections"]["mssql_dev"]["credentials"]["fields"] == {
        "username": "DPONE_CONN_MSSQL_DEV_USERNAME",
        "password": "DPONE_CONN_MSSQL_DEV_PASSWORD",
    }
    assert registry["connections"]["clickhouse_dev"]["credentials"]["fields"] == {
        "username": "DPONE_CONN_CLICKHOUSE_DEV_USERNAME",
        "password": "DPONE_CONN_CLICKHOUSE_DEV_PASSWORD",
    }

    code, stdout, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 0, stderr
    rerun = json.loads(stdout)
    assert rerun["passed"] is True
    assert {change["action"] for change in rerun["changes"]} == {"no_op"}
    assert rerun["rollback_journal"]["entries"] == []

    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    pipeline = json.loads(stdout)
    assert pipeline["passed"] is True
    assert pipeline["pipeline_id"] == "orders_daily"
    assert {"action": "delete", "path": "pipelines/orders_daily/pipeline.yaml"} in pipeline["rollback_journal"][
        "entries"
    ]
    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    domain_path = tmp_path / "domains" / "sales.yaml"
    test_path = tmp_path / "tests" / "orders_daily.test.yaml"
    fixture_path = tmp_path / "tests" / "fixtures" / "orders_daily.input.jsonl"
    assert pipeline_path.exists()
    assert domain_path.exists()
    assert test_path.exists()
    assert fixture_path.exists()

    source = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    assert source["authoring"] == {"mode": "flow", "source": "pipelines/orders_daily/pipeline.yaml"}
    assert source["kind"] == "dpone.flow.v1"
    assert source["processes"][0]["source"]["type"] == "mssql"
    assert source["processes"][0]["sink"]["type"] == "clickhouse"

    code, stdout, stderr = _run_cli(["check", "pipelines/orders_daily", "--format", "json"], capsys)

    assert code == 0, stderr
    check = json.loads(stdout)
    assert check["passed"] is True
    assert check["mode"] == "static"
    assert check["network"] is False
    assert check["secrets"] is False

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev", "--format", "json"],
        capsys,
    )

    assert code == 0, stderr
    connection_check = json.loads(stdout)
    assert connection_check["passed"] is True
    assert connection_check["mode"] == "connections"
    assert connection_check["handshake"] == "configuration_only"
    assert sorted(connection_check["connection_refs"]) == ["clickhouse_dev", "mssql_dev"]

    code, stdout, stderr = _run_cli(["airflow", "preview", "orders_daily", "--format", "json"], capsys)

    assert code == 0, stderr
    preview = json.loads(stdout)
    assert preview["passed"] is True
    assert preview["deployment"]["runnable"] is False
    assert preview["deployment"]["runtime_artifact_delivery"]["mode"] == "local_preview"
    index_path = tmp_path / preview["airflow_index_path"]
    assert index_path.exists()
    index = json.loads(index_path.read_text(encoding="utf-8"))
    assert index["schema"] == "dpone.airflow-deployment-index.v1"
    assert index["dag_specs"][0]["artifact_ref"].startswith("cache://releases/")
    assert "../" not in index["dag_specs"][0]["artifact_ref"]
    assert index["dag_specs"][0]["bytes"] > 0
    assert index["workload_packs"][0]["bytes"] > 0
    assert index["runtime_artifact_delivery"]["mode"] == "local_preview"
    pointer_path = tmp_path / ".dpone-cache" / "current-pointer.json"
    pointer_schema = json.loads(
        (_REPO_ROOT / "docs/schemas/gitops/current-pointer.schema.json").read_text(encoding="utf-8")
    )
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    jsonschema.validate(pointer, pointer_schema)
    assert pointer["environment"] == "local-preview"
    assert pointer["release_id"] == preview["release"]["release_id"]
    assert pointer["deployment_id"] == preview["deployment"]["deployment_id"]
    assert pointer["promoted_by"] == "local://dpone-airflow-preview"

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            preview["release"]["release_id"],
            "--environment",
            "dev",
            *_strict_airflow_build_args("sha256:" + "e" * 64),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1, stderr
    preview_build = json.loads(stdout)
    assert preview_build["errors"][0]["code"] == "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED"

    strict_release_id = _write_airflow_build_release(tmp_path)
    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            strict_release_id,
            "--environment",
            "dev",
            *_strict_airflow_build_args("sha256:" + "e" * 64),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    build = json.loads(stdout)
    assert build["passed"] is True
    assert build["deployment"]["deployment_type"] == "environment"
    assert build["deployment"]["runtime_artifact_delivery"]["mode"] == "init_fetch"
    assert build["deployment"]["runtime_artifact_delivery"]["artifact_registry_ref"] == "dpone-dev-artifacts"
    assert Path(build["deployment_dir"]).exists()

    code, stdout, stderr = _run_cli(["airflow", "explain", "orders_daily", "--format", "json"], capsys)

    assert code == 0, stderr
    explain = json.loads(stdout)
    assert explain["kind"] == "dpone.airflow-explain.v1"
    assert GitOpsSchemaValidator().validate(explain, expected_kind="dpone.airflow-explain.v1") == ()
    assert explain["artifact_state"]["dag_spec"] == "materialized"
    assert explain["artifact_state"]["airflow_pack"] == "materialized"
    assert explain["operator_diagnostics"]["status"] == "materialized"
    assert explain["operator_diagnostics"]["release_id"] == preview["release"]["release_id"]
    assert explain["operator_diagnostics"]["deployment_id"] == preview["deployment"]["deployment_id"]
    assert explain["operator_diagnostics"]["runtime_artifact_delivery"]["mode"] == "local_preview"
    assert explain["operator_diagnostics"]["parse_side_effects"] == {
        "network": False,
        "metadata_db": False,
        "airflow_variables": False,
        "airflow_connections": False,
        "vault": False,
        "kubernetes": False,
        "cache_refresh": False,
    }

    code, stdout, stderr = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code == 3, stderr
    sample = json.loads(stdout)
    assert sample["passed"] is False
    assert sample["result"]["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED"
    assert sample["result"]["errors"][0]["schema"] == "dpone.error.v1"
    assert sample["result"]["errors"][0]["stage"] == "mssql_clickhouse_safe_sample_copier"
    assert sample["result"]["errors"][0]["severity"] == "error"
    assert sample["safe_sample"]["local_deployment"]["status"] == "promoted"
    assert sample["safe_sample"]["target"] == "temporary"
    assert "binding_resolver" in sample["safe_sample"]["available_contracts"]
    assert "cache_materializer" in sample["safe_sample"]["available_contracts"]
    assert "safe_sample_source_request" in sample["safe_sample"]["available_contracts"]
    assert "safe_sample_runtime_runner" in sample["safe_sample"]["available_contracts"]
    assert "safe_sample_runtime_evidence_writer" in sample["safe_sample"]["available_contracts"]
    assert "temporary_target_plan" in sample["safe_sample"]["available_contracts"]
    assert "temporary_target_lifecycle_executor" in sample["safe_sample"]["available_contracts"]
    assert "temporary_target_adapter_registry" in sample["safe_sample"]["available_contracts"]
    assert "clickhouse_temporary_target_adapter" in sample["safe_sample"]["available_contracts"]
    assert sample["safe_sample"]["runtime_readiness"]["schema"] == "dpone.safe-sample-runtime-readiness.v1"
    assert sample["safe_sample"]["runtime_readiness"]["ready"] is True
    assert sample["safe_sample"]["runtime_readiness"]["blockers"] == []
    assert sample["safe_sample"]["policy"]["environment"] == "development"
    assert sample["safe_sample"]["policy_result"]["passed"] is True
    assert sample["safe_sample"]["temporary_target_plan"]["connection_ref"] == "clickhouse_dev"
    assert sample["safe_sample"]["temporary_target_plan"]["temporary_table"]["schema"] == "dpone_tmp_development"
    assert sample["safe_sample"]["temporary_target_plan"]["pii_policy"] == "masked"
    assert sample["safe_sample"]["source_request"]["schema"] == "dpone.safe-sample-source-request.v1"
    assert sample["safe_sample"]["source_request"]["status"] == "planned"
    assert sample["safe_sample"]["source_request"]["mode"] == "pushdown"
    assert sample["safe_sample"]["source_request"]["sample_rows"] == 1000
    assert sample["safe_sample"]["source_request"]["source_read_only"] is True
    assert sample["safe_sample"]["source_request"]["target"]["connection_ref"] == "clickhouse_dev"
    assert sample["safe_sample"]["certified_copy_request"] == {
        "schema": "dpone.safe-sample-certified-copy-request.v1",
        "certification_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
        "source": {
            "type": "mssql",
            "connection_ref": "mssql_dev",
            "table": {"schema": "dbo", "name": "orders"},
        },
        "sink": {
            "type": "clickhouse",
            "connection_ref": "clickhouse_dev",
            "temporary_table": sample["safe_sample"]["temporary_target_plan"]["temporary_table"],
        },
        "strategy": "incremental_merge",
        "sample_rows": 1000,
        "max_bytes": 10 * 1024**3,
        "timeout_seconds": 300,
        "source_read_only": True,
        "pii_policy": "masked",
        "proof": "route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
    }
    assert sample["safe_sample"]["runtime_run"]["schema"] == "dpone.safe-sample-runtime-run.v1"
    assert sample["safe_sample"]["runtime_run"]["execution_status"] == "failed"
    assert (
        sample["safe_sample"]["runtime_run"]["runtime_execution"]["data_copy"]["errors"][0]["code"]
        == "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED"
    )
    evidence_path = tmp_path / sample["safe_sample"]["runtime_run"]["evidence_write"]["path"]
    assert evidence_path.exists()
    evidence_payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence_payload["schema"] == "dpone.safe-sample-runtime-execution.v1"
    assert evidence_payload["data_copy"]["copy_request"]["certification_id"] == (
        "mssql_clickhouse_incremental_merge_airflow_kpo"
    )
    assert sample["safe_sample"]["execution_plan"]["schema"] == "dpone.safe-sample-execution-plan.v1"
    runtime_handoff = sample["safe_sample"]["runtime_handoff"]
    assert runtime_handoff["schema"] == "dpone.safe-sample-runtime-handoff.v1"
    assert (
        GitOpsSchemaValidator().validate(
            runtime_handoff,
            expected_kind="dpone.safe-sample-runtime-handoff.v1",
        )
        == ()
    )
    assert runtime_handoff["plan_path"].endswith("safe-sample-execution-plan.json")
    assert (tmp_path / runtime_handoff["plan_path"]).exists()
    handoff_plan = json.loads((tmp_path / runtime_handoff["plan_path"]).read_text(encoding="utf-8"))
    assert handoff_plan == sample["safe_sample"]["execution_plan"]
    assert runtime_handoff["command"] == (
        f"dpone ops safe-sample-runtime-run --plan-json {runtime_handoff['plan_path']}"
    )
    assert runtime_handoff["live_copy_command"].startswith(
        f"dpone ops safe-sample-runtime-run --plan-json {runtime_handoff['plan_path']} --pipeline-source "
    )
    assert "--enable-live-copy" in runtime_handoff["live_copy_command"]
    assert (
        sample["safe_sample"]["execution_plan"]["artifact_pinning"]["release_id"]
        == sample["safe_sample"]["local_deployment"]["release_id"]
    )
    assert (
        sample["safe_sample"]["execution_plan"]["artifact_pinning"]["deployment_id"]
        == sample["safe_sample"]["local_deployment"]["deployment_id"]
    )
    assert sample["safe_sample"]["execution_plan"]["blockers"] == []
    assert "runnable_deployment_set" not in sample["safe_sample"]["blockers"]
    assert "certified_source_data_copier" not in sample["safe_sample"]["blockers"]
    assert "temporary_target_runtime_execution" not in sample["safe_sample"]["blockers"]
    assert "temporary_target_adapter_wiring" not in sample["safe_sample"]["blockers"]
    assert "temporary_target_executor" not in sample["safe_sample"]["blockers"]
    assert "cache_materializer" not in sample["safe_sample"]["blockers"]

    code, stdout, stderr = _run_cli(
        [
            "run",
            "pipelines/orders_daily",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--environment",
            "production",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 4, stderr
    prod_sample = json.loads(stdout)
    assert prod_sample["safe_sample"]["policy_result"]["passed"] is False
    assert (
        prod_sample["safe_sample"]["policy_result"]["capabilities"]["proof"]
        == "route_certification:mssql_clickhouse_incremental_merge_airflow_kpo"
    )
    assert prod_sample["result"]["errors"][0]["code"] == "DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED"
    assert prod_sample["result"]["errors"][0]["schema"] == "dpone.error.v1"

    source["processes"][0]["sink"]["strategy"]["mode"] = "full_refresh"
    pipeline_path.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        [
            "run",
            "pipelines/orders_daily",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--environment",
            "production",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 4, stderr
    uncertified_prod_sample = json.loads(stdout)
    assert uncertified_prod_sample["safe_sample"]["policy_result"]["passed"] is False
    assert uncertified_prod_sample["safe_sample"]["source_request"]["status"] == "blocked"
    assert [error["code"] for error in uncertified_prod_sample["safe_sample"]["source_request"]["errors"]] == [
        "DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED"
    ]
    assert {error["code"] for error in uncertified_prod_sample["result"]["errors"]} == {
        "DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED"
    }

    source["processes"][0]["source"]["sampling"] = {
        "mode": "pushdown",
        "proof": "connector_capability",
        "estimated_read_bytes": 1024 * 1024,
    }
    pipeline_path.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        [
            "run",
            "pipelines/orders_daily",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--environment",
            "production",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 4, stderr
    declared_prod_sample = json.loads(stdout)
    assert declared_prod_sample["safe_sample"]["policy_result"]["passed"] is False
    assert declared_prod_sample["safe_sample"]["policy_result"]["capabilities"]["proof"] == "connector_capability"
    assert declared_prod_sample["result"]["errors"][0]["code"] == "DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED"
    assert declared_prod_sample["result"]["errors"][0]["schema"] == "dpone.error.v1"


@pytest.mark.parametrize(
    "args",
    [
        ["run", "pipelines/missing", "--sample", "1000", "--format", "json"],
        ["run", "pipelines/missing", "--target", "temporary", "--format", "json"],
    ],
)
def test_safe_sample_cli_rejects_incomplete_sample_target_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    args: list[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(args, capsys)

    assert code == 2, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["result"]["status"] == "invalid"
    assert payload["result"]["errors"][0]["schema"] == "dpone.error.v1"
    assert payload["result"]["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID"
    assert payload["result"]["errors"][0]["stage"] == "safe_sample_cli_arguments"
    assert payload["result"]["errors"][0]["severity"] == "error"
    assert payload["safe_sample"]["schema"] == "dpone.safe-sample-cli-argument-validation.v1"
    assert payload["safe_sample"]["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID"
    assert "runtime_run" not in payload["safe_sample"]


def test_safe_sample_argument_fix_preserves_budget_environment_and_run_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "run",
            "orders_daily",
            "--sample",
            "42",
            "--environment",
            "production",
            "--run-id",
            "audit-42",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2, stderr
    payload = json.loads(stdout)
    command = payload["result"]["errors"][0]["fixes"][0]["command"]
    assert shlex.split(command) == [
        "dpone",
        "run",
        "orders_daily",
        "--sample",
        "42",
        "--target",
        "temporary",
        "--environment",
        "production",
        "--run-id",
        "audit-42",
    ]


@pytest.mark.parametrize("sample_rows", ["0", "-5"])
def test_safe_sample_cli_rejects_non_positive_sample_budget_before_pipeline_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_rows: str,
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["run", "pipelines/missing", "--sample", sample_rows, "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code == 2, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["result"]["status"] == "invalid"
    assert payload["result"]["errors"][0]["schema"] == "dpone.error.v1"
    assert payload["result"]["errors"][0]["code"] == "DPONE_RUNTIME_SAMPLE_SIZE_INVALID"
    assert payload["result"]["errors"][0]["stage"] == "safe_sample_cli_arguments"
    assert payload["result"]["errors"][0]["severity"] == "error"
    assert payload["safe_sample"]["schema"] == "dpone.safe-sample-cli-argument-validation.v1"
    assert payload["safe_sample"]["errors"][0]["code"] == "DPONE_RUNTIME_SAMPLE_SIZE_INVALID"
    assert "runtime_run" not in payload["safe_sample"]


def test_safe_sample_cli_argument_validation_payload_matches_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["run", "pipelines/missing", "--sample", "0", "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code == 2, stderr
    payload = json.loads(stdout)
    schema_path = _REPO_ROOT / "docs/schemas/gitops/safe-sample-cli-argument-validation.schema.json"
    assert schema_path.exists()
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    jsonschema.validate(payload["safe_sample"], schema)


def test_safe_sample_cli_reports_missing_pipeline_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["run", "pipelines/missing", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code == 4, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["result"]["errors"][0]["code"] == "DPONE_PIPELINE_SOURCE_NOT_FOUND"
    assert "Traceback" not in stdout
    assert payload["safe_sample"]["local_deployment"]["status"] == "failed"


def test_safe_sample_cli_text_error_includes_code_and_fix_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["run", "pipelines/missing", "--sample", "1000"], capsys)

    assert code == 2, stderr
    assert "DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID" in stdout
    assert "dpone run pipelines/missing --sample 1000 --target temporary" in stdout
    assert "Traceback" not in stdout


def test_safe_sample_cli_text_output_shows_runtime_evidence_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    _run_cli(["airflow", "preview", "orders_daily", "--format", "json"], capsys)

    code, stdout, stderr = _run_cli(
        [
            "run",
            "pipelines/orders_daily",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--run-id",
            "local-sample",
        ],
        capsys,
    )

    assert code == 3, stderr
    assert "dpone safe sample handoff prepared" in stdout
    assert "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED" in stdout
    assert "- evidence:" in stdout
    assert "platform prepares the signed authorization overlay; rerun the same dpone run command" in stdout
    assert "runtime status:" not in stdout
    assert "data outcome:" not in stdout
    assert "runtime evidence:" not in stdout
    assert "runtime handoff:" not in stdout
    assert "live copy handoff:" not in stdout
    assert "--enable-live-copy" not in stdout
    assert (
        tmp_path
        / ".dpone-cache"
        / "safe-sample-runs"
        / "orders_daily"
        / "local-sample"
        / "safe-sample-runtime-execution.json"
    ).exists()
    assert "Traceback" not in stdout


def test_safe_sample_cli_markdown_output_shows_runtime_evidence_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    _run_cli(["airflow", "preview", "orders_daily", "--format", "json"], capsys)

    code, stdout, stderr = _run_cli(
        [
            "run",
            "pipelines/orders_daily",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--run-id",
            "local-sample",
            "--format",
            "md",
        ],
        capsys,
    )

    assert code == 3, stderr
    assert "# dpone safe sample handoff prepared" in stdout
    assert "- evidence: `" in stdout
    assert ("- next: `platform prepares the signed authorization overlay; rerun the same dpone run command`") in stdout
    assert "runtime status:" not in stdout
    assert "runtime evidence:" not in stdout
    assert "runtime handoff:" not in stdout
    assert "live copy handoff:" not in stdout
    assert "--enable-live-copy" not in stdout


def test_safe_sample_cli_executes_local_init_fetch_for_runnable_current_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    code, stdout, stderr = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code == 3, stderr
    payload = json.loads(stdout)
    assert payload["result"]["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED"
    runtime_run = payload["safe_sample"]["runtime_run"]
    init_fetch = runtime_run["runtime_execution"]["init_fetch"]
    assert init_fetch["schema"] == "dpone.init-fetch-result.v1"
    assert init_fetch["passed"] is True
    current_index = json.loads((_safe_sample_cache_root(tmp_path) / "current" / "airflow-index.json").read_text())
    assert init_fetch["artifacts"][0]["artifact_ref"] == current_index["workload_packs"][0]["artifact_ref"]
    assert runtime_run["runtime_execution"]["data_copy"]["status"] == "blocked"
    assert runtime_run["runtime_execution"]["data_copy"]["copy_request"]["certification_id"] == (
        "mssql_clickhouse_incremental_merge_airflow_kpo"
    )
    run_id = payload["run_id"]
    assert run_id == payload["safe_sample"]["run_id"]
    assert (
        tmp_path
        / ".dpone-cache"
        / "safe-sample-runs"
        / "orders_daily"
        / run_id
        / "runtime-artifacts"
        / "init-fetch-manifest.json"
    ).exists()


def test_safe_sample_cli_generates_unique_run_identity_when_omitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )

    run_ids: list[str] = []
    evidence_paths: list[str] = []
    for _ in range(2):
        code, stdout, stderr = _run_cli(
            ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
            capsys,
        )
        assert code == 3, stderr
        payload = json.loads(stdout)
        run_ids.append(payload["run_id"])
        evidence_paths.append(payload["safe_sample"]["runtime_run"]["evidence_write"]["path"])

    assert len(set(run_ids)) == 2
    assert len(set(evidence_paths)) == 2
    assert all((tmp_path / path).exists() for path in evidence_paths)


def test_golden_path_safe_sample_self_materializes_local_runnable_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    _run_cli(["check", "pipelines/orders_daily", "--format", "json"], capsys)
    _run_cli(["airflow", "preview", "orders_daily", "--format", "json"], capsys)
    airflow_current_index = tmp_path / ".dpone-cache" / "current" / "airflow-index.json"
    preview_current_bytes = airflow_current_index.read_bytes()

    code, stdout, stderr = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code == 3, stderr
    payload = json.loads(stdout)
    assert payload["safe_sample"]["local_deployment"]["status"] == "promoted"
    assert payload["safe_sample"]["execution_plan"]["deployment_context"]["runnable"] is True
    assert payload["safe_sample"]["runtime_readiness"]["blockers"] == []
    assert payload["safe_sample"]["runtime_readiness"]["ready"] is True
    assert payload["safe_sample"]["runtime_run"]["runtime_execution"]["init_fetch"]["passed"] is True
    assert (
        payload["safe_sample"]["runtime_run"]["runtime_execution"]["data_copy"]["errors"][0]["code"]
        == "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED"
    )
    assert payload["result"]["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED"
    safe_sample_cache = _safe_sample_cache_root(tmp_path)
    current_index = json.loads((safe_sample_cache / "current" / "airflow-index.json").read_text())
    assert current_index["workload_packs"][0]["id"] == "orders_daily"
    assert current_index["runtime_artifact_delivery"]["mode"] == "init_fetch"
    current_pointer = json.loads((safe_sample_cache / "current-pointer.json").read_text())
    assert current_pointer["promoted_by"] == "local://dpone-safe-sample"
    assert airflow_current_index.read_bytes() == preview_current_bytes
    assert json.loads(preview_current_bytes)["runtime_artifact_delivery"]["mode"] == "local_preview"
    pack_ref = str(current_index["workload_packs"][0]["artifact_ref"])
    pack = json.loads((safe_sample_cache / pack_ref.removeprefix("cache://")).read_text())
    manifest_dependency = next(item for item in pack["workload_dependencies"] if item.get("kind") == "manifest")
    source_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    assert manifest_dependency == {
        "kind": "manifest",
        "path": "pipelines/orders_daily/pipeline.yaml",
        "sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
    }


def test_beginner_safe_sample_automatically_uses_prepared_live_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from types import SimpleNamespace

    from dpone.commands import run_safe_sample_runtime_selection
    from dpone.readiness.safe_sample_auto_live_runtime import AutoLiveSafeSampleRuntime
    from dpone.readiness.safe_sample_live_input_discovery import LiveSafeSampleInputDiscovery
    from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunReport

    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    data_copier = object()
    target_executor = object()
    verification = {"schema": "dpone.route-attestation-verification.v1", "decision": "verified"}
    runtime_calls: list[dict[str, object]] = []

    def prepare(plan: object, **kwargs: object) -> AutoLiveSafeSampleRuntime:
        assert kwargs["pipeline_source_path"] == "pipelines/orders_daily"
        return AutoLiveSafeSampleRuntime(
            execution_mode="live_copy",
            discovery=LiveSafeSampleInputDiscovery(
                status="ready",
                deployment_id=getattr(getattr(plan, "deployment_context"), "deployment_id"),
                pipeline_id="orders_daily",
            ),
            assembly=SimpleNamespace(
                plan=plan,
                data_copier=data_copier,
                temporary_target_executor=target_executor,
                route_attestation_verification=SimpleNamespace(to_dict=lambda: verification),
            ),
        )

    def run(plan: object, **kwargs: object) -> SafeSampleRuntimeRunReport:
        runtime_calls.append({"plan": plan, **kwargs})
        context = getattr(plan, "deployment_context")
        return SafeSampleRuntimeRunReport(
            release_id=getattr(context, "release_id"),
            deployment_id=getattr(context, "deployment_id"),
            execution_status="succeeded",
            data_outcome="passed",
            runtime_execution={"execution_status": "succeeded", "data_outcome": "passed", "errors": []},
            evidence_write={"path": ".dpone-cache/safe-sample-runs/orders_daily/evidence.json"},
            errors=(),
        )

    monkeypatch.setattr(run_safe_sample_runtime_selection, "prepare_auto_live_safe_sample_runtime", prepare)
    monkeypatch.setattr(run_safe_sample_runtime_selection, "run_local_safe_sample_runtime_handoff", run)

    code, stdout, stderr = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is True
    assert payload["safe_sample"]["execution_mode"] == "live_copy"
    assert payload["safe_sample"]["live_selection"] == {
        "status": "ready",
        "deployment_id": payload["safe_sample"]["local_deployment"]["deployment_id"],
        "pipeline_id": "orders_daily",
        "authorization_overlay_profile": "deployment_scoped_v1",
        "errors": [],
        "execution_mode": "live_copy",
    }
    assert len(runtime_calls) == 1
    assert runtime_calls[0]["data_copier"] is data_copier
    assert runtime_calls[0]["temporary_target_executor"] is target_executor
    assert runtime_calls[0]["route_attestation_verification"] == verification


def test_beginner_safe_sample_fails_closed_for_partial_live_overlay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.commands import run_safe_sample_runtime_selection
    from dpone.readiness.safe_sample_auto_live_runtime import AutoLiveSafeSampleRuntime
    from dpone.readiness.safe_sample_live_input_discovery import LiveSafeSampleInputDiscovery

    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    error = {
        "schema": "dpone.error.v1",
        "code": "DPONE_SAFE_SAMPLE_LIVE_INPUTS_INCOMPLETE",
        "stage": "safe_sample_live_input_discovery",
        "severity": "error",
        "message": "Deployment-scoped live inputs are incomplete.",
        "fixes": [],
    }

    def prepare(plan: object, **kwargs: object) -> AutoLiveSafeSampleRuntime:
        del kwargs
        return AutoLiveSafeSampleRuntime(
            execution_mode="blocked",
            discovery=LiveSafeSampleInputDiscovery(
                status="incomplete",
                deployment_id=getattr(getattr(plan, "deployment_context"), "deployment_id"),
                pipeline_id="orders_daily",
                errors=(error,),
            ),
            errors=(error,),
        )

    monkeypatch.setattr(run_safe_sample_runtime_selection, "prepare_auto_live_safe_sample_runtime", prepare)
    monkeypatch.setattr(
        run_safe_sample_runtime_selection,
        "run_local_safe_sample_runtime_handoff",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("blocked live input must not execute runtime")),
    )

    code, stdout, stderr = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code == 3, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["safe_sample"]["execution_mode"] == "blocked"
    assert payload["safe_sample"]["live_selection"]["status"] == "incomplete"
    assert payload["result"]["errors"] == [error]
    plan_path = tmp_path / payload["safe_sample"]["runtime_handoff"]["plan_path"]
    assert plan_path.exists()
    assert not (plan_path.parent / "safe-sample-runtime-execution.json").exists()


def test_beginner_safe_sample_returns_structured_error_for_reserved_overlay_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    source_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    source["metadata"]["id"] = "current"
    source_path.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        [
            "run",
            "pipelines/orders_daily",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--run-id",
            "reserved-overlay-id",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 4, stderr
    payload = json.loads(stdout)
    assert payload["safe_sample"]["execution_mode"] == "blocked"
    assert payload["result"]["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID"
    assert "Traceback" not in stdout
    run_dir = tmp_path / ".dpone-cache" / "safe-sample-runs" / "current" / "reserved-overlay-id"
    assert not (run_dir / "safe-sample-execution-plan.json").exists()
    assert not (run_dir / "safe-sample-runtime-execution.json").exists()


def test_safe_sample_reuses_only_semantically_current_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )

    _, first_stdout, _ = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )
    first = json.loads(first_stdout)["safe_sample"]["local_deployment"]
    _, second_stdout, _ = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )
    second = json.loads(second_stdout)["safe_sample"]["local_deployment"]

    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    pipeline = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    pipeline["metadata"]["domain"] = "finance"
    pipeline_path.write_text(yaml.safe_dump(pipeline, sort_keys=False), encoding="utf-8")
    _, changed_stdout, _ = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )
    changed = json.loads(changed_stdout)["safe_sample"]["local_deployment"]

    assert first["status"] == "promoted"
    assert second["status"] == "already_runnable"
    assert second["release_id"] == first["release_id"]
    assert changed["status"] == "promoted"
    assert changed["release_id"] != first["release_id"]


def test_safe_sample_preparation_failure_never_reuses_older_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.commands import run_safe_sample_cmd, run_safe_sample_runtime_selection
    from dpone.readiness.airflow_local_safe_sample_deployment import LocalSafeSampleDeploymentResult

    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )
    safe_sample_cache = _safe_sample_cache_root(tmp_path)
    old_index = json.loads((safe_sample_cache / "current" / "airflow-index.json").read_text())
    source = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["metadata"]["domain"] = "changed-domain"
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    preparation_error = {
        "schema": "dpone.error.v1",
        "code": "DPONE_LOCAL_SAFE_SAMPLE_DEPLOYMENT_FAILED",
        "stage": "local_safe_sample_deployment",
        "severity": "error",
        "message": "new release preparation failed",
        "fixes": [],
    }
    monkeypatch.setattr(
        run_safe_sample_cmd,
        "ensure_local_safe_sample_deployment",
        lambda **kwargs: LocalSafeSampleDeploymentResult(
            status="failed",
            environment="dev",
            error=preparation_error,
        ),
    )

    def unexpected_call(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("older current must not be loaded or executed")

    monkeypatch.setattr(run_safe_sample_cmd, "resolve_verified_airflow_deployment_context", unexpected_call)
    monkeypatch.setattr(run_safe_sample_runtime_selection, "run_local_safe_sample_runtime_handoff", unexpected_call)

    code, stdout, _ = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )
    result = json.loads(stdout)

    assert code == 3
    assert result["safe_sample"]["execution_plan"]["deployment_context"] is None
    assert result["safe_sample"]["execution_plan"]["blockers"] == [preparation_error]
    assert "runtime_run" not in result["safe_sample"]
    current_index = json.loads((safe_sample_cache / "current" / "airflow-index.json").read_text())
    assert current_index["release_id"] == old_index["release_id"]


def test_safe_sample_never_repairs_corrupt_immutable_release_in_place(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )
    safe_sample_cache = _safe_sample_cache_root(tmp_path)
    index = json.loads((safe_sample_cache / "current" / "airflow-index.json").read_text())
    artifact_ref = index["workload_packs"][0]["artifact_ref"]
    pack_path = safe_sample_cache / artifact_ref.removeprefix("cache://")
    corrupt = b"x" * pack_path.stat().st_size
    pack_path.chmod(0o600)
    pack_path.write_bytes(corrupt)

    code, stdout, _ = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )
    payload = json.loads(stdout)

    assert code != 0
    assert payload["safe_sample"]["local_deployment"]["status"] == "failed"
    assert payload["safe_sample"]["local_deployment"]["error"]["code"] == ("DPONE_LOCAL_SAFE_SAMPLE_DEPLOYMENT_FAILED")
    assert payload["safe_sample"]["execution_plan"]["runnable"] is False
    assert payload["safe_sample"]["execution_plan"]["deployment_context"] is None
    assert [item["code"] for item in payload["safe_sample"]["execution_plan"]["blockers"]] == [
        "DPONE_LOCAL_SAFE_SAMPLE_DEPLOYMENT_FAILED"
    ]
    assert "runtime_run" not in payload["safe_sample"]
    assert pack_path.read_bytes() == corrupt


def test_inline_workload_archive_has_canonical_gzip_header(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    source.write_text("schema: dpone.batch.v1\n", encoding="utf-8")

    first = base64.b64decode(inline_workload_archive(repo_root=tmp_path, paths=("pipeline.yaml",)))
    second = base64.b64decode(inline_workload_archive(repo_root=tmp_path, paths=("pipeline.yaml",)))

    assert first == second
    assert first[4:8] == b"\x00\x00\x00\x00"


def test_inline_archive_pack_and_release_are_deterministic_across_processes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_root.mkdir()
    monkeypatch.chdir(first_root)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    shutil.copytree(first_root, second_root)
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    processes = [
        context.Process(target=_process_safe_sample_fingerprints, args=(str(root), results))
        for root in (first_root, second_root)
    ]

    for process in processes:
        process.start()
    observed = [results.get(timeout=30) for _ in processes]
    for process in processes:
        process.join(timeout=30)

    assert [process.exitcode for process in processes] == [0, 0]
    assert [item[0] for item in observed] == ["ok", "ok"]
    assert observed[0][1:] == observed[1][1:]


def test_safe_sample_never_fills_missing_file_in_published_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )
    safe_sample_cache = _safe_sample_cache_root(tmp_path)
    index = json.loads((safe_sample_cache / "current" / "airflow-index.json").read_text())
    artifact_ref = index["workload_packs"][0]["artifact_ref"]
    pack_path = safe_sample_cache / artifact_ref.removeprefix("cache://")
    pack_path.chmod(0o600)
    pack_path.parent.chmod(0o700)
    pack_path.unlink()

    code, stdout, _ = _run_cli(
        ["run", "pipelines/orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )
    payload = json.loads(stdout)

    assert code != 0
    assert payload["safe_sample"]["local_deployment"]["status"] == "failed"
    assert not pack_path.exists()


def test_self_service_scaffolding_conflict_reports_diff_without_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 0, stderr
    dag_path = tmp_path / "dags" / "dpone.py"
    dag_path.write_text("# user custom dag\n", encoding="utf-8")

    code, stdout, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)

    assert code == 1, stderr
    payload = json.loads(stdout)
    conflicts = [change for change in payload["changes"] if change["action"] == "conflict"]
    assert len(conflicts) == 1
    assert conflicts[0]["path"] == "dags/dpone.py"
    assert "--- existing/dags/dpone.py" in conflicts[0]["diff"]
    assert "+++ desired/dags/dpone.py" in conflicts[0]["diff"]
    assert "-# user custom dag" in conflicts[0]["diff"]
    assert "+from airflow.providers.dpone import load_and_acknowledge_dpone_dags" in conflicts[0]["diff"]
    assert payload["rollback_journal"]["entries"] == []
    assert dag_path.read_text(encoding="utf-8") == "# user custom dag\n"


def test_init_project_modified_loader_text_is_blocking_and_actionable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    dag_path = tmp_path / "dags" / "dpone.py"
    dag_path.write_text("# user custom dag\n", encoding="utf-8")

    code, stdout, stderr = _run_cli(["init", "project", "--airflow"], capsys)

    assert code == 1
    assert stderr == ""
    assert "- conflict: dags/dpone.py" in stdout
    assert "- reason: file exists with different content" in stdout
    assert "- preserved: existing files were left unchanged" in stdout
    assert "- inspect: dpone init project --airflow --format json" in stdout
    assert "- next: merge the guarded loader, then rerun dpone init project --airflow" in stdout
    assert "dpone init pipeline orders_daily" not in stdout
    assert dag_path.read_text(encoding="utf-8") == "# user custom dag\n"


@pytest.mark.parametrize(
    ("args", "heading"),
    (
        (["check", "missing"], "dpone check: FAILED"),
        (["airflow", "preview", "missing"], "dpone airflow preview: FAILED"),
    ),
)
def test_missing_pipeline_text_keeps_command_context_and_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    args: list[str],
    heading: str,
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(args, capsys)

    assert code == 1
    assert stderr == ""
    assert heading in stdout
    assert "- pipeline: missing" in stdout
    assert "- error: DPONE_PIPELINE_SOURCE_NOT_FOUND: Pipeline source was not found" in stdout
    assert "dpone init pipeline missing --recipe mssql-to-clickhouse-incremental --airflow" in stdout
    assert "dpone self-service: FAILED" not in stdout
    assert not (tmp_path / "pipelines").exists()


def test_missing_pipeline_json_includes_structured_fix_without_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["check", "missing", "--format", "json"], capsys)

    payload = json.loads(stdout)
    error = payload["errors"][0]
    assert code == 1
    assert stderr == ""
    assert error["code"] == "DPONE_PIPELINE_SOURCE_NOT_FOUND"
    assert error["path"] == "pipelines/missing/pipeline.yaml"
    assert error["fixes"] == [
        {
            "id": "init_pipeline",
            "safety": "manual",
            "command": "dpone init pipeline missing --recipe mssql-to-clickhouse-incremental --airflow",
        }
    ]
    assert not (tmp_path / "pipelines").exists()


def test_self_service_errors_emit_dpone_error_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = _error_schema()
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["init", "pipeline", "orders_daily", "--recipe", "missing-recipe", "--airflow", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    error = payload["errors"][0]
    jsonschema.validate(error, schema)
    assert error["schema"] == "dpone.error.v1"
    assert error["stage"] == "init_pipeline"
    assert error["severity"] == "error"
    assert error["entity"] == {"kind": "pipeline", "id": "orders_daily"}
    assert error["fixes"][0]["safety"] == "manual"


def test_airflow_build_errors_emit_dpone_error_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = _error_schema()
    monkeypatch.chdir(tmp_path)
    missing_release_id = "sha256:" + "a" * 64
    runtime_image_digest = "sha256:" + "b" * 64

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            missing_release_id,
            "--environment",
            "dev",
            *_strict_airflow_build_args(runtime_image_digest),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    error = payload["errors"][0]
    jsonschema.validate(error, schema)
    assert error["schema"] == "dpone.error.v1"
    assert error["stage"] == "airflow_build"
    assert error["severity"] == "error"
    assert error["entity"] == {"kind": "release", "id": missing_release_id}
    assert error["fixes"][0]["id"] == "materialize_release_set"
    assert error["fixes"][0]["safety"] == "manual"


def test_airflow_build_failure_text_is_command_specific_and_actionable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    missing_release_id = "sha256:" + "a" * 64
    runtime_image_digest = "sha256:" + "b" * 64

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            missing_release_id,
            "--environment",
            "dev",
            *_strict_airflow_build_args(runtime_image_digest),
        ],
        capsys,
    )

    assert code == 1, stderr
    assert stderr == ""
    assert "dpone airflow build: FAILED" in stdout
    assert "- environment: dev" in stdout
    assert f"- release: {missing_release_id}" in stdout
    assert "- issue: DPONE_RELEASE_NOT_FOUND: required JSON file is missing" in stdout
    assert "- action: materialize release-set, then rerun dpone airflow build" in stdout
    assert "- details: rerun with --format json for structured errors and projection diagnostics" in stdout
    assert str(tmp_path) not in stdout
    assert "dpone self-service: FAILED" not in stdout
    assert "Traceback" not in stdout


@pytest.mark.parametrize(
    "missing_option",
    ["--registry-config-map-name", "--registry-config-sha256"],
)
def test_airflow_build_rejects_partial_registry_config_arguments_before_side_effects(
    missing_option: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    release_id = _write_airflow_build_release(tmp_path)
    runtime_image_digest = "sha256:" + "b" * 64
    build_args = [
        "airflow",
        "build",
        "--release-id",
        release_id,
        "--environment",
        "dev",
        *_strict_airflow_build_args(runtime_image_digest),
        "--format",
        "json",
    ]
    option_index = build_args.index(missing_option)
    del build_args[option_index : option_index + 2]

    code, stdout, stderr = _run_cli(build_args, capsys)

    assert code == 2
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["errors"][0]["code"] == "DPONE_DEPLOYMENT_CONFIG_REF_INVALID"
    assert not (tmp_path / ".dpone-cache" / "deployments").exists()


@pytest.mark.parametrize(
    "missing_option",
    [
        "--composition-supervisor-pvc",
        "--composition-child-uid-start",
        "--composition-child-gid-start",
        "--composition-child-identity-count",
    ],
)
def test_airflow_build_rejects_partial_composition_supervisor_group_before_side_effects(
    missing_option: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    release_id = _write_airflow_build_release(tmp_path)
    runtime_image_digest = "sha256:" + "b" * 64
    build_args = [
        "airflow",
        "build",
        "--release-id",
        release_id,
        "--environment",
        "dev",
        *_strict_airflow_build_args(runtime_image_digest),
        *_composition_supervisor_args(),
        "--format",
        "json",
    ]
    option_index = build_args.index(missing_option)
    del build_args[option_index : option_index + 2]

    code, stdout, stderr = _run_cli(build_args, capsys)

    assert code == 2
    assert stderr == ""
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_COMPOSITION_SUPERVISOR_GROUP_INCOMPLETE"
    assert not (tmp_path / ".dpone-cache" / "deployments").exists()


def test_airflow_build_forbids_composition_supervisor_for_v1_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    release_id = _write_airflow_build_release(tmp_path)
    runtime_image_digest = "sha256:" + "b" * 64

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            release_id,
            "--environment",
            "dev",
            *_strict_airflow_build_args(runtime_image_digest),
            *_composition_supervisor_args(),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1
    assert stderr == ""
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_COMPOSITION_SUPERVISOR_FORBIDDEN"
    assert not (tmp_path / ".dpone-cache" / "deployments").exists()


@pytest.mark.parametrize(
    ("argument_edits", "expected_code", "expected_exit"),
    [
        (
            {
                "--trust-tier": "production",
            },
            "DPONE_DEPLOYMENT_TRUST_POLICY_REQUIRED",
            2,
        ),
        (
            {
                "--runtime-image-ref": "registry.example/dpone-runtime@sha256:" + "e" * 64,
            },
            "DPONE_DEPLOYMENT_RUNTIME_IMAGE_INVALID",
            1,
        ),
        (
            {
                "--artifact-registry-ref": "cache://current/prod",
            },
            "DPONE_DEPLOYMENT_ARTIFACT_REGISTRY_UNPINNED",
            1,
        ),
        (
            {
                "--trust-policy-config-map-name": "dpone-artifact-trust-policy",
            },
            "DPONE_DEPLOYMENT_CONFIG_REF_INVALID",
            2,
        ),
        (
            {
                "--trust-policy-sha256": "sha256:" + "f" * 64,
            },
            "DPONE_DEPLOYMENT_CONFIG_REF_INVALID",
            2,
        ),
        (
            {
                "--trust-policy-config-map-key": "custom-policy.json",
            },
            "DPONE_DEPLOYMENT_CONFIG_REF_INVALID",
            2,
        ),
    ],
)
def test_airflow_build_validation_matrix_fails_before_deployment_write(
    argument_edits: dict[str, str],
    expected_code: str,
    expected_exit: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    release_id = _write_airflow_build_release(tmp_path)
    runtime_image_digest = "sha256:" + "b" * 64
    build_args = [
        "airflow",
        "build",
        "--release-id",
        release_id,
        "--environment",
        "dev",
        *_strict_airflow_build_args(runtime_image_digest),
    ]
    for option, value in argument_edits.items():
        if option in build_args:
            build_args[build_args.index(option) + 1] = value
        else:
            build_args.extend([option, value])
    build_args.extend(["--format", "json"])

    code, stdout, stderr = _run_cli(build_args, capsys)

    assert code == expected_exit, stderr
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["errors"][0]["code"] == expected_code
    assert payload["errors"][0]["stage"] == "airflow_build"
    assert not (tmp_path / ".dpone-cache" / "deployments").exists()


def test_airflow_build_json_commits_to_exact_persisted_index_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    release_id = _write_airflow_build_release(tmp_path)
    runtime_image_digest = "sha256:" + "b" * 64

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            release_id,
            "--environment",
            "dev",
            *_strict_airflow_build_args(runtime_image_digest),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    index_path = tmp_path / payload["deployment_dir"] / "airflow-index.json"
    index_bytes = index_path.read_bytes()
    assert payload["airflow_index_artifact"] == {
        "artifact_ref": (
            f"cache://deployments/dev/{payload['deployment']['deployment_id'].replace(':', '-')}/airflow-index.json"
        ),
        "sha256": "sha256:" + hashlib.sha256(index_bytes).hexdigest(),
        "bytes": len(index_bytes),
    }
    assert payload["airflow_index"]["credential_runtime_ref"] == "[REDACTED]"


def test_airflow_build_legacy_invocation_returns_actionable_v2_migration_without_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    release_id = "sha256:" + "a" * 64
    runtime_image_digest = "sha256:" + "b" * 64

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            release_id,
            "--environment",
            "dev",
            "--runtime-image-digest",
            runtime_image_digest,
            "--artifact-registry-ref",
            "dpone-dev-artifacts",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2
    assert stderr == ""
    payload = json.loads(stdout)
    error = payload["errors"][0]
    assert error["code"] == "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED"
    assert error["fixes"] == [
        {
            "id": "regenerate_strict_v2_deployment",
            "safety": "manual",
            "command": "dpone airflow build --help",
        }
    ]
    assert not (tmp_path / ".dpone-cache").exists()


@pytest.mark.parametrize(
    "unsafe_value",
    [
        "password=must-not-leak",
        "https://user:must-not-leak@example.test/runtime",
        "/Users/alice/must-not-leak",
    ],
)
def test_airflow_build_parser_errors_redact_rejected_values(
    unsafe_value: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            "sha256:" + "a" * 64,
            "--environment",
            "dev",
            "--runtime-image-digest",
            "sha256:" + "b" * 64,
            "--artifact-registry-ref",
            "dpone-dev-artifacts",
            "--trust-tier",
            unsafe_value,
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2
    assert stdout == ""
    assert "must-not-leak" not in stderr
    assert "/Users/alice" not in stderr
    assert "Traceback" not in stderr


def test_airflow_build_invalid_registry_has_safe_json_and_text_parity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    release_id = _write_airflow_build_release(tmp_path)
    runtime_image_digest = "sha256:" + "b" * 64
    unsafe_registry_ref = "https://user:password@registry.example/secret"
    base_args = [
        "airflow",
        "build",
        "--release-id",
        release_id,
        "--environment",
        "dev",
        *_strict_airflow_build_args(runtime_image_digest),
    ]
    base_args[base_args.index("--artifact-registry-ref") + 1] = unsafe_registry_ref

    json_code, json_stdout, json_stderr = _run_cli([*base_args, "--format", "json"], capsys)
    text_code, text_stdout, text_stderr = _run_cli(base_args, capsys)

    assert json_code == text_code == 1
    assert json_stderr == text_stderr == ""
    expected_code = "DPONE_DEPLOYMENT_ARTIFACT_REGISTRY_INVALID"
    assert json.loads(json_stdout)["errors"][0]["code"] == expected_code
    assert expected_code in text_stdout
    assert unsafe_registry_ref not in json_stdout
    assert unsafe_registry_ref not in text_stdout
    assert "password" not in json_stdout
    assert "password" not in text_stdout
    assert not (tmp_path / ".dpone-cache" / "deployments").exists()


def test_airflow_build_text_output_summarizes_deployment_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    release_id = _write_airflow_build_release(tmp_path)
    runtime_image_digest = "sha256:" + "b" * 64

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            release_id,
            "--environment",
            "dev",
            *_strict_airflow_build_args(runtime_image_digest),
        ],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone airflow build: OK" in stdout
    assert "- environment: dev" in stdout
    assert f"- release: {release_id}" in stdout
    assert "- deployment: sha256:" in stdout
    assert "- runnable: yes" in stdout
    assert "- dag specs: 1" in stdout
    assert "- workload packs: 1" in stdout
    assert "- runtime delivery: init_fetch" in stdout
    assert "- artifact registry: dpone-dev-artifacts" in stdout
    assert f"- airflow bundle: git:{'7' * 40}" in stdout
    assert "- output: .dpone-cache/deployments/dev/sha256-" in stdout
    assert "- airflow index: .dpone-cache/deployments/dev/sha256-" in stdout
    assert "- action: dpone airflow cache-sync --deployment-dir .dpone-cache/deployments/dev/sha256-" in stdout
    assert '--allowed-promoter "${DPONE_CI_IDENTITY:?set DPONE_CI_IDENTITY}"' in stdout
    assert (
        '--environment dev --promoted-by "${DPONE_CI_IDENTITY:?set DPONE_CI_IDENTITY}" '
        '--allowed-promoter "${DPONE_CI_IDENTITY:?set DPONE_CI_IDENTITY}" '
        "--expect-current-absent --confirm-promote"
    ) in stdout
    assert "- details: rerun with --format json for fingerprints and full deployment projection" in stdout
    assert "<ci-identity>" not in stdout
    assert str(tmp_path) not in stdout
    assert "dpone self-service: OK" not in stdout


def test_connection_check_blocks_env_var_resolver_in_prod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    prod_env = tmp_path / "environments" / "prod"
    prod_env.mkdir(parents=True)
    (prod_env / "binding-set.yaml").write_text(
        (tmp_path / "environments" / "dev" / "binding-set.yaml")
        .read_text(encoding="utf-8")
        .replace("environment: dev", "environment: prod"),
        encoding="utf-8",
    )
    (prod_env / "credential-runtime.yaml").write_text(
        "schema: dpone.credential-runtime.v1\nenvironment: prod\n",
        encoding="utf-8",
    )
    registry_dir = tmp_path / "platform" / "connection-registries"
    (registry_dir / "prod.yaml").write_text(
        (registry_dir / "dev.yaml").read_text(encoding="utf-8").replace("environment: dev", "environment: prod"),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "prod", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert {error["code"] for error in payload["errors"]} == {"DPONE_ENV_VAR_RESOLVER_FORBIDDEN_IN_PROD"}
    jsonschema = pytest.importorskip("jsonschema")
    schema = _error_schema()
    for error in payload["errors"]:
        jsonschema.validate(error, schema)
        assert error["schema"] == "dpone.error.v1"
        assert error["stage"] == "check_connections"
        assert error["severity"] == "error"
        assert error["fixes"][0]["id"] == "replace_env_var_resolver"
        assert error["fixes"][0]["safety"] == "manual"


def test_connection_check_error_schema_validates_all_reported_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = _error_schema()
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    registry_path = tmp_path / "platform" / "connection-registries" / "dev.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["connections"]["mssql_dev"]["credentials"]["resolver"] = "vault_kv"
    registry["connections"]["mssql_dev"]["credentials"]["path"] = "/v1/kv/data/dpone/dev/mssql"
    registry["connections"]["mssql_dev"]["credentials"].pop("support", None)
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["errors"]
    for error in payload["errors"]:
        jsonschema.validate(error, schema)
        assert error["schema"] == "dpone.error.v1"
        assert error["stage"] == "check_connections"


def _error_schema() -> dict[str, object]:
    return json.loads((_REPO_ROOT / "docs/schemas/gitops/error.schema.json").read_text(encoding="utf-8"))


def test_connection_check_requires_env_var_development_support(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    registry_path = tmp_path / "platform" / "connection-registries" / "dev.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["connections"]["mssql_dev"]["credentials"].pop("support")
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert {error["code"] for error in payload["errors"]} == {"DPONE_ENV_VAR_SUPPORT_REQUIRED"}


def test_connection_check_rejects_environment_config_mismatches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    binding_path = tmp_path / "environments" / "dev" / "binding-set.yaml"
    binding = yaml.safe_load(binding_path.read_text(encoding="utf-8"))
    binding["environment"] = "prod"
    binding_path.write_text(yaml.safe_dump(binding, sort_keys=False), encoding="utf-8")
    registry_path = tmp_path / "platform" / "connection-registries" / "dev.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["environment"] = "prod"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    runtime_path = tmp_path / "environments" / "dev" / "credential-runtime.yaml"
    runtime = yaml.safe_load(runtime_path.read_text(encoding="utf-8"))
    runtime["environment"] = "prod"
    runtime_path.write_text(yaml.safe_dump(runtime, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert {error["code"] for error in payload["errors"]} == {
        "DPONE_BINDING_SET_ENVIRONMENT_MISMATCH",
        "DPONE_CONNECTION_REGISTRY_ENVIRONMENT_MISMATCH",
        "DPONE_CREDENTIAL_RUNTIME_ENVIRONMENT_MISMATCH",
    }
    assert all(error["expected_environment"] == "dev" for error in payload["errors"])
    assert all(error["actual_environment"] == "prod" for error in payload["errors"])


def test_connection_check_rejects_secret_material_in_credential_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    runtime_path = tmp_path / "environments" / "dev" / "credential-runtime.yaml"
    runtime_path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.credential-runtime.v1",
                "environment": "dev",
                "vault": {
                    "address": "https://vault.internal",
                    "namespace": "data-platform",
                    "auth": {
                        "method": "kubernetes",
                        "role": "dpone-runtime-dev",
                        "token": "must-not-leak",
                        "jwt": "must-not-leak",
                        "secret_id": "must-not-leak",
                        "password": "must-not-leak",
                        "vault_token": "must-not-leak",
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert {error["code"] for error in payload["errors"]} == {
        "DPONE_CREDENTIAL_RUNTIME_SECRET_MATERIAL_FORBIDDEN",
    }
    assert "password" in payload["errors"][0]["message"]
    assert "vault_token" in payload["errors"][0]["message"]
    assert "must-not-leak" not in stdout


def test_connection_check_accepts_binding_aliases_to_platform_registry_refs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    pipeline = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    pipeline["processes"][0]["source"]["connection_ref"] = "orders_source"
    pipeline["processes"][0]["sink"]["connection_ref"] = "orders_sink"
    pipeline_path.write_text(yaml.safe_dump(pipeline, sort_keys=False), encoding="utf-8")
    binding_path = tmp_path / "environments" / "dev" / "binding-set.yaml"
    binding = yaml.safe_load(binding_path.read_text(encoding="utf-8"))
    binding["bindings"] = {
        "orders_source": {"connection_ref": "mssql_platform"},
        "orders_sink": {"connection_ref": "clickhouse_platform"},
    }
    binding_path.write_text(yaml.safe_dump(binding, sort_keys=False), encoding="utf-8")
    registry_path = tmp_path / "platform" / "connection-registries" / "dev.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["connections"] = {
        "mssql_platform": registry["connections"]["mssql_dev"],
        "clickhouse_platform": registry["connections"]["clickhouse_dev"],
    }
    registry["connections"]["mssql_platform"]["credentials"] = {
        "resolver": "airflow_connection",
        "connection_id": "mssql_prod",
        "execution_mode": "operator_bridge",
    }
    registry["connections"]["clickhouse_platform"]["credentials"] = {
        "resolver": "airflow_connection",
        "connection_id": "clickhouse_prod",
        "execution_mode": "operator_bridge",
    }
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev", "--format", "json"],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is True
    assert payload["errors"] == []
    assert payload["connection_refs"] == ["orders_sink", "orders_source"]
    assert payload["resolved_connection_refs"] == ["clickhouse_platform", "mssql_platform"]
    assert payload["airflow_connection_bridge"]["connections"] == [
        {
            "connection_ref": "orders_sink",
            "registry_connection_ref": "clickhouse_platform",
            "connection_id": "clickhouse_prod",
            "env_name": "AIRFLOW_CONN_CLICKHOUSE_PROD",
        },
        {
            "connection_ref": "orders_source",
            "registry_connection_ref": "mssql_platform",
            "connection_id": "mssql_prod",
            "env_name": "AIRFLOW_CONN_MSSQL_PROD",
        },
    ]


def test_connection_check_rejects_kubernetes_secret_volume_path_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    prod_env = tmp_path / "environments" / "prod"
    prod_env.mkdir(parents=True)
    (prod_env / "binding-set.yaml").write_text(
        (tmp_path / "environments" / "dev" / "binding-set.yaml")
        .read_text(encoding="utf-8")
        .replace("environment: dev", "environment: prod"),
        encoding="utf-8",
    )
    (prod_env / "credential-runtime.yaml").write_text(
        "schema: dpone.credential-runtime.v1\nenvironment: prod\n",
        encoding="utf-8",
    )
    registry_dir = tmp_path / "platform" / "connection-registries"
    (registry_dir / "prod.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "mssql_dev": {
                        "type": "mssql",
                        "connection": {"host": "mssql.internal", "port": 1433, "database": "dwh"},
                        "credentials": {
                            "resolver": "kubernetes_secret_volume",
                            "secret_name": "mssql-dev",
                            "mount_path": "/run/secrets/dpone/mssql",
                            "fields": {"username": "../username", "password": "password"},
                        },
                    },
                    "clickhouse_dev": {
                        "type": "clickhouse",
                        "connection": {"host": "clickhouse.internal", "port": 9000, "database": "analytics"},
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "path": "dpone/prod/credentials/clickhouse",
                            "fields": {"username": "username", "password": "password"},
                            "version_policy": "latest",
                            "resolution_scope": "workload_start",
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "prod", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert {error["code"] for error in payload["errors"]} == {"DPONE_KUBERNETES_SECRET_VOLUME_FIELD_PATH_INVALID"}


def test_connection_check_rejects_kubernetes_secret_volume_empty_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    prod_env = tmp_path / "environments" / "prod"
    prod_env.mkdir(parents=True)
    (prod_env / "binding-set.yaml").write_text(
        (tmp_path / "environments" / "dev" / "binding-set.yaml")
        .read_text(encoding="utf-8")
        .replace("environment: dev", "environment: prod"),
        encoding="utf-8",
    )
    (prod_env / "credential-runtime.yaml").write_text(
        "schema: dpone.credential-runtime.v1\nenvironment: prod\n",
        encoding="utf-8",
    )
    registry_dir = tmp_path / "platform" / "connection-registries"
    (registry_dir / "prod.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "mssql_dev": {
                        "type": "mssql",
                        "connection": {"host": "mssql.internal", "port": 1433, "database": "dwh"},
                        "credentials": {
                            "resolver": "kubernetes_secret_volume",
                            "secret_name": "mssql-dev",
                            "mount_path": "/run/secrets/dpone/mssql",
                            "fields": {},
                        },
                    },
                    "clickhouse_dev": {
                        "type": "clickhouse",
                        "connection": {"host": "clickhouse.internal", "port": 9000, "database": "analytics"},
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "path": "dpone/prod/credentials/clickhouse",
                            "fields": {"username": "username", "password": "password"},
                            "version_policy": "latest",
                            "resolution_scope": "workload_start",
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "prod", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert {error["code"] for error in payload["errors"]} == {"DPONE_KUBERNETES_SECRET_VOLUME_FIELDS_INVALID"}


def test_connection_check_rejects_kubernetes_secret_api_empty_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    prod_env = tmp_path / "environments" / "prod"
    prod_env.mkdir(parents=True)
    (prod_env / "binding-set.yaml").write_text(
        (tmp_path / "environments" / "dev" / "binding-set.yaml")
        .read_text(encoding="utf-8")
        .replace("environment: dev", "environment: prod"),
        encoding="utf-8",
    )
    (prod_env / "credential-runtime.yaml").write_text(
        "schema: dpone.credential-runtime.v1\nenvironment: prod\n",
        encoding="utf-8",
    )
    registry_dir = tmp_path / "platform" / "connection-registries"
    (registry_dir / "prod.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "mssql_dev": {
                        "type": "mssql",
                        "connection": {"host": "mssql.internal", "port": 1433, "database": "dwh"},
                        "credentials": {
                            "resolver": "kubernetes_secret_api",
                            "namespace": "airflow-example",
                            "name": "mssql-prod",
                            "fields": {},
                        },
                    },
                    "clickhouse_dev": {
                        "type": "clickhouse",
                        "connection": {"host": "clickhouse.internal", "port": 9000, "database": "analytics"},
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "path": "dpone/prod/credentials/clickhouse",
                            "fields": {"username": "username", "password": "password"},
                            "version_policy": "latest",
                            "resolution_scope": "workload_start",
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "prod", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert {error["code"] for error in payload["errors"]} == {"DPONE_KUBERNETES_SECRET_API_FIELDS_INVALID"}


def test_connection_check_requires_vault_rotation_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    prod_env = tmp_path / "environments" / "prod"
    prod_env.mkdir(parents=True)
    (prod_env / "binding-set.yaml").write_text(
        (tmp_path / "environments" / "dev" / "binding-set.yaml")
        .read_text(encoding="utf-8")
        .replace("environment: dev", "environment: prod"),
        encoding="utf-8",
    )
    (prod_env / "credential-runtime.yaml").write_text(
        "schema: dpone.credential-runtime.v1\nenvironment: prod\n",
        encoding="utf-8",
    )
    registry_dir = tmp_path / "platform" / "connection-registries"
    (registry_dir / "prod.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "mssql_dev": {
                        "type": "mssql",
                        "connection": {"host": "mssql.internal", "port": 1433, "database": "dwh"},
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "kv_version": 2,
                            "path": "dpone/prod/credentials/mssql",
                            "fields": {"username": "username", "password": "password"},
                        },
                    },
                    "clickhouse_dev": {
                        "type": "clickhouse",
                        "connection": {"host": "clickhouse.internal", "port": 9000, "database": "analytics"},
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "kv_version": 2,
                            "path": "dpone/prod/credentials/clickhouse",
                            "fields": {"username": "username", "password": "password"},
                            "version_policy": "pinned",
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "prod", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    codes = [error["code"] for error in payload["errors"]]
    assert codes.count("DPONE_VAULT_VERSION_POLICY_REQUIRED") == 1
    assert codes.count("DPONE_VAULT_RESOLUTION_SCOPE_REQUIRED") == 2


def test_connection_check_rejects_empty_vault_field_mapping_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    prod_env = tmp_path / "environments" / "prod"
    prod_env.mkdir(parents=True)
    (prod_env / "binding-set.yaml").write_text(
        (tmp_path / "environments" / "dev" / "binding-set.yaml")
        .read_text(encoding="utf-8")
        .replace("environment: dev", "environment: prod"),
        encoding="utf-8",
    )
    (prod_env / "credential-runtime.yaml").write_text(
        "schema: dpone.credential-runtime.v1\nenvironment: prod\n",
        encoding="utf-8",
    )
    registry_dir = tmp_path / "platform" / "connection-registries"
    (registry_dir / "prod.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "mssql_dev": {
                        "type": "mssql",
                        "connection": {"host": "mssql.internal", "port": 1433, "database": "dwh"},
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "kv_version": 2,
                            "path": "dpone/prod/credentials/mssql",
                            "fields": {"username": "", "password": "password"},
                            "version_policy": "latest",
                            "resolution_scope": "workload_start",
                        },
                    },
                    "clickhouse_dev": {
                        "type": "clickhouse",
                        "connection": {"host": "clickhouse.internal", "port": 9000, "database": "analytics"},
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "kv_version": 2,
                            "path": "dpone/prod/credentials/clickhouse",
                            "fields": {"username": "username", "password": "password"},
                            "version_policy": "latest",
                            "resolution_scope": "workload_start",
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "prod", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert {error["code"] for error in payload["errors"]} == {"DPONE_VAULT_FIELDS_INVALID"}


def test_connection_check_accepts_airflow_connection_operator_bridge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    _write_prod_airflow_connection_bridge(tmp_path)

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "prod", "--format", "json"],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is True
    schema = json.loads((_REPO_ROOT / "docs/schemas/gitops/connection-check.schema.json").read_text(encoding="utf-8"))
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(payload, schema)
    assert payload["schema"] == "dpone.connection-check.v1"
    assert payload["errors"] == []
    assert payload["airflow_connection_bridge"] == {
        "required": True,
        "execution_mode": "operator_bridge",
        "resolver_location": "operator_execution",
        "parse_safe": True,
        "secrets": False,
        "required_connection_ids": ["clickhouse_prod", "mssql_prod"],
        "connections": [
            {
                "connection_ref": "clickhouse_dev",
                "registry_connection_ref": "clickhouse_dev",
                "connection_id": "clickhouse_prod",
                "env_name": "AIRFLOW_CONN_CLICKHOUSE_PROD",
            },
            {
                "connection_ref": "mssql_dev",
                "registry_connection_ref": "mssql_dev",
                "connection_id": "mssql_prod",
                "env_name": "AIRFLOW_CONN_MSSQL_PROD",
            },
        ],
        "projection": {
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "clickhouse_dev",
                    "registry_connection_ref": "clickhouse_dev",
                    "connection_id": "clickhouse_prod",
                    "secret_key": "AIRFLOW_CONN_CLICKHOUSE_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/clickhouse_dev",
                    "fields": {"uri": "uri"},
                },
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "mssql_prod",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                },
            ],
        },
        "next_actions": ["dpone gitops airflow connection-bridge-plan --artifact-dir .dpone/gitops/airflow"],
    }
    bridge_json = json.dumps(payload["airflow_connection_bridge"])
    assert "mssql://" not in bridge_json
    assert "clickhouse://" not in bridge_json
    assert "password" not in bridge_json.lower()


def test_connection_check_rejects_unsupported_credential_resolver(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    registry_path = tmp_path / "platform" / "connection-registries" / "dev.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["connections"]["mssql_dev"]["credentials"] = {
        "resolver": "hashi_vault",
        "path": "dpone/dev/credentials/mssql",
        "fields": {"username": "username", "password": "password"},
    }
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert {error["code"] for error in payload["errors"]} == {"DPONE_CREDENTIAL_RESOLVER_UNSUPPORTED"}
    assert payload["errors"][0]["resolver"] == "hashi_vault"
    assert "vault_kv" in payload["errors"][0]["supported_resolvers"]


def test_connection_check_rejects_airflow_connection_without_operator_bridge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    prod_env = tmp_path / "environments" / "prod"
    prod_env.mkdir(parents=True)
    (prod_env / "binding-set.yaml").write_text(
        (tmp_path / "environments" / "dev" / "binding-set.yaml")
        .read_text(encoding="utf-8")
        .replace("environment: dev", "environment: prod"),
        encoding="utf-8",
    )
    (prod_env / "credential-runtime.yaml").write_text(
        "schema: dpone.credential-runtime.v1\nenvironment: prod\n",
        encoding="utf-8",
    )
    registry_dir = tmp_path / "platform" / "connection-registries"
    (registry_dir / "prod.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "mssql_dev": {
                        "type": "mssql",
                        "credentials": {
                            "resolver": "airflow_connection",
                            "connection_id": "mssql_prod",
                        },
                    },
                    "clickhouse_dev": {
                        "type": "clickhouse",
                        "credentials": {
                            "resolver": "airflow_connection",
                            "connection_id": "clickhouse_prod",
                            "execution_mode": "runtime_image",
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "prod", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert [error["code"] for error in payload["errors"]] == [
        "DPONE_AIRFLOW_CONNECTION_EXECUTION_MODE_INVALID",
        "DPONE_AIRFLOW_CONNECTION_EXECUTION_MODE_INVALID",
    ]


def test_static_check_reports_legacy_connection_type_vault_path_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    source = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    source["processes"][0]["source"].pop("connection_ref")
    source["processes"][0]["source"]["connection_type"] = "vault"
    source["processes"][0]["source"]["vault_path"] = "dpone/dev/credentials/mssql"
    pipeline_path.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(["check", "pipelines/orders_daily", "--format", "json"], capsys)

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_LEGACY_CONNECTION_CONFIG_FOUND"
    assert payload["errors"][0]["entity"] == {"kind": "authoring_path", "id": "processes[0].source"}
    assert payload["errors"][0]["suggested_connection_ref"] == "mssql_dev"


def test_connection_check_reports_legacy_registry_entry_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    registry_path = tmp_path / "platform" / "connection-registries" / "dev.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["connections"]["mssql_dev"] = {
        "type": "mssql",
        "connection_type": "vault",
        "vault_path": "dpone/dev/credentials/mssql",
    }
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_LEGACY_CONNECTION_REGISTRY_ENTRY_FOUND"
    assert payload["errors"][0]["entity"] == {"kind": "connection_ref", "id": "mssql_dev"}
    assert payload["errors"][0]["resolver"] == "vault_kv"


def test_live_check_emits_structured_fail_closed_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--live", "--environment", "dev", "--format", "json"],
        capsys,
    )

    assert code == 3, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["mode"] == "live"
    assert payload["network"] is False
    assert payload["secrets"] is False
    assert payload["planned_network"] is True
    assert payload["planned_secrets"] is True
    assert payload["planned_source_queries"] == "bounded_probes"
    assert payload["handshake"] == "configuration_only"
    assert sorted(payload["connection_refs"]) == ["clickhouse_dev", "mssql_dev"]
    assert payload["live_preflight"] == "runner_not_configured"
    live_preflight = payload["live_preflight_report"]
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads((_REPO_ROOT / "docs" / "schemas" / "gitops" / "live-preflight.schema.json").read_text())
    jsonschema.Draft202012Validator(schema).validate(live_preflight)
    assert live_preflight["schema"] == "dpone.live-preflight.v1"
    assert live_preflight["passed"] is False
    assert live_preflight["runner"] == "not_configured"
    assert live_preflight["planned_source_queries"] == "bounded_probes"
    assert sorted(live_preflight["connection_refs"]) == ["clickhouse_dev", "mssql_dev"]
    assert sorted(live_preflight["resolved_connection_refs"]) == ["clickhouse_dev", "mssql_dev"]
    assert {probe["probe"] for probe in live_preflight["probes"]} == {
        "credential_resolution",
        "bounded_source_probe",
        "bounded_sink_probe",
    }
    assert {probe["status"] for probe in live_preflight["probes"]} == {"blocked"}
    assert [error["code"] for error in payload["errors"]] == ["DPONE_LIVE_CHECK_RUNNER_NOT_CONFIGURED"]
    assert live_preflight["errors"] == payload["errors"]


def test_live_check_uses_configured_runner_contract(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    service.init_project(airflow=True)
    service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    )

    result = build_airflow_self_service_service(
        root=tmp_path,
        live_preflight_runner=_PassingLivePreflightRunner(),
    ).check("pipelines/orders_daily", mode="live", environment="dev")

    payload = result.to_dict()
    jsonschema = pytest.importorskip("jsonschema")
    connection_schema = json.loads(
        (_REPO_ROOT / "docs" / "schemas" / "gitops" / "connection-check.schema.json").read_text(encoding="utf-8")
    )
    live_schema = json.loads(
        (_REPO_ROOT / "docs" / "schemas" / "gitops" / "live-preflight.schema.json").read_text(encoding="utf-8")
    )
    assert result.passed is True
    assert result.exit_code is None
    assert payload["live_preflight"] == "runner_configured"
    assert payload["network"] is True
    assert payload["secrets"] is True
    assert payload["source_queries"] is True
    assert payload["planned_source_queries"] == "bounded_probes"
    assert payload["errors"] == []
    live_preflight = payload["live_preflight_report"]
    jsonschema.Draft202012Validator(connection_schema).validate(payload)
    jsonschema.Draft202012Validator(live_schema).validate(live_preflight)
    assert live_preflight["passed"] is True
    assert live_preflight["runner"] == "configured"
    assert live_preflight["errors"] == []
    assert {probe["status"] for probe in live_preflight["probes"]} == {"passed"}
    assert sorted(live_preflight["resolved_connection_refs"]) == ["clickhouse_dev", "mssql_dev"]


def test_live_check_runner_exception_is_structured_and_redacted(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    service.init_project(airflow=True)
    service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    )

    result = build_airflow_self_service_service(
        root=tmp_path,
        live_preflight_runner=_FailingLivePreflightRunner(),
    ).check("pipelines/orders_daily", mode="live", environment="dev")

    payload = result.to_dict()
    assert result.passed is False
    assert result.exit_code == 3
    assert payload["live_preflight"] == "runner_configured"
    assert [error["code"] for error in payload["errors"]] == ["DPONE_LIVE_CHECK_RUNNER_FAILED"]
    message = payload["errors"][0]["message"]
    assert f"password={REDACTION_TOKEN}" in message
    assert f"token={REDACTION_TOKEN}" in message
    assert "must-not-leak" not in json.dumps(payload)
    assert "Traceback" not in json.dumps(payload)


def test_live_check_runner_returned_errors_are_redacted(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    service.init_project(airflow=True)
    service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    )

    result = build_airflow_self_service_service(
        root=tmp_path,
        live_preflight_runner=_ReturnedSecretErrorLivePreflightRunner(),
    ).check("pipelines/orders_daily", mode="live", environment="dev")

    payload = result.to_dict()
    rendered = json.dumps(payload)
    assert result.passed is False
    assert result.exit_code == 3
    assert payload["live_preflight"] == "runner_configured"
    assert [error["code"] for error in payload["errors"]] == ["DPONE_LIVE_CHECK_PROBE_FAILED"]
    assert payload["errors"] == payload["live_preflight_report"]["errors"]
    assert f"password={REDACTION_TOKEN}" in rendered
    assert f"token={REDACTION_TOKEN}" in rendered
    assert f"vault_token={REDACTION_TOKEN}" in rendered
    assert f"api_key={REDACTION_TOKEN}" in rendered
    assert "must-not-leak" not in rendered
    assert "super-secret" not in rendered
    assert "hidden-value" not in rendered


def test_live_check_text_output_summarizes_preflight_without_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--live", "--environment", "dev"],
        capsys,
    )

    assert code == 3, stderr
    assert "dpone check live: FAILED" in stdout
    assert "- target: pipelines/orders_daily" in stdout
    assert "- runner: runner_not_configured" in stdout
    assert "- network: planned" in stdout
    assert "- secrets: planned" in stdout
    assert "- source queries: planned bounded probes" in stdout
    assert "- connections: clickhouse_dev, mssql_dev" in stdout
    assert "- planned probes: credential_resolution, bounded_source_probe, bounded_sink_probe" in stdout
    assert "- error: DPONE_LIVE_CHECK_RUNNER_NOT_CONFIGURED:" in stdout
    assert "- details: rerun with --format json for the full live-preflight report" in stdout
    assert "dpone self-service: FAILED" not in stdout
    assert "network/secrets/source queries" not in stdout
    assert "Traceback" not in stdout


def test_connection_check_text_output_summarizes_configuration_only_refs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev"],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone check connections: OK" in stdout
    assert "- handshake: configuration_only" in stdout
    assert "- connection refs: clickhouse_dev, mssql_dev" in stdout
    assert "- resolved refs: clickhouse_dev, mssql_dev" in stdout
    assert "- network: no" in stdout
    assert "- secrets: no" in stdout
    assert "- source queries: no" in stdout
    assert "- bridge: not required" in stdout
    assert "- next: dpone airflow preview orders_daily" in stdout
    assert "Traceback" not in stdout


def test_connection_check_text_output_reports_airflow_bridge_without_secret_topology(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    _write_prod_airflow_connection_bridge(tmp_path)

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "prod"],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone check connections: OK" in stdout
    assert "- bridge: required (operator_bridge)" in stdout
    assert "- bridge connections: clickhouse_dev, mssql_dev" in stdout
    assert "- action: dpone gitops airflow connection-bridge-plan --artifact-dir .dpone/gitops/airflow" in stdout
    assert "secret_name" not in stdout
    assert "secret_key" not in stdout
    assert "mount_path" not in stdout
    assert "/run/secrets" not in stdout
    assert "AIRFLOW_CONN_" not in stdout
    assert "password" not in stdout.lower()
    assert "mssql://" not in stdout


def test_connection_check_failed_text_output_keeps_connection_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    (tmp_path / "platform" / "connection-registries" / "dev.yaml").unlink()

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev"],
        capsys,
    )

    assert code == 1, stderr
    assert "dpone check connections: FAILED" in stdout
    assert "- handshake: configuration_only" in stdout
    assert "- connection refs: clickhouse_dev, mssql_dev" in stdout
    assert "- resolved refs: clickhouse_dev, mssql_dev" in stdout
    assert "- bridge: not required" in stdout
    assert "- error: DPONE_CONNECTION_REGISTRY_NOT_FOUND: required connection configuration file is missing" in stdout
    assert "- details: rerun with --format json for the full connection-check report" in stdout
    assert "dpone self-service: FAILED" not in stdout
    assert "Traceback" not in stdout


def test_connection_check_failed_text_output_redacts_unsupported_resolver_topology(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    registry_path = tmp_path / "platform" / "connection-registries" / "dev.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["connections"]["mssql_dev"]["credentials"] = {
        "resolver": "hashi_vault",
        "path": "dpone/dev/credentials/mssql",
        "fields": {"username": "username", "password": "password"},
    }
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev"],
        capsys,
    )

    assert code == 1, stderr
    assert "dpone check connections: FAILED" in stdout
    assert "- connection refs: clickhouse_dev, mssql_dev" in stdout
    assert "- error: DPONE_CREDENTIAL_RESOLVER_UNSUPPORTED:" in stdout
    assert "hashi_vault" in stdout
    assert "dpone/dev/credentials/mssql" not in stdout
    assert "password" not in stdout.lower()
    assert "Traceback" not in stdout


def test_legacy_init_command_still_scaffolds_classic_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "--source-type",
            "mssql",
            "--sink-type",
            "clickhouse",
            "--source-connection",
            "mssql_dev",
            "--sink-connection",
            "clickhouse_dev",
            "--source-schema",
            "dbo",
            "--source-table",
            "orders",
            "--target-schema",
            "analytics",
            "--target-table",
            "orders",
            "--out",
            "orders.yaml",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["manifest_path"] == "orders.yaml"
    assert (tmp_path / "orders.yaml").exists()


def test_airflow_verify_attestation_failure_is_actionable_json_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    secret_path = tmp_path / "must-not-leak" / "release-set.json"

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "verify-attestation",
            "--subject",
            str(secret_path),
            "--bundle",
            str(tmp_path / "bundle.jsonl"),
            "--trust-policy",
            str(tmp_path / "policy.json"),
            "--expected-trust-policy-sha256",
            "sha256:" + "a" * 64,
            "--expected-trust-tier",
            "production",
            "--format",
            "json",
        ],
        capsys,
    )

    payload = json.loads(stdout)
    assert code == 4
    assert stderr == ""
    assert payload["status"] == "failed"
    assert payload["errors"][0]["code"] == "DPONE_ARTIFACT_ATTESTATION_INPUT_INVALID"
    assert payload["errors"][0]["fixes"]
    assert payload["errors"][0]["docs_url"].endswith("DPONE_ARTIFACT_ATTESTATION_INPUT_INVALID.md")
    assert str(secret_path) not in stdout


def test_airflow_verify_attestation_success_is_json_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        airflow_artifact_attestation_cmd,
        "verify_runtime_artifact_attestation",
        lambda **_: SelfServiceResult(
            passed=True,
            details={
                "schema": "dpone.runtime-artifact-attestation-verification.v1",
                "status": "passed",
                "subject_sha256": "sha256:" + "b" * 64,
                "verifier": "github_artifact_attestation_v1",
                "verifier_version": "2.93.0",
                "verified_attestations": 1,
            },
            exit_code=0,
        ),
    )

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "verify-attestation",
            "--subject",
            "release-set.json",
            "--bundle",
            "bundle.jsonl",
            "--trust-policy",
            "policy.json",
            "--expected-trust-policy-sha256",
            "sha256:" + "a" * 64,
            "--expected-trust-tier",
            "production",
            "--format",
            "json",
        ],
        capsys,
    )

    payload = json.loads(stdout)
    assert code == 0
    assert stderr == ""
    assert payload["status"] == "passed"
    assert payload["verified_attestations"] == 1
