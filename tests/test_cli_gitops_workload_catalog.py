from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path
from textwrap import dedent

import pytest
import yaml
from jsonschema import Draft202012Validator

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.airflow_pack_cmd import cmd_gitops_airflow_pack, cmd_gitops_airflow_reconcile
from dpone.commands.gitops.gitlab_cmd import cmd_gitops_gitlab_render_child_pipeline
from dpone.commands.gitops.workloads_cmd import cmd_gitops_workloads_explain, cmd_gitops_workloads_list
from dpone.gitops.changed_files import resolve_changed_files

ROOT = Path(__file__).resolve().parents[1]


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(text).strip() + "\n", encoding="utf-8")
    return path


def _catalog(tmp_path: Path) -> Path:
    root = tmp_path / "dpone_workloads"
    _write(
        root / "gitops.yaml",
        """
        gitops:
          version: 1
          defaults:
            runner: airflow
            resources_profile: safe_worker
            outcome_mode: xcom_then_gate
            image: registry.example/dpone:dev
            airflow:
              execution:
                task_executor: KubernetesExecutor
                deferrable: false
                on_finish_action: delete_succeeded_pod
                get_logs: true
                logging_interval_seconds: 60
              service_account_name: airflow-sa
              image_pull_secret: regcred
              xcom_sidecar_image: registry.example/alpine:3.23.4
              connection_projection:
                mode: unsafe_airflow_env
                connection_ids: [mssql_prod, ClickHouse]
                database_overrides:
                  ClickHouse: DWH_Raw
                scheme_overrides:
                  ClickHouse: clickhouse
                query_overrides:
                  mssql_prod:
                    trust_server_certificate: "yes"
          environments:
            dev:
              namespace: airflow-dev
              runner_policy: advisory
          includes:
            - path: gitops/domains/**/*.yaml
        """,
    )
    _write(
        root / "gitops" / "domains" / "sales.yaml",
        """
        domain: sales
        defaults:
          owner: data-office
        workloads:
          work-item_account_sales:
            manifest: ../../manifests/mssql/account_sales.yaml
            schedule: "0 6 * * *"
            resources_profile: throughput
        """,
    )
    _write(root / "manifests" / "mssql" / "account_sales.yaml", "source: {}\nsink: {}")
    return root / "gitops.yaml"


def test_workloads_list_and_explain_cli_are_json_and_secret_free(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)

    list_code = cmd_gitops_workloads_list(
        Namespace(workload_set=str(workload_set), env="dev", output=None, format="json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    list_payload = json.loads(capsys.readouterr().out)
    explain_code = cmd_gitops_workloads_explain(
        Namespace(
            workload_id="work-item_account_sales",
            workload_set=str(workload_set),
            env="dev",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    explain_payload = json.loads(capsys.readouterr().out)

    assert list_code == 0
    assert explain_code == 0
    assert list_payload["kind"] == "gitops.workloads"
    assert list_payload["workloads"][0]["workload_id"] == "work-item_account_sales"
    assert explain_payload["kind"] == "gitops.workload_explain"
    assert explain_payload["workload"]["effective_config"]["namespace"] == "airflow-dev"
    assert "password" not in json.dumps(explain_payload).lower()
    assert str(tmp_path) not in json.dumps(explain_payload)


def test_airflow_pack_cli_builds_single_compact_workload_pack(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    output = ".dpone/gitops/airflow/work-item_account_sales/airflow-pack.json"

    code = cmd_gitops_airflow_pack(
        Namespace(
            workload="work-item_account_sales",
            workload_set=str(workload_set),
            env="dev",
            output_path=output,
            artifact_dir=None,
            bundle_path=None,
            image=None,
            image_digest=None,
            mode="plan",
            runner_policy="advisory",
            include_live_gates=False,
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    written = json.loads((tmp_path / output).read_text(encoding="utf-8"))

    assert code == 0
    assert payload == written
    assert payload["kind"] == "gitops.airflow_pack"
    assert payload["meta"] == {"kind": "gitops.airflow_pack", "path": output}
    assert payload["airflow"]["execution"] == {
        "task_executor": "KubernetesExecutor",
        "deferrable": False,
        "on_finish_action": "delete_succeeded_pod",
        "get_logs": True,
        "logging_interval_seconds": 60,
    }
    assert payload["workload"]["workload_id"] == "work-item_account_sales"
    assert payload["workload"]["manifest"] == "dpone_workloads/manifests/mssql/account_sales.yaml"
    assert payload["effective_config"]["schedule"] == "0 6 * * *"
    assert payload["effective_config"]["resources_profile"] == "throughput"
    assert payload["effective_config"]["outcome_mode"] == "xcom_then_gate"
    assert payload["kpo_kwargs"]["image"] == "registry.example/dpone:dev"
    assert "resources_profile" not in payload["kpo_kwargs"]
    assert "outcome_mode" not in payload["kpo_kwargs"]
    assert payload["schema_version"] == "3"
    assert payload["runtime_command"].startswith(
        "status=0; mkdir -p .dpone/runs/work-item_account_sales; DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1 dpone run "
    )
    assert payload["runtime_command"].index("mkdir -p .dpone/runs/work-item_account_sales") < payload[
        "runtime_command"
    ].index("dpone run ")
    assert "dpone gitops airflow xcom-from-evidence" in payload["runtime_command"]
    assert (
        "--runtime-evidence-path .dpone/runs/work-item_account_sales/runtime-evidence.json" in payload["runtime_command"]
    )
    assert "--stderr-path .dpone/runs/work-item_account_sales/runtime-stderr.log" in payload["runtime_command"]
    assert "runtime-stderr.log" in payload["runtime_command"]
    assert payload["runtime_command"].endswith("exit 0")
    assert payload["pod_spec"]["kind"] == "Pod"
    assert payload["pod_spec"]["spec"]["serviceAccountName"] == "airflow-sa"
    assert payload["pod_spec"]["spec"]["imagePullSecrets"] == [{"name": "regcred"}]
    assert payload["pod_spec"]["spec"]["containers"][0]["workingDir"] == "/workspace/repo"
    assert payload["pod_spec"]["spec"]["initContainers"][0]["name"] == "dpone-inline-workload-bootstrap"
    assert payload["connection_projection"]["mode"] == "unsafe_airflow_env"
    assert payload["connection_projection"]["connection_ids"] == ["mssql_prod", "ClickHouse"]
    assert payload["connection_projection"]["database_overrides"] == {"ClickHouse": "DWH_Raw"}
    assert payload["xcom"]["sidecar_image"] == "registry.example/alpine:3.23.4"
    assert payload["outcome_gate"]["task_id"] == "work-item_account_sales__dpone_outcome_gate"
    assert payload["artifact_index"]["airflow_pack"] == output
    assert payload["steps"][0]["name"] == "dpone_runtime"
    assert payload["runtime_selection"] == {}


def test_airflow_pack_cli_renders_compact_pack_markdown(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)

    code = cmd_gitops_airflow_pack(
        Namespace(
            workload="work-item_account_sales",
            workload_set=str(workload_set),
            env="dev",
            output_path=".dpone/gitops/airflow/work-item_account_sales/airflow-pack.json",
            artifact_dir=None,
            bundle_path=None,
            image=None,
            image_digest=None,
            mode="plan",
            runner_policy="advisory",
            include_live_gates=False,
            output=None,
            format="markdown",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    rendered = capsys.readouterr().out

    assert code == 0
    assert rendered.startswith("# Gitops Airflow Pack")
    assert '"workload_id": "work-item_account_sales"' in rendered


def test_airflow_reconcile_writes_packs_for_affected_workloads(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    changed = _write(tmp_path / "changed.txt", "dpone_workloads/gitops/domains/sales.yaml\n")

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=changed.relative_to(tmp_path).as_posix(),
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["kind"] == "gitops.airflow_reconcile"
    assert payload["affected_workloads"] == ["work-item_account_sales"]
    assert payload["packs"][0].endswith("work-item_account_sales/airflow-pack.json")
    assert (tmp_path / payload["packs"][0]).is_file()


def test_airflow_reconcile_writes_full_catalog_to_requested_output_root(tmp_path: Path, monkeypatch, capsys) -> None:
    from tests.airflow_dag_spec_repo import (
        dag_declaration,
        manifest_ref,
        write_domain,
        write_manifest,
        write_workload_set,
    )

    monkeypatch.chdir(tmp_path)
    first = write_manifest(tmp_path, "first")
    second = write_manifest(tmp_path, "second")
    write_domain(
        tmp_path,
        domain="sales",
        workloads={
            "sales_first": manifest_ref(first),
            "sales_second": manifest_ref(second),
        },
        dags={
            "DAG__sales__all__refresh": dag_declaration(
                workloads=["sales_first", "sales_second"],
                wiring={"mode": "explicit", "dependencies": {}},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/parse-fixture/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["selection_mode"] == "all"
    assert payload["affected_workloads"] == ["sales_first", "sales_second"]
    assert len(payload["packs"]) == 2
    assert payload["dag_specs"] == [".dpone/parse-fixture/gitops/airflow/_dags/DAG__sales__all__refresh.dag-spec.json"]
    for path in (*payload["packs"], *payload["dag_specs"]):
        assert (tmp_path / path).is_file()
    assert not (tmp_path / ".dpone/gitops/airflow").exists()
    schema = json.loads((ROOT / "docs/schemas/gitops/airflow-reconcile.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(payload)
    historical_payload = dict(payload)
    historical_payload.pop("selection_mode")
    Draft202012Validator(schema).validate(historical_payload)


def test_full_reconcile_materializes_every_catalog_workload_into_release(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
    from tests.airflow_dag_spec_repo import (
        dag_declaration,
        manifest_ref,
        write_domain,
        write_manifest,
    )

    monkeypatch.chdir(tmp_path)
    first = write_manifest(tmp_path, "first")
    second = write_manifest(tmp_path, "second")
    for manifest in (first, second):
        manifest_path = tmp_path / manifest
        manifest_payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        for endpoint in ("source", "sink"):
            manifest_payload[endpoint]["connection_ref"] = manifest_payload[endpoint].pop("connection_id")
        manifest_path.write_text(yaml.safe_dump(manifest_payload, sort_keys=False), encoding="utf-8")
    write_domain(
        tmp_path,
        domain="sales",
        workloads={
            "sales_first": manifest_ref(first),
            "sales_second": manifest_ref(second),
        },
        dags={
            "DAG__sales__all__refresh": dag_declaration(
                workloads=["sales_first", "sales_second"],
                wiring={"mode": "explicit", "dependencies": {}},
            )
        },
    )
    workload_set = _write(
        tmp_path / "dpone_workloads/gitops/gitops.yaml",
        """
        gitops:
          version: 1
          defaults:
            runner: airflow
            image: registry.example/dpone:0.73.22
            airflow:
              execution:
                task_executor: KubernetesExecutor
                deferrable: false
                on_finish_action: delete_succeeded_pod
              connection_projection:
                mode: kubernetes_secret_volume
                secret_name: dpone-airflow-connection-bridge
                mount_path: /run/secrets/dpone/airflow-connections
                payload_format: airflow_connection_uri
                secret_values: false
                connection_ids: [pg_src, pg_dst]
                connections:
                  - connection_id: pg_src
                    connection_ref: pg_src
                    registry_connection_ref: pg_src
                    secret_key: AIRFLOW_CONN_PG_SRC
                    mount_path: /run/secrets/dpone/airflow-connections/pg_src
                    fields: {uri: uri}
                  - connection_id: pg_dst
                    connection_ref: pg_dst
                    registry_connection_ref: pg_dst
                    secret_key: AIRFLOW_CONN_PG_DST
                    mount_path: /run/secrets/dpone/airflow-connections/pg_dst
                    fields: {uri: uri}
          environments:
            dev:
              namespace: airflow-dev
              runner_policy: advisory
          includes:
            - path: domains/*.yaml
        """,
    )
    output_root = ".dpone/release-fixture/gitops"

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=output_root,
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    reconcile = json.loads(capsys.readouterr().out)
    release = materialize_compact_pack_release(
        pack_root=tmp_path / output_root / "airflow",
        cache_root=tmp_path / ".dpone/release-fixture/cache",
        xcom_sidecar_image="registry.example/airflow/xcom@sha256:" + "ab" * 32,
    )

    assert code == 0
    assert reconcile["affected_workloads"] == ["sales_first", "sales_second"]
    assert release.passed, release.blockers
    assert release.dag_ids == ("DAG__sales__all__refresh",)
    assert release.workload_ids == ("sales_first", "sales_second")


def test_airflow_reconcile_rejects_full_and_changed_file_selection(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=["dpone_workloads/gitops/domains/sales.yaml"],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["selection_mode"] == "all"
    assert payload["meta"]["path"] == ".dpone/gitops"
    assert payload["packs"] == []
    assert payload["dag_specs"] == []
    assert [item["code"] for item in payload["blockers"]] == ["reconcile_selection_conflict"]


@pytest.mark.parametrize("changed_file", ["empty.txt", "missing.txt"])
def test_airflow_reconcile_rejects_full_and_explicit_changed_file_argument(
    tmp_path: Path,
    monkeypatch,
    capsys,
    changed_file: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    if changed_file == "empty.txt":
        _write(tmp_path / changed_file, "\n")

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=changed_file,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["blockers"] == [
        {
            "code": "reconcile_selection_conflict",
            "message": "--all-workloads cannot be combined with changed-file selection",
            "path": "dpone_workloads/gitops.yaml",
            "source": "dpone gitops airflow reconcile",
        }
    ]
    assert not (tmp_path / ".dpone").exists()


@pytest.mark.parametrize("output_dir", ["../outside", "/tmp/outside"])
def test_airflow_reconcile_rejects_output_outside_repository(
    tmp_path: Path,
    monkeypatch,
    capsys,
    output_dir: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    outside = tmp_path.parent / "outside" / "airflow" / "_dags"
    stale = _write(outside / "STALE.dag-spec.json", "{}")

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=output_dir,
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert [item["code"] for item in payload["blockers"]] == ["invalid_path"]
    assert not tuple(outside.rglob("airflow-pack.json"))
    assert payload["packs"] == []
    assert payload["dag_specs"] == []
    assert stale.is_file()


def test_airflow_reconcile_rejects_symlink_output_before_pack_write(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    outside = tmp_path.parent / "outside-symlink"
    outside.mkdir(exist_ok=True)
    (tmp_path / "escape").symlink_to(outside, target_is_directory=True)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir="escape",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert [item["code"] for item in payload["blockers"]] == ["invalid_path"]


def test_airflow_reconcile_turns_output_symlink_loop_into_invalid_path(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    loop = tmp_path / "loop"
    loop.symlink_to(loop.name, target_is_directory=True)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir="loop",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["meta"]["path"] == ""
    assert [item["code"] for item in payload["blockers"]] == ["invalid_path"]
    assert payload["packs"] == []
    assert payload["dag_specs"] == []


def test_airflow_reconcile_rejects_nested_pack_symlink_before_any_write(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    output_root = tmp_path / ".dpone/gitops/airflow"
    outside = tmp_path.parent / "outside-pack-symlink"
    output_root.mkdir(parents=True)
    outside.mkdir(exist_ok=True)
    (output_root / "work-item_account_sales").symlink_to(outside, target_is_directory=True)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert [item["code"] for item in payload["blockers"]] == ["invalid_path"]
    assert payload["packs"] == []
    assert payload["dag_specs"] == []
    assert not tuple(outside.rglob("airflow-pack.json"))
    assert not (output_root / "_dags").exists()


def test_airflow_reconcile_rejects_dag_spec_symlink_before_pack_write(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    airflow_root = tmp_path / ".dpone/gitops/airflow"
    outside = tmp_path.parent / "outside-dag-spec-symlink"
    airflow_root.mkdir(parents=True)
    outside.mkdir(exist_ok=True)
    (airflow_root / "_dags").symlink_to(outside, target_is_directory=True)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert [item["code"] for item in payload["blockers"]] == ["invalid_path"]
    assert payload["packs"] == []
    assert not tuple(airflow_root.rglob("airflow-pack.json"))
    assert not tuple(outside.iterdir())


def test_airflow_reconcile_build_failure_writes_no_partial_artifacts(tmp_path: Path, monkeypatch, capsys) -> None:
    from tests.airflow_dag_spec_repo import manifest_ref, write_domain, write_manifest, write_workload_set

    monkeypatch.chdir(tmp_path)
    good = write_manifest(tmp_path, "a_good")
    missing = write_manifest(tmp_path, "z_missing")
    (tmp_path / missing).unlink()
    write_domain(
        tmp_path,
        domain="sales",
        workloads={
            "a_good": manifest_ref(good),
            "z_missing": manifest_ref(missing),
        },
        dags={},
    )
    workload_set = write_workload_set(tmp_path)
    output_dir = ".dpone/parse-fixture/gitops"

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=output_dir,
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert [item["code"] for item in payload["blockers"]] == ["reconcile_artifact_build_failed"]
    assert payload["blockers"][0]["path"] == missing
    assert "Verify the manifest and workload configuration" in payload["blockers"][0]["message"]
    assert payload["packs"] == []
    assert payload["dag_specs"] == []
    assert not (tmp_path / output_dir).exists()


@pytest.mark.parametrize("error_type", [ValueError, RuntimeError])
def test_airflow_reconcile_does_not_mask_unexpected_builder_failure(
    tmp_path: Path,
    monkeypatch,
    error_type: type[Exception],
) -> None:
    from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder

    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)

    def fail_unexpectedly(*_args: object, **_kwargs: object) -> None:
        raise error_type("unexpected implementation failure")

    monkeypatch.setattr(AirflowCompactPackBuilder, "build", fail_unexpectedly)

    with pytest.raises(error_type, match="unexpected implementation failure"):
        cmd_gitops_airflow_reconcile(
            Namespace(
                workload_set=str(workload_set),
                changed_files=[],
                changed_files_file=None,
                all_workloads=True,
                env="dev",
                output_dir=".dpone/gitops",
                output=None,
                format="json",
            ),
            ctx=_ctx(tmp_path),
            logger=logging.getLogger("test"),
        )

    assert not (tmp_path / ".dpone").exists()


def test_airflow_reconcile_does_not_mask_unexpected_dag_spec_builder_value_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder

    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)

    def fail_unexpectedly(*_args: object, **_kwargs: object) -> None:
        raise ValueError("unexpected DAG-spec implementation failure")

    monkeypatch.setattr(AirflowDagSpecBuilder, "build", fail_unexpectedly)

    with pytest.raises(ValueError, match="unexpected DAG-spec implementation failure"):
        cmd_gitops_airflow_reconcile(
            Namespace(
                workload_set=str(workload_set),
                changed_files=[],
                changed_files_file=None,
                all_workloads=True,
                env="dev",
                output_dir=".dpone/gitops",
                output=None,
                format="json",
            ),
            ctx=_ctx(tmp_path),
            logger=logging.getLogger("test"),
        )

    assert not (tmp_path / ".dpone").exists()


@pytest.mark.parametrize("changed_file", ["missing.txt", "../outside.txt", "/tmp/outside.txt"])
def test_airflow_reconcile_rejects_unsafe_or_unreadable_changed_files_input(
    tmp_path: Path,
    monkeypatch,
    capsys,
    changed_file: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=changed_file,
            all_workloads=False,
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["blockers"][0]["code"] in {"changed_files_file_read_failed", "invalid_path"}
    assert payload["packs"] == []
    assert payload["dag_specs"] == []
    assert not (tmp_path / ".dpone").exists()


def test_changed_files_reader_does_not_mask_unexpected_runtime_failure(tmp_path: Path) -> None:
    class BrokenReader:
        def read_text(self, path: Path, *, encoding: str = "utf-8") -> str:
            _ = path, encoding
            raise RuntimeError("unexpected reader implementation failure")

    args = Namespace(changed_files=[], changed_files_file="changed.txt", from_ref=None, to_ref=None)

    with pytest.raises(RuntimeError, match="unexpected reader implementation failure"):
        resolve_changed_files(fs=BrokenReader(), repo_root=tmp_path, args=args)


def test_airflow_reconcile_rejects_symlinked_output_mirror_before_artifact_writes(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    outside = tmp_path.parent / "outside-output-mirror"
    outside.mkdir(exist_ok=True)
    (tmp_path / ".ci").symlink_to(outside, target_is_directory=True)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/gitops",
            output=".ci/reconcile.json",
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert [item["code"] for item in payload["blockers"]] == ["invalid_path"]
    assert payload["packs"] == []
    assert payload["dag_specs"] == []
    assert not tuple(outside.iterdir())
    assert not (tmp_path / ".dpone").exists()


@pytest.mark.parametrize(
    "output",
    [
        ".",
        ".dpone/gitops/airflow",
        ".dpone/gitops/airflow/work-item_account_sales/airflow-pack.json",
        ".dpone/gitops/airflow/_dags/STALE.dag-spec.json",
    ],
)
def test_airflow_reconcile_rejects_output_mirror_artifact_collisions_before_any_write(
    tmp_path: Path,
    monkeypatch,
    capsys,
    output: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/gitops",
            output=output,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert [item["code"] for item in payload["blockers"]] == ["invalid_path"]
    assert payload["packs"] == []
    assert payload["dag_specs"] == []
    assert not (tmp_path / ".dpone").exists()


def test_airflow_reconcile_never_overwrites_stale_managed_artifact_with_output_mirror(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    pack_path = ".dpone/gitops/airflow/work-item_account_sales/airflow-pack.json"

    first_code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    capsys.readouterr()
    original = (tmp_path / pack_path).read_text(encoding="utf-8")

    second_code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=False,
            env="dev",
            output_dir=".dpone/gitops",
            output=pack_path,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert first_code == 0
    assert second_code == 2
    assert [item["code"] for item in payload["blockers"]] == ["invalid_path"]
    assert (tmp_path / pack_path).read_text(encoding="utf-8") == original
    assert json.loads(original)["kind"] == "gitops.airflow_pack"


def test_airflow_reconcile_selection_conflict_cannot_write_into_managed_namespace(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    output = ".dpone/gitops/airflow/work-item_account_sales/airflow-pack.json"

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=["dpone_workloads/gitops.yaml"],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/gitops",
            output=output,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert [item["code"] for item in payload["blockers"]] == [
        "reconcile_selection_conflict",
        "invalid_path",
    ]
    assert not (tmp_path / output).exists()


def test_airflow_reconcile_rejects_dag_spec_output_mirror_collision_before_any_write(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from tests.airflow_dag_spec_repo import (
        dag_declaration,
        manifest_ref,
        write_domain,
        write_manifest,
        write_workload_set,
    )

    monkeypatch.chdir(tmp_path)
    manifest = write_manifest(tmp_path, "orders")
    dag_id = "DAG__sales__orders__refresh"
    write_domain(
        tmp_path,
        domain="sales",
        workloads={"orders": manifest_ref(manifest)},
        dags={
            dag_id: dag_declaration(
                workloads=["orders"],
                wiring={"mode": "explicit", "dependencies": {}},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)
    output = f".dpone/gitops/airflow/_dags/{dag_id}.dag-spec.json"

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/gitops",
            output=output,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert [item["code"] for item in payload["blockers"]] == ["invalid_path"]
    assert payload["packs"] == []
    assert payload["dag_specs"] == []
    assert not tuple((tmp_path / ".dpone").rglob("airflow-pack.json"))
    assert not tuple((tmp_path / ".dpone").rglob("*.dag-spec.json"))


def test_airflow_reconcile_aggregates_compact_pack_warnings(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=None,
            all_workloads=True,
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert "DPONE_AIRFLOW_PROCESS_PLAN_COMPATIBILITY_FALLBACK" in {item["code"] for item in payload["warnings"]}


def test_airflow_reconcile_report_constructor_remains_backward_compatible() -> None:
    from dpone.services.gitops.airflow_compact_pack_service import GitOpsAirflowReconcileReport

    report = GitOpsAirflowReconcileReport("workloads.yaml", "dev", ("orders",), ("orders/airflow-pack.json",))

    assert report.selection_mode == "affected"
    assert report.to_jsonable()["selection_mode"] == "affected"


def test_airflow_reconcile_writes_dag_specs_for_domain_dags_block(tmp_path: Path, monkeypatch, capsys) -> None:
    from tests.airflow_dag_spec_repo import (
        dag_declaration,
        manifest_ref,
        write_domain,
        write_manifest,
        write_workload_set,
    )

    monkeypatch.chdir(tmp_path)
    manifest = write_manifest(tmp_path, "account_sales")
    write_domain(
        tmp_path,
        domain="sales",
        workloads={"work-item_account_sales": manifest_ref(manifest)},
        dags={
            "DAG__sales__account_activity__refresh": dag_declaration(
                workloads=["work-item_account_sales"],
                wiring={"mode": "explicit", "dependencies": {}},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)
    changed = _write(tmp_path / "changed.txt", "dpone_workloads/gitops/domains/sales.yaml\n")

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=changed.relative_to(tmp_path).as_posix(),
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["dag_specs"] == [".dpone/gitops/airflow/_dags/DAG__sales__account_activity__refresh.dag-spec.json"]
    assert (tmp_path / payload["dag_specs"][0]).is_file()


def test_airflow_reconcile_cli_renders_markdown(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=["dpone_workloads/manifests/mssql/account_sales.yaml"],
            changed_files_file=None,
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="markdown",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    rendered = capsys.readouterr().out

    assert code == 0
    assert rendered.startswith("# Gitops Airflow Reconcile")
    assert '"affected_workloads": [' in rendered


def test_gitlab_child_pipeline_renderer_emits_one_job_per_affected_workload(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = _catalog(tmp_path)
    changed = _write(tmp_path / "changed.txt", "dpone_workloads/manifests/mssql/account_sales.yaml\n")

    code = cmd_gitops_gitlab_render_child_pipeline(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=changed.relative_to(tmp_path).as_posix(),
            env="dev",
            output=".dpone/gitops/child.yml",
            format="yaml",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    rendered = capsys.readouterr().out

    assert code == 0
    assert "validate:dpone-gitops-work-item-account-sales:" in rendered
    assert "dpone gitops airflow reconcile" in rendered
    assert (tmp_path / ".dpone/gitops/child.yml").read_text(encoding="utf-8") == rendered
