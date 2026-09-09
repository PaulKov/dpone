from __future__ import annotations

import base64
import builtins
import hashlib
import io
import json
import socket
import tarfile
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft7Validator

from dpone.cli import main as cli_main
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.airflow_compact_pack_bootstrap import inline_workload_archive, inline_workload_bootstrap
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.gitops.workload_dependencies import WorkloadDependencyResolver
from dpone.manifest.authoring import AuthoringCompilationError, default_authoring_compiler
from dpone.manifest.recipe_catalog import RecipeCatalogService
from dpone.readiness.airflow_local_workload_pack import (
    LocalWorkloadPackBuildError,
    build_verified_local_airflow_workload_pack,
)
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.services.hermetic_test_service import HermeticTestService
from tests.mssql_asset_registry_fixtures import write_mssql_connection_registry


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _write_yaml(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _artifact_pin(root: Path, path: Path, ref: str) -> dict[str, str]:
    return {
        "ref": ref,
        "artifact_ref": path.relative_to(root).as_posix(),
        "sha256": _digest(path),
    }


def _parameter_schema() -> dict[str, Any]:
    defaults = {
        "source_connection_ref": ("string", "mssql_dev", "connection_ref"),
        "sink_connection_ref": ("string", "clickhouse_dev", "connection_ref"),
        "source_schema": ("string", "dbo", "identifier"),
        "source_table": ("string", "orders", "identifier"),
        "target_schema": ("string", "analytics", "identifier"),
        "target_table": ("string", "orders", "identifier"),
        "strategy": ("string", "incremental_merge", "identifier"),
        "unique_key": ("string", "id", "identifier"),
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "properties": {
            name: {"type": kind, "default": default, "x-dpone-format": value_format}
            for name, (kind, default, value_format) in defaults.items()
        },
        "required": list(defaults),
    }


def _component_payload() -> dict[str, Any]:
    return {
        "schema": "dpone.component.v1",
        "id": "mssql-clickhouse-load",
        "version": "1.0.0",
        "owner": "data-platform",
        "status": "stable",
        "processes": [
            {
                "name": {"$context": "pipeline_id"},
                "source": {
                    "type": "mssql",
                    "connection_ref": {"$param": "source_connection_ref"},
                    "table": {
                        "schema": {"$param": "source_schema"},
                        "name": {"$param": "source_table"},
                    },
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": {"$param": "sink_connection_ref"},
                    "table": {
                        "schema": {"$param": "target_schema"},
                        "name": {"$param": "target_table"},
                    },
                    "strategy": {
                        "mode": {"$param": "strategy"},
                        "unique_key": {"$param": "unique_key"},
                    },
                },
            }
        ],
    }


def _create_external_catalog(root: Path) -> dict[str, Path]:
    component_path = root / "platform/recipes/components/mssql-clickhouse-load-1.0.0.yaml"
    _write_yaml(component_path, _component_payload())

    profile_path = root / "platform/recipes/profiles/incremental-defaults-1.0.0.yaml"
    _write_yaml(
        profile_path,
        {
            "schema": "dpone.profile.v1",
            "id": "incremental-defaults",
            "version": "1.0.0",
            "owner": "data-platform",
            "status": "stable",
            "values": {
                "source_schema": "dbo",
                "target_schema": "analytics",
                "target_table": "orders",
                "strategy": "incremental_merge",
                "unique_key": "id",
            },
            "locked_parameters": ["strategy"],
        },
    )

    recipe_path = root / "platform/recipes/recipes/governed-mssql-clickhouse-1.2.0.yaml"
    profile_pin = _artifact_pin(root, profile_path, "incremental-defaults@1.0.0")
    _write_yaml(
        recipe_path,
        {
            "schema": "dpone.recipe.v1",
            "id": "governed-mssql-clickhouse",
            "version": "1.2.0",
            "owner": "data-platform",
            "status": "stable",
            "description": "Governed incremental table ingestion",
            "domain": "sales",
            "parameter_schema": _parameter_schema(),
            "override_allowlist": ["source_connection_ref", "sink_connection_ref", "source_table"],
            "default_profile_ref": "incremental-defaults@1.0.0",
            "profiles": [dict(profile_pin)],
            "components": [_artifact_pin(root, component_path, "mssql-clickhouse-load@1.0.0")],
        },
    )

    catalog_path = root / "platform/recipes/catalog.yaml"
    _write_yaml(
        catalog_path,
        {
            "schema": "dpone.recipe-catalog.v1",
            "catalog_id": "data-platform",
            "artifacts": [
                {
                    "kind": "recipe",
                    **_artifact_pin(root, recipe_path, "governed-mssql-clickhouse@1.2.0"),
                }
            ],
        },
    )
    _write_yaml(
        root / "dpone.yaml",
        {
            "schema": "dpone.project.v1",
            "authoring": {
                "primary_source_policy": "one_per_pipeline",
                "recipe_catalog": {
                    "path": "platform/recipes/catalog.yaml",
                    "trusted_catalog_ids": ["data-platform"],
                },
            },
        },
    )
    write_mssql_connection_registry(root, include=("mssql_dev",))
    return {
        "catalog": catalog_path,
        "recipe": recipe_path,
        "profile": profile_path,
        "component": component_path,
    }


def _refresh_recipe_closure_pins(paths: dict[str, Path]) -> None:
    recipe = yaml.safe_load(paths["recipe"].read_text(encoding="utf-8"))
    recipe["profiles"][0]["sha256"] = _digest(paths["profile"])
    recipe["components"][0]["sha256"] = _digest(paths["component"])
    _write_yaml(paths["recipe"], recipe)
    catalog = yaml.safe_load(paths["catalog"].read_text(encoding="utf-8"))
    catalog["artifacts"][0]["sha256"] = _digest(paths["recipe"])
    _write_yaml(paths["catalog"], catalog)


def _scaffold(root: Path, *, answers: Path | None = None):
    return build_airflow_self_service_service(root=root).init_pipeline(
        pipeline_id="orders_daily",
        recipe="governed-mssql-clickhouse@1.2.0",
        airflow=True,
        authoring_mode="flow",
        profile=None,
        answers=answers,
    )


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def test_external_recipe_scaffold_compiles_to_same_semantics_as_explicit_flow(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)

    result = _scaffold(tmp_path)

    assert result.passed is True
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    assert "processes" not in source
    assert source["recipe"]["ref"] == "governed-mssql-clickhouse@1.2.0"
    assert source["metadata"]["domain"] == "sales"
    assert result.details["recipe_resolution"]["status"] == "stable"

    compilation = default_authoring_compiler().compile(source, source_path=source_path, project_root=tmp_path)
    explicit = {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": "pipelines/explicit/pipeline.yaml"},
        "metadata": {
            "id": "orders_daily",
            "domain": "sales",
            "tags": ["dpone", "airflow"],
            "airflow": True,
        },
        "processes": [dict(compilation.processes[0])],
    }
    explicit_path = tmp_path / "pipelines/explicit/pipeline.yaml"
    explicit_path.parent.mkdir(parents=True)
    explicit_compilation = default_authoring_compiler().compile(
        explicit,
        source_path=explicit_path,
        project_root=tmp_path,
    )
    assert compilation.semantic_fingerprint == explicit_compilation.semantic_fingerprint
    assert [item.kind for item in compilation.dependencies] == ["component", "profile", "recipe"]
    assert compilation.recipe_provenance is not None


def test_external_recipe_scaffold_emits_a_passing_hermetic_test(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed is True

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.passed is True
    assert report.tests[0].process == {"name": "orders_daily", "strategy": "incremental_merge"}


def test_external_recipe_scaffold_normalizes_comma_delimited_composite_test_key(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    profile = yaml.safe_load(paths["profile"].read_text(encoding="utf-8"))
    profile["values"]["unique_key"] = "tenant_id, order_id"
    _write_yaml(paths["profile"], profile)
    recipe = yaml.safe_load(paths["recipe"].read_text(encoding="utf-8"))
    recipe["parameter_schema"]["properties"]["unique_key"].pop("x-dpone-format")
    _write_yaml(paths["recipe"], recipe)
    _refresh_recipe_closure_pins(paths)

    assert _scaffold(tmp_path).passed is True

    fixture = [
        json.loads(line)
        for line in (tmp_path / "tests/fixtures/orders_daily.input.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert fixture == [
        {"dpone_test_status": "ready", "order_id": 1, "tenant_id": 1},
        {"dpone_test_status": "ready", "order_id": 2, "tenant_id": 2},
    ]
    assert report.passed is True


def test_external_recipe_scaffold_uses_runtime_unique_key_precedence_for_fixture(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    component = yaml.safe_load(paths["component"].read_text(encoding="utf-8"))
    process = component["processes"][0]
    process["source"]["options"] = {"unique_key": "tenant_id, order_id"}
    process["sink"]["strategy"].pop("unique_key")
    _write_yaml(paths["component"], component)
    _refresh_recipe_closure_pins(paths)

    assert _scaffold(tmp_path).passed is True

    fixture = [
        json.loads(line)
        for line in (tmp_path / "tests/fixtures/orders_daily.input.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert fixture == [
        {"dpone_test_status": "ready", "order_id": 1, "tenant_id": 1},
        {"dpone_test_status": "ready", "order_id": 2, "tenant_id": 2},
    ]
    assert report.passed is True


def test_external_recipe_scaffold_preserves_synthetic_status_merge_key(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    profile = yaml.safe_load(paths["profile"].read_text(encoding="utf-8"))
    profile["values"]["unique_key"] = "dpone_test_status"
    _write_yaml(paths["profile"], profile)
    _refresh_recipe_closure_pins(paths)

    assert _scaffold(tmp_path).passed is True

    fixture = [
        json.loads(line)
        for line in (tmp_path / "tests/fixtures/orders_daily.input.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert fixture == [{"dpone_test_status": 1}, {"dpone_test_status": 2}]
    assert report.passed is True


def test_external_recipe_scaffold_fails_before_writes_when_starter_test_is_unsupported(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    profile = yaml.safe_load(paths["profile"].read_text(encoding="utf-8"))
    profile["values"]["strategy"] = "auto"
    _write_yaml(paths["profile"], profile)
    _refresh_recipe_closure_pins(paths)

    result = _scaffold(tmp_path)

    assert result.passed is False
    assert result.exit_code == 1
    assert result.errors[0]["code"] == "DPONE_RECIPE_HERMETIC_TEST_UNSUPPORTED"
    assert result.errors[0]["fixes"] == [
        {
            "id": "choose_hermetic_recipe",
            "safety": "manual",
            "command": "dpone recipe show governed-mssql-clickhouse@1.2.0",
        }
    ]
    assert not (tmp_path / "pipelines/orders_daily/pipeline.yaml").exists()
    assert not (tmp_path / "tests/orders_daily.test.yaml").exists()


def test_external_recipe_scaffold_rejects_unsupported_secondary_process_before_writes(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    component = yaml.safe_load(paths["component"].read_text(encoding="utf-8"))
    secondary = json.loads(json.dumps(component["processes"][0]))
    secondary["name"] = "orders_quality"
    secondary["source"]["query"] = "SELECT 1"
    component["processes"].append(secondary)
    _write_yaml(paths["component"], component)
    _refresh_recipe_closure_pins(paths)

    result = _scaffold(tmp_path)

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_HERMETIC_TEST_UNSUPPORTED"
    assert not (tmp_path / "pipelines/orders_daily/pipeline.yaml").exists()
    assert not (tmp_path / "tests/orders_daily.test.yaml").exists()
    assert not (tmp_path / "tests/fixtures/orders_daily.input.jsonl").exists()


def test_pack_dependency_resolver_keeps_full_recipe_closure(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed

    dependencies = WorkloadDependencyResolver().resolve(
        repo_root=tmp_path,
        manifest="pipelines/orders_daily/pipeline.yaml",
    )

    assert {item.kind for item in dependencies} == {"manifest", "recipe", "profile", "component"}


def test_preview_release_evidence_contains_complete_ordered_recipe_closure(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed

    result = build_airflow_self_service_service(root=tmp_path).preview("orders_daily")

    assert result.passed is True
    provenance = result.details["release"]["provenance"]["recipe_resolution"]
    assert provenance["domain"] == "sales"
    assert provenance["profile_ref"] == "incremental-defaults@1.0.0"
    closure = provenance["closure"]
    assert [(item["kind"], item["path"], item["sha256"]) for item in closure] == sorted(
        (item["kind"], item["path"], item["sha256"]) for item in closure
    )
    assert {item["kind"] for item in closure} == {"recipe", "profile", "component"}


def test_component_mutation_fails_compile_and_pack_closed(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    paths["component"].write_text("schema: dpone.component.v1\n", encoding="utf-8")

    with pytest.raises(AuthoringCompilationError) as exc_info:
        default_authoring_compiler().compile(source, source_path=source_path, project_root=tmp_path)
    assert exc_info.value.code == "DPONE_RECIPE_DIGEST_MISMATCH"

    with pytest.raises(ValueError, match="recipe_digest_mismatch"):
        WorkloadDependencyResolver().resolve(
            repo_root=tmp_path,
            manifest="pipelines/orders_daily/pipeline.yaml",
        )


def test_recipe_mutation_between_compile_and_pack_has_stable_drift_code(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    compilation = default_authoring_compiler().compile(source, source_path=source_path, project_root=tmp_path)
    paths["component"].write_text("schema: dpone.component.v1\n", encoding="utf-8")

    with pytest.raises(LocalWorkloadPackBuildError) as exc_info:
        build_verified_local_airflow_workload_pack(
            root=tmp_path,
            source_path=source_path,
            pipeline_payload=source,
            pipeline_id="orders_daily",
            runtime_image="registry.example/dpone:dev",
            runtime_image_digest="sha256:" + "a" * 64,
            expected_authoring_dependencies=compilation.dependencies,
        )

    assert exc_info.value.code == "DPONE_RECIPE_SOURCE_CHANGED_DURING_BUILD"


def test_compact_pack_rejects_dependency_mutation_during_inline_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed
    workload = GitOpsWorkloadDefinition(
        workload_id="orders_daily",
        manifest="pipelines/orders_daily/pipeline.yaml",
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "registry.example/dpone:dev", "airflow": {}},
        provenance={},
    )

    def mutate_then_archive(**kwargs: Any) -> dict[str, object]:
        paths["component"].write_bytes(paths["component"].read_bytes() + b"\n")
        return inline_workload_bootstrap(**kwargs)

    monkeypatch.setattr("dpone.gitops.airflow_compact_pack.inline_workload_bootstrap", mutate_then_archive)

    report = AirflowCompactPackBuilder().build(workload=workload, output_path="pack.json", repo_root=tmp_path)

    assert report.passed is False
    assert report.pod_spec == {}
    assert [blocker.code for blocker in report.blockers] == ["workload_dependency_changed_during_bootstrap"]
    assert report.blockers[0].path == "platform/recipes/components/mssql-clickhouse-load-1.0.0.yaml"


def test_sensitive_answers_fail_before_scaffold_writes(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    answers = tmp_path / "answers.yaml"
    _write_yaml(answers, {"password": "do-not-echo"})

    result = _scaffold(tmp_path, answers=answers)

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_ANSWERS_UNSAFE"
    assert result.errors[0]["schema"] == "dpone.error.v1"
    assert result.errors[0]["stage"] == "init_pipeline"
    assert result.errors[0]["docs_url"].endswith("DPONE_RECIPE_ANSWERS_UNSAFE.md")
    assert result.errors[0]["fixes"] == [
        {
            "id": "inspect_recipe_parameters",
            "safety": "manual",
            "command": "dpone recipe show governed-mssql-clickhouse@1.2.0",
        }
    ]
    assert result.exit_code == 4
    assert "do-not-echo" not in str(result.to_dict())
    assert not (tmp_path / "pipelines/orders_daily/pipeline.yaml").exists()


@pytest.mark.parametrize("unsafe_value", ("vault://prod/orders", "env://PASSWORD", "${TOKEN}"))
def test_credential_like_answer_values_are_redacted(tmp_path: Path, unsafe_value: str) -> None:
    _create_external_catalog(tmp_path)
    answers = tmp_path / "answers.yaml"
    _write_yaml(answers, {"source_table": unsafe_value})

    result = _scaffold(tmp_path, answers=answers)

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_ANSWERS_UNSAFE"
    assert unsafe_value not in json.dumps(result.to_dict())


def test_builtin_recipe_rejects_external_only_options_without_regression(tmp_path: Path) -> None:
    result = build_airflow_self_service_service(root=tmp_path).init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode="flow",
        profile="incremental-defaults@1.0.0",
        answers=None,
    )

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_OPTION_UNSUPPORTED"
    assert not (tmp_path / "pipelines/orders_daily/pipeline.yaml").exists()


def test_allowed_answer_overrides_component_without_leaking_answer_file(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    answers = tmp_path / "answers.yaml"
    _write_yaml(answers, {"source_table": "orders_archive"})

    result = _scaffold(tmp_path, answers=answers)
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    compilation = default_authoring_compiler().compile(source, source_path=source_path, project_root=tmp_path)

    assert result.passed is True
    assert source["recipe"]["parameters"] == {"source_table": "orders_archive"}
    assert compilation.processes[0]["source"]["table"]["name"] == "orders_archive"
    assert "answers.yaml" not in source_path.read_text(encoding="utf-8")


def test_locked_profile_parameter_is_not_overridable(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    answers = tmp_path / "answers.yaml"
    _write_yaml(answers, {"strategy": "full_refresh"})

    result = _scaffold(tmp_path, answers=answers)

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_OVERRIDE_FORBIDDEN"
    assert not (tmp_path / "pipelines/orders_daily/pipeline.yaml").exists()


def test_recipe_cli_discovers_pins_and_validates_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = _create_external_catalog(tmp_path)
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["recipe", "list", "--format", "json"], capsys)
    listed = json.loads(stdout)
    assert code == 0, stderr
    assert "governed-mssql-clickhouse@1.2.0" in {item["ref"] for item in listed["recipes"]}
    external = next(item for item in listed["recipes"] if item["ref"] == "governed-mssql-clickhouse@1.2.0")
    assert external["route_id"] == "mssql:clickhouse:incremental_merge"
    assert external["scaffoldable"] is True
    assert external["scaffold_argv"][-2:] == [
        "--recipe",
        "governed-mssql-clickhouse@1.2.0",
    ]

    code, stdout, stderr = _run_cli(
        ["recipe", "show", "governed-mssql-clickhouse@1.2.0", "--format", "json"],
        capsys,
    )
    shown = json.loads(stdout)
    assert code == 0, stderr
    assert shown["owner"] == "data-platform"
    assert {item["name"] for item in shown["parameters"]} >= {"source_table", "sink_connection_ref"}
    assert shown["source"] == "mssql"
    assert shown["sink"] == "clickhouse"
    assert shown["strategy"] == "incremental_merge"

    code, stdout, stderr = _run_cli(
        [
            "recipe",
            "list",
            "--source",
            "mssql",
            "--sink",
            "clickhouse",
            "--strategy",
            "incremental_merge",
            "--format",
            "json",
        ],
        capsys,
    )
    filtered = json.loads(stdout)
    assert code == 0, stderr
    assert "governed-mssql-clickhouse@1.2.0" in {item["ref"] for item in filtered["recipes"]}

    code, stdout, stderr = _run_cli(
        ["recipe", "pin", paths["component"].relative_to(tmp_path).as_posix(), "--format", "json"],
        capsys,
    )
    pinned = json.loads(stdout)
    assert code == 0, stderr
    assert pinned["kind"] == "component"
    assert pinned["sha256"] == _digest(paths["component"])

    code, stdout, stderr = _run_cli(["recipe", "validate", "--format", "json"], capsys)
    validated = json.loads(stdout)
    assert code == 0, stderr
    assert validated["status"] == "valid"
    assert validated["validated_artifacts"] == 3


def test_recipe_cli_uses_structured_error_contract_and_stable_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _create_external_catalog(tmp_path)
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["recipe", "show", "governed-mssql-clickhouse@latest", "--format", "json"],
        capsys,
    )

    payload = json.loads(stdout)
    assert code == 2, stderr
    assert payload["passed"] is False
    assert payload["errors"] == [
        {
            "schema": "dpone.error.v1",
            "code": "DPONE_RECIPE_REF_INVALID",
            "stage": "recipe_show",
            "severity": "error",
            "message": "recipe ref must use exact id@MAJOR.MINOR.PATCH syntax.",
            "fixes": [
                {
                    "id": "choose_exact_recipe_ref",
                    "safety": "manual",
                    "command": "dpone recipe list",
                }
            ],
            "entity": {"kind": "recipe", "id": "governed-mssql-clickhouse@latest"},
            "docs_url": "docs/errors/DPONE_RECIPE_REF_INVALID.md",
        }
    ]


@pytest.mark.parametrize("invalid_kind", ("profile_type", "unknown_context"))
def test_recipe_validate_uses_authoritative_parameter_and_placeholder_rules(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    invalid_kind: str,
) -> None:
    paths = _create_external_catalog(tmp_path)
    if invalid_kind == "profile_type":
        profile = yaml.safe_load(paths["profile"].read_text(encoding="utf-8"))
        profile["values"]["source_schema"] = 42
        _write_yaml(paths["profile"], profile)
    else:
        component = yaml.safe_load(paths["component"].read_text(encoding="utf-8"))
        component["processes"][0]["name"] = {"$context": "unknown"}
        _write_yaml(paths["component"], component)
    _refresh_recipe_closure_pins(paths)
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["recipe", "validate", "--format", "json"], capsys)

    payload = json.loads(stdout)
    assert code == 1, stderr
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] in {
        "DPONE_RECIPE_COMPONENT_INVALID",
        "DPONE_RECIPE_PARAMETER_SCHEMA_INVALID",
    }


def test_public_recipe_schemas_accept_the_executable_fixture(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed
    root = Path(__file__).resolve().parents[1]
    contracts = {
        "recipe-catalog.schema.json": yaml.safe_load(paths["catalog"].read_text(encoding="utf-8")),
        "recipe.schema.json": yaml.safe_load(paths["recipe"].read_text(encoding="utf-8")),
        "profile.schema.json": yaml.safe_load(paths["profile"].read_text(encoding="utf-8")),
        "component.schema.json": yaml.safe_load(paths["component"].read_text(encoding="utf-8")),
        "etl-flow-manifest.schema.json": yaml.safe_load(
            (tmp_path / "pipelines/orders_daily/pipeline.yaml").read_text(encoding="utf-8")
        ),
    }
    for filename, instance in contracts.items():
        schema = json.loads((root / "src/dpone/schema" / filename).read_text(encoding="utf-8"))
        Draft7Validator.check_schema(schema)
        Draft7Validator(schema).validate(instance)


def test_public_recipe_schemas_reject_runtime_invalid_parameter_and_component_contracts(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    root = Path(__file__).resolve().parents[1]
    recipe = yaml.safe_load(paths["recipe"].read_text(encoding="utf-8"))
    recipe["parameter_schema"]["properties"]["source_schema"]["pattern"] = ".*"
    component = yaml.safe_load(paths["component"].read_text(encoding="utf-8"))
    component["processes"][0]["command"] = "curl example.invalid"

    recipe_errors = tuple(
        Draft7Validator(json.loads((root / "src/dpone/schema/recipe.schema.json").read_text())).iter_errors(recipe)
    )
    component_errors = tuple(
        Draft7Validator(json.loads((root / "src/dpone/schema/component.schema.json").read_text())).iter_errors(
            component
        )
    )

    assert recipe_errors
    assert component_errors


def test_public_flow_schema_rejects_invalid_recipe_domain(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed
    source = yaml.safe_load((tmp_path / "pipelines/orders_daily/pipeline.yaml").read_text(encoding="utf-8"))
    source["metadata"]["domain"] = "Sales/Unsafe"
    root = Path(__file__).resolve().parents[1]
    schema = json.loads((root / "src/dpone/schema/etl-flow-manifest.schema.json").read_text(encoding="utf-8"))

    assert tuple(Draft7Validator(schema).iter_errors(source))


def test_compact_pack_executes_materialized_canonical_ir_not_recipe_dsl(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed
    workload = GitOpsWorkloadDefinition(
        workload_id="orders_daily",
        manifest="pipelines/orders_daily/pipeline.yaml",
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "registry.example/dpone:dev", "airflow": {}},
        provenance={},
    )

    payload = (
        AirflowCompactPackBuilder()
        .build(
            workload=workload,
            output_path="packs/orders_daily.airflow-pack.json",
            repo_root=tmp_path,
        )
        .to_jsonable()
    )

    runtime_manifest = payload["runtime_manifest"]
    assert runtime_manifest["kind"] == "canonical_manifest"
    assert runtime_manifest["path"].endswith(".orders_daily.dpone.batch.json")
    assert Path(runtime_manifest["path"]).parent == Path(workload.manifest).parent
    assert runtime_manifest["path"] in payload["runtime_command"]
    assert "dpone run pipelines/orders_daily/pipeline.yaml" not in payload["runtime_command"]
    init_script = payload["pod_spec"]["spec"]["initContainers"][0]["args"][0]
    archive_b64 = (
        init_script.split("<<'DPONE_INLINE_WORKLOAD_TAR_EOF' | tar -xz -C /workspace/repo\n", 1)[1]
        .split("\nDPONE_INLINE_WORKLOAD_TAR_EOF", 1)[0]
        .strip()
    )
    with tarfile.open(fileobj=io.BytesIO(base64.b64decode(archive_b64)), mode="r:gz") as archive:
        canonical = json.loads(archive.extractfile(runtime_manifest["path"]).read())
    assert canonical["kind"] == "dpone.batch.v1"
    assert "recipe" not in canonical
    assert canonical["schemas"]["dbo"]["tables"][0]["id"] == "orders_daily"


def test_materialized_canonical_ir_and_inline_archive_are_byte_deterministic(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed
    workload = GitOpsWorkloadDefinition(
        workload_id="orders_daily",
        manifest="pipelines/orders_daily/pipeline.yaml",
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "registry.example/dpone:dev", "airflow": {}},
        provenance={},
    )
    builder = AirflowCompactPackBuilder()

    first = builder.build(workload=workload, output_path="pack.json", repo_root=tmp_path).to_jsonable()
    second = builder.build(workload=workload, output_path="pack.json", repo_root=tmp_path).to_jsonable()

    assert first["runtime_manifest"] == second["runtime_manifest"]
    assert first["runtime_manifest"]["sha256"].startswith("sha256:")
    assert (
        first["pod_spec"]["spec"]["initContainers"][0]["args"]
        == second["pod_spec"]["spec"]["initContainers"][0]["args"]
    )


@pytest.mark.parametrize("generated_path", ("../escape.json", "/tmp/escape.json", "dir\\escape.json"))
def test_inline_runtime_manifest_rejects_unsafe_generated_path(tmp_path: Path, generated_path: str) -> None:
    (tmp_path / "pipeline.yaml").write_text("kind: dpone.batch.v1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="inline_workload_path_unsafe"):
        inline_workload_archive(
            repo_root=tmp_path,
            paths=("pipeline.yaml",),
            generated_files={generated_path: b"{}\n"},
        )


def test_compact_pack_without_repo_root_is_non_runnable_and_does_not_invent_a_digest() -> None:
    workload = GitOpsWorkloadDefinition(
        workload_id="orders_daily",
        manifest="pipelines/orders_daily/pipeline.yaml",
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "registry.example/dpone:dev", "airflow": {}},
        provenance={},
    )

    payload = AirflowCompactPackBuilder().build(workload=workload, output_path="pack.json").to_jsonable()

    assert payload["runtime_manifest"] == {
        "kind": "unmaterialized_manifest",
        "path": "pipelines/orders_daily/pipeline.yaml",
        "sha256": None,
    }
    assert payload["runtime_command"] == ""
    assert [item["code"] for item in payload["blockers"]] == ["runtime_manifest_repo_root_required"]


def test_catalog_rejects_duplicate_recipe_identity(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    catalog = yaml.safe_load(paths["catalog"].read_text(encoding="utf-8"))
    catalog["artifacts"].append(dict(catalog["artifacts"][0]))
    _write_yaml(paths["catalog"], catalog)

    result = _scaffold(tmp_path)

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_CATALOG_INVALID"
    assert not (tmp_path / "pipelines/orders_daily/pipeline.yaml").exists()


def test_untrusted_catalog_is_a_safety_violation(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    project = yaml.safe_load((tmp_path / "dpone.yaml").read_text(encoding="utf-8"))
    project["authoring"]["recipe_catalog"]["trusted_catalog_ids"] = ["other-platform"]
    _write_yaml(tmp_path / "dpone.yaml", project)

    result = _scaffold(tmp_path)

    assert result.passed is False
    assert result.exit_code == 4
    assert result.errors[0]["code"] == "DPONE_RECIPE_CATALOG_UNTRUSTED"


def test_symlinked_recipe_artifact_is_rejected(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    original = paths["component"].read_bytes()
    external = tmp_path.parent / f"{tmp_path.name}-component.yaml"
    external.write_bytes(original)
    paths["component"].unlink()
    paths["component"].symlink_to(external)

    result = _scaffold(tmp_path)

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_ARTIFACT_INVALID"


def test_recipe_pin_rejects_symlink_even_when_target_stays_inside_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = _create_external_catalog(tmp_path)
    link = tmp_path / "platform/recipes/components/linked.yaml"
    link.symlink_to(paths["component"])
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["recipe", "pin", link.relative_to(tmp_path).as_posix(), "--format", "json"],
        capsys,
    )

    payload = json.loads(stdout)
    assert code == 1, stderr
    assert payload["errors"][0]["code"] == "DPONE_RECIPE_ARTIFACT_INVALID"


def test_yaml_alias_in_component_is_rejected_before_expansion(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    paths["component"].write_text(
        "schema: dpone.component.v1\n"
        "id: mssql-clickhouse-load\n"
        "version: 1.0.0\n"
        "owner: data-platform\n"
        "status: stable\n"
        "processes: &items\n  - name: orders_daily\n    source: {type: mssql}\n    sink: {type: clickhouse}\n"
        "duplicate: *items\n",
        encoding="utf-8",
    )
    _refresh_recipe_closure_pins(paths)

    result = _scaffold(tmp_path)

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_ARTIFACT_INVALID"


@pytest.mark.parametrize(
    "unsafe_value",
    (
        {"command": "curl example.invalid"},
        {"query": "{{ unsafe }}"},
        {"$param": "undeclared_parameter"},
    ),
)
def test_component_rejects_executable_or_unknown_placeholders(tmp_path: Path, unsafe_value: object) -> None:
    paths = _create_external_catalog(tmp_path)
    component = yaml.safe_load(paths["component"].read_text(encoding="utf-8"))
    component["processes"][0]["unsafe"] = unsafe_value
    _write_yaml(paths["component"], component)
    _refresh_recipe_closure_pins(paths)

    result = _scaffold(tmp_path)

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_COMPONENT_INVALID"


def test_profile_value_must_match_recipe_parameter_type(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    profile = yaml.safe_load(paths["profile"].read_text(encoding="utf-8"))
    profile["values"]["source_schema"] = 42
    _write_yaml(paths["profile"], profile)
    _refresh_recipe_closure_pins(paths)

    result = _scaffold(tmp_path)

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_PARAMETER_SCHEMA_INVALID"


def test_recipe_source_rejects_processes_and_domain_mismatch(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    assert _scaffold(tmp_path).passed
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    source["processes"] = []

    with pytest.raises(AuthoringCompilationError) as ambiguity:
        default_authoring_compiler().compile(source, source_path=source_path, project_root=tmp_path)
    assert ambiguity.value.code == "DPONE_AUTHORING_SOURCE_AMBIGUOUS"

    source.pop("processes")
    source["metadata"]["domain"] = "finance"
    with pytest.raises(AuthoringCompilationError) as mismatch:
        default_authoring_compiler().compile(source, source_path=source_path, project_root=tmp_path)
    assert mismatch.value.code == "DPONE_RECIPE_ARTIFACT_INVALID"


def test_domain_first_external_recipe_rejects_domain_override_before_writes(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)
    project = yaml.safe_load((tmp_path / "dpone.yaml").read_text(encoding="utf-8"))
    project["layout"] = {
        "mode": "domain_first",
        "root": "workloads",
        "pipeline_id_scope": "project",
    }
    project["airflow"] = {"enabled": True}
    _write_yaml(tmp_path / "dpone.yaml", project)
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="governed-mssql-clickhouse@1.2.0",
        domain="crm",
        airflow=True,
        authoring_mode="flow",
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_PIPELINE_DOMAIN_MISMATCH"
    assert result.errors[0]["entity"] == {"kind": "pipeline", "id": "orders_daily"}
    assert not (tmp_path / "workloads/crm/pipelines/orders_daily").exists()


def test_domain_first_external_recipe_domain_mismatch_precedes_ownership_preflight(
    tmp_path: Path,
) -> None:
    _create_external_catalog(tmp_path)
    project = yaml.safe_load((tmp_path / "dpone.yaml").read_text(encoding="utf-8"))
    project["layout"] = {
        "mode": "domain_first",
        "root": "workloads",
        "pipeline_id_scope": "project",
    }
    project["airflow"] = {"enabled": True}
    _write_yaml(tmp_path / "dpone.yaml", project)

    result = build_airflow_self_service_service(root=tmp_path).init_pipeline(
        pipeline_id="orders_daily",
        recipe="governed-mssql-clickhouse@1.2.0",
        domain="crm",
        airflow=True,
        authoring_mode="flow",
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_PIPELINE_DOMAIN_MISMATCH"
    assert not (tmp_path / "workloads").exists()


@pytest.mark.parametrize("mutated_input", ("answers", "catalog"))
def test_domain_first_external_recipe_inputs_are_pinned_through_scaffold_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutated_input: str,
) -> None:
    paths = _create_external_catalog(tmp_path)
    project = yaml.safe_load((tmp_path / "dpone.yaml").read_text(encoding="utf-8"))
    project["layout"] = {
        "mode": "domain_first",
        "root": "workloads",
        "pipeline_id_scope": "project",
    }
    project["airflow"] = {"enabled": True}
    _write_yaml(tmp_path / "dpone.yaml", project)
    answers = tmp_path / "answers.yaml"
    _write_yaml(answers, {"source_table": "orders"})
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_domain(
        domain="sales",
        owner_team="data-sales",
        owner_contact="sales@example.com",
        approver_team="data-platform",
    ).passed
    original_resolve = RecipeCatalogService.resolve_for_scaffold

    def resolve_then_mutate(catalog_service: RecipeCatalogService, **kwargs: object):
        resolution = original_resolve(catalog_service, **kwargs)
        path = answers if mutated_input == "answers" else paths["catalog"]
        path.write_bytes(path.read_bytes() + b"\n")
        return resolution

    monkeypatch.setattr(RecipeCatalogService, "resolve_for_scaffold", resolve_then_mutate)

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="governed-mssql-clickhouse@1.2.0",
        domain="sales",
        airflow=True,
        authoring_mode="flow",
        answers=answers,
    )

    assert not result.passed
    assert any(change.action == "conflict" and change.path == "project-authority" for change in result.changes)
    assert not (tmp_path / "workloads/sales/pipelines/orders_daily").exists()


def test_deprecated_artifact_emits_deterministic_compile_warning(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    component = yaml.safe_load(paths["component"].read_text(encoding="utf-8"))
    component["status"] = "deprecated"
    _write_yaml(paths["component"], component)
    _refresh_recipe_closure_pins(paths)
    assert _scaffold(tmp_path).passed
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))

    compilation = default_authoring_compiler().compile(source, source_path=source_path, project_root=tmp_path)

    assert compilation.deprecated_aliases == ("DPONE_RECIPE_ARTIFACT_DEPRECATED:component:mssql-clickhouse-load@1.0.0",)
    assert compilation.recipe_provenance["deprecated_refs"] == ["component:mssql-clickhouse-load@1.0.0"]


def test_recipe_scaffold_is_idempotent_and_fingerprints_are_deterministic(tmp_path: Path) -> None:
    _create_external_catalog(tmp_path)

    first = _scaffold(tmp_path)
    first_preview = build_airflow_self_service_service(root=tmp_path).preview("orders_daily")
    second = _scaffold(tmp_path)
    second_preview = build_airflow_self_service_service(root=tmp_path).preview("orders_daily")

    assert first.passed and second.passed
    assert {change.action for change in second.changes} == {"no_op"}
    assert first_preview.details["release"]["release_id"] == second_preview.details["release"]["release_id"]
    assert (
        first_preview.details["release"]["provenance"]["semantic_fingerprint"]
        == second_preview.details["release"]["provenance"]["semantic_fingerprint"]
    )


def test_recipe_static_paths_do_not_call_network_or_import_runtime_integrations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _create_external_catalog(tmp_path)
    forbidden_imports = ("airflow", "hvac", "vault_kv_client", "kubernetes")
    imported: list[str] = []
    original_import = builtins.__import__

    def guarded_import(name: str, *args: object, **kwargs: object) -> object:
        if name in forbidden_imports or name.startswith(tuple(f"{item}." for item in forbidden_imports)):
            imported.append(name)
            raise AssertionError(f"runtime integration imported during recipe compilation: {name}")
        return original_import(name, *args, **kwargs)

    def blocked_network(*args: object, **kwargs: object) -> object:
        raise AssertionError("network called during recipe compilation")

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(socket, "create_connection", blocked_network)
    monkeypatch.setattr(socket.socket, "connect", blocked_network)

    result = _scaffold(tmp_path)
    check = build_airflow_self_service_service(root=tmp_path).check(tmp_path / "pipelines/orders_daily")
    preview = build_airflow_self_service_service(root=tmp_path).preview("orders_daily")

    assert result.passed and check.passed and preview.passed
    assert imported == []
