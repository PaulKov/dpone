from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from dpone.commands.airflow_self_service_rendering import self_service_check_text
from dpone.contracts.credential_env import CONNECTION_REF_PATTERN
from dpone.gitops.workload_dependencies import WorkloadDependencyError, WorkloadDependencyResolver
from dpone.manifest.authoring import AuthoringCompilation, AuthoringCompilationError, AuthoringCompiler
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.manifest.migration_scanner import _scan_legacy_manifests
from dpone.readiness.airflow_authoring_dependency_integrity import (
    AuthoringDependencyIntegrityError,
    verify_authoring_dependency_parity,
)
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from tests.mssql_asset_registry_fixtures import write_mssql_connection_registry


def _process() -> dict[str, object]:
    return {
        "name": "orders_daily",
        "source": {
            "type": "mssql",
            "connection_ref": "mssql_dev",
            "table": {"schema": "dbo", "name": "orders"},
        },
        "sink": {
            "type": "clickhouse",
            "connection_ref": "clickhouse_dev",
            "table": {"schema": "analytics", "name": "orders"},
            "strategy": {"mode": "incremental_merge", "unique_key": "id"},
        },
    }


def _flow(path: Path) -> dict[str, object]:
    return {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": path.as_posix()},
        "metadata": {"id": "orders_daily", "domain": "sales", "tags": ["airflow", "dpone"]},
        "processes": [_process()],
    }


def _classic(path: Path) -> dict[str, object]:
    return {
        "kind": "dpone.batch.v1",
        "authoring": {"mode": "classic", "source": path.as_posix()},
        "metadata": {"id": "orders_daily", "domain": "sales", "tags": ["dpone", "airflow"]},
        "defaults": {},
        "schemas": {
            "dbo": {
                "tables": [
                    {
                        "table": "orders",
                        "id": "orders_daily",
                        "overrides": _process(),
                    }
                ]
            }
        },
    }


def test_classic_and_flow_compile_to_the_same_semantic_fingerprint(tmp_path: Path) -> None:
    path = Path("pipelines/orders_daily/pipeline.yaml")
    compiler = AuthoringCompiler()

    flow = compiler.compile(_flow(path), source_path=tmp_path / path, project_root=tmp_path)
    classic = compiler.compile(_classic(path), source_path=tmp_path / path, project_root=tmp_path)

    assert flow.source_kind == "dpone.flow.v1"
    assert flow.canonical_kind == "dpone.batch.v1"
    assert flow.semantic_fingerprint == classic.semantic_fingerprint
    assert flow.source_fingerprint != classic.source_fingerprint
    assert flow.deprecated_aliases == ()
    assert flow.processes == classic.processes


def test_flow_compilation_pins_declared_sql_file(tmp_path: Path) -> None:
    relative_path = Path("pipelines/orders_daily/pipeline.yaml")
    payload = _flow(relative_path)
    process = payload["processes"][0]
    assert isinstance(process, dict)
    source = process["source"]
    assert isinstance(source, dict)
    source["query"] = {"sql_file": "query.sql"}
    sql_file = tmp_path / "pipelines/orders_daily/query.sql"
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("SELECT 1\n", encoding="utf-8")

    compilation = AuthoringCompiler().compile(
        payload,
        source_path=tmp_path / relative_path,
        project_root=tmp_path,
    )

    assert [(dependency.kind, dependency.path) for dependency in compilation.dependencies] == [
        ("sql_file", "pipelines/orders_daily/query.sql")
    ]


@pytest.mark.parametrize("authoring_mode", ["flow", "classic"])
def test_authoring_parity_rejects_sql_change_for_single_file_modes(tmp_path: Path, authoring_mode: str) -> None:
    relative_path = Path("pipelines/orders_daily/pipeline.yaml")
    payload = _flow(relative_path) if authoring_mode == "flow" else _classic(relative_path)
    if authoring_mode == "flow":
        process = payload["processes"][0]
    else:
        process = payload["schemas"]["dbo"]["tables"][0]["overrides"]
    assert isinstance(process, dict)
    source = process["source"]
    assert isinstance(source, dict)
    source["query"] = {"sql_file": "query.sql"}
    source_path = tmp_path / relative_path
    source_path.parent.mkdir(parents=True)
    source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    sql_file = source_path.parent / "query.sql"
    sql_file.write_text("SELECT 1\n", encoding="utf-8")
    compilation = AuthoringCompiler().compile(payload, source_path=source_path, project_root=tmp_path)

    sql_file.write_text("SELECT 2\n", encoding="utf-8")
    changed = AuthoringCompiler().compile(payload, source_path=source_path, project_root=tmp_path)
    dependencies = WorkloadDependencyResolver().resolve(repo_root=tmp_path, manifest=relative_path.as_posix())

    assert changed.source_fingerprint != compilation.source_fingerprint
    with pytest.raises(AuthoringDependencyIntegrityError):
        verify_authoring_dependency_parity(
            expected=compilation.dependencies,
            pack={"workload_dependencies": [dependency.to_jsonable() for dependency in dependencies]},
        )


def test_workload_dependency_resolver_rejects_symlinked_primary_manifest(tmp_path: Path) -> None:
    source_dir = tmp_path / "pipelines/orders_daily"
    source_dir.mkdir(parents=True)
    target = source_dir / "real.yaml"
    target.write_text(yaml.safe_dump(_flow(Path("pipelines/orders_daily/pipeline.yaml"))), encoding="utf-8")
    (source_dir / "pipeline.yaml").symlink_to(target)

    with pytest.raises(ValueError, match="manifest_read_failed"):
        WorkloadDependencyResolver().resolve(
            repo_root=tmp_path,
            manifest="pipelines/orders_daily/pipeline.yaml",
        )


def test_workload_dependency_resolver_translates_missing_sql_file(tmp_path: Path) -> None:
    relative_path = Path("pipelines/orders_daily/pipeline.yaml")
    payload = _flow(relative_path)
    source = payload["processes"][0]["source"]
    assert isinstance(source, dict)
    source["query"] = {"sql_file": "missing.sql"}
    path = tmp_path / relative_path
    path.parent.mkdir(parents=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(WorkloadDependencyError, match="sql_file_read_failed"):
        WorkloadDependencyResolver().resolve(repo_root=tmp_path, manifest=relative_path.as_posix())


def test_manifest_router_loads_flow_through_the_canonical_batch_compiler(tmp_path: Path) -> None:
    path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    path.parent.mkdir(parents=True)
    payload = _flow(Path("pipelines/orders_daily/pipeline.yaml"))
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    loaded = ManifestLoaderRouter().load(path, metadata_only=True)

    assert loaded.kind == "dpone.batch.v1"
    assert loaded.source_kind == "dpone.flow.v1"
    assert [process.name for process in loaded.processes] == ["orders_daily"]
    assert [process.selector for process in loaded.processes] == ["orders_daily"]


def test_released_batch_processes_shape_is_a_deprecated_read_alias(tmp_path: Path) -> None:
    path = Path("pipelines/orders_daily/pipeline.yaml")
    payload = _flow(path)
    payload["kind"] = "dpone.batch.v1"
    payload["authoring"] = {"mode": "classic", "source": path.as_posix()}

    result = AuthoringCompiler().compile(payload, source_path=tmp_path / path, project_root=tmp_path)

    assert result.canonical_kind == "dpone.batch.v1"
    assert result.deprecated_aliases == ("DPONE_LEGACY_SELF_SERVICE_BATCH_PROCESSES",)


def test_manifest_router_loads_released_batch_processes_alias(tmp_path: Path) -> None:
    relative_path = Path("pipelines/orders_daily/pipeline.yaml")
    path = tmp_path / relative_path
    path.parent.mkdir(parents=True)
    payload = _flow(relative_path)
    payload["kind"] = "dpone.batch.v1"
    payload["authoring"] = {"mode": "classic", "source": relative_path.as_posix()}
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    loaded = ManifestLoaderRouter().load(path, metadata_only=True)

    assert loaded.kind == "dpone.batch.v1"
    assert [process.name for process in loaded.processes] == ["orders_daily"]


def test_released_pipeline_v1_shape_remains_readable(tmp_path: Path) -> None:
    relative_path = Path("pipelines/orders_daily/pipeline.yaml")
    path = tmp_path / relative_path
    path.parent.mkdir(parents=True)
    payload = {"schema": "dpone.pipeline.v1", "metadata": {"id": "orders_daily"}, "processes": [_process()]}
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    compilation = AuthoringCompiler().compile(payload, source_path=path, project_root=tmp_path)
    loaded = ManifestLoaderRouter().load(path, metadata_only=True)

    assert compilation.source_kind == "dpone.pipeline.v1"
    assert compilation.deprecated_aliases == ("DPONE_LEGACY_SELF_SERVICE_PIPELINE_V1",)
    assert loaded.kind == "dpone.batch.v1"
    assert loaded.source_kind == "dpone.pipeline.v1"
    assert [process.name for process in loaded.processes] == ["orders_daily"]


def test_authoring_compiler_rejects_ambiguous_batch_source(tmp_path: Path) -> None:
    path = Path("pipelines/orders_daily/pipeline.yaml")
    payload = _classic(path)
    payload["processes"] = [_process()]

    with pytest.raises(AuthoringCompilationError) as exc_info:
        AuthoringCompiler().compile(payload, source_path=tmp_path / path, project_root=tmp_path)

    assert exc_info.value.code == "DPONE_AUTHORING_SOURCE_AMBIGUOUS"


def test_new_classic_scaffold_is_accepted_by_the_canonical_manifest_loader(tmp_path: Path) -> None:
    result = build_airflow_self_service_service(root=tmp_path).init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode="classic",
    )
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"

    assert result.passed is True
    loaded = ManifestLoaderRouter().load(source_path, metadata_only=True)
    assert loaded.kind == "dpone.batch.v1"
    assert [process.name for process in loaded.processes] == ["orders_daily"]


def test_flow_scaffold_uses_the_public_flow_kind(tmp_path: Path) -> None:
    result = build_airflow_self_service_service(root=tmp_path).init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode="flow",
    )

    assert result.passed is True
    payload = yaml.safe_load((tmp_path / "pipelines/orders_daily/pipeline.yaml").read_text(encoding="utf-8"))
    assert payload["kind"] == "dpone.flow.v1"
    assert payload["authoring"]["mode"] == "flow"


def test_static_check_reports_authoring_identity(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode="flow",
    )

    result = service.check("pipelines/orders_daily")

    assert result.passed is True
    assert result.details["authoring_mode"] == "flow"
    assert result.details["source_kind"] == "dpone.flow.v1"
    assert result.details["canonical_kind"] == "dpone.batch.v1"
    assert str(result.details["source_fingerprint"]).startswith("sha256:")
    assert str(result.details["semantic_fingerprint"]).startswith("sha256:")
    assert result.details["deprecated_aliases"] == []


def test_self_service_uses_the_injected_authoring_compiler(tmp_path: Path) -> None:
    compiler = Mock(wraps=AuthoringCompiler())
    service = build_airflow_self_service_service(root=tmp_path, authoring_compiler=compiler)
    service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    )

    result = service.check("orders_daily")

    assert result.passed is True
    assert compiler.compile.call_count == 1


def test_preview_release_records_authoring_provenance(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode="flow",
    )
    write_mssql_connection_registry(tmp_path, include=("mssql_dev",))

    result = service.preview("orders_daily")

    assert result.passed is True
    release = result.details["release"]
    provenance = release["provenance"]
    assert provenance["authoring_mode"] == "flow"
    assert str(provenance["source_fingerprint"]).startswith("sha256:")
    assert str(provenance["semantic_fingerprint"]).startswith("sha256:")
    assert provenance["deprecated_aliases"] == []


def test_default_check_text_surfaces_legacy_authoring_alias() -> None:
    rendered = self_service_check_text(
        {
            "passed": True,
            "mode": "static",
            "network": False,
            "secrets": False,
            "source_queries": False,
            "deprecated_aliases": ["DPONE_LEGACY_SELF_SERVICE_PIPELINE_V1"],
        },
        target="pipelines/orders_daily",
    )

    assert "deprecated input: DPONE_LEGACY_SELF_SERVICE_PIPELINE_V1" in rendered


def test_kind_and_authoring_mode_must_agree(tmp_path: Path) -> None:
    path = Path("pipelines/orders_daily/pipeline.yaml")
    payload = _flow(path)
    payload["authoring"] = {"mode": "classic", "source": path.as_posix()}

    with pytest.raises(AuthoringCompilationError) as exc_info:
        AuthoringCompiler().compile(payload, source_path=tmp_path / path, project_root=tmp_path)

    assert exc_info.value.code == "DPONE_AUTHORING_MODE_KIND_MISMATCH"


def test_authoring_source_must_match_the_primary_file(tmp_path: Path) -> None:
    path = Path("pipelines/orders_daily/pipeline.yaml")
    payload = _flow(path)
    payload["authoring"] = {"mode": "flow", "source": "pipelines/other/pipeline.yaml"}

    with pytest.raises(AuthoringCompilationError) as exc_info:
        AuthoringCompiler().compile(payload, source_path=tmp_path / path, project_root=tmp_path)

    assert exc_info.value.code == "DPONE_AUTHORING_SOURCE_PATH_MISMATCH"


def test_authoring_source_without_explicit_root_uses_strict_path_reconstruction(tmp_path: Path) -> None:
    actual_relative = Path("pipelines/orders_daily/pipeline.yaml")
    payload = _flow(actual_relative)
    compiler = AuthoringCompiler()

    result = compiler.compile(payload, source_path=tmp_path / actual_relative)
    payload["authoring"] = {
        "mode": "flow",
        "source": "other/pipelines/orders_daily/pipeline.yaml",
    }

    assert result.source_kind == "dpone.flow.v1"
    with pytest.raises(AuthoringCompilationError) as exc_info:
        compiler.compile(payload, source_path=tmp_path / actual_relative)
    assert exc_info.value.code == "DPONE_AUTHORING_SOURCE_PATH_MISMATCH"


def test_flow_and_classic_scaffolds_validate_against_public_schemas(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    flow_root = tmp_path / "flow"
    classic_root = tmp_path / "classic"
    build_airflow_self_service_service(root=flow_root).init_pipeline(
        pipeline_id="orders_flow",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode="flow",
    )
    build_airflow_self_service_service(root=classic_root).init_pipeline(
        pipeline_id="orders_classic",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode="classic",
    )
    root = Path(__file__).parents[1]
    flow_schema = json.loads((root / "src/dpone/schema/etl-flow-manifest.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads((root / "src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))
    flow = yaml.safe_load((flow_root / "pipelines/orders_flow/pipeline.yaml").read_text(encoding="utf-8"))
    classic = yaml.safe_load((classic_root / "pipelines/orders_classic/pipeline.yaml").read_text(encoding="utf-8"))

    jsonschema.validate(flow, flow_schema)
    jsonschema.validate(classic, batch_schema)


def test_authoring_schemas_use_the_runtime_connection_ref_contract() -> None:
    root = Path(__file__).parents[1]
    flow_schema = json.loads((root / "src/dpone/schema/etl-flow-manifest.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads((root / "src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))

    assert flow_schema["definitions"]["endpoint"]["properties"]["connection_ref"]["pattern"] == (CONNECTION_REF_PATTERN)
    process_fragment = batch_schema["definitions"]["process_fragment"]["properties"]
    assert process_fragment["source"]["properties"]["connection_ref"]["pattern"] == CONNECTION_REF_PATTERN
    assert process_fragment["sink"]["properties"]["connection_ref"]["pattern"] == CONNECTION_REF_PATTERN


def test_batch_schema_rejects_read_only_processes_compatibility_alias() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    root = Path(__file__).parents[1]
    schema = json.loads((root / "src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))
    path = Path("pipelines/orders_daily/pipeline.yaml")
    payload = _flow(path)
    payload["kind"] = "dpone.batch.v1"
    payload["authoring"] = {"mode": "classic", "source": path.as_posix()}

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, schema)
    assert AuthoringCompiler().compile(payload, source_path=path).deprecated_aliases == (
        "DPONE_LEGACY_SELF_SERVICE_BATCH_PROCESSES",
    )


def test_legacy_batch_migration_skips_flow_authoring(tmp_path: Path) -> None:
    relative_path = Path("pipelines/orders_daily/pipeline.yaml")
    path = tmp_path / relative_path
    path.parent.mkdir(parents=True)
    path.write_text(yaml.safe_dump(_flow(relative_path), sort_keys=False), encoding="utf-8")

    assert list(_scan_legacy_manifests(path, recursive=False)) == []


def test_folder_mode_requires_explicit_fragments_without_generated_artifacts(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    init_result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode="folder",
    )
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    payload = _flow(Path("pipelines/orders_daily/pipeline.yaml"))
    payload["authoring"] = {"mode": "folder", "source": "pipelines/orders_daily/pipeline.yaml"}
    source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    check_result = service.check("orders_daily")
    preview_result = service.preview("orders_daily")

    assert init_result.passed is True
    assert check_result.errors[0]["code"] == "DPONE_AUTHORING_FOLDER_FRAGMENTS_INVALID"
    assert preview_result.errors[0]["code"] == "DPONE_AUTHORING_FOLDER_FRAGMENTS_INVALID"
    assert not (tmp_path / ".dpone-cache").exists()


def test_authoring_compilation_preserves_legacy_positional_optional_fields() -> None:
    compilation = AuthoringCompilation(
        "classic",
        "dpone.batch.v1",
        "dpone.batch.v1",
        {},
        (),
        "sha256:source",
        "sha256:semantic",
        ("legacy_alias",),
        (),
        None,
    )

    assert compilation.deprecated_aliases == ("legacy_alias",)
    assert compilation.pipeline_id is None
