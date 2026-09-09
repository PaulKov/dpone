from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from dpone.gitops.workload_dependencies import WorkloadDependencyError, WorkloadDependencyResolver
from dpone.manifest import authoring_folder, confined_files
from dpone.manifest.authoring import AuthoringCompilationError, AuthoringCompiler, default_authoring_compiler
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness.airflow_authoring_dependency_integrity import (
    AuthoringDependencyIntegrityError,
    source_file_provenance,
    verify_authoring_dependency_parity,
)
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.services.safe_sample_policy import load_pipeline_source_snapshot_from_file
from tests.mssql_asset_registry_fixtures import write_mssql_connection_registry

_AUTHORING_ERROR_CODES = (
    "DPONE_AUTHORING_COMPILATION_FAILED",
    "DPONE_AUTHORING_DEPENDENCY_READ_FAILED",
    "DPONE_AUTHORING_FOLDER_FRAGMENTS_INVALID",
    "DPONE_AUTHORING_FOLDER_FRAGMENT_DUPLICATE",
    "DPONE_AUTHORING_FOLDER_FRAGMENT_INVALID",
    "DPONE_AUTHORING_FOLDER_FRAGMENT_LIMIT_EXCEEDED",
    "DPONE_AUTHORING_FOLDER_FRAGMENT_NOT_FOUND",
    "DPONE_AUTHORING_FOLDER_FRAGMENT_TOO_LARGE",
    "DPONE_AUTHORING_FOLDER_LOADER_UNAVAILABLE",
    "DPONE_AUTHORING_FOLDER_PATH_INVALID",
    "DPONE_AUTHORING_FOLDER_SYMLINK_FORBIDDEN",
    "DPONE_AUTHORING_FOLDER_TOTAL_SIZE_EXCEEDED",
    "DPONE_AUTHORING_FOLDER_YAML_ALIAS_FORBIDDEN",
    "DPONE_AUTHORING_FOLDER_YAML_BUDGET_EXCEEDED",
    "DPONE_AUTHORING_MODE_KIND_MISMATCH",
    "DPONE_AUTHORING_PACK_BUILD_FAILED",
    "DPONE_AUTHORING_SOURCE_AMBIGUOUS",
    "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD",
    "DPONE_AUTHORING_SOURCE_INVALID",
    "DPONE_AUTHORING_SOURCE_PATH_INVALID",
    "DPONE_AUTHORING_SOURCE_PATH_MISMATCH",
    "DPONE_AUTHORING_STRUCTURE_INVALID",
    "DPONE_PIPELINE_PROCESS_INVALID",
    "DPONE_PIPELINE_PROCESS_MISSING",
)


def _process(name: str = "orders_daily") -> dict[str, object]:
    return {
        "name": name,
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


def _folder_root() -> dict[str, object]:
    return {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "folder", "source": "pipelines/orders_daily/pipeline.yaml"},
        "metadata": {"id": "orders_daily", "domain": "sales", "tags": ["dpone", "airflow"]},
        "fragments": ["steps/load.yaml"],
    }


def _flow_root() -> dict[str, object]:
    root = _folder_root()
    root["authoring"] = {"mode": "flow", "source": "pipelines/orders_daily/pipeline.yaml"}
    root.pop("fragments")
    root["processes"] = [_process()]
    return root


def _write_folder(root: Path, *, fragment: dict[str, object] | None = None) -> Path:
    source_path = root / "pipelines/orders_daily/pipeline.yaml"
    fragment_path = source_path.parent / "steps/load.yaml"
    fragment_path.parent.mkdir(parents=True)
    source_path.write_text(yaml.safe_dump(_folder_root(), sort_keys=False), encoding="utf-8")
    fragment_path.write_text(
        yaml.safe_dump(fragment or {"kind": "dpone.flow-fragment.v1", "processes": [_process()]}, sort_keys=False),
        encoding="utf-8",
    )
    return source_path


def test_folder_and_flow_have_the_same_semantic_fingerprint(tmp_path: Path) -> None:
    source_path = _write_folder(tmp_path)
    compiler = default_authoring_compiler()

    folder = compiler.compile(_folder_root(), source_path=source_path, project_root=tmp_path)
    flow = AuthoringCompiler().compile(_flow_root(), source_path=source_path, project_root=tmp_path)

    assert folder.authoring_mode == "folder"
    assert folder.semantic_fingerprint == flow.semantic_fingerprint
    assert folder.source_fingerprint != flow.source_fingerprint
    assert [dependency.path for dependency in folder.dependencies] == ["pipelines/orders_daily/steps/load.yaml"]
    assert all(dependency.sha256.startswith("sha256:") for dependency in folder.dependencies)


def test_folder_loader_reads_only_explicit_fragments(tmp_path: Path) -> None:
    source_path = _write_folder(tmp_path)
    orphan = source_path.parent / "steps/orphan.yaml"
    orphan.write_text("not: [valid", encoding="utf-8")

    result = default_authoring_compiler().compile(
        _folder_root(),
        source_path=source_path,
        project_root=tmp_path,
    )

    assert [process["name"] for process in result.processes] == ["orders_daily"]
    assert [dependency.path for dependency in result.dependencies] == ["pipelines/orders_daily/steps/load.yaml"]


def test_folder_loader_accepts_the_bounded_100_fragment_contract(tmp_path: Path) -> None:
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source_path.parent.mkdir(parents=True)
    refs = [f"steps/part-{index:03d}.yaml" for index in range(100)]
    for index, ref in enumerate(refs):
        path = source_path.parent / ref
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            yaml.safe_dump(
                {"kind": "dpone.flow-fragment.v1", "processes": [_process(f"orders_{index:03d}")]},
                sort_keys=False,
            ),
            encoding="utf-8",
        )

    result = authoring_folder.BoundedYamlFolderFragmentLoader().load(
        source_path=source_path,
        project_source="pipelines/orders_daily/pipeline.yaml",
        fragment_refs=refs,
        project_root=tmp_path,
    )

    assert len(result.processes) == 100
    assert [dependency.path for dependency in result.dependencies] == [f"pipelines/orders_daily/{ref}" for ref in refs]


def test_folder_source_identity_is_canonical_but_pack_pin_is_byte_exact(tmp_path: Path) -> None:
    source_path = _write_folder(tmp_path)
    compiler = default_authoring_compiler()
    before = compiler.compile(_folder_root(), source_path=source_path, project_root=tmp_path)
    fragment = source_path.parent / "steps/load.yaml"
    fragment.write_text(yaml.safe_dump({"processes": [_process()], "kind": "dpone.flow-fragment.v1"}), encoding="utf-8")
    after = compiler.compile(_folder_root(), source_path=source_path, project_root=tmp_path)

    assert before.source_fingerprint == after.source_fingerprint
    assert before.semantic_fingerprint == after.semantic_fingerprint
    assert before.dependencies[0].sha256 != after.dependencies[0].sha256


def test_pack_build_rejects_fragment_change_after_compilation(tmp_path: Path) -> None:
    source_path = _write_folder(tmp_path)
    compilation = default_authoring_compiler().compile(_folder_root(), source_path=source_path, project_root=tmp_path)
    fragment = source_path.parent / "steps/load.yaml"
    fragment.write_text(
        yaml.safe_dump({"kind": "dpone.flow-fragment.v1", "processes": [_process("changed")]}, sort_keys=False),
        encoding="utf-8",
    )
    dependencies = WorkloadDependencyResolver().resolve(
        repo_root=tmp_path,
        manifest="pipelines/orders_daily/pipeline.yaml",
    )
    pack = {"workload_dependencies": [dependency.to_jsonable() for dependency in dependencies]}

    with pytest.raises(AuthoringDependencyIntegrityError):
        verify_authoring_dependency_parity(expected=compilation.dependencies, pack=pack)


def test_fragment_sql_file_is_normalized_and_pinned_from_fragment_directory(tmp_path: Path) -> None:
    process = _process()
    source = process["source"]
    assert isinstance(source, dict)
    source["query"] = {"sql_file": "query.sql"}
    source_path = _write_folder(
        tmp_path,
        fragment={"kind": "dpone.flow-fragment.v1", "processes": [process]},
    )
    sql_file = source_path.parent / "steps/query.sql"
    sql_file.write_text("SELECT * FROM dbo.orders\n", encoding="utf-8")

    compilation = default_authoring_compiler().compile(_folder_root(), source_path=source_path, project_root=tmp_path)
    dependencies = WorkloadDependencyResolver().resolve(
        repo_root=tmp_path,
        manifest="pipelines/orders_daily/pipeline.yaml",
    )

    compiled_source = compilation.processes[0]["source"]
    assert isinstance(compiled_source, dict)
    assert compiled_source["query"]["sql_file"] == "steps/query.sql"
    assert [(dependency.kind, dependency.path) for dependency in dependencies] == [
        ("manifest", "pipelines/orders_daily/pipeline.yaml"),
        ("authoring_fragment", "pipelines/orders_daily/steps/load.yaml"),
        ("sql_file", "pipelines/orders_daily/steps/query.sql"),
    ]
    assert [(dependency.kind, dependency.path) for dependency in compilation.dependencies] == [
        ("authoring_fragment", "pipelines/orders_daily/steps/load.yaml"),
        ("sql_file", "pipelines/orders_daily/steps/query.sql"),
    ]
    verify_authoring_dependency_parity(
        expected=compilation.dependencies,
        pack={"workload_dependencies": [dependency.to_jsonable() for dependency in dependencies]},
    )
    assert [
        item["kind"]
        for item in source_file_provenance(
            {"workload_dependencies": [dependency.to_jsonable() for dependency in dependencies]}
        )
    ] == ["manifest", "authoring_fragment", "sql_file"]


def test_pack_build_rejects_sql_change_after_compilation(tmp_path: Path) -> None:
    process = _process()
    source = process["source"]
    assert isinstance(source, dict)
    source["query"] = {"sql_file": "query.sql"}
    source_path = _write_folder(
        tmp_path,
        fragment={"kind": "dpone.flow-fragment.v1", "processes": [process]},
    )
    sql_file = source_path.parent / "steps/query.sql"
    sql_file.write_text("SELECT 1\n", encoding="utf-8")
    compilation = default_authoring_compiler().compile(_folder_root(), source_path=source_path, project_root=tmp_path)

    sql_file.write_text("SELECT 2\n", encoding="utf-8")
    dependencies = WorkloadDependencyResolver().resolve(
        repo_root=tmp_path,
        manifest="pipelines/orders_daily/pipeline.yaml",
    )

    with pytest.raises(AuthoringDependencyIntegrityError):
        verify_authoring_dependency_parity(
            expected=compilation.dependencies,
            pack={"workload_dependencies": [dependency.to_jsonable() for dependency in dependencies]},
        )


@pytest.mark.parametrize(
    ("fragment_ref", "code"),
    [
        ("../outside.yaml", "DPONE_AUTHORING_FOLDER_PATH_INVALID"),
        ("/tmp/outside.yaml", "DPONE_AUTHORING_FOLDER_PATH_INVALID"),
        (r"steps\load.yaml", "DPONE_AUTHORING_FOLDER_PATH_INVALID"),
    ],
)
def test_folder_loader_rejects_unsafe_paths(tmp_path: Path, fragment_ref: str, code: str) -> None:
    source_path = _write_folder(tmp_path)
    payload = _folder_root()
    payload["fragments"] = [fragment_ref]

    with pytest.raises(AuthoringCompilationError) as exc_info:
        default_authoring_compiler().compile(payload, source_path=source_path, project_root=tmp_path)

    assert exc_info.value.code == code


@pytest.mark.parametrize(
    ("fragments", "code"),
    [
        ([], "DPONE_AUTHORING_FOLDER_FRAGMENTS_INVALID"),
        (["steps/load.yaml", "steps/load.yaml"], "DPONE_AUTHORING_FOLDER_FRAGMENT_DUPLICATE"),
        ([f"steps/{index}.yaml" for index in range(101)], "DPONE_AUTHORING_FOLDER_FRAGMENT_LIMIT_EXCEEDED"),
        (["steps/missing.yaml"], "DPONE_AUTHORING_FOLDER_FRAGMENT_NOT_FOUND"),
    ],
)
def test_folder_loader_rejects_invalid_fragment_sets(tmp_path: Path, fragments: list[str], code: str) -> None:
    source_path = _write_folder(tmp_path)
    payload = _folder_root()
    payload["fragments"] = fragments

    with pytest.raises(AuthoringCompilationError) as exc_info:
        default_authoring_compiler().compile(payload, source_path=source_path, project_root=tmp_path)

    assert exc_info.value.code == code


def test_workload_dependency_resolver_translates_expected_folder_error(tmp_path: Path) -> None:
    source_path = _write_folder(tmp_path)
    (source_path.parent / "steps/load.yaml").unlink()

    with pytest.raises(WorkloadDependencyError, match="authoring_folder_fragment_not_found"):
        WorkloadDependencyResolver().resolve(
            repo_root=tmp_path,
            manifest="pipelines/orders_daily/pipeline.yaml",
        )


def test_folder_loader_enforces_total_bytes_and_yaml_depth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_path = _write_folder(tmp_path)
    second = source_path.parent / "steps/second.yaml"
    first_content = (source_path.parent / "steps/load.yaml").read_bytes()
    second.write_bytes(first_content)
    payload = _folder_root()
    payload["fragments"] = ["steps/load.yaml", "steps/second.yaml"]
    monkeypatch.setattr(authoring_folder, "MAX_TOTAL_BYTES", len(first_content) * 2 - 1)

    with pytest.raises(AuthoringCompilationError) as total_error:
        default_authoring_compiler().compile(payload, source_path=source_path, project_root=tmp_path)
    assert total_error.value.code == "DPONE_AUTHORING_FOLDER_TOTAL_SIZE_EXCEEDED"

    monkeypatch.setattr(authoring_folder, "MAX_TOTAL_BYTES", 8 * 1024 * 1024)
    monkeypatch.setattr(authoring_folder, "MAX_YAML_DEPTH", 2)
    payload["fragments"] = ["steps/load.yaml"]
    with pytest.raises(AuthoringCompilationError) as depth_error:
        default_authoring_compiler().compile(payload, source_path=source_path, project_root=tmp_path)
    assert depth_error.value.code == "DPONE_AUTHORING_FOLDER_YAML_BUDGET_EXCEEDED"


@pytest.mark.parametrize(
    "fragment",
    [
        {"kind": "wrong", "processes": [_process()]},
        {"kind": "dpone.flow-fragment.v1", "processes": []},
        {"kind": "dpone.flow-fragment.v1", "processes": [_process()], "metadata": {}},
        {"kind": "dpone.flow-fragment.v1", "processes": [_process()], "fragments": ["nested.yaml"]},
    ],
)
def test_folder_loader_rejects_invalid_fragment_contracts(tmp_path: Path, fragment: dict[str, object]) -> None:
    source_path = _write_folder(tmp_path, fragment=fragment)

    with pytest.raises(AuthoringCompilationError) as exc_info:
        default_authoring_compiler().compile(_folder_root(), source_path=source_path, project_root=tmp_path)

    assert exc_info.value.code == "DPONE_AUTHORING_FOLDER_FRAGMENT_INVALID"


def test_folder_scaffold_conflict_is_atomic(tmp_path: Path) -> None:
    conflict = tmp_path / "pipelines/orders_daily/steps/load.yaml"
    conflict.parent.mkdir(parents=True)
    conflict.write_text("user owned\n", encoding="utf-8")

    result = build_airflow_self_service_service(root=tmp_path).init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode="folder",
    )

    assert result.passed is False
    assert conflict.read_text(encoding="utf-8") == "user owned\n"
    assert not (tmp_path / "pipelines/orders_daily/pipeline.yaml").exists()
    assert not (tmp_path / "domains/sales.yaml").exists()


def test_folder_loader_rejects_symlinked_fragment(tmp_path: Path) -> None:
    source_path = _write_folder(tmp_path)
    real = source_path.parent / "steps/load.yaml"
    real.rename(source_path.parent / "steps/real.yaml")
    real.symlink_to(source_path.parent / "steps/real.yaml")

    with pytest.raises(AuthoringCompilationError) as exc_info:
        default_authoring_compiler().compile(_folder_root(), source_path=source_path, project_root=tmp_path)

    assert exc_info.value.code == "DPONE_AUTHORING_FOLDER_SYMLINK_FORBIDDEN"


def test_folder_loader_rejects_parent_swap_to_symlink_during_fd_walk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_path = _write_folder(tmp_path)
    steps = source_path.parent / "steps"
    original_steps = source_path.parent / "steps-original"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "load.yaml").write_text(
        yaml.safe_dump({"kind": "dpone.flow-fragment.v1", "processes": [_process("outside")]}),
        encoding="utf-8",
    )
    original_open = confined_files.os.open
    swapped = False

    def racing_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        if path == "steps" and dir_fd is not None and not swapped:
            steps.rename(original_steps)
            steps.symlink_to(outside, target_is_directory=True)
            swapped = True
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(confined_files.os, "open", racing_open)

    with pytest.raises(AuthoringCompilationError) as exc_info:
        default_authoring_compiler().compile(_folder_root(), source_path=source_path, project_root=tmp_path)

    assert swapped is True
    assert exc_info.value.code == "DPONE_AUTHORING_FOLDER_SYMLINK_FORBIDDEN"


def test_folder_loader_rejects_yaml_aliases_and_oversized_files(tmp_path: Path) -> None:
    source_path = _write_folder(tmp_path)
    fragment = source_path.parent / "steps/load.yaml"
    fragment.write_text(
        "kind: dpone.flow-fragment.v1\nprocesses: &items\n  - name: x\ncopy: *items\n",
        encoding="utf-8",
    )

    with pytest.raises(AuthoringCompilationError) as alias_error:
        default_authoring_compiler().compile(_folder_root(), source_path=source_path, project_root=tmp_path)
    assert alias_error.value.code == "DPONE_AUTHORING_FOLDER_YAML_ALIAS_FORBIDDEN"

    fragment.write_bytes(b"x" * (1024 * 1024 + 1))
    with pytest.raises(AuthoringCompilationError) as size_error:
        default_authoring_compiler().compile(_folder_root(), source_path=source_path, project_root=tmp_path)
    assert size_error.value.code == "DPONE_AUTHORING_FOLDER_FRAGMENT_TOO_LARGE"


def test_folder_loader_rejects_duplicate_yaml_keys(tmp_path: Path) -> None:
    source_path = _write_folder(tmp_path)
    fragment = source_path.parent / "steps/load.yaml"
    fragment.write_text(
        """kind: dpone.flow-fragment.v1
processes:
  - name: orders_daily
    source: {type: mssql}
    sink:
      type: clickhouse
      strategy:
        mode: auto
        mode: full_refresh
""",
        encoding="utf-8",
    )

    with pytest.raises(AuthoringCompilationError) as exc_info:
        default_authoring_compiler().compile(_folder_root(), source_path=source_path, project_root=tmp_path)

    assert exc_info.value.code == "DPONE_AUTHORING_FOLDER_FRAGMENT_INVALID"


def test_folder_scaffold_checks_loads_and_previews_with_pinned_dependency(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)

    init_result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode="folder",
    )
    write_mssql_connection_registry(tmp_path, include=("mssql_dev",))
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    check_result = service.check("orders_daily")
    loaded = ManifestLoaderRouter().load(source_path, metadata_only=True)
    preview_result = service.preview("orders_daily")

    assert init_result.passed is True
    assert (tmp_path / "pipelines/orders_daily/steps/load.yaml").exists()
    assert check_result.passed is True
    assert check_result.details["authoring_mode"] == "folder"
    assert [item["kind"] for item in check_result.details["source_files"]] == [
        "primary_source",
        "authoring_fragment",
    ]
    assert loaded.kind == "dpone.batch.v1"
    assert loaded.source_kind == "dpone.flow.v1"
    assert preview_result.passed is True
    release_id = preview_result.details["release"]["release_id"].replace(":", "-")
    pack_path = tmp_path / ".dpone-cache" / "releases" / release_id / "packs/orders_daily.airflow-pack.json"
    pack = json.loads(pack_path.read_text(encoding="utf-8"))
    dependencies = pack["workload_dependencies"]
    assert [item["kind"] for item in dependencies] == ["manifest", "authoring_fragment"]
    assert dependencies[1]["path"] == "pipelines/orders_daily/steps/load.yaml"

    snapshot = load_pipeline_source_snapshot_from_file(source_path)
    assert [dependency.path for dependency in snapshot.dependencies] == ["pipelines/orders_daily/steps/load.yaml"]


def test_folder_root_and_fragment_match_public_json_schemas(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    source_path = _write_folder(tmp_path)
    root = Path(__file__).parents[1]
    root_schema = json.loads((root / "src/dpone/schema/etl-flow-manifest.schema.json").read_text(encoding="utf-8"))
    fragment_schema = json.loads(
        (root / "src/dpone/schema/etl-flow-fragment-manifest.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(yaml.safe_load(source_path.read_text(encoding="utf-8")), root_schema)
    jsonschema.validate(
        yaml.safe_load((source_path.parent / "steps/load.yaml").read_text(encoding="utf-8")),
        fragment_schema,
    )


def test_authoring_errors_have_executable_catalog_pages_and_check_links(tmp_path: Path) -> None:
    docs_root = Path(__file__).parents[1] / "docs/errors"
    assert all((docs_root / f"{code}.md").is_file() for code in _AUTHORING_ERROR_CODES)
    source_path = _write_folder(tmp_path)
    payload = _folder_root()
    payload["fragments"] = ["steps/missing.yaml"]
    source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    result = build_airflow_self_service_service(root=tmp_path).check("orders_daily")

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_AUTHORING_FOLDER_FRAGMENT_NOT_FOUND"
    assert result.errors[0]["docs_url"] == ("docs/errors/DPONE_AUTHORING_FOLDER_FRAGMENT_NOT_FOUND.md")
