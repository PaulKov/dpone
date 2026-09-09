from __future__ import annotations

import importlib.metadata
import json
import shutil
import subprocess
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from dpone.adapters.dbt_executable import current_environment_dbt_executable
from dpone.adapters.dbt_workflow_selection import (
    DbtCliSelectionResolver,
    ManifestPreviewSelectionResolver,
)
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_project_bundle import (
    DbtProjectBundle,
    DbtProjectBundleArtifact,
)
from dpone.readiness.dbt_publish_atomic_publisher import DbtArtifactTreePublisher
from dpone.readiness.dbt_sqlserver_project_policy import (
    DbtSqlserverProjectPolicyValidator,
)
from dpone.runtime.dbt_project_bundle import (
    build_dbt_project_bundle,
    extract_dbt_project_bundle,
    verify_dbt_project_bundle_tree,
)
from dpone.services.dbt_publish_artifact_writer import DbtArtifactWriter

ROOT = Path(__file__).parents[1]
DEMO = ROOT / "examples" / "dbt-inline-publishing"
RUNTIME_FIXTURE = ROOT / "tests" / "fixtures" / "dbt-runtime-correctness-v1"
_AUTHORITY_MANIFEST = json.loads((DEMO / "fixtures" / "manifest.v12.json").read_text(encoding="utf-8"))


def _manifest() -> bytes:
    return json.dumps(
        {
            "metadata": deepcopy(_AUTHORITY_MANIFEST["metadata"]),
            "macros": deepcopy(_AUTHORITY_MANIFEST["macros"]),
            "nodes": {
                "model.analytics.orders": {
                    "unique_id": "model.analytics.orders",
                    "name": "orders",
                    "resource_type": "model",
                    "language": "sql",
                    "config": {
                        "enabled": True,
                        "materialized": "table",
                        "as_columnstore": False,
                        "indexes": [],
                        "drop_unmanaged_indexes": False,
                        "prefer_single_alter_column": False,
                    },
                    "fqn": ["analytics", "orders"],
                    "database": "DWH",
                    "schema": "mart",
                    "alias": "orders",
                    "depends_on": {
                        "nodes": ["model.analytics.stg_orders"],
                        "macros": [],
                    },
                },
                "model.analytics.stg_orders": {
                    "unique_id": "model.analytics.stg_orders",
                    "name": "stg_orders",
                    "resource_type": "model",
                    "language": "sql",
                    "config": {
                        "enabled": True,
                        "materialized": "view",
                        "as_columnstore": False,
                        "indexes": [],
                        "drop_unmanaged_indexes": False,
                        "prefer_single_alter_column": False,
                    },
                    "fqn": ["analytics", "stg_orders"],
                    "database": "DWH",
                    "schema": "mart",
                    "alias": "stg_orders",
                    "depends_on": {
                        "nodes": ["source.raw.orders"],
                        "macros": [],
                    },
                },
                "test.analytics.orders_not_null": {
                    "unique_id": "test.analytics.orders_not_null",
                    "resource_type": "test",
                    "language": "sql",
                    "config": {
                        "enabled": True,
                        "materialized": "test",
                    },
                    "fqn": ["analytics", "orders_not_null"],
                    "depends_on": {
                        "nodes": ["model.analytics.orders"],
                        "macros": [],
                    },
                },
            },
            "unit_tests": {
                "unit_test.analytics.orders_unit": {
                    "unique_id": "unit_test.analytics.orders_unit",
                    "resource_type": "unit_test",
                    "config": {"enabled": True},
                    "fqn": ["analytics", "orders_unit"],
                    "depends_on": {
                        "nodes": ["model.analytics.orders"],
                        "macros": [],
                    },
                    "model": "orders",
                },
            },
            "parent_map": {
                "model.analytics.orders": ["model.analytics.stg_orders"],
                "model.analytics.stg_orders": ["source.raw.orders"],
                "test.analytics.orders_not_null": ["model.analytics.orders"],
                "unit_test.analytics.orders_unit": ["model.analytics.orders"],
            },
            "child_map": {
                "model.analytics.orders": [
                    "test.analytics.orders_not_null",
                    "unit_test.analytics.orders_unit",
                ],
                "model.analytics.stg_orders": ["model.analytics.orders"],
                "source.raw.orders": ["model.analytics.stg_orders"],
                "test.analytics.orders_not_null": [],
                "unit_test.analytics.orders_unit": [],
            },
        }
    ).encode()


def test_manifest_preview_selection_closes_upstream_and_tests() -> None:
    selection = ManifestPreviewSelectionResolver().resolve(
        project_root=Path("."),
        manifest_bytes=_manifest(),
        selected_unique_ids=("model.analytics.orders",),
        profiles_dir=None,
        profile_name="dpone_runtime",
        target_name="runtime",
        dbt_core_version="1.12.3",
        dbt_adapter="sqlserver",
        dbt_adapter_version="1.11.1",
    )

    assert selection.authority == "manifest_preview"
    assert selection.selectors == ("+fqn:analytics.orders",)
    assert selection.selected_graph_unique_ids == (
        "model.analytics.orders",
        "model.analytics.stg_orders",
        "test.analytics.orders_not_null",
        "unit_test.analytics.orders_unit",
    )
    assert selection.expected_run_result_unique_ids == (
        "model.analytics.orders",
        "model.analytics.stg_orders",
        "test.analytics.orders_not_null",
        "unit_test.analytics.orders_unit",
    )


def test_dbt_cli_selection_uses_exact_toolchain_and_bounded_argv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[str, ...]] = []
    environments: list[dict[str, str]] = []

    def runner(args, **kwargs):
        observed.append(args)
        environments.append(kwargs["env"])
        assert kwargs["shell"] is False
        assert not Path(args[args.index("--target-path") + 1]).is_relative_to(tmp_path)
        assert not Path(args[args.index("--log-path") + 1]).is_relative_to(tmp_path)
        if "parse" in args:
            target = Path(args[args.index("--target-path") + 1])
            target.mkdir(parents=True)
            (target / "manifest.json").write_bytes(_manifest())
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
        return subprocess.CompletedProcess(
            args,
            0,
            stdout=(
                b'{"unique_id":"model.analytics.stg_orders"}\n'
                b'{"unique_id":"model.analytics.orders"}\n'
                b'{"unique_id":"test.analytics.orders_not_null"}\n'
                b'{"unique_id":"unit_test.analytics.orders_unit"}\n'
            ),
            stderr=b"",
        )

    versions = {"dbt-core": "1.12.3", "dbt-sqlserver": "1.11.1"}
    monkeypatch.setenv("DBT_TARGET", "ambient-target-must-not-leak")
    monkeypatch.setenv("DPONE_DBT_CONTEXT", "ambient-context-must-not-leak")
    selection = DbtCliSelectionResolver(
        runner=runner,
        package_version=versions.__getitem__,
        dbt_executable="/locked/python-environment/bin/dbt",
    ).resolve(
        project_root=tmp_path,
        manifest_bytes=_manifest(),
        selected_unique_ids=("model.analytics.orders",),
        profiles_dir=tmp_path / "profiles",
        profile_name="analytics",
        target_name="production",
        dbt_core_version="1.12.3",
        dbt_adapter="sqlserver",
        dbt_adapter_version="1.11.1",
    )

    assert selection.authority == "dbt_cli"
    assert selection.selected_graph_unique_ids == (
        "model.analytics.orders",
        "model.analytics.stg_orders",
        "test.analytics.orders_not_null",
        "unit_test.analytics.orders_unit",
    )
    assert selection.expected_run_result_unique_ids == (
        "model.analytics.orders",
        "model.analytics.stg_orders",
        "test.analytics.orders_not_null",
        "unit_test.analytics.orders_unit",
    )
    assert len(observed) == 2
    assert all(args[0] == "/locked/python-environment/bin/dbt" for args in observed)
    assert "parse" in observed[0]
    assert "ls" in observed[1]
    assert "--profiles-dir" in observed[1]
    assert observed[1][observed[1].index("--profile") + 1] == "analytics"
    assert observed[1][observed[1].index("--target") + 1] == "production"
    assert "unit_test" in observed[1]
    assert "+fqn:analytics.orders" in observed[1]
    assert "--vars" in observed[1]
    assert all("DBT_TARGET" not in item for item in environments)
    assert all("DPONE_DBT_CONTEXT" not in item for item in environments)


@pytest.mark.skipif(
    not Path(current_environment_dbt_executable()).is_file(),
    reason="dbt CLI is not installed in the active Python environment",
)
def test_dbt_cli_exact_fqn_selector_executes_with_certified_toolchain(
    tmp_path: Path,
) -> None:
    expected_versions = {"dbt-core": "1.12.3", "dbt-sqlserver": "1.11.1"}
    if {
        distribution: importlib.metadata.version(distribution) for distribution in expected_versions
    } != expected_versions:
        pytest.skip("certified dbt/sqlserver toolchain is not installed")
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text(
        "\n".join(
            (
                "dpone_runtime_fixture:",
                "  target: runtime_fixture",
                "  outputs:",
                "    runtime_fixture:",
                "      type: sqlserver",
                "      driver: ODBC Driver 18 for SQL Server",
                "      server: localhost",
                "      port: 1433",
                "      database: DWH_Stage",
                "      schema: dpone_fixture",
                "      authentication: sql",
                "      user: fixture",
                "      password: fixture",
                "      encrypt: true",
                "      trust_cert: true",
                "      threads: 1",
                "",
            )
        ),
        encoding="utf-8",
    )
    initial_target = tmp_path / "initial-target"
    invocation = DbtInvocationContext.canonical()
    completed = subprocess.run(
        (
            current_environment_dbt_executable(),
            "--quiet",
            "--no-use-colors",
            "parse",
            "--project-dir",
            str(RUNTIME_FIXTURE),
            "--profiles-dir",
            str(profiles_dir),
            "--profile",
            "dpone_runtime_fixture",
            "--target",
            "runtime_fixture",
            "--target-path",
            str(initial_target),
            "--log-path",
            str(tmp_path / "initial-logs"),
            "--vars",
            invocation.selection_vars_json(),
        ),
        cwd=RUNTIME_FIXTURE,
        check=False,
        shell=False,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=120,
        env=invocation.environment(home=str(tmp_path / "initial-home")),
    )
    assert completed.returncode == 0, completed.stderr.decode(errors="replace")
    manifest = (initial_target / "manifest.json").read_bytes()

    selection = DbtCliSelectionResolver().resolve(
        project_root=RUNTIME_FIXTURE,
        manifest_bytes=manifest,
        selected_unique_ids=("model.dpone_runtime_fixture.orders",),
        profiles_dir=profiles_dir,
        profile_name="dpone_runtime_fixture",
        target_name="runtime_fixture",
        dbt_core_version="1.12.3",
        dbt_adapter="sqlserver",
        dbt_adapter_version="1.11.1",
    )

    assert selection.selectors == ("+fqn:dpone_runtime_fixture.orders",)
    assert "model.dpone_runtime_fixture.ephemeral_orders" in (selection.selected_graph_unique_ids)
    assert "model.dpone_runtime_fixture.ephemeral_orders" in (selection.expected_run_result_unique_ids)
    assert any(item.startswith("unit_test.dpone_runtime_fixture.") for item in selection.selected_graph_unique_ids)
    assert any(item.startswith("unit_test.dpone_runtime_fixture.") for item in selection.expected_run_result_unique_ids)


def test_dbt_cli_selection_rejects_manifest_from_another_project_snapshot(
    tmp_path: Path,
) -> None:
    def runner(args, **_kwargs):
        if "parse" in args:
            target = Path(args[args.index("--target-path") + 1])
            target.mkdir(parents=True)
            foreign = json.loads(_manifest())
            foreign["parent_map"]["model.analytics.orders"] = []
            (target / "manifest.json").write_text(json.dumps(foreign), encoding="utf-8")
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
        pytest.fail("selection must not run after manifest coherence fails")

    resolver = DbtCliSelectionResolver(
        runner=runner,
        package_version={"dbt-core": "1.12.3", "dbt-sqlserver": "1.11.1"}.__getitem__,
    )

    with pytest.raises(ValueError, match="does not describe the captured project"):
        resolver.resolve(
            project_root=tmp_path,
            manifest_bytes=_manifest(),
            selected_unique_ids=("model.analytics.orders",),
            profiles_dir=None,
            profile_name="dpone_runtime",
            target_name="runtime",
            dbt_core_version="1.12.3",
            dbt_adapter="sqlserver",
            dbt_adapter_version="1.11.1",
        )


def test_dbt_cli_selection_fails_closed_on_toolchain_drift(tmp_path: Path) -> None:
    resolver = DbtCliSelectionResolver(
        runner=lambda *_args, **_kwargs: pytest.fail("dbt must not run"),
        package_version=lambda _distribution: "1.11.0",
    )

    with pytest.raises(ValueError, match="certified lock"):
        resolver.resolve(
            project_root=tmp_path,
            manifest_bytes=_manifest(),
            selected_unique_ids=("model.analytics.orders",),
            profiles_dir=None,
            profile_name="dpone_runtime",
            target_name="runtime",
            dbt_core_version="1.12.3",
            dbt_adapter="sqlserver",
            dbt_adapter_version="1.11.1",
        )


def test_artifact_writer_requires_explicit_release_authorities() -> None:
    with pytest.raises(TypeError):
        DbtArtifactWriter()
    with pytest.raises(TypeError):
        DbtArtifactWriter(
            selection_resolver=ManifestPreviewSelectionResolver(),
        )
    with pytest.raises(TypeError):
        DbtArtifactWriter(
            bundle_operations=RuntimeDbtProjectBundleOperations(),
        )


@pytest.mark.parametrize(
    ("failed_stage", "expected_events"),
    [
        (None, ["build", "extract", "verify", "select", "publish"]),
        ("build", ["build"]),
        ("extract", ["build", "extract"]),
        ("select", ["build", "extract", "verify", "select"]),
        ("verify", ["build", "extract", "verify"]),
    ],
)
def test_release_build_ports_run_in_order_before_publication(
    tmp_path: Path,
    failed_stage: str | None,
    expected_events: list[str],
) -> None:
    events: list[str] = []

    def record(stage: str) -> None:
        events.append(stage)
        if stage == failed_stage:
            raise RuntimeError(f"{stage} failed")

    class RecordingBundleOperations:
        def build(self, project_root: Path) -> DbtProjectBundleArtifact:
            record("build")
            return build_dbt_project_bundle(project_root)

        def extract(self, archive: bytes, destination: Path) -> DbtProjectBundle:
            record("extract")
            return extract_dbt_project_bundle(archive, destination)

        def verify(self, archive: bytes, destination: Path) -> DbtProjectBundle:
            record("verify")
            return verify_dbt_project_bundle_tree(archive, destination)

    class RecordingSelectionResolver:
        def resolve(self, **kwargs: object):
            record("select")
            return ManifestPreviewSelectionResolver().resolve(**kwargs)

    class RecordingPublisher:
        def publish(self, output_dir: Path, files: dict[str, bytes]) -> None:
            record("publish")
            DbtArtifactTreePublisher().publish(output_dir, files)

    report = build_dbt_dpone_compiler().build(
        DEMO / "fixtures" / "manifest.v12.json",
        profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
    )
    assert report.passed
    output = tmp_path / "compiled"

    written = DbtArtifactWriter(
        selection_resolver=RecordingSelectionResolver(),
        bundle_operations=RecordingBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=DEMO),
        publisher=RecordingPublisher(),
    ).write(report, output, project_root=DEMO)

    assert events == expected_events
    assert written.passed is (failed_stage is None)
    assert output.exists() is (failed_stage is None)


@pytest.mark.parametrize(
    ("overlap_kind", "expected_code"),
    [
        ("foreign_publish", "DPONE_DBT_CROSS_WORKFLOW_DEPENDENCY_UNSUPPORTED"),
        ("shared_non_publish", "DPONE_DBT_WORKFLOW_GRAPH_OVERLAP"),
    ],
)
def test_authoritative_release_rejects_cross_workflow_selection_overlap(
    tmp_path: Path,
    overlap_kind: str,
    expected_code: str,
) -> None:
    manifest_payload = json.loads((DEMO / "fixtures" / "manifest.v12.json").read_text(encoding="utf-8"))
    history_id = "model.dpone_dbt_demo.competitive_pricing_history"
    current_id = "model.dpone_dbt_demo.competitive_pricing"
    history = manifest_payload["nodes"][history_id]
    history["config"]["meta"]["dpone"]["publish"]["workflow"] = "pricing_history"
    history["depends_on"]["nodes"] = []
    manifest_payload["parent_map"][history_id] = []
    manifest_payload["child_map"][current_id] = [
        child for child in manifest_payload["child_map"][current_id] if child != history_id
    ]
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

    profiles = yaml.safe_load((DEMO / "dpone" / "dbt-publish-profiles.yml").read_text(encoding="utf-8"))
    profiles["workflows"]["pricing_history"] = {
        **profiles["workflows"]["competitive_pricing"],
        "owner": "pricing-history-data",
    }
    profiles_path = tmp_path / "dbt-publish-profiles.yml"
    profiles_path.write_text(yaml.safe_dump(profiles), encoding="utf-8")
    report = build_dbt_dpone_compiler(root=DEMO).build(
        manifest_path,
        profiles_path=profiles_path,
    )
    assert report.passed

    class OverlappingResolver:
        def resolve(self, **kwargs: object):
            selection = ManifestPreviewSelectionResolver().resolve(**kwargs)
            selected_unique_ids = kwargs["selected_unique_ids"]
            if overlap_kind == "foreign_publish" and selected_unique_ids != (history_id,):
                return selection
            overlap_id = current_id if overlap_kind == "foreign_publish" else "model.dpone_dbt_demo.shared_stage"
            selected_graph = tuple(sorted({*selection.selected_graph_unique_ids, overlap_id}))
            expected_results = tuple(sorted({*selection.expected_run_result_unique_ids, overlap_id}))
            return replace(
                selection,
                selected_graph_unique_ids=selected_graph,
                expected_run_result_unique_ids=expected_results,
            )

    output = tmp_path / "compiled"
    written = DbtArtifactWriter(
        selection_resolver=OverlappingResolver(),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=DEMO),
    ).write(report, output, project_root=DEMO)

    assert not written.passed
    assert [issue.code for issue in written.blockers] == [
        expected_code,
    ]
    assert written.blockers[0].path
    assert written.blockers[0].remediation
    assert written.artifacts == {}
    assert not output.exists()


def test_release_selection_uses_verified_bundle_snapshot_and_rejects_mutation(
    tmp_path: Path,
) -> None:
    project = tmp_path / "authoring"
    shutil.copytree(DEMO, project)
    report = build_dbt_dpone_compiler().build(
        project / "fixtures" / "manifest.v12.json",
        profiles_path=project / "dpone" / "dbt-publish-profiles.yml",
    )
    assert report.passed
    author_model = project / "models" / "competitive_pricing.sql"
    original = author_model.read_bytes()

    def runner(args, **_kwargs):
        snapshot = Path(args[args.index("--project-dir") + 1])
        assert snapshot != project
        assert (snapshot / "models" / "competitive_pricing.sql").read_bytes() == original
        author_model.write_text("select 'author changed after snapshot'\n", encoding="utf-8")
        if "parse" in args:
            target = Path(args[args.index("--target-path") + 1])
            target.mkdir(parents=True)
            (target / "manifest.json").write_bytes((project / "fixtures" / "manifest.v12.json").read_bytes())
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
        (snapshot / "models" / "competitive_pricing.sql").write_text(
            "select 'selection mutated snapshot'\n",
            encoding="utf-8",
        )
        selectors = args[args.index("--select") + 1 :]
        stdout = b"".join(
            json.dumps({"unique_id": selector.removeprefix("+")}).encode() + b"\n" for selector in selectors
        )
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr=b"")

    versions = {"dbt-core": "1.12.3", "dbt-sqlserver": "1.11.1"}
    written = DbtArtifactWriter(
        selection_resolver=DbtCliSelectionResolver(
            runner=runner,
            package_version=versions.__getitem__,
        ),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(
            project_root=project,
        ),
        dbt_profiles_dir=project / "profiles",
    ).write(report, tmp_path / "compiled", project_root=project)

    assert not written.passed
    assert {issue.code for issue in written.blockers} == {"DPONE_DBT_COMPILE_FAILED"}
    assert author_model.read_text(encoding="utf-8") == "select 'author changed after snapshot'\n"


@pytest.mark.parametrize("unsafe_case", ["flag", "dispatch"])
def test_release_revalidates_project_policy_on_the_immutable_snapshot(
    tmp_path: Path,
    unsafe_case: str,
) -> None:
    project = tmp_path / "authoring"
    shutil.copytree(DEMO, project)
    report = build_dbt_dpone_compiler().build(
        project / "fixtures" / "manifest.v12.json",
        profiles_path=project / "dpone" / "dbt-publish-profiles.yml",
    )
    assert report.passed
    selection_called = False

    class MutatingBundleOperations:
        def build(self, project_root: Path) -> DbtProjectBundleArtifact:
            project_file = project_root / "dbt_project.yml"
            project_text = project_file.read_text(encoding="utf-8")
            if unsafe_case == "flag":
                project_text = project_text.replace(
                    "dbt_sqlserver_use_dbt_transactions: true",
                    "dbt_sqlserver_use_dbt_transactions: false",
                )
            else:
                project_text += "\ndispatch:\n  - macro_namespace: dbt\n    search_order: [analytics, dbt]\n"
            project_file.write_text(project_text, encoding="utf-8")
            return build_dbt_project_bundle(project_root)

        def extract(self, archive: bytes, destination: Path) -> DbtProjectBundle:
            return extract_dbt_project_bundle(archive, destination)

        def verify(self, archive: bytes, destination: Path) -> DbtProjectBundle:
            return verify_dbt_project_bundle_tree(archive, destination)

    class RecordingSelectionResolver:
        def resolve(self, **_kwargs: object) -> None:
            nonlocal selection_called
            selection_called = True

    output = tmp_path / "compiled"
    written = DbtArtifactWriter(
        selection_resolver=RecordingSelectionResolver(),  # type: ignore[arg-type]
        bundle_operations=MutatingBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=project),
    ).write(report, output, project_root=project)

    assert not written.passed
    assert written.blockers[0].code == ("DPONE_DBT_SQLSERVER_PROJECT_POLICY_INVALID")
    expected_field = "dbt_sqlserver_use_dbt_transactions" if unsafe_case == "flag" else "dispatch"
    assert expected_field in written.blockers[0].message
    assert selection_called is False
    assert not output.exists()


def test_artifact_writer_preserves_package_readiness_error_code(
    tmp_path: Path,
) -> None:
    project = tmp_path / "authoring"
    shutil.copytree(DEMO, project)
    (project / "packages.yml").write_text(
        "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n",
        encoding="utf-8",
    )
    (project / "package-lock.yml").unlink(missing_ok=True)
    report = build_dbt_dpone_compiler().build(
        project / "fixtures" / "manifest.v12.json",
        profiles_path=project / "dpone" / "dbt-publish-profiles.yml",
    )
    output = tmp_path / "compiled"

    written = DbtArtifactWriter(
        selection_resolver=ManifestPreviewSelectionResolver(),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(
            project_root=project,
        ),
    ).write(report, output, project_root=project)

    assert not written.passed
    assert {issue.code for issue in written.blockers} == {"DPONE_DBT_PACKAGE_LOCK_REQUIRED"}
    assert written.artifacts == {}
    assert not output.exists()
