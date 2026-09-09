"""Compatibility contract for the deprecated ``dpone.dbt_publish`` facade."""

from __future__ import annotations

import ast
import importlib
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.contracts.dbt_publish_compatibility import (
    DBT_PUBLISH_DEPRECATION_START_VERSION,
    DBT_PUBLISH_EARLIEST_REMOVAL_VERSION,
    DBT_PUBLISH_MINIMUM_MINOR_RELEASES,
    DBT_PUBLISH_MINIMUM_SUPPORT_MONTHS,
    DBT_RAW_PHYSICAL_OVERRIDE_DEPRECATION_START_VERSION,
    DBT_RAW_PHYSICAL_OVERRIDE_EARLIEST_REMOVAL_VERSION,
    DBT_RAW_PHYSICAL_OVERRIDE_MINIMUM_DEPRECATION_RELEASES,
)
from dpone.contracts.dbt_publish_models import (
    CompiledDbtModel as CanonicalCompiledDbtModel,
)
from dpone.contracts.dbt_publish_models import (
    DbtColumnArtifact,
    DbtPublishIssue,
    DbtPublishStrategyPolicy,
    DbtWorkflowProfile,
)
from dpone.contracts.dbt_publish_models import (
    DbtManifestArtifact as CanonicalDbtManifestArtifact,
)
from dpone.contracts.dbt_publish_models import (
    DbtModelArtifact as CanonicalDbtModelArtifact,
)
from dpone.contracts.dbt_publish_models import (
    DbtPublishIntent as CanonicalDbtPublishIntent,
)
from dpone.contracts.dbt_publish_models import (
    DbtPublishProfile as CanonicalDbtPublishProfile,
)
from dpone.contracts.dbt_release import (
    DBT_RELEASE_WIRE_CONTRACT,
    DBT_SELECTION_AUTHORITY,
    dbt_release_authority_violation,
)
from dpone.readiness.dbt_airflow_execution_pack import (
    DbtAirflowExecutionPackBuilder as CanonicalDbtAirflowExecutionPackBuilder,
)
from dpone.services.dbt_publish_compiler import (
    DbtDponeCompiler as CanonicalDbtDponeCompiler,
)
from dpone.services.dbt_publish_planning import (
    DbtPublishPlanner as CanonicalDbtPublishPlanner,
)

pytestmark = pytest.mark.filterwarnings(
    "ignore:dpone.dbt_publish is deprecated starting in dpone 0.73.21:DeprecationWarning"
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_LEGACY_PACKAGE = _REPOSITORY_ROOT / "src" / "dpone" / "dbt_publish"
_DEMO = _REPOSITORY_ROOT / "examples" / "dbt-inline-publishing"
_MODULE_MIGRATIONS = {
    "airflow_artifact_projection": "dpone.readiness.dbt_airflow_artifact_projection",
    "airflow_execution_pack": "dpone.readiness.dbt_airflow_execution_pack",
    "artifact_reader": "dpone.adapters.dbt_publish_artifact_reader",
    "artifact_writer": "dpone.services.dbt_publish_artifact_writer",
    "assembly": "dpone.app.dbt_publish_composition",
    "atomic_publisher": "dpone.readiness.dbt_publish_atomic_publisher",
    "capability_policy": "dpone.readiness.dbt_publish_capability_policy",
    "compiler": "dpone.services.dbt_publish_compiler",
    "intent_resolver": "dpone.manifest.dbt_publish_intent_resolver",
    "model_compiler": "dpone.services.dbt_publish_model_compiler",
    "models": "dpone.contracts.dbt_publish_models",
    "planning": "dpone.services.dbt_publish_planning",
    "profiles": "dpone.manifest.dbt_publish_profiles",
    "release_assets": "dpone.services.dbt_release_assets",
    "release_builder": "dpone.services.dbt_release_builder",
    "release_materializer": "dpone.readiness.dbt_publish_release_materializer",
    "schema_contract_authoring": "dpone.contracts.dbt_publish_schema_contract_authoring",
    "schema_contract_common": "dpone.contracts.dbt_publish_schema_contract_common",
    "schema_contract_policy": "dpone.contracts.dbt_publish_schema_contract_policy",
    "schema_contract_runtime": "dpone.contracts.dbt_publish_schema_contract_runtime",
    "schema_contracts": "dpone.contracts.dbt_publish_schema_contracts",
}
_COMPATIBILITY_ADAPTERS = {
    "artifact_writer": {"DbtArtifactWriter", "DbtExecutionPackBuilder"},
    "compiler": {"DbtDponeCompiler"},
    "models": {
        "CompiledDbtModel",
        "DbtCompileReport",
        "DbtManifestArtifact",
        "DbtModelArtifact",
        "DbtPublishProfile",
    },
    "planning": {"DbtPublishPlanner"},
}


def _canonical_model() -> CanonicalDbtModelArtifact:
    return CanonicalDbtModelArtifact(
        unique_id="model.analytics.orders",
        name="orders",
        original_file_path="models/orders.sql",
        database="warehouse",
        schema="dbo",
        alias="orders",
        materialized="incremental",
        contract_enforced=True,
        columns=("order_id",),
        column_contracts=(
            DbtColumnArtifact(
                name="order_id",
                data_type="bigint",
                nullable=False,
                constraints=("not_null",),
            ),
        ),
        group="analytics",
        tags=(),
        meta={},
        unique_key=("order_id",),
        depends_on=(),
    )


@pytest.mark.parametrize(("legacy_name", "canonical_name"), _MODULE_MIGRATIONS.items())
def test_legacy_modules_reexport_canonical_public_names(legacy_name: str, canonical_name: str) -> None:
    legacy = importlib.import_module(f"dpone.dbt_publish.{legacy_name}")
    canonical = importlib.import_module(canonical_name)

    public_names = getattr(canonical, "__all__", ())
    assert public_names
    assert getattr(legacy, "__all__") == public_names
    for name in public_names:
        if name in _COMPATIBILITY_ADAPTERS.get(legacy_name, set()):
            assert getattr(legacy, name) is not getattr(canonical, name)
        else:
            assert getattr(legacy, name) is getattr(canonical, name)


def test_legacy_facade_contains_no_domain_implementation() -> None:
    implementation_nodes = (ast.AsyncFunctionDef, ast.ClassDef, ast.FunctionDef)
    offenders = []
    for path in sorted(_LEGACY_PACKAGE.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if path.stem not in _COMPATIBILITY_ADAPTERS and any(
            isinstance(node, implementation_nodes) for node in ast.walk(tree)
        ):
            offenders.append(path.name)

    assert offenders == []


def test_legacy_positional_model_contracts_keep_original_field_order() -> None:
    legacy = importlib.import_module("dpone.dbt_publish.models")
    model = legacy.DbtModelArtifact(
        "model.analytics.orders",
        "orders",
        "models/orders.sql",
        "warehouse",
        "dbo",
        "orders",
        "incremental",
        True,
        ("order_id",),
        "analytics",
        ("daily",),
        {"owner": "analytics"},
        ("order_id",),
        ("model.analytics.customers",),
        ("test.analytics.orders_unique",),
    )
    manifest = legacy.DbtManifestArtifact(
        "target/manifest.json",
        12,
        "1.12.3",
        "invocation-1",
        "analytics",
        (model,),
    )
    profile = legacy.DbtPublishProfile(
        "mart",
        "mssql",
        "source",
        "clickhouse",
        "sink",
        "analytics",
        "staging",
        "registry.example/dpone@sha256:" + "a" * 64,
        {"fetch_size": 1_000},
        {"cluster": "analytics"},
        {"namespace": "data"},
        {"clickhouse": {"engine": "MergeTree"}},
        {"max_parallelism": 2},
        {"preset": "strict"},
        {"enabled": True},
    )
    warning = DbtPublishIssue("legacy_preview", "preview", "models/orders.sql", "warning")
    compiled = legacy.CompiledDbtModel(
        model,
        CanonicalDbtPublishIntent(True, "mart", "daily"),
        profile,
        {"mode": "incremental_merge"},
        {"storage": {"clickhouse": {}}},
        "dbt_daily_orders",
        {"name": "dbt_daily_orders"},
        (warning,),
    )
    report = legacy.DbtCompileReport(
        "target/manifest.json",
        12,
        (compiled,),
        (),
        (warning,),
        (),
        {"manifest": "manifest.yaml"},
        "dpone.dbt_publish_compile.v1",
    )

    assert model.group == "analytics"
    assert model.tags == ("daily",)
    assert model.meta == {"owner": "analytics"}
    assert model.unique_key == ("order_id",)
    assert model.depends_on == ("model.analytics.customers",)
    assert model.test_ids == ("test.analytics.orders_unique",)
    assert model.column_contracts == ()
    assert manifest.schema_version == 12
    assert manifest.models == (model,)
    assert manifest.sha256 == ""
    assert profile.source_options == {"fetch_size": 1_000}
    assert profile.lineage == {"enabled": True}
    assert profile.certification is None
    assert profile.state == {}
    assert compiled.warnings == (warning,)
    assert compiled.route_capability == {}
    assert report.models == (compiled,)
    assert report.warnings == (warning,)
    assert report.artifacts == {"manifest": "manifest.yaml"}
    assert report.schema == "dpone.dbt_publish_compile.v1"


def test_legacy_compiler_constructor_and_build_shape_execute() -> None:
    legacy = importlib.import_module("dpone.dbt_publish.compiler")

    class MissingReader:
        def read(self, path: str) -> tuple[None, tuple[DbtPublishIssue, ...]]:
            return None, (DbtPublishIssue("missing", "missing", path),)

    compiler = legacy.DbtDponeCompiler(
        reader=MissingReader(),
        resolver=object(),
        model_compiler=object(),
    )
    report = compiler.build("target/missing-manifest.json")

    assert compiler.compatibility_mode == "local_preview"
    assert report.blockers[0].code == "missing"


def test_legacy_compiler_adapts_old_ports_without_production_authority() -> None:
    legacy = importlib.import_module("dpone.dbt_publish.compiler")
    model = _canonical_model()
    artifact = CanonicalDbtManifestArtifact(
        path="target/manifest.json",
        sha256="sha256:" + "a" * 64,
        schema_version=12,
        dbt_version="1.12.3",
        invocation_id="invocation-1",
        project_name="analytics",
        models=(model,),
    )
    intent = CanonicalDbtPublishIntent(
        enabled=True,
        profile="mart",
        workflow="daily",
        strategy_mode="incremental_merge",
    )
    profile = CanonicalDbtPublishProfile(
        name="mart",
        source_type="mssql",
        source_connection_ref="source",
        sink_type="clickhouse",
        sink_connection_ref="sink",
        target_schema="analytics",
        staging_schema="staging",
        runtime_image="registry.example/dpone@sha256:" + "a" * 64,
    )
    workflow = DbtWorkflowProfile(
        name="daily",
        schedule=None,
        start_date="2026-01-01",
        timezone="UTC",
        owner="analytics",
        tags=(),
    )

    class Reader:
        def read(
            self,
            path: str,
        ) -> tuple[CanonicalDbtManifestArtifact, tuple[DbtPublishIssue, ...]]:
            return artifact, ()

    class Resolver:
        def resolve(
            self,
            candidate: CanonicalDbtModelArtifact,
        ) -> tuple[CanonicalDbtPublishIntent, tuple[DbtPublishIssue, ...]]:
            return intent, ()

    class OldModelCompiler:
        def compile(
            self,
            candidate: CanonicalDbtModelArtifact,
            publish_intent: CanonicalDbtPublishIntent,
            publish_profile: CanonicalDbtPublishProfile,
        ) -> CanonicalCompiledDbtModel:
            return CanonicalCompiledDbtModel(
                model=candidate,
                intent=publish_intent,
                profile=publish_profile,
                strategy={"mode": "incremental_merge"},
                physical_design={},
                workload_id="dbt_daily_orders",
                manifest={"name": "dbt_daily_orders"},
            )

    class Registry:
        def profile(self, name: str) -> CanonicalDbtPublishProfile | None:
            return profile if name == profile.name else None

        def workflow(self, name: str) -> DbtWorkflowProfile | None:
            return workflow if name == workflow.name else None

        def strategy_policy(self, name: str) -> DbtPublishStrategyPolicy | None:
            if name != profile.name:
                return None
            return DbtPublishStrategyPolicy(allowed_strategies=("incremental_merge",))

    class Loader:
        def load(self, manifest_path: str, explicit_path: str | None = None) -> tuple[Registry, tuple[()]]:
            return Registry(), ()

    class OldRouteCapabilities:
        def resolve(
            self,
            *,
            source: str,
            sink: str,
            strategy: str,
            path: str,
        ) -> tuple[dict[str, object], tuple[()]]:
            return {
                "snapshot_id": "sha256:" + "b" * 64,
                "route_id": f"{source}:{sink}:{strategy}",
                "support": "supported",
                "variant_id": "mssql:clickhouse:incremental_merge|native|strict|kpo",
                "transport": "native",
                "schema_evolution": "strict",
                "airflow_runtime_mode": "kpo",
                "certification_level": "production-certified",
                "evidence_status": "PASS",
                "evidence_refs": ["sha256:" + "c" * 64],
                "evidence_reason_codes": [],
            }, ()

    report = legacy.DbtDponeCompiler(
        reader=Reader(),
        resolver=Resolver(),
        model_compiler=OldModelCompiler(),
        profile_loader=Loader(),
        route_capabilities=OldRouteCapabilities(),
    ).build("target/manifest.json")

    assert report.passed
    assert report.models[0].route_capability["certification_level"] == "experimental"
    assert report.models[0].route_capability["evidence_status"] == "UNVERIFIED"
    assert report.models[0].route_capability["evidence_refs"] == []
    assert "legacy_dbt_publish_preview_only" in report.models[0].route_capability["evidence_reason_codes"]
    assert (
        dbt_release_authority_violation(
            {
                "selection_authority": DBT_SELECTION_AUTHORITY,
                "producer": {
                    "dpone_version": "0.73.20",
                    "wire_contract": DBT_RELEASE_WIRE_CONTRACT,
                },
                "provenance": {
                    "source_snapshot_sha256": "sha256:" + "d" * 64,
                    "selection_fingerprints": ["sha256:" + "e" * 64],
                    "route_certifications": [report.models[0].route_capability],
                },
            }
        )
        == "publishable dbt releases require current production-certified route evidence"
    )


def test_legacy_planner_two_argument_strategy_shape_executes() -> None:
    legacy = importlib.import_module("dpone.dbt_publish.planning")
    model = _canonical_model()
    intent = CanonicalDbtPublishIntent(
        enabled=True,
        profile="mart",
        workflow="daily",
        strategy_mode="incremental_merge",
    )

    strategy, issues = legacy.DbtPublishPlanner().strategy(model, intent)

    assert strategy["mode"] == "incremental_merge"
    assert strategy["unique_key"] == ["order_id"]
    assert issues == ()


def test_legacy_execution_pack_builder_accepts_workflow_and_is_preview_only() -> None:
    legacy = importlib.import_module("dpone.dbt_publish.artifact_writer")
    workflow = SimpleNamespace(
        workflow="daily",
        models=(
            SimpleNamespace(
                model=SimpleNamespace(name="orders", group="analytics"),
                profile=SimpleNamespace(
                    runtime={"namespace": "data"},
                    runtime_image="registry.example/dpone@sha256:" + "a" * 64,
                ),
            ),
        ),
    )

    pack = legacy.DbtExecutionPackBuilder().build(workflow)

    assert pack["runtime_artifact_delivery"] == {"mode": "local_preview"}
    assert pack["compatibility"]["authority"] == "none"
    assert pack["runtime_command"] == ""
    assert "provider_execution" not in pack


def test_legacy_artifact_writer_constructor_keeps_preview_only_authority(tmp_path: Path) -> None:
    legacy = importlib.import_module("dpone.dbt_publish.artifact_writer")
    report = build_dbt_dpone_compiler().build(
        _DEMO / "fixtures" / "manifest.v12.json",
        profiles_path=_DEMO / "dpone" / "dbt-publish-profiles.yml",
    )

    written = legacy.DbtArtifactWriter().write(
        report,
        tmp_path / "compiled",
        project_root=_DEMO,
    )

    assert written.passed
    assert legacy.DbtArtifactWriter.compatibility_mode == "local_preview"
    release = json.loads((tmp_path / "compiled" / "release-set.json").read_text(encoding="utf-8"))
    assert release["selection_authority"] == "manifest_preview"
    assert dbt_release_authority_violation(release) == "publishable dbt releases require dbt-authoritative selection"


def test_canonical_production_services_keep_strict_call_shapes() -> None:
    model = _canonical_model()
    intent = CanonicalDbtPublishIntent(True, "mart", "daily")

    with pytest.raises(TypeError):
        CanonicalDbtDponeCompiler()  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        CanonicalDbtPublishPlanner().strategy(model, intent)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        CanonicalDbtAirflowExecutionPackBuilder().build(SimpleNamespace())  # type: ignore[arg-type]


def test_legacy_facade_does_not_warn_before_deprecation_start() -> None:
    modules = ", ".join(repr(f"dpone.dbt_publish.{name}") for name in _MODULE_MIGRATIONS)
    script = f"""
import importlib
import warnings
from unittest.mock import patch

with patch("dpone.contracts.dbt_publish_compatibility.installed_version", return_value="0.73.20"):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        for module_name in ({modules},):
            importlib.import_module(module_name)

assert not [item for item in caught if item.category is DeprecationWarning], caught
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=_REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_legacy_facade_emits_one_process_wide_deprecation_warning_from_start_version() -> None:
    modules = ", ".join(repr(f"dpone.dbt_publish.{name}") for name in _MODULE_MIGRATIONS)
    script = f"""
import importlib
import warnings
from unittest.mock import patch

with patch("dpone.contracts.dbt_publish_compatibility.installed_version", return_value="0.73.21"):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        for module_name in ({modules},):
            importlib.import_module(module_name)

deprecations = [item for item in caught if item.category is DeprecationWarning]
assert len(deprecations) == 1, deprecations
assert "dpone.dbt_publish" in str(deprecations[0].message)
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=_REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_canonical_imports_do_not_load_legacy_facade() -> None:
    modules = ", ".join(repr(name) for name in _MODULE_MIGRATIONS.values())
    script = f"""
import importlib
import sys
import warnings

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always", DeprecationWarning)
    for module_name in ({modules},):
        importlib.import_module(module_name)

assert "dpone.dbt_publish" not in sys.modules
assert not [item for item in caught if item.category is DeprecationWarning], caught
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=_REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_deprecation_window_is_release_and_calendar_gated() -> None:
    assert DBT_PUBLISH_DEPRECATION_START_VERSION == "0.73.21"
    assert DBT_PUBLISH_MINIMUM_MINOR_RELEASES == 2
    assert DBT_PUBLISH_EARLIEST_REMOVAL_VERSION == "0.75.0"
    assert DBT_PUBLISH_MINIMUM_SUPPORT_MONTHS == 12

    compatibility = (_REPOSITORY_ROOT / "docs" / "compatibility.md").read_text(encoding="utf-8")
    reference = (_REPOSITORY_ROOT / "docs" / "dbt-self-service-reference.md").read_text(encoding="utf-8")
    for document in (compatibility, reference):
        assert "two subsequent minor releases" in document or "two later minor releases" in document
        assert "12 months" in document
        assert "later condition wins" in document


def test_raw_physical_override_deprecation_boundary_matches_docs() -> None:
    assert DBT_RAW_PHYSICAL_OVERRIDE_DEPRECATION_START_VERSION == "0.73.21"
    assert DBT_RAW_PHYSICAL_OVERRIDE_MINIMUM_DEPRECATION_RELEASES == 1
    assert DBT_RAW_PHYSICAL_OVERRIDE_EARLIEST_REMOVAL_VERSION == "0.74.0"

    compatibility = (_REPOSITORY_ROOT / "docs" / "compatibility.md").read_text(encoding="utf-8")
    reference = (_REPOSITORY_ROOT / "docs" / "dbt-self-service-reference.md").read_text(encoding="utf-8")
    for document in (compatibility, reference):
        assert "Raw physical-design override deprecation starts in `0.73.21`" in document
        assert "earliest removal is `0.74.0`" in document
        assert "named `physical_design.profile`" in document


def test_runtime_compatibility_facades_resolve_canonical_implementations_lazily() -> None:
    legacy_execution = importlib.import_module("dpone.runtime.dbt_execution")
    execution = importlib.import_module("dpone.runtime.dbt_execution_service")
    legacy_profiles = importlib.import_module("dpone.adapters.dbt_profile_files")
    profiles = importlib.import_module("dpone.adapters.dbt_runtime_profile")

    assert legacy_execution.DbtExecutionService is execution.DbtExecutionService
    assert legacy_execution.DbtExecutionInterval is execution.DbtExecutionInterval
    assert legacy_profiles.TemporaryDbtProfileStore is profiles.TemporaryDbtProfileStore
