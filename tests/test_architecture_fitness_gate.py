from __future__ import annotations

import logging
from argparse import Namespace
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

import pytest

from dpone.app.context import AppContext
from dpone.metrics.architecture_fitness import ArchitectureFitnessThresholds, analyze_architecture_fitness
from dpone.metrics.import_graph import collect_internal_deps
from dpone.metrics.python_imports import build_module_files
from dpone.services.docs.check_architecture_fitness_service import (
    CheckArchitectureFitnessService,
    _resolve_max_avg_clustering,
)


@pytest.fixture(scope="module")
def repository_deps() -> Mapping[str, frozenset[str]]:
    """Scan fixed repository sources once per module, with no cross-run cache.

    Every dependency assertion consumes the full graph. Both mapping and edges
    are immutable so an earlier test cannot weaken a later assertion. Temporary
    repository and threshold tests retain their independent analysis calls.
    """

    package_root = Path("src/dpone")
    deps = collect_internal_deps(
        package_root,
        module_files=[module.path for module in build_module_files(package_root, package_name="dpone")],
        package_name="dpone",
    )
    return MappingProxyType({module: frozenset(edges) for module, edges in deps.items()})


def test_repository_dependency_snapshot_is_shared_and_immutable(
    repository_deps: Mapping[str, frozenset[str]], request: pytest.FixtureRequest
) -> None:
    assert request.getfixturevalue("repository_deps") is repository_deps
    assert isinstance(repository_deps, MappingProxyType)
    assert repository_deps
    assert all(isinstance(edges, frozenset) for edges in repository_deps.values())


def test_architecture_fitness_detects_large_class_without_failing_by_default(tmp_path: Path) -> None:
    package = tmp_path / "dpone"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    methods = "\n".join(f"    def method_{idx}(self):\n        return {idx}" for idx in range(5))
    (package / "large_class.py").write_text(f"class LargeClass:\n{methods}\n", encoding="utf-8")

    report = analyze_architecture_fitness(
        package,
        repo_root=tmp_path,
        thresholds=ArchitectureFitnessThresholds(max_class_methods=2),
    )

    assert report.ok
    assert any(finding.class_name == "LargeClass" for finding in report.class_findings)


def test_architecture_fitness_can_fail_on_class_responsibility(tmp_path: Path) -> None:
    package = tmp_path / "dpone"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "large_class.py").write_text(
        "class LargeClass:\n    def one(self):\n        return 1\n    def two(self):\n        return 2\n",
        encoding="utf-8",
    )

    report = analyze_architecture_fitness(
        package,
        repo_root=tmp_path,
        thresholds=ArchitectureFitnessThresholds(max_class_methods=1, fail_on_class_warnings=True),
    )

    assert not report.ok
    assert report.class_findings[0].severity == "error"


def test_architecture_fitness_service_passes_current_repo_defaults() -> None:
    args = Namespace(
        package="src/dpone",
        top=20,
        target_avg_clustering=0.18,
        max_avg_clustering=0.25,
        max_cross_layer_ratio=0.35,
        target_module_ce=40,
        max_module_ce=60,
        max_class_methods=24,
        max_class_loc=450,
        fail_on_class_warnings=False,
        format="json",
    )

    exit_code, payload = CheckArchitectureFitnessService(ctx=AppContext.from_env(logger=logging.getLogger("test"))).run(
        args
    )

    assert exit_code == 0
    assert isinstance(payload, dict)
    assert payload["ok"] is True


def test_architecture_fitness_service_uses_repository_clustering_budget_by_default(tmp_path: Path) -> None:
    budget_dir = tmp_path / "docs" / "benchmarks"
    budget_dir.mkdir(parents=True)
    (budget_dir / "quality_budgets.yml").write_text(
        "global:\n  max_avg_clustering: 0.123\n",
        encoding="utf-8",
    )

    assert _resolve_max_avg_clustering(repo_root=tmp_path, explicit_value=None) == 0.123
    assert _resolve_max_avg_clustering(repo_root=tmp_path, explicit_value=0.2) == 0.2


def test_architecture_fitness_current_repo_stays_inside_green_clustering_target() -> None:
    max_clustering = _resolve_max_avg_clustering(repo_root=Path.cwd(), explicit_value=None)
    report = analyze_architecture_fitness(
        Path("src/dpone"),
        repo_root=Path.cwd(),
        thresholds=ArchitectureFitnessThresholds(
            target_avg_clustering=0.18,
            max_avg_clustering=max_clustering,
        ),
        top_n=20,
    )

    assert report.avg_clustering <= max_clustering


def test_architecture_fitness_current_repo_stays_inside_pre_release_cross_layer_budget() -> None:
    max_clustering = _resolve_max_avg_clustering(repo_root=Path.cwd(), explicit_value=None)
    report = analyze_architecture_fitness(
        Path("src/dpone"),
        repo_root=Path.cwd(),
        thresholds=ArchitectureFitnessThresholds(
            target_avg_clustering=0.18,
            max_avg_clustering=max_clustering,
            max_cross_layer_ratio=0.300,
        ),
        top_n=20,
    )

    assert report.avg_clustering <= max_clustering
    assert report.cross_layer_ratio <= 0.300
    assert report.ok is True


def test_architecture_fitness_current_repo_has_no_class_responsibility_warnings() -> None:
    report = analyze_architecture_fitness(
        Path("src/dpone"),
        repo_root=Path.cwd(),
        thresholds=ArchitectureFitnessThresholds(),
        top_n=20,
    )

    assert report.class_findings == ()


def test_etl_processor_stays_below_target_module_fanout(repository_deps: Mapping[str, frozenset[str]]) -> None:
    deps = repository_deps

    assert len(deps["dpone.runtime.etl.processor"]) <= 11


def test_nested_load_service_stays_below_target_module_fanout(repository_deps: Mapping[str, frozenset[str]]) -> None:
    deps = repository_deps

    assert len(deps["dpone.runtime.etl.nested_load"]) <= 12


def test_mssql_source_strategies_stay_below_target_module_fanout(repository_deps: Mapping[str, frozenset[str]]) -> None:
    deps = repository_deps

    assert len(deps["dpone.runtime.sources.strategies.mssql.mssql_strategies"]) <= 8


def test_mssql_source_strategy_facade_delegates_concrete_implementations() -> None:
    facade = Path("src/dpone/runtime/sources/strategies/mssql/mssql_strategies.py")
    text = facade.read_text(encoding="utf-8")

    assert len(text.splitlines()) <= 80
    forbidden_fragments = {
        "class MSSQLBaseExtractStrategy",
        "class MSSQLFullExtractStrategy",
        "class MSSQLIncrementalExtractStrategy",
        "def extract(",
        "def fetch_schema(",
        "def _database(",
        "def _build_select_query(",
        "def _get_max_column_value(",
    }
    offenders = [fragment for fragment in forbidden_fragments if fragment in text]
    assert offenders == []


def test_clickhouse_sink_stays_below_target_module_fanout(repository_deps: Mapping[str, frozenset[str]]) -> None:
    deps = repository_deps

    assert len(deps["dpone.runtime.sinks.clickhouse_sink"]) <= 14


def test_bigquery_sink_stays_below_target_module_fanout(repository_deps: Mapping[str, frozenset[str]]) -> None:
    deps = repository_deps

    assert len(deps["dpone.runtime.sinks.bigquery"]) <= 14


def test_bigquery_sink_delegates_concrete_strategy_wiring() -> None:
    bigquery_sink = Path("src/dpone/runtime/sinks/bigquery.py")
    forbidden_fragments = {
        "BigQueryFullRefreshStrategy",
        "BigQueryIncrementAppendStrategy",
        "BigQueryIncrementMergeStrategy",
        "BigQueryPartitionReplaceStrategy",
        "BigQueryReplaceStrategy",
        "BigQuerySCD2Strategy",
        "BigQuerySnapshotDiffStrategy",
        "BackfillStrategy",
    }
    offenders = [
        f"{bigquery_sink}:{idx}:{line.strip()}"
        for idx, line in enumerate(bigquery_sink.read_text(encoding="utf-8").splitlines(), 1)
        if any(fragment in line for fragment in forbidden_fragments)
    ]

    assert offenders == []


def test_postgres_sink_stays_below_target_module_fanout(repository_deps: Mapping[str, frozenset[str]]) -> None:
    deps = repository_deps

    assert len(deps["dpone.runtime.sinks.postgres"]) <= 14


def test_postgres_sink_delegates_concrete_strategy_wiring() -> None:
    postgres_sink = Path("src/dpone/runtime/sinks/postgres.py")
    forbidden_fragments = {
        "PostgresFullRefreshStrategy",
        "PostgresIncrementAppendStrategy",
        "PostgresIncrementMergeStrategy",
        "PostgresPartitionReplaceStrategy",
        "PostgresReplaceStrategy",
        "PostgresSCD2Strategy",
        "PostgresSnapshotDiffStrategy",
        "BackfillStrategy",
    }
    offenders = [
        f"{postgres_sink}:{idx}:{line.strip()}"
        for idx, line in enumerate(postgres_sink.read_text(encoding="utf-8").splitlines(), 1)
        if any(fragment in line for fragment in forbidden_fragments)
    ]

    assert offenders == []


def test_strategy_intelligence_package_facade_stays_lazy(repository_deps: Mapping[str, frozenset[str]]) -> None:
    deps = repository_deps
    facade = Path("src/dpone/strategy_intelligence/__init__.py")
    offenders = [
        f"{facade}:{idx}:{line.strip()}"
        for idx, line in enumerate(facade.read_text(encoding="utf-8").splitlines(), 1)
        if line.startswith("from dpone.strategy_intelligence.")
    ]

    assert len(deps["dpone.strategy_intelligence"]) <= 2
    assert offenders == []


def test_ops_command_handlers_use_catalog_boundary_only() -> None:
    handler_paths = [
        Path("src/dpone/services/ops/command_handlers_core.py"),
        Path("src/dpone/services/ops/command_handlers_artifacts.py"),
        Path("src/dpone/services/ops/command_handlers_release.py"),
        Path("src/dpone/services/ops/command_handlers_routes.py"),
    ]

    forbidden_imports: list[str] = []
    for path in handler_paths:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("from dpone.ops.") and "service_catalog" not in line:
                forbidden_imports.append(f"{path}: {line}")

    assert forbidden_imports == []


def test_commands_and_services_do_not_import_concrete_app_context() -> None:
    scanned_roots = [Path("src/dpone/commands"), Path("src/dpone/services")]
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(("from dpone.app.context import", "from ..app.context import")):
                    offenders.append(f"{path}:{idx}:{stripped}")
                if stripped.startswith(("from ...app.context import", "import dpone.app.context")):
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_commands_and_services_do_not_import_broad_output_facade() -> None:
    scanned_roots = [Path("src/dpone/commands"), Path("src/dpone/services")]
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(("from dpone.output import", "from ...output import")):
                    offenders.append(f"{path}:{idx}:{stripped}")
                if stripped.startswith(("from ..output import", "import dpone.output")):
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_runtime_code_uses_logging_port_instead_of_etl_logging_facade() -> None:
    scanned_roots = [Path("src/dpone/runtime"), Path("src/dpone/lib")]
    allowed_paths = {Path("src/dpone/runtime/etl_logging/__init__.py")}
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            if path in allowed_paths or Path("src/dpone/runtime/etl_logging") in path.parents:
                continue
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(("from dpone.runtime.etl_logging import", "import dpone.runtime.etl_logging")):
                    offenders.append(f"{path}:{idx}:{stripped}")
                if stripped.startswith(("from .etl_logging import", "from ..etl_logging import")):
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_runtime_code_uses_domain_logging_ports_instead_of_broad_logging_port() -> None:
    scanned_roots = [Path("src/dpone/runtime"), Path("src/dpone/lib")]
    allowed_paths = {
        Path("src/dpone/runtime/artifact_logging.py"),
        Path("src/dpone/runtime/connector_logging.py"),
        Path("src/dpone/runtime/logging_core.py"),
        Path("src/dpone/runtime/logging_port.py"),
        Path("src/dpone/runtime/process_logging.py"),
        Path("src/dpone/runtime/reconciliation_logging.py"),
        Path("src/dpone/runtime/sink_logging.py"),
        Path("src/dpone/runtime/state_logging.py"),
    }
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            if path in allowed_paths:
                continue
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(("from dpone.runtime.logging_port import", "import dpone.runtime.logging_port")):
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_runtime_code_uses_narrow_row_artifact_modules() -> None:
    scanned_roots = [Path("src/dpone/runtime")]
    allowed_paths = {
        Path("src/dpone/runtime/artifacts.py"),
        Path("src/dpone/runtime/row_artifacts.py"),
    }
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            if path in allowed_paths:
                continue
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(
                    ("from dpone.runtime.row_artifacts import", "import dpone.runtime.row_artifacts")
                ):
                    offenders.append(f"{path}:{idx}:{stripped}")
                if stripped.startswith(("from .row_artifacts import", "from ..row_artifacts import")):
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_runtime_code_uses_narrow_source_contract_modules() -> None:
    scanned_roots = [Path("src/dpone/runtime")]
    allowed_paths = {
        Path("src/dpone/runtime/sources/base.py"),
        Path("src/dpone/runtime/sources/__init__.py"),
    }
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            if path in allowed_paths:
                continue
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(("from dpone.runtime.sources.base import", "import dpone.runtime.sources.base")):
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_runtime_code_uses_narrow_sink_contract_modules() -> None:
    scanned_roots = [Path("src/dpone/runtime")]
    allowed_paths = {
        Path("src/dpone/runtime/sinks/base.py"),
        Path("src/dpone/runtime/sinks/__init__.py"),
    }
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            if path in allowed_paths:
                continue
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(("from dpone.runtime.sinks.base import", "import dpone.runtime.sinks.base")):
                    offenders.append(f"{path}:{idx}:{stripped}")
                if stripped.startswith("from dpone.runtime.sinks import ") and any(
                    symbol in stripped for symbol in ("AbstractSink", "LoadPayload", "LoadResult")
                ):
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_internal_code_imports_load_strategy_from_narrow_contract_module() -> None:
    scanned_roots = [Path("src/dpone")]
    allowed_paths = {
        Path("src/dpone/config/__init__.py"),
        Path("src/dpone/config/load_config.py"),
        Path("src/dpone/config/load_strategy.py"),
    }
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            if path in allowed_paths:
                continue
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith("from dpone.config import ") and "LoadStrategy" in stripped:
                    offenders.append(f"{path}:{idx}:{stripped}")
                if stripped.startswith("from dpone.config.load_config import ") and "LoadStrategy" in stripped:
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_etl_processor_delegates_strategy_and_source_state_policy() -> None:
    processor = Path("src/dpone/runtime/etl/processor.py")
    forbidden_fragments = {
        "from dpone.config.load_strategy import LoadStrategy",
        "from dpone.runtime.kafka.offsets import KafkaOffsetState",
        "from dpone.runtime.state import XMinState",
        "from dpone.runtime.state import RunStateStorage",
    }
    offenders: list[str] = []
    for idx, line in enumerate(processor.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()
        if stripped in forbidden_fragments:
            offenders.append(f"{processor}:{idx}:{stripped}")

    assert offenders == []


def test_command_registry_core_delegates_to_focused_registry_sections(
    repository_deps: Mapping[str, frozenset[str]],
) -> None:
    registry_core = Path("src/dpone/commands/registry_core.py")
    allowed_registry_modules = {
        "dpone.commands.base",
        "dpone.commands.registry_platform",
        "dpone.commands.registry_runtime",
        "dpone.commands.registry_top",
    }
    concrete_command_imports: list[str] = []
    deps = repository_deps

    for idx, line in enumerate(registry_core.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith(("from . import", "from dpone.commands import")):
            concrete_command_imports.append(f"{registry_core}:{idx}:{stripped}")

    unexpected_deps = set(deps.get("dpone.commands.registry_core", set())) - allowed_registry_modules

    assert concrete_command_imports == []
    assert unexpected_deps == set()


def test_docs_registry_delegates_to_focused_registry_sections(repository_deps: Mapping[str, frozenset[str]]) -> None:
    registry_docs = Path("src/dpone/commands/registry_docs.py")
    allowed_registry_modules = {
        "dpone.commands.base",
        "dpone.commands.func_command",
        "dpone.commands.registry_docs_checks",
        "dpone.commands.registry_docs_updates",
    }
    concrete_command_imports: list[str] = []
    deps = repository_deps

    for idx, line in enumerate(registry_docs.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith(("from .docs import", "from dpone.commands.docs import")):
            concrete_command_imports.append(f"{registry_docs}:{idx}:{stripped}")

    unexpected_deps = set(deps.get("dpone.commands.registry_docs", set())) - allowed_registry_modules

    assert concrete_command_imports == []
    assert unexpected_deps == set()


def test_ops_catalog_facades_delegate_to_focused_catalog_sections(
    repository_deps: Mapping[str, frozenset[str]],
) -> None:
    deps = repository_deps
    allowed_deps = {
        "dpone.ops.catalog_artifacts": {
            "dpone.ops.catalog_artifacts_certification",
            "dpone.ops.catalog_artifacts_credentials",
            "dpone.ops.catalog_artifacts_documentation",
            "dpone.ops.catalog_artifacts_lineage",
            "dpone.ops.catalog_artifacts_runtime",
            "dpone.ops.catalog_evidence",
            "dpone.ops.catalog_protocols",
        },
        "dpone.ops.catalog_release": {
            "dpone.ops.catalog_protocols",
            "dpone.ops.catalog_readiness",
            "dpone.ops.catalog_release_gates",
            "dpone.ops.catalog_release_governance",
            "dpone.ops.catalog_release_risk",
            "dpone.ops.catalog_release_sections",
            "dpone.ops.catalog_route_conformance",
            "dpone.ops.catalog_route_onboarding",
        },
    }
    unexpected_deps = {
        module: sorted(set(deps.get(module, set())) - allowed)
        for module, allowed in allowed_deps.items()
        if set(deps.get(module, set())) - allowed
    }

    assert unexpected_deps == {}


def test_core_and_cdc_ops_catalogs_delegate_to_focused_catalog_sections(
    repository_deps: Mapping[str, frozenset[str]],
) -> None:
    deps = repository_deps
    allowed_deps = {
        "dpone.ops.catalog_core": {
            "dpone.ops.catalog_core_certification",
            "dpone.ops.catalog_core_contracts",
            "dpone.ops.catalog_core_evidence",
            "dpone.ops.catalog_core_package",
            "dpone.ops.catalog_core_rollback",
            "dpone.ops.catalog_protocols",
        },
        "dpone.ops.catalog_cdc": {
            "dpone.ops.catalog_cdc_evidence",
            "dpone.ops.catalog_cdc_materialization",
            "dpone.ops.catalog_cdc_recovery",
            "dpone.ops.catalog_cdc_runtime",
            "dpone.ops.catalog_protocols",
        },
    }
    unexpected_deps = {
        module: sorted(set(deps.get(module, set())) - allowed)
        for module, allowed in allowed_deps.items()
        if set(deps.get(module, set())) - allowed
    }

    assert unexpected_deps == {}


def test_readiness_ops_catalog_delegates_to_functional_subcatalog_sections(
    repository_deps: Mapping[str, frozenset[str]],
) -> None:
    deps = repository_deps
    section_modules = {
        "dpone.ops.catalog_readiness_assessment",
        "dpone.ops.catalog_readiness_certification",
        "dpone.ops.catalog_readiness_refresh",
        "dpone.ops.catalog_readiness_state",
    }

    assert set(deps.get("dpone.ops.catalog_readiness", set())) <= section_modules
    assert len(deps.get("dpone.ops.catalog_readiness", set())) <= 10
    assert section_modules <= set(deps)
    assert {
        module: len(deps.get(module, set())) for module in section_modules if len(deps.get(module, set())) > 10
    } == {}


def test_nested_certification_readiness_checks_delegate_runtime_mechanics_to_runtime_suite(
    repository_deps: Mapping[str, frozenset[str]],
) -> None:
    deps = repository_deps
    allowed_deps = {
        "dpone.readiness.nested_live_certification",
        "dpone.runtime.normalization.certification_checks",
    }

    assert set(deps.get("dpone.readiness.nested_certification_checks", set())) <= allowed_deps


def test_cdc_ops_services_delegate_runtime_wiring_to_shared_adapter_boundary(
    repository_deps: Mapping[str, frozenset[str]],
) -> None:
    deps = repository_deps
    forbidden_direct_deps = {
        "dpone.runtime.cdc.compare_readers",
        "dpone.runtime.cdc.live_adapters",
        "dpone.runtime.cdc.local_runtime",
        "dpone.runtime.cdc.retention_probes",
        "dpone.runtime.cdc.runtime_models",
        "dpone.runtime.credentials.config",
        "dpone.runtime.credentials.factory",
    }

    assert set(deps.get("dpone.ops.cdc.retention_resync", set())) & forbidden_direct_deps == set()
    assert set(deps.get("dpone.ops.cdc.compare_repair", set())) & forbidden_direct_deps == set()


def test_runtime_bootstrap_delegates_to_focused_bootstrap_sections(
    repository_deps: Mapping[str, frozenset[str]],
) -> None:
    deps = repository_deps
    allowed_deps = {
        "dpone.ports.process_runner",
        "dpone.ports.runtime_hydrator",
        "dpone.runtime.bootstrap_hydrator",
        "dpone.runtime.bootstrap_runner",
    }
    unexpected_deps = set(deps.get("dpone.runtime.bootstrap", set())) - allowed_deps

    assert unexpected_deps == set()


def test_runtime_code_uses_named_implementation_modules_instead_of_impl_suffixes() -> None:
    scanned_roots = [
        Path("src/dpone/runtime/cdc"),
        Path("src/dpone/runtime/connectors"),
        Path("src/dpone/runtime/sinks"),
        Path("src/dpone/runtime/sources"),
    ]
    allowed_shims = {
        Path("src/dpone/runtime/cdc/postgres_impl.py"),
        Path("src/dpone/runtime/connectors/api/appsflyer_impl.py"),
        Path("src/dpone/runtime/connectors/api/google_sheets_impl.py"),
        Path("src/dpone/runtime/connectors/bigquery_impl.py"),
        Path("src/dpone/runtime/sinks/clickhouse_impl.py"),
        Path("src/dpone/runtime/sinks/strategies/mssql/mssql_strategies_impl.py"),
        Path("src/dpone/runtime/sources/strategies/api/yandex_webmaster/common_impl.py"),
        Path("src/dpone/runtime/sources/strategies/postgres/postgres_base_impl.py"),
    }
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            if path in allowed_shims:
                continue
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if "_impl" in stripped and (
                    stripped.startswith(("from dpone.runtime.", "import dpone.runtime."))
                    or stripped.startswith(("from .", "from ..", "from ..."))
                ):
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_internal_code_uses_narrow_error_modules_instead_of_errors_facade() -> None:
    scanned_roots = [Path("src/dpone")]
    allowed_paths = {
        Path("src/dpone/contracts/errors.py"),
        Path("src/dpone/core/errors.py"),
    }
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            if path in allowed_paths:
                continue
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(("from dpone.contracts.errors import", "import dpone.contracts.errors")):
                    offenders.append(f"{path}:{idx}:{stripped}")
                if stripped.startswith(
                    (
                        "from .contracts.errors import",
                        "from ..contracts.errors import",
                        "from ...contracts.errors import",
                    )
                ):
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_domain_code_uses_domain_configuration_errors() -> None:
    scanned_roots = [Path("src/dpone")]
    allowed_paths = {
        Path("src/dpone/cli/main.py"),
        Path("src/dpone/contracts/__init__.py"),
        Path("src/dpone/contracts/configuration_errors.py"),
        Path("src/dpone/contracts/errors.py"),
        Path("src/dpone/dag/errors.py"),
        Path("src/dpone/manifest/errors.py"),
        Path("src/dpone/runtime/errors.py"),
        Path("src/dpone/schema/errors.py"),
        Path("src/dpone/services/docs/errors.py"),
    }
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*.py"):
            if path in allowed_paths:
                continue
            for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith("from dpone.contracts.configuration_errors import ETLConfigurationError"):
                    offenders.append(f"{path}:{idx}:{stripped}")

    assert offenders == []


def test_legacy_transfer_config_aliases_are_only_documented_in_migration_appendix() -> None:
    scanned_roots = [Path("docs"), Path("examples")]
    allowed_paths = {Path("docs/migration/config-aliases.md")}
    legacy_aliases = (
        "bulk_mode",
        "bcp_batch_size",
        "bcp_packet_size",
        "bcp_timeout_seconds",
        "bcp_table_lock",
        "clickhouse_bulk_mode",
        "clickhouse_insert_settings",
        "clickhouse_http_host",
        "clickhouse_http_port",
        "parallel_load_workers",
        "partition_workers",
    )
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in {".md", ".yml", ".yaml"}:
                continue
            if path in allowed_paths:
                continue
            text = path.read_text(encoding="utf-8")
            for alias in legacy_aliases:
                if alias in text:
                    offenders.append(f"{path}: {alias}")

    assert offenders == []


def test_legacy_technical_column_names_are_isolated_to_compatibility_paths() -> None:
    scanned_roots = [Path("src"), Path("docs"), Path("examples")]
    allowed_paths = {
        Path("src/dpone/contracts/technical_columns.py"),
        Path("docs/load-lineage.md"),
    }
    legacy_tokens = (
        "meta__load_dtm",
        "meta__update_dtm",
        "meta__delete_dtm",
        "meta__xmin",
        "__dpone_deleted_marker",
        "__dpone_op",
        "__dpone_lsn",
        "__dpone_operation",
        "__dpone_version",
        "__dpone_kafka_key",
    )
    offenders: list[str] = []
    for root in scanned_roots:
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in {".py", ".md", ".json", ".yml", ".yaml"}:
                continue
            if path in allowed_paths:
                continue
            text = path.read_text(encoding="utf-8")
            for token in legacy_tokens:
                if token in text:
                    offenders.append(f"{path}: {token}")

    assert offenders == []
