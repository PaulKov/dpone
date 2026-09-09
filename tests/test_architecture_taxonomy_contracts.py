from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _imports_for(path: str) -> set[str]:
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports


def test_bigquery_tech_tables_is_compatibility_facade() -> None:
    from dpone.runtime.reconciliation.bigquery import BigQueryReconciliationStore
    from dpone.runtime.reconciliation.tech_tables import BigQueryTechTables

    assert issubclass(BigQueryTechTables, BigQueryReconciliationStore)


def test_generic_reconciliation_modules_do_not_import_vendor_adapters() -> None:
    generic_paths = [
        "src/dpone/runtime/reconciliation/base.py",
        "src/dpone/runtime/reconciliation/manager.py",
        "src/dpone/runtime/reconciliation/tech_tables.py",
        "src/dpone/runtime/reconciliation/soft_delete/registry.py",
    ]
    forbidden = {
        "google.cloud",
        "google.cloud.bigquery",
        "google.cloud.exceptions",
        "dpone.runtime.connectors.bigquery",
        "dpone.runtime.reconciliation.soft_delete.bigquery",
        "dpone.runtime.reconciliation.soft_delete.clickhouse",
        "dpone.runtime.reconciliation.soft_delete.mssql",
        "dpone.runtime.reconciliation.soft_delete.postgres",
    }

    for path in generic_paths:
        assert not (_imports_for(path) & forbidden), path


def test_type_mapper_is_compatibility_facade_for_type_mapping_package() -> None:
    source = (ROOT / "src/dpone/runtime/support/data_type_mapper.py").read_text(encoding="utf-8")

    assert "from dpone.runtime.support.type_mapping" in source
    assert "class _PgTypeParser" not in source
    assert "class _ClickHouseTypeParser" not in source
    assert "class _BigQueryDialect" not in source
    assert "class DataTypeMapper" not in source


def test_api_base_is_compatibility_facade_for_api_infrastructure_modules() -> None:
    source = (ROOT / "src/dpone/runtime/connectors/api/base.py").read_text(encoding="utf-8")

    assert '"AbstractAPIConnector": "dpone.runtime.connectors.api.connector:AbstractAPIConnector"' in source
    assert "class ThreadSafeRateLimiter" not in source
    assert "class ParallelTaskExecutor" not in source
    assert "class AbstractAPIConnector" not in source

    from dpone.runtime.connectors.api.base import AbstractAPIConnector, APICredentials, PaginationConfig

    assert AbstractAPIConnector.__name__ == "AbstractAPIConnector"
    assert APICredentials(endpoint="https://api.example.com").endpoint == "https://api.example.com"
    assert PaginationConfig().strategy == "page"


def test_api_registry_loads_declarative_provider_specs_from_adapter_package() -> None:
    source = (ROOT / "src/dpone/runtime/api_registry.py").read_text(encoding="utf-8")

    assert "from dpone.runtime.api_providers.specs import load_runtime_specs" in source
    assert "def _omnidesk_kwargs" not in source
    assert "def _appsflyer_kwargs" not in source
    assert "def _fasttrack_kwargs" not in source


def test_nullability_taxonomy_docs_define_reusable_contract() -> None:
    docs = (ROOT / "docs/developer-type-system.md").read_text(encoding="utf-8")

    assert "## Source-agnostic nullability contract" in docs
    assert "`NullabilityPolicy`" in docs
    assert "`TargetNullabilityDialect`" in docs
    assert "source mapper returns a target type" in docs
    assert "ClickHouseNullInsertPolicy remains ClickHouse-specific" in docs


def test_clickhouse_nullable_key_docs_define_escape_hatch_and_best_practice() -> None:
    docs = "\n".join(
        [
            (ROOT / "docs/clickhouse.md").read_text(encoding="utf-8"),
            (ROOT / "docs/physical-design.md").read_text(encoding="utf-8"),
            (ROOT / "docs/source-sink/mssql-to-clickhouse.md").read_text(encoding="utf-8"),
            (ROOT / "docs/developer-type-system.md").read_text(encoding="utf-8"),
        ]
    )

    assert "allow_nullable_key" in docs
    assert "escape hatch" in docs
    assert "strongly discouraged" in docs
    assert "NULLS_LAST" in docs
    assert "non_nullable_by_default" in docs
    assert "https://clickhouse.com/docs/operations/settings/merge-tree-settings#allow_nullable_key" in docs
    assert (
        "https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree#primary-keys-and-indexes-in-queries"
        in docs
    )


def test_target_table_settings_docs_define_product_grade_taxonomy() -> None:
    docs = "\n".join(
        [
            (ROOT / "docs/developer-type-system.md").read_text(encoding="utf-8"),
            (ROOT / "docs/physical-design.md").read_text(encoding="utf-8"),
        ]
    )

    assert "Physical Design Target Settings Contract" in docs
    assert "TableSettingsOptions" in docs
    assert "PhysicalSettingRegistry" in docs
    assert "PhysicalSettingClassifier" in docs
    assert "TargetTableSettingsDialect" in docs
    assert "ClickHouseTableSettingsDialect" in docs
    assert "wrong-context" in docs
    assert "clickhouse_bulk.insert_settings" in docs


def test_physical_reconciliation_docs_define_reusable_contract() -> None:
    docs = "\n".join(
        [
            (ROOT / "docs/developer-type-system.md").read_text(encoding="utf-8"),
            (ROOT / "docs/physical-design.md").read_text(encoding="utf-8"),
            (ROOT / "docs/clickhouse.md").read_text(encoding="utf-8"),
        ]
    )

    assert "Physical Design Drift & Reconciliation Contract" in docs
    assert "TargetPhysicalIntrospector" in docs
    assert "PhysicalDesignDriftDetector" in docs
    assert "PhysicalDesignReconciler" in docs
    assert "TargetPhysicalMigrationDialect" in docs
    assert "schema physical-diff" in docs
    assert "auto_safe" in docs
    assert "ALTER TABLE ... MODIFY SETTING" in docs


def test_shadow_migration_taxonomy_is_generic_with_clickhouse_dialect_adapter() -> None:
    docs = "\n".join(
        [
            (ROOT / "docs/developer-schema-migration-control.md").read_text(encoding="utf-8"),
            (ROOT / "docs/schema-migration-control.md").read_text(encoding="utf-8"),
            (ROOT / "docs/architecture.md").read_text(encoding="utf-8"),
        ]
    )
    imports = _imports_for("src/dpone/readiness/shadow_migration.py")

    assert "ShadowMigrationPlanner" in docs
    assert "TargetShadowMigrationDialect" in docs
    assert "ClickHouseShadowMigrationDialect" in docs
    assert "ColumnProjectionPlanner" in docs
    assert "EXCHANGE TABLES" in docs
    assert "contract" in docs
    assert "dpone.runtime.sinks.clickhouse_shadow_migration" not in imports
