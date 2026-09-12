"""Cold readiness imports must not activate unrelated planning dependencies."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


def _fresh(code: str) -> None:
    root = Path(__file__).resolve().parents[1]
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            str(root / path)
            for path in ("src", "packages/dpone-airflow-pack/src", "packages/dpone-native-accel/src", ".")
        ),
    }
    result = subprocess.run(
        [sys.executable, "-B", "-c", code], cwd=root, env=environment, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("module", ["dpone.readiness", "dpone.readiness.python_import_health"])
def test_cold_import_does_not_load_planning_or_routes(module: str) -> None:
    _fresh(f"""
import importlib, sys
importlib.import_module({module!r})
for prefix in ('dpone.readiness.managed', 'dpone.manifest', 'dpone.runtime.sources', 'dpone.ops.routes'):
    assert not any(name == prefix or name.startswith(prefix + '.') or name.startswith(prefix + '_')
                   for name in sys.modules), prefix
""")


# Captured from the pre-fix public initializer, independently of its lazy map.
EXPECTED_EXPORTS = {
    "CDCBackend": "dpone.readiness.cdc:CDCBackend",
    "CDCConfig": "dpone.readiness.cdc:CDCConfig",
    "CDCOffset": "dpone.readiness.cdc:CDCOffset",
    "build_mssql_cdc_enable_sql": "dpone.readiness.cdc:build_mssql_cdc_enable_sql",
    "build_mssql_change_tracking_enable_sql": "dpone.readiness.cdc:build_mssql_change_tracking_enable_sql",
    "build_postgres_slot_sql": "dpone.readiness.cdc:build_postgres_slot_sql",
    "CertificationMatrix": "dpone.readiness.certification:CertificationMatrix",
    "CertificationResult": "dpone.readiness.certification:CertificationResult",
    "CertificationStatus": "dpone.readiness.certification:CertificationStatus",
    "ColumnDef": "dpone.readiness.schema_evolution:ColumnDef",
    "DdlCapabilityRegistry": "dpone.readiness.ddl_governance:DdlCapabilityRegistry",
    "DdlGovernancePolicy": "dpone.readiness.ddl_governance:DdlGovernancePolicy",
    "GovernedDdlExecutor": "dpone.readiness.ddl_execution:GovernedDdlExecutor",
    "ExpandContractService": "dpone.readiness.schema_workflows:ExpandContractService",
    "SchemaChange": "dpone.readiness.schema_evolution:SchemaChange",
    "SchemaPlan": "dpone.readiness.schema_evolution:SchemaPlan",
    "ErrorClassifier": "dpone.readiness.observability:ErrorClassifier",
    "JsonPartitionManifestStore": "dpone.readiness.resumability:JsonPartitionManifestStore",
    "PartitionManifest": "dpone.readiness.resumability:PartitionManifest",
    "PartitionRunStatus": "dpone.readiness.resumability:PartitionRunStatus",
    "PipelineRunMetrics": "dpone.readiness.observability:PipelineRunMetrics",
    "ConnectorScaffoldService": "dpone.readiness.managed:ConnectorScaffoldService",
    "ExecutionPlanService": "dpone.readiness.managed:ExecutionPlanService",
    "PerformanceAdvisor": "dpone.readiness.managed:PerformanceAdvisor",
    "QualityService": "dpone.readiness.managed:QualityService",
    "RunArtifactWriter": "dpone.readiness.managed:RunArtifactWriter",
    "StateInspectorService": "dpone.readiness.managed:StateInspectorService",
    "OnlineSchemaPlanner": "dpone.readiness.ddl_governance:OnlineSchemaPlanner",
    "SchemaChangeLedger": "dpone.readiness.ddl_governance:SchemaChangeLedger",
    "SchemaApprovalService": "dpone.readiness.schema_workflows:SchemaApprovalService",
    "SchemaHistoryRegistry": "dpone.readiness.schema_history:SchemaHistoryRegistry",
    "SchemaNotificationService": "dpone.readiness.schema_notifications:SchemaNotificationService",
    "SchemaComparator": "dpone.readiness.schema_evolution:SchemaComparator",
    "SchemaEvolutionPolicy": "dpone.readiness.schema_evolution:SchemaEvolutionPolicy",
}

EXPECTED_SUBMODULES = (
    "cdc",
    "certification",
    "ddl_execution",
    "ddl_governance",
    "ddl_policy_decisions",
    "managed",
    "managed_artifacts",
    "managed_bulk_path",
    "managed_models",
    "managed_native_projection",
    "managed_native_transfer_plan",
    "managed_performance",
    "managed_plan_warnings",
    "managed_planning",
    "managed_planning_r1",
    "managed_planning_snapshot",
    "managed_quality",
    "managed_scaffold",
    "managed_source_impact",
    "managed_state",
    "managed_templates",
    "managed_utils",
    "mssql_native_planning",
    "native_snapshot_planning",
    "observability",
    "physical_design",
    "physical_design_models",
    "physical_reconciliation_approval",
    "postgres_mssql_correctness_profile",
    "postgres_mssql_correctness_route",
    "resolved_process_route",
    "resumability",
    "schema_contracts",
    "schema_evolution",
    "schema_evolution_ddl",
    "schema_evolution_models",
    "schema_history",
    "schema_notifications",
    "schema_type_compatibility",
    "schema_workflows",
    "studio_ui_assets",
    "target_type_resolvers",
)


def test_ordered_exports_owner_identity_type_hints_and_pickle() -> None:
    _fresh(f"""
import importlib, pickle, typing
import dpone.readiness as readiness
expected = {EXPECTED_EXPORTS!r}
assert readiness.__all__ == list(expected)
for name, target in expected.items():
    module, attribute = target.split(':')
    value = getattr(readiness, name)
    owner = getattr(importlib.import_module(module), attribute)
    assert value is owner and getattr(readiness, name) is owner
    assert readiness.__dict__[name] is owner
    assert pickle.loads(pickle.dumps(value)) is owner
    typing.get_type_hints(value)
assert typing.get_type_hints(readiness.CDCConfig)['backend'] is readiness.CDCBackend
value = readiness.CDCOffset(readiness.CDCBackend.POSTGRES_LOGICAL, 'sample-offset')
assert pickle.loads(pickle.dumps(value)) == value
assert value.to_state()['backend'] == 'postgres_logical'
namespace = {{}}
exec('from dpone.readiness import *', namespace)
assert {{name for name in namespace if not name.startswith('__')}} == set(expected)
assert all(namespace[name] is getattr(readiness, name) for name in expected)
""")


def test_dir_and_unknown_attributes_do_not_load_submodules() -> None:
    _fresh(f"""
import sys
import dpone.readiness as readiness
before = set(sys.modules)
assert set({tuple(EXPECTED_EXPORTS)!r}) | set({EXPECTED_SUBMODULES!r}) <= set(dir(readiness))
assert set(sys.modules) == before
for name in ('not_a_readiness_export', '__missing_readiness_attribute__'):
    try:
        getattr(readiness, name)
    except AttributeError as error:
        assert str(error) == f"module 'dpone.readiness' has no attribute {{name!r}}"
    else:
        raise AssertionError(name)
assert set(sys.modules) == before
""")


def test_previously_exposed_submodules_and_normal_from_import_fallback() -> None:
    _fresh(f"""
import importlib, types
import dpone.readiness as readiness
for name in {EXPECTED_SUBMODULES!r}:
    value = getattr(readiness, name)
    assert isinstance(value, types.ModuleType)
    assert value is importlib.import_module('dpone.readiness.' + name)
    assert getattr(readiness, name) is value
from dpone.readiness import python_import_startup_state
assert python_import_startup_state is importlib.import_module('dpone.readiness.python_import_startup_state')
""")


def test_concurrent_first_access_preserves_defining_objects() -> None:
    _fresh("""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import dpone.readiness as readiness
barrier = Barrier(8)
def first_access(_):
    barrier.wait(timeout=10)
    return readiness.ExecutionPlanService, readiness.managed
with ThreadPoolExecutor(max_workers=8) as executor:
    results = list(executor.map(first_access, range(8)))
from dpone.readiness.managed_planning import ExecutionPlanService
assert all(service is ExecutionPlanService and module is readiness.managed for service, module in results)
""")
