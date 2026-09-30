"""Keep diagnostic imports independent of unrelated managed planning services."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.doctor_import_test_support import _run_outer_probe


@pytest.mark.parametrize("module", ["dpone.readiness", "dpone.readiness.python_import_health"])
def test_diagnostic_import_does_not_initialize_managed_planning(module: str, clean_doctor_probe_python: Path) -> None:
    """Eager service re-exports must not consume the import-health startup budget."""
    script = f"""
import importlib, json, sys
importlib.import_module({module!r})
print(json.dumps({{
    'planning_loaded': 'dpone.readiness.managed_planning' in sys.modules,
    'performance_loaded': 'dpone.readiness.managed_performance' in sys.modules,
}}))
"""
    result = _run_outer_probe(clean_doctor_probe_python, script)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"planning_loaded": False, "performance_loaded": False}


def test_managed_exports_keep_public_names_and_canonical_identity(clean_doctor_probe_python: Path) -> None:
    """Lazy attribute, named and star imports expose the original service objects."""
    script = """
import importlib
import sys
import dpone.readiness as readiness
expected = {
    'ConnectorScaffoldService': 'dpone.readiness.managed_scaffold',
    'ExecutionPlanService': 'dpone.readiness.managed_planning',
    'PerformanceAdvisor': 'dpone.readiness.managed_performance',
    'QualityService': 'dpone.readiness.managed_quality',
    'RunArtifactWriter': 'dpone.readiness.managed_artifacts',
    'StateInspectorService': 'dpone.readiness.managed_state',
}
assert readiness.__all__ == [
    'CDCBackend', 'CDCConfig', 'CDCOffset', 'build_mssql_cdc_enable_sql',
    'build_mssql_change_tracking_enable_sql', 'build_postgres_slot_sql',
    'CertificationMatrix', 'CertificationResult', 'CertificationStatus',
    'ColumnDef', 'DdlCapabilityRegistry', 'DdlGovernancePolicy',
    'GovernedDdlExecutor', 'ExpandContractService', 'SchemaChange', 'SchemaPlan',
    'ErrorClassifier', 'JsonPartitionManifestStore', 'PartitionManifest',
    'PartitionRunStatus', 'PipelineRunMetrics', 'ConnectorScaffoldService',
    'ExecutionPlanService', 'PerformanceAdvisor', 'QualityService',
    'RunArtifactWriter', 'StateInspectorService', 'OnlineSchemaPlanner',
    'SchemaChangeLedger', 'SchemaApprovalService', 'SchemaHistoryRegistry',
    'SchemaNotificationService', 'SchemaComparator', 'SchemaEvolutionPolicy',
]
assert expected.keys() <= set(dir(readiness))
assert not set(expected.values()).intersection(sys.modules)
assert readiness.managed is importlib.import_module('dpone.readiness.managed')
for name, module in expected.items():
    observed = getattr(readiness, name)
    canonical = getattr(importlib.import_module(module), name)
    assert observed is canonical
    assert getattr(readiness, name) is observed
    named = {}
    exec('from dpone.readiness import ' + name, named)
    assert named[name] is canonical
star = {}
exec('from dpone.readiness import *', star)
assert set(star) - {'__builtins__'} == set(readiness.__all__)
assert all(star[name] is getattr(readiness, name) for name in expected)
"""
    result = _run_outer_probe(clean_doctor_probe_python, script)

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize(
    "statement",
    ["from dpone.readiness import ExecutionPlanService", "from dpone.readiness import *"],
    ids=["named", "star"],
)
def test_import_forms_resolve_an_uncached_service(statement: str, clean_doctor_probe_python: Path) -> None:
    """Named and star imports must work before any package service is cached."""
    script = f"""
import dpone.readiness as readiness
assert 'ExecutionPlanService' not in vars(readiness)
namespace = {{}}
exec({statement!r}, namespace)
from dpone.readiness.managed_planning import ExecutionPlanService
assert namespace['ExecutionPlanService'] is ExecutionPlanService
assert readiness.ExecutionPlanService is ExecutionPlanService
"""
    result = _run_outer_probe(clean_doctor_probe_python, script)

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize("name", ["not_a_readiness_export", "InitBundleResult"])
def test_unknown_top_level_export_does_not_load_managed_planning(name: str, clean_doctor_probe_python: Path) -> None:
    """Unknown names must not leak the broader child facade or trigger its imports."""
    script = f"""
import sys
import dpone.readiness as readiness
try:
    getattr(readiness, {name!r})
except AttributeError as error:
    assert {name!r} in str(error)
    assert 'dpone.readiness' in str(error)
else:
    raise AssertionError('unexpected public export')
assert 'dpone.readiness.managed_planning' not in sys.modules
"""
    result = _run_outer_probe(clean_doctor_probe_python, script)

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
