"""Schema and immutable-report invariants for the versioned compile result."""

from copy import deepcopy
from dataclasses import replace

import jsonschema
import pytest

from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from dpone.contracts.dbt_workspace import DbtWorkspaceCompileReport
from tests.test_dbt_source_inventory import _inventory
from tests.test_dbt_workspace_compile_service import _service


def test_compile_report_and_source_inventory_have_generated_schemas(tmp_path):
    service, _, _ = _service(tmp_path)
    report = service.compile(tmp_path, output_dir=tmp_path / "out")
    schema = dbt_schema_contracts()["dpone.dbt-workspace-compile.v1"]
    jsonschema.validate(report.to_dict(), schema)
    invalid = report.to_dict()
    invalid["passed"] = False
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid, schema)
    with pytest.raises(ValueError):
        replace(report, subject_sha256=None)
    with pytest.raises(ValueError):
        DbtWorkspaceCompileReport(report.check, report.output_dir)


def test_source_inventory_shape_and_ordered_payload_kinds():
    inventory = _inventory().to_dict()
    schema = dbt_schema_contracts()["dpone.dbt-source-snapshot.v2"]
    jsonschema.validate(inventory, schema)
    invalid = deepcopy(inventory)
    ids = invalid["projects"][0]["workflows"][0]["runtime_payload_ids"]
    ids[0], ids[1] = ids[1], ids[0]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid, schema)
