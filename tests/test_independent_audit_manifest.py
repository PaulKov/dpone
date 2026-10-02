"""Independent audit survives authoring and remains a required pack capability."""

import json
from pathlib import Path
from types import SimpleNamespace

import jsonschema
import pytest
import yaml

from dpone.gitops.airflow_connection_projection_closure import (
    AirflowConnectionProjectionClosureError,
    close_connection_projection,
    required_runtime_connection_refs,
)
from dpone.manifest.authoring import default_authoring_compiler
from dpone.manifest.batch_compiler_impl import BatchManifestCompiler
from dpone.manifest.errors import ManifestConfigurationError
from tests.test_airflow_folder_authoring_v1 import _folder_root, _write_folder
from tests.test_independent_audit_selection import _config


def _compile(config, mode, tmp_path):
    if mode == "batch":
        return (
            BatchManifestCompiler()
            .compile(
                {"kind": "dpone.batch.v1", "defaults": config, "schemas": {"raw": {"tables": ["events"]}}},
                manifest_path=tmp_path / "pipeline.yaml",
            )[0]
            .raw_config
        )
    payload = _folder_root()
    if mode == "folder":
        path = _write_folder(tmp_path, fragment={"kind": "dpone.flow-fragment.v1", "processes": [config]})
    else:
        payload["authoring"]["mode"] = "flow"
        del payload["fragments"]
        payload["processes"] = [config]
        path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    return default_authoring_compiler().compile(payload, source_path=path, project_root=tmp_path).processes[0]


@pytest.mark.parametrize("mode", ["batch", "flow", "folder"])
def test_audit_selection_survives_all_authoring_modes(mode, tmp_path):
    config = _config()
    compiled = _compile(config, mode, tmp_path)
    assert compiled["sink"]["options"] == config["sink"]["options"]
    assert compiled["state"] == {"type": "disabled"}


@pytest.mark.parametrize("mode", ["batch", "flow", "folder"])
@pytest.mark.parametrize("defect", ["null", "source", "duplicate", "provisioning"])
def test_authoring_rejects_invalid_selection_before_pack(mode, defect, tmp_path):
    config = _config()
    audit = config["sink"]["options"]["load_governance"]["audit"]
    if defect == "null":
        audit["storage"] = None
    elif defect == "source":
        config["source"]["options"] = config["sink"].pop("options")
    elif defect == "duplicate":
        audit.update(loads_table="audit", steps_table="AUDIT")
    else:
        audit["storage"]["provisioning"] = "runtime"
    with pytest.raises(ManifestConfigurationError, match="audit"):
        _compile(config, mode, tmp_path)


@pytest.mark.parametrize("mode", ["env", "kubernetes_secret_volume"])
def test_pack_requires_audit_even_when_source_state_disabled(mode):
    refs = required_runtime_connection_refs([SimpleNamespace(raw_config=_config())])
    assert refs == ("audit-main", "sink-main", "source-main")
    projection = {
        "mode": mode,
        "connections": [{"connection_ref": name, "connection_id": name} for name in refs if name != "audit-main"],
    }
    with pytest.raises(AirflowConnectionProjectionClosureError, match="audit-main"):
        close_connection_projection(projection, required_refs=refs)
    projection["connections"].append({"connection_ref": "audit-main", "connection_id": "metadata"})
    closed = close_connection_projection(projection, required_refs=refs)
    assert any(item["connection_id"] == "metadata" for item in closed["connections"])


@pytest.mark.parametrize("filename", ["etl-config.schema.json", "etl-batch-manifest.schema.json"])
def test_schema_accepts_closed_audit_selector(filename):
    root = Path(__file__).parents[1]
    path = root / "src/dpone/schema" / filename
    schema = json.loads(path.read_text())

    # Validate the actual published nested policy, independently of semantic compile.
    def policies(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "audit" and isinstance(value, dict) and "properties" in value:
                    yield value
                yield from policies(value)
        elif isinstance(node, list):
            for item in node:
                yield from policies(item)

    audit_schemas = list(policies(schema))
    assert audit_schemas
    audit = _config()["sink"]["options"]["load_governance"]["audit"]
    for policy in audit_schemas:
        jsonschema.validate(audit, policy)
        audit["storage"]["database"] = "forbidden"
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(audit, policy)
        del audit["storage"]["database"]


def test_dbt_policy_does_not_silently_admit_the_new_runtime_selector():
    from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts

    root = Path(__file__).parents[1]
    policy = yaml.safe_load((root / "examples/dbt-inline-publishing/dpone/dbt-publish-profiles.yml").read_text())
    schema = dbt_schema_contracts()[policy["schema"]]
    jsonschema.validate(policy, schema)
    profile = next(iter(policy["profiles"].values()))
    profile["sink"]["options"] = _config()["sink"]["options"]
    with pytest.raises(jsonschema.ValidationError, match="storage"):
        jsonschema.validate(policy, schema)
