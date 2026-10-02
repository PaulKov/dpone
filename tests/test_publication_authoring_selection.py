"""Public MSSQL selection remains closed across authoring and deployment."""

import json
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from jsonschema import Draft7Validator

from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.dag.process_config_parser import ETLProcessConfigParser
from dpone.gitops.airflow_connection_projection_closure import (
    AirflowConnectionProjectionClosureError,
    close_connection_projection,
    required_runtime_connection_refs,
)
from dpone.manifest.errors import ManifestConfigurationError
from dpone.runtime.credentials.authority import resolve_runtime_connections
from dpone.runtime.governance.quality_replay_identity import admission_digest
from tests.test_declarative_replay_selection import manifest
from tests.test_independent_audit_manifest import _compile
from tests.test_publication_authority_composition import _BINDING
from tests.test_publication_runtime_binding import setup
from tests.test_quality_replay_identity import config as identity_config


def selected():
    raw = manifest()
    for endpoint, alias in (("source", "source-main"), ("sink", "sink-main")):
        raw[endpoint].pop("connection_id")
        raw[endpoint]["connection_ref"] = alias
    raw["sink"]["options"].pop("durable_quality_replay")
    raw["sink"]["options"]["publication_authority"] = asdict(_BINDING)
    raw["state"] = {"type": "disabled"}
    return raw


@pytest.mark.parametrize("mode", ["batch", "flow", "folder"])
def test_public_selection_survives_compilation_and_load_options(mode, tmp_path):
    raw = selected()
    compiled = _compile(raw, mode, tmp_path)
    load = LoadConfigBuilder().build(compiled)
    expected = asdict(_BINDING)
    assert compiled["sink"]["options"]["publication_authority"] == expected
    assert load.options["publication_authority"] == expected
    assert load.options["sink_options"]["publication_authority"] == expected


def damage(raw, defect):
    options = raw["sink"]["options"]
    if defect == "null":
        options["publication_authority"] = None
    elif defect == "source":
        raw["source"]["options"] = {"publication_authority": options.pop("publication_authority")}
    elif defect == "root":
        raw["publication_authority"] = options.pop("publication_authority")
    elif defect == "budget":
        raw["sink"]["strategy"].pop("max_source_bytes")
    elif defect == "partition":
        raw["sink"]["strategy"]["mode"] = "partition_replace"
    elif defect == "external":
        options["physical_design"]["storage"]["clickhouse"]["cluster"]["replication_mode"] = "external"
    elif defect == "extra":
        options["publication_authority"]["password"] = "synthetic-do-not-echo"
    else:
        options["publication_authority"]["connection_ref"] = "https://invalid.example/secret"


@pytest.mark.parametrize("mode", ["batch", "flow", "folder"])
@pytest.mark.parametrize("defect", ["null", "source", "root", "budget", "partition", "external", "extra", "alias"])
def test_all_authoring_modes_reject_invalid_selection(mode, defect, tmp_path):
    raw = selected()
    damage(raw, defect)
    with pytest.raises(ManifestConfigurationError, match="publication_authority"):
        _compile(raw, mode, tmp_path)


@pytest.mark.parametrize("defect", ["null", "source", "root", "budget", "partition", "external", "extra", "alias"])
def test_python_load_builder_has_same_admission(defect):
    raw = selected()
    damage(raw, defect)
    with pytest.raises(ValueError, match="publication_authority"):
        LoadConfigBuilder().build(raw)


def test_metadata_parser_retains_public_selection_without_runtime_io():
    process = ETLProcessConfigParser().parse(selected(), metadata_only=True)
    assert process.load_config.options["publication_authority"] == asdict(_BINDING)


def test_legacy_projection_does_not_acquire_metadata_dependency():
    raw = selected()
    raw["sink"]["options"].pop("publication_authority")
    assert required_runtime_connection_refs([SimpleNamespace(raw_config=raw)]) == ("sink-main", "source-main")


def test_frozen_dbt_profile_does_not_inherit_new_public_option():
    path = Path(__file__).parents[1] / "examples/dbt-inline-publishing/dpone/dbt-publish-profiles.yml"
    policy = yaml.safe_load(path.read_text())
    validator = Draft7Validator(dbt_schema_contracts()[policy["schema"]])
    assert not list(validator.iter_errors(policy))
    profile = next(iter(policy["profiles"].values()))
    profile["sink"].setdefault("options", {})["publication_authority"] = asdict(_BINDING)
    assert list(validator.iter_errors(policy))


@pytest.mark.parametrize("mode", ["env", "kubernetes_secret_volume"])
def test_projection_includes_independent_authority_with_disabled_state(mode):
    refs = required_runtime_connection_refs([SimpleNamespace(raw_config=selected())])
    assert refs == ("metadata", "sink-main", "source-main")
    projection = {
        "mode": mode,
        "connections": [{"connection_ref": ref, "connection_id": ref} for ref in refs if ref != "metadata"],
    }
    with pytest.raises(AirflowConnectionProjectionClosureError, match="metadata"):
        close_connection_projection(projection, required_refs=refs)
    projection["connections"].append({"connection_ref": "metadata", "connection_id": "metadata-physical"})
    assert len(close_connection_projection(projection, required_refs=refs)["connections"]) == 3


@pytest.mark.parametrize("defect", ["missing", "root", "sink", "source", "raw-source"])
def test_runtime_rejects_split_selection_before_any_resolution(defect):
    events, context, raw, load = setup()
    load.options.update(
        publication_authority=asdict(_BINDING), sink_options={"publication_authority": asdict(_BINDING)}
    )
    if defect == "missing":
        load.options.pop("publication_authority")
    elif defect == "root":
        load.options["publication_authority"]["service_id"] = "other"
    elif defect == "sink":
        load.options["sink_options"]["publication_authority"]["database"] = "Other_Metadata"
    elif defect == "source":
        load.options["source_options"] = {"publication_authority": asdict(_BINDING)}
    else:
        raw["source"]["options"] = {"publication_authority": asdict(_BINDING)}
    with pytest.raises(ValueError, match="publication_authority"):
        resolve_runtime_connections(config=raw, load_config=load, context=context)
    assert events == []


@pytest.mark.parametrize(
    "family", ["etl-config", "etl-batch-manifest", "etl-flow-manifest", "etl-flow-fragment-manifest"]
)
def test_published_schemas_expose_closed_selector(family):
    root = Path(__file__).parents[1] / "src/dpone/schema"
    schema = json.loads((root / f"{family}.schema.json").read_text())
    if family == "etl-config":
        sink = schema["properties"]["sink"]
    elif family == "etl-batch-manifest":
        sink = schema["definitions"]["process_fragment"]["properties"]["sink"]
    else:
        sink = schema["definitions"]["process"]["properties"]["sink"]["allOf"][1]
    selector = sink["properties"]["options"]["properties"]["publication_authority"]
    validator = Draft7Validator(selector)
    assert not list(validator.iter_errors(asdict(_BINDING)))
    for value in [None, {}, {**asdict(_BINDING), "unknown": True}, {**asdict(_BINDING), "backend": "keepermap"}]:
        assert list(validator.iter_errors(value))
    for field in ("service_id", "environment"):
        for text in ("\x00", "a\x00", "\x00a", "a\x7fb", "a\x85b", " leading", "trailing "):
            assert list(validator.iter_errors({**asdict(_BINDING), field: text}))


def replay_options(binding):
    return {"publication_authority": deepcopy(binding), "sink_options": {"publication_authority": deepcopy(binding)}}


def test_replay_identity_binds_storage_namespace_but_not_logical_alias():
    binding = asdict(_BINDING)
    original = admission_digest(identity_config(**replay_options(binding)))
    assert original != admission_digest(identity_config())
    assert (
        admission_digest(identity_config(**replay_options({**binding, "connection_ref": "rotated-alias"}))) == original
    )
    for field in ("database", "schema", "service_id", "environment"):
        assert admission_digest(identity_config(**replay_options({**binding, field: "other"}))) != original


@pytest.mark.parametrize("defect", ["missing", "conflict", "source", "nested", "invalid"])
def test_replay_identity_rejects_unvalidated_selector(defect):
    options = replay_options(asdict(_BINDING))
    if defect == "missing":
        options.pop("publication_authority")
    elif defect == "conflict":
        options["sink_options"]["publication_authority"]["service_id"] = "other"
    elif defect == "source":
        options["source_options"] = {"publication_authority": asdict(_BINDING)}
    elif defect == "nested":
        options["sink_options"]["sink_options"] = {"publication_authority": asdict(_BINDING)}
    else:
        options["publication_authority"] = None
    with pytest.raises(ValueError):
        admission_digest(identity_config(**options))
