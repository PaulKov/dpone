"""Versioned invocation targets preserve effective identity and shipped v1 bytes."""

from dataclasses import replace

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError, canonical_fingerprint
from dpone.contracts.dbt_execution_pack import DbtExecutionPack
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2
from tests.test_dbt_runtime_execution import _pack

V2 = "dpone.dbt-execution-pack.v2"


def _payload(*, base_schema="base"):
    raw = _pack().to_dict()
    raw["schema"] = V2
    raw["invocation_target"] = {"database": raw["profile"]["database"], "schema": base_schema}
    return _rehash(raw)


def _rehash(raw):
    return {**raw, "pack_sha256": canonical_fingerprint({k: v for k, v in raw.items() if k != "pack_sha256"})}


def test_v2_renders_base_without_changing_effective_profile_or_payload():
    raw = _payload()
    pack = DbtExecutionPack.from_mapping(raw)
    assert pack.to_dict() == raw
    assert pack.invocation_profile() == replace(pack.profile, schema="base")
    assert pack.profile.schema == raw["profile"]["schema"]
    pack.require_wire_contract(DBT_RUNTIME_WIRE_V2)


def test_v1_keeps_exact_mapping_and_original_profile_object():
    legacy = _pack()
    raw = legacy.to_dict()
    assert "invocation_target" not in raw
    parsed = DbtExecutionPack.from_mapping(raw)
    assert parsed.to_dict() == raw
    assert parsed.invocation_profile() is parsed.profile
    parsed.require_wire_contract(DBT_RUNTIME_WIRE_V1)


@pytest.mark.parametrize(
    "target",
    [
        None,
        {},
        {"database": "db"},
        {"schema": "base"},
        [],
        "base",
        {"database": "db", "schema": "base", "extra": True},
        {"database": "db", "schema": ""},
        {"database": 123, "schema": "base"},
    ],
)
def test_v2_rejects_invalid_base_even_with_recomputed_fingerprint(target):
    raw = _payload()
    raw["invocation_target"] = target
    with pytest.raises(DbtPublishingError):
        DbtExecutionPack.from_mapping(_rehash(raw))


def test_v2_requires_base_and_v1_rejects_added_base():
    raw = _payload()
    raw.pop("invocation_target")
    with pytest.raises(DbtPublishingError):
        DbtExecutionPack.from_mapping(_rehash(raw))
    raw = _payload()
    raw["schema"] = "dpone.dbt-execution-pack.v1"
    with pytest.raises(DbtPublishingError):
        DbtExecutionPack.from_mapping(_rehash(raw))


@pytest.mark.parametrize("field", ["invocation_target", "profile"])
def test_both_targets_are_fingerprint_bound(field):
    raw = _payload()
    raw[field]["schema"] = "tampered"
    with pytest.raises(DbtPublishingError):
        DbtExecutionPack.from_mapping(raw)


@pytest.mark.parametrize("schema,wire", [("v1", DBT_RUNTIME_WIRE_V2), ("v2", DBT_RUNTIME_WIRE_V1), ("v2", "unknown")])
def test_wire_pack_pairing_rejects_mixed_or_unknown_versions(schema, wire):
    pack = _pack() if schema == "v1" else DbtExecutionPack.from_mapping(_payload())
    with pytest.raises(DbtPublishingError):
        pack.require_wire_contract(wire)


def test_v2_builder_does_not_change_default_v1_builder():
    from dpone.contracts.dbt_execution_pack import DbtInvocationTarget

    legacy = _pack()
    generated = {
        "schema",
        "pack_sha256",
        "dbt_core_version",
        "dbt_adapter_version",
        "manifest_schema_version",
        "run_results_schema_version",
    }
    values = {key: getattr(legacy, key) for key in legacy.to_dict() if key not in generated}
    current = DbtExecutionPack.build_v2(
        **values, invocation_target=DbtInvocationTarget(legacy.profile.database, "base")
    )
    assert current.to_dict() == _payload()
    assert DbtExecutionPack.build(**values) == legacy


def test_published_schemas_keep_versions_closed():
    import jsonschema

    from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts

    schemas = dbt_schema_contracts()
    legacy, current = _pack().to_dict(), _payload()
    jsonschema.validate(legacy, schemas[legacy["schema"]])
    jsonschema.validate(current, schemas[V2])
    for raw, schema in ((legacy, schemas[V2]), (current, schemas[legacy["schema"]])):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(raw, schema)


@pytest.mark.parametrize("field", ["database", "schema"])
@pytest.mark.parametrize("length", [256, 257])
def test_base_target_length_matches_published_schema(field, length):
    import jsonschema

    from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts

    raw = _payload()
    raw["invocation_target"][field] = "x" * length
    raw = _rehash(raw)
    schema = dbt_schema_contracts()[V2]
    if length == 256:
        assert DbtExecutionPack.from_mapping(raw).to_dict() == raw
        jsonschema.validate(raw, schema)
    else:
        with pytest.raises(DbtPublishingError):
            DbtExecutionPack.from_mapping(raw)
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(raw, schema)
