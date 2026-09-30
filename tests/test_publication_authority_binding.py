"""Publication identity must survive aliases without hiding storage drift."""

from dataclasses import FrozenInstanceError, replace
from importlib import import_module

import pytest

from dpone.contracts.runtime_connection import ResolvedConnectionDescriptor


def _api():
    return import_module("dpone.contracts.publication_authority_binding")


def _config():
    return dict(
        backend="mssql",
        connection_ref="metadata",
        database="Example_System",
        schema="dbo",
        service_id="example-clickhouse",
        environment="test",
    )


def _binding():
    return _api().PublicationAuthorityBinding.from_mapping(_config())


def test_alias_rotation_preserves_target_slot():
    binding = _binding()
    alias = replace(binding, connection_ref="metadata_rotated")
    assert _api().publication_slot_key(binding, "a" * 64) == _api().publication_slot_key(alias, "a" * 64)
    assert _api().publication_binding_digest(binding, endpoint_identity="b" * 64) == _api().publication_binding_digest(
        alias, endpoint_identity="b" * 64
    )


@pytest.mark.parametrize("field,value", [("environment", "prod"), ("service_id", "another-service")])
def test_distinct_services_and_environments_do_not_share_slot(field, value):
    binding = _binding()
    assert _api().publication_slot_key(binding, "a" * 64) != _api().publication_slot_key(
        replace(binding, **{field: value}), "a" * 64
    )


def test_distinct_targets_do_not_share_slot():
    assert _api().publication_slot_key(_binding(), "a" * 64) != _api().publication_slot_key(_binding(), "b" * 64)


@pytest.mark.parametrize("field,value", [("database", "Other_System"), ("schema", "system")])
def test_storage_move_changes_binding_not_logical_target(field, value):
    binding = _binding()
    moved = replace(binding, **{field: value})
    assert _api().publication_slot_key(binding, "a" * 64) == _api().publication_slot_key(moved, "a" * 64)
    assert _api().publication_binding_digest(binding, endpoint_identity="b" * 64) != _api().publication_binding_digest(
        moved, endpoint_identity="b" * 64
    )


def test_resolved_endpoint_drift_changes_binding():
    assert _api().publication_binding_digest(
        _binding(), endpoint_identity="a" * 64
    ) != _api().publication_binding_digest(_binding(), endpoint_identity="b" * 64)


@pytest.mark.parametrize("field", tuple(_config()))
@pytest.mark.parametrize("value", [None, "", " ", True, 42, "bad\x00value", " padded"])
def test_invalid_identity_fields_are_rejected_without_echoing_values(field, value):
    data = _config()
    data[field] = value
    with pytest.raises(ValueError, match="publication_authority"):
        _api().PublicationAuthorityBinding.from_mapping(data)


@pytest.mark.parametrize("field", tuple(_config()))
def test_missing_identity_fields_fail_closed(field):
    data = _config()
    del data[field]
    with pytest.raises(ValueError, match="publication_authority"):
        _api().PublicationAuthorityBinding.from_mapping(data)


def test_unknown_field_is_not_silently_ignored():
    data = _config()
    data["password"] = "synthetic-secret"
    with pytest.raises(ValueError) as error:
        _api().PublicationAuthorityBinding.from_mapping(data)
    assert "synthetic-secret" not in str(error.value)


@pytest.mark.parametrize("field,value", [("backend", "keepermap"), ("database", "x.y"), ("schema", "x];DROP")])
def test_wrong_backend_or_ambiguous_sql_location_is_rejected(field, value):
    with pytest.raises(ValueError, match="publication_authority"):
        replace(_binding(), **{field: value})


def test_binding_is_immutable_and_detached_from_input():
    config = _config()
    binding = _api().PublicationAuthorityBinding.from_mapping(config)
    config["environment"] = "prod"
    assert binding.environment == "test"
    with pytest.raises(FrozenInstanceError):
        binding.schema = "other"


@pytest.mark.parametrize(
    "connection_type,database,schema",
    [("clickhouse", "Example_System", "dbo"), ("mssql", "Wrong_System", "dbo"), ("mssql", "Example_System", "wrong")],
)
def test_connection_registry_must_match_explicit_storage(connection_type, database, schema):
    descriptor = ResolvedConnectionDescriptor(connection_type, {"database": database, "schema": schema})
    with pytest.raises(ValueError, match="publication_authority"):
        _binding().require_descriptor(descriptor)


def test_valid_registry_location_is_admitted_without_credentials():
    _binding().require_descriptor(
        ResolvedConnectionDescriptor("mssql", {"database": "Example_System", "schema": "dbo"})
    )


@pytest.mark.parametrize("value", ["", "a", "A" * 64, None, "z" * 64])
def test_invalid_identity_digests_are_rejected(value):
    with pytest.raises(ValueError):
        _api().publication_slot_key(_binding(), value)
    with pytest.raises(ValueError):
        _api().publication_binding_digest(_binding(), endpoint_identity=value)
