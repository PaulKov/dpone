"""Pure and fake-observation source-schema authority contracts."""

from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import fields, replace
from typing import Any

import pytest

from dpone.contracts.postgres_mssql_type_authority import PostgresMssqlTypePolicyAuthorityV1
from dpone.contracts.postgres_mssql_type_derivation import derive_type_decision
from dpone.contracts.postgres_mssql_type_target_enums import PostgresMssqlSourceScalarFamilyV1
from dpone.runtime.sources.strategies.postgres.postgres_schema_metadata import PostgresFetchedSchema
from dpone.runtime.support.postgres_mssql_projection_models import (
    PostgresMssqlColumnProjection,
    PostgresMssqlSchemaProjection,
)
from dpone.type_system.source_sink.provenance import SourceColumnProvenance
from tests.test_postgres_mssql_r1_source_schema_runtime import FakeCatalogConnector, _policy, _shape, issue_authority
from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import bind_behavior_scenario


def _typed(call) -> BaseException:
    try:
        call()
    except Exception as exc:  # noqa: BLE001 - scenario asserts stable typed reason.
        assert type(exc) is not AssertionError
        return exc
    raise AssertionError("typed rejection required")


def _catalog_exact(modules: dict[str, Any]) -> dict[str, bool]:
    policy = _policy()
    shape = policy.resolve_catalog_shape(11, 20, -1)
    return {
        "type_oid": shape.source_type_oid == 20,
        "type_modifier": shape.source_typmod == -1,
        "unique_decision": shape is policy.ordered_decisions[0].source_shape,
    }


def _catalog_missing(modules: dict[str, Any]) -> dict[str, bool]:
    del modules
    policy = _policy()
    oid = _typed(lambda: policy.resolve_catalog_shape(11, 23, -1))
    typmod = _typed(lambda: policy.resolve_catalog_shape(11, 20, 0))
    return {
        "missing_type_oid": getattr(oid, "reason", "") != "",
        "missing_type_modifier": getattr(typmod, "reason", "") != "",
    }


def _catalog_ambiguous(modules: dict[str, Any]) -> dict[str, bool]:
    del modules
    policy = _policy()
    forged = object.__new__(PostgresMssqlTypePolicyAuthorityV1)
    object.__setattr__(forged, "policy_version", policy.policy_version)
    object.__setattr__(forged, "ordered_decisions", policy.ordered_decisions * 2)
    error = _typed(lambda: forged.resolve_catalog_shape(11, 20, -1))
    return {"duplicate_type_oid_typmod": getattr(error, "reason", "") != ""}


def _observed_exact(modules: dict[str, Any]) -> dict[str, bool]:
    authority, scope, *_ = issue_authority(modules)
    column = authority.ordered_columns[0]
    results = {
        "bool_int_separation": getattr(_typed(lambda: replace(column, relation_oid=True)), "reason", "") != "",
        "digest_exact_bytes": getattr(
            _typed(lambda: replace(column, selected_source_authority_sha256=bytearray(b"x" * 32))), "reason", ""
        )
        != "",
        "source_shape_exact_type": getattr(_typed(lambda: replace(column, source_shape=object())), "reason", "") != "",
        "source_column_ref_exact_type": getattr(
            _typed(lambda: replace(column, source_column_ref=object())), "reason", ""
        )
        != "",
    }
    scope.close_if_active()
    return results


def _domain_version(modules: dict[str, Any], *, aggregate: bool) -> dict[str, bool]:
    authority, scope, *_ = issue_authority(modules)
    value = authority if aggregate else authority.ordered_columns[0]
    owner = (
        modules["authority"].PostgresMssqlSelectedRelationSchemaAuthorityV1
        if aggregate
        else modules["models"].PostgresMssqlObservedSourceColumnV1
    )
    payload = value.canonical_bytes
    wrong_domain = b"x" + payload[1:]
    wrong_version = bytearray(payload)
    wrong_version[-2] ^= 1
    results = {
        "canonical_domain": getattr(_typed(lambda: owner.from_canonical_bytes(wrong_domain)), "reason", "")
        == "wrong_domain",
        "contract_version": _typed(lambda: owner.from_canonical_bytes(bytes(wrong_version))) is not None,
        "trailing_bytes": getattr(_typed(lambda: owner.from_canonical_bytes(payload + b"x")), "reason", "") != "",
    }
    if aggregate:
        results["observation_profile"] = (
            getattr(_typed(lambda: replace(value, observation_profile="other")), "reason", "") != ""
        )
    scope.close_if_active()
    return results


def _identifier_domain(modules: dict[str, Any]) -> dict[str, bool]:
    authority, scope, *_ = issue_authority(modules)
    column = authority.ordered_columns[0]
    cases = {
        "name.nonempty": {"name": ""},
        "name.no_nul": {"name": "a\0b"},
        "name.utf8_63": {"name": "x" * 64},
        "name.NFC": {"name": "cafe\u0301"},
        "name.trim_equal": {"name": " x"},
        "type_name.nonempty": {"type_name": ""},
        "type_name.no_nul": {"type_name": "a\0b"},
        "type_name.utf8_63": {"type_name": "x" * 64},
    }
    scope.close_if_active()
    return {key: _typed(lambda values=values: replace(column, **values)) is not None for key, values in cases.items()}


def _catalog_identity(modules: dict[str, Any]) -> dict[str, bool]:
    authority, scope, *_ = issue_authority(modules)
    column = authority.ordered_columns[0]
    mutations = {
        "pg_catalog_namespace": {"type_namespace_oid": 12},
        "base_type_kind": {"type_kind": "d"},
        "generated_empty": {"generated_kind": "s"},
        "identity_kind": {"identity_kind": "x"},
        "shape_oid": {"type_oid": 23},
        "shape_typmod": {"type_modifier": 0},
        "shape_family_name": {"type_name": "int4"},
        "collation_uint32": {"collation_oid": 2**32},
    }
    scope.close_if_active()
    return {
        key: _typed(lambda values=values: replace(column, **values)) is not None for key, values in mutations.items()
    }


def _roundtrip(modules: dict[str, Any]) -> dict[str, bool]:
    authority, scope, *_ = issue_authority(modules)
    decoded = type(authority).from_canonical_bytes(authority.canonical_bytes)
    results = {
        "encode_decode_encode": decoded.canonical_bytes == authority.canonical_bytes,
        "selected_document_digest": hashlib.sha256(authority.selected_source_document_utf8).digest()
        == authority.selected_source_authority_sha256,
        "ordered_column_bytes": tuple(c.canonical_bytes for c in decoded.ordered_columns)
        == tuple(c.canonical_bytes for c in authority.ordered_columns),
        "type_policy_bytes": decoded.type_policy_authority.canonical_bytes
        == authority.type_policy_authority.canonical_bytes,
    }
    scope.close_if_active()
    return results


def _relation_splice(modules: dict[str, Any]) -> dict[str, bool]:
    authority, scope, *_ = issue_authority(modules)
    column = authority.ordered_columns[0]
    foreign = replace(column, relation_oid=22002)
    error = _typed(lambda: replace(authority, ordered_columns=(foreign,)))
    scope.close_if_active()
    return {
        "selected_digest": bool(column.selected_source_authority_sha256),
        "namespace_oid": column.namespace_oid == 2200,
        "relation_oid": getattr(error, "reason", "") == "authority_splice",
        "same_database_foreign_relation": getattr(error, "reason", "") == "authority_splice",
    }


def _policy_splice(modules: dict[str, Any]) -> dict[str, bool]:
    authority, scope, *_ = issue_authority(modules)
    extra = _shape(23, family=PostgresMssqlSourceScalarFamilyV1.INT4)
    base = _shape()
    foreign = PostgresMssqlTypePolicyAuthorityV1.create(
        tuple(derive_type_decision(item, maximum_input_bytes=1024) for item in (base, extra)),
        (base, extra),
    )
    error = _typed(lambda: replace(authority, type_policy_authority=foreign))
    scope.close_if_active()
    return {
        "foreign_type_policy": foreign.digest != authority.type_policy_authority.digest,
        "policy_digest": getattr(error, "reason", "") == "authority_splice",
        "catalog_resolution": foreign.resolve_catalog_shape(11, 20, -1) == base,
    }


def _reference_splice(modules: dict[str, Any]) -> dict[str, bool]:
    authority, scope, *_ = issue_authority(modules)
    column = authority.ordered_columns[0]
    ref = authority.type_policy_authority.source_column_ref(
        ordinal=2, name="other", nullable=True, source_shape=column.source_shape
    )
    error = _typed(lambda: replace(column, source_column_ref=ref))
    scope.close_if_active()
    return {
        "foreign_source_column_ref": getattr(error, "reason", "") != "",
        "ordinal": ref.ordinal == 2,
        "name": ref.name == "other",
        "nullable": ref.nullable is True,
        "source_shape": ref.source_shape == column.source_shape,
    }


def _column_order(modules: dict[str, Any]) -> dict[str, bool]:
    authority, scope, *_ = issue_authority(modules)
    one = authority.ordered_columns[0]
    two = replace(
        one,
        projection_ordinal=2,
        attribute_number=3,
        name="second",
        source_column_ref=authority.type_policy_authority.source_column_ref(
            ordinal=2, name="second", nullable=False, source_shape=one.source_shape
        ),
    )
    bad_ordinal = _typed(lambda: replace(authority, ordered_columns=(two,)))
    bad_name = _typed(lambda: replace(authority, ordered_columns=(one, replace(two, name="COLUMN_1"))))
    scope.close_if_active()

    gap_connector = FakeCatalogConnector(columns=2)
    gap_connector.attribute_numbers = (1, 3)
    gap_authority, gap_scope, *_ = issue_authority(modules, connector=gap_connector)
    gap_scope.close_if_active()
    duplicate_connector = FakeCatalogConnector(columns=2)
    duplicate_connector.attribute_numbers = (1, 1)
    duplicate_error = _typed(lambda: issue_authority(modules, connector=duplicate_connector))
    decreasing_connector = FakeCatalogConnector(columns=2)
    decreasing_connector.attribute_numbers = (3, 1)
    decreasing_error = _typed(lambda: issue_authority(modules, connector=decreasing_connector))
    return {
        "projection_ordinal_contiguous": getattr(bad_ordinal, "reason", "") != "",
        "attribute_number_strict": getattr(duplicate_error, "reason", "") == "column_ordinal_invalid"
        and getattr(decreasing_error, "reason", "") == "column_ordinal_invalid",
        "physical_gap_allowed": tuple(column.attribute_number for column in gap_authority.ordered_columns) == (1, 3),
        "duplicate_name_casefold_rejected": getattr(bad_name, "reason", "") != "",
    }


def _column_count(modules: dict[str, Any]) -> dict[str, bool]:
    one, one_scope, *_ = issue_authority(modules, connector=FakeCatalogConnector(columns=1))
    maximum, max_scope, *_ = issue_authority(modules, connector=FakeCatalogConnector(columns=1024))
    empty_error = _typed(lambda: issue_authority(modules, connector=FakeCatalogConnector(columns=0)))
    overflow = _typed(lambda: issue_authority(modules, connector=FakeCatalogConnector(columns=1025)))
    one_scope.close_if_active()
    max_scope.close_if_active()
    return {
        "zero_rejected": getattr(empty_error, "reason", "") == "column_count_invalid",
        "one_accepted": len(one.ordered_columns) == 1,
        "1024_accepted": len(maximum.ordered_columns) == 1024,
        "1025_rejected": getattr(overflow, "reason", "") == "column_count_invalid",
    }


_FAILURES = {
    "wrong_domain": "permanent_input_error",
    "wrong_version": "operator_intervention",
    "malformed_canonical_bytes": "permanent_input_error",
    "exact_type_violation": "permanent_input_error",
    "snapshot_lease_mismatch": "retryable_source",
    "source_authority_mismatch": "operator_intervention",
    "relation_lock_not_proven": "retryable_source",
    "relation_profile_unsupported": "permanent_capability_error",
    "metadata_permission_denied": "operator_intervention",
    "catalog_observation_failed": "retryable_source",
    "column_count_invalid": "permanent_capability_error",
    "column_ordinal_invalid": "operator_intervention",
    "column_identifier_unsupported": "permanent_capability_error",
    "column_type_identity_invalid": "operator_intervention",
    "source_column_unsupported": "permanent_capability_error",
    "type_policy_mismatch": "operator_intervention",
    "authority_splice": "operator_intervention",
    "snapshot_cleanup_failed": "retryable_source",
    "internal_invariant_violation": "operator_intervention",
}

_FAILURE_MESSAGES = {
    "wrong_domain": "Source schema authority uses an unsupported canonical domain.",
    "wrong_version": "Source schema authority uses an unsupported contract version.",
    "malformed_canonical_bytes": "Source schema authority bytes are not canonical.",
    "exact_type_violation": "Source schema authority uses an inexact model type.",
    "snapshot_lease_mismatch": "Verified PostgreSQL snapshot ownership is no longer valid.",
    "source_authority_mismatch": "PostgreSQL source identity differs from approved authority.",
    "relation_lock_not_proven": "PostgreSQL relation lock authority was not proved.",
    "relation_profile_unsupported": "PostgreSQL relation profile is not supported by R1.",
    "metadata_permission_denied": "PostgreSQL catalog permission is insufficient.",
    "catalog_observation_failed": "PostgreSQL catalog observation did not complete.",
    "column_count_invalid": "PostgreSQL column count is outside the R1 range.",
    "column_ordinal_invalid": "PostgreSQL column order is not canonical.",
    "column_identifier_unsupported": "PostgreSQL column identifier is not supported by R1.",
    "column_type_identity_invalid": "PostgreSQL column type identity is inconsistent.",
    "source_column_unsupported": "PostgreSQL column capability is not supported by R1.",
    "type_policy_mismatch": "PostgreSQL type policy does not cover the observed schema.",
    "authority_splice": "Independently valid source authorities do not share one relation scope.",
    "snapshot_cleanup_failed": "PostgreSQL snapshot cleanup did not complete.",
    "internal_invariant_violation": "Source schema authority invariant was not satisfied.",
}


def _failures(modules: dict[str, Any]) -> dict[str, bool]:
    error_type = modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1
    results = {}
    messages = set()
    for reason, recovery in _FAILURES.items():
        error = error_type(reason)
        results[f"{reason}:{recovery}"] = error.reason == reason and error.recovery.value == recovery
        messages.add(str(error))
    results["stable_redacted_message_per_reason"] = len(messages) == len(_FAILURES) and all(
        "secret" not in item for item in messages
    )
    unknown = error_type.from_internal_reason("future_unknown")
    results["unknown_reason_maps_to_internal_invariant_violation"] = unknown.reason == "internal_invariant_violation"
    return results


def test_failure_reason_recovery_and_message_table_is_exact() -> None:
    """The user-facing recovery table is a closed, independently fixed oracle."""

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    error_type = modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1
    observed = {reason: (error_type(reason).recovery.value, str(error_type(reason))) for reason in _FAILURES}
    expected = {reason: (_FAILURES[reason], _FAILURE_MESSAGES[reason]) for reason in _FAILURES}
    assert observed == expected


@pytest.mark.parametrize("system_identifier", ("0", str(2**64), "01"))
def test_v1_system_identifier_rejects_noncanonical_or_out_of_range_with_valid_digest(
    system_identifier: str,
) -> None:
    """A matching digest cannot bless an invalid physical-cluster identifier."""

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import (
        _SELECTED_AUTHORITY_GOLDENS,
        _feature_modules,
    )

    modules = _feature_modules()
    authority, scope, *_ = issue_authority(modules)
    document = json.loads(_SELECTED_AUTHORITY_GOLDENS[1])
    document["system_identifier"] = system_identifier
    preimage = json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    error = _typed(
        lambda: replace(
            authority,
            selected_source_document_utf8=preimage,
            selected_source_authority_sha256=hashlib.sha256(preimage).digest(),
        )
    )
    scope.close_if_active()

    assert getattr(error, "reason", "") == "malformed_canonical_bytes"
    assert error.__cause__ is error.__context__ is None


@pytest.mark.parametrize("system_identifier", ("1", str(2**64 - 1)))
def test_v1_system_identifier_accepts_closed_unsigned_decimal_boundaries(
    system_identifier: str,
) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import (
        _SELECTED_AUTHORITY_GOLDENS,
        _feature_modules,
    )

    modules = _feature_modules()
    document = json.loads(_SELECTED_AUTHORITY_GOLDENS[1])
    document["system_identifier"] = system_identifier
    decoded = modules["models"].PostgresSelectedRelationAuthorityDocumentV1.from_document(document)
    assert decoded.system_identifier == system_identifier


def _projection(modules: dict[str, Any]):
    authority, scope, *_ = issue_authority(modules)
    factory = modules["boundary"].build_r1_postgres_fetched_schema
    adapter = modules["projection"].PostgresMssqlSourceSchemaProjectionAdapterV1(fetched_schema_factory=factory)
    result = adapter.project(authority)
    scope.close_if_active()
    assert type(result) is PostgresFetchedSchema
    return result


def _fields_scenario(modules: dict[str, Any], kind: str) -> dict[str, bool]:
    result = _projection(modules)
    if kind == "provenance":
        value = result.relation_metadata[0]
        names = tuple(item.name for item in fields(SourceColumnProvenance))
    elif kind == "column":
        value = result.target_projection.columns[0]
        names = tuple(item.name for item in fields(PostgresMssqlColumnProjection))
    else:
        assert type(result.target_projection) is PostgresMssqlSchemaProjection
        return {
            "PostgresFetchedSchema.relation_schema": result.relation_schema == (("column_1", "bigint"),),
            "PostgresFetchedSchema.projected_schema": result.projected_schema == (("column_1", "bigint"),),
            "PostgresFetchedSchema.relation_metadata": len(result.relation_metadata) == 1,
            "PostgresFetchedSchema.target_projection": result.target_projection is not None,
            "PostgresMssqlSchemaProjection.columns": len(result.target_projection.columns) == 1,
            "PostgresMssqlSchemaProjection.retained_target_columns": result.target_projection.retained_target_columns
            == (),
        }
    if kind == "provenance":
        expected = {
            "name": "column_1",
            "declared_type": "bigint",
            "nullable": False,
            "type_schema": "pg_catalog",
            "type_name": "int8",
            "type_kind": "b",
            "type_category": None,
            "domain_schema": None,
            "domain_name": None,
            "datetime_precision": None,
            "interval_type": None,
            "interval_precision": None,
            "character_set": None,
            "collation_schema": None,
            "collation_name": None,
            "has_default": None,
            "default_expression_sha256": None,
            "is_identity": False,
            "identity_generation": None,
            "generation_kind": "NEVER",
            "generation_expression_sha256": None,
        }
    else:
        expected = {
            "name": "column_1",
            "source_name": "column_1",
            "wire_position": 0,
            "source_type": "bigint",
            "source_native_mssql_type": "bigint",
            "projected_type": "bigint",
            "target_type": "bigint",
            "transfer_representation": "integer text",
            "requires_explicit_contract": False,
            "explicit_contract_source": None,
            "nullable": False,
            "source_collation": None,
            "collation": None,
        }
    assert tuple(expected) == names
    return {name: getattr(value, name) == expected[name] for name in names}


def _no_legacy_projection(modules: dict[str, Any]) -> dict[str, bool]:
    authority, scope, connector, *_ = issue_authority(modules)
    calls: list[dict[str, object]] = []

    def factory(**kwargs):
        calls.append(kwargs)
        return modules["boundary"].build_r1_postgres_fetched_schema(**kwargs)

    adapter = modules["projection"].PostgresMssqlSourceSchemaProjectionAdapterV1(fetched_schema_factory=factory)
    legacy_projection = importlib.import_module("dpone.runtime.support.postgres_mssql_projection")
    legacy_mapper = importlib.import_module("dpone.type_system.source_sink.postgres_mssql")

    def poison(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("legacy projection dependency was touched")

    original_projector = getattr(legacy_projection, "project_postgres_mssql_relation")
    original_mapper = getattr(legacy_mapper, "PostgresMssqlTypeMapper")
    setattr(legacy_projection, "project_postgres_mssql_relation", poison)
    setattr(legacy_mapper, "PostgresMssqlTypeMapper", poison)
    before = len(connector.events)
    try:
        result = adapter.project(authority)
    finally:
        setattr(legacy_projection, "project_postgres_mssql_relation", original_projector)
        setattr(legacy_mapper, "PostgresMssqlTypeMapper", original_mapper)
        scope.close_if_active()
    return {
        "no_load_config_read": not hasattr(adapter, "load_config"),
        "no_legacy_mapper": result.target_projection is not None,
        "no_catalog_io": len(connector.events) == before + 1,  # terminal rollback only
        "injected_constructor_only": len(calls) == 1 and type(result) is PostgresFetchedSchema,
    }


bind_behavior_scenario("catalog-shape.exact-resolution", _catalog_exact)
bind_behavior_scenario("catalog-shape.missing", _catalog_missing)
bind_behavior_scenario("catalog-shape.ambiguous", _catalog_ambiguous)
bind_behavior_scenario("observed-column.exact-types", _observed_exact)
bind_behavior_scenario("observed-column.domain-version", lambda modules: _domain_version(modules, aggregate=False))
bind_behavior_scenario("observed-column.identifier-domain", _identifier_domain)
bind_behavior_scenario("observed-column.catalog-identity", _catalog_identity)
bind_behavior_scenario("authority.domain-version", lambda modules: _domain_version(modules, aggregate=True))
bind_behavior_scenario("authority.canonical-roundtrip", _roundtrip)
bind_behavior_scenario("authority.relation-splice", _relation_splice)
bind_behavior_scenario("authority.policy-splice", _policy_splice)
bind_behavior_scenario("authority.reference-splice", _reference_splice)
bind_behavior_scenario("authority.column-order", _column_order)
bind_behavior_scenario("authority.column-count", _column_count)
bind_behavior_scenario("failure.closed-reason-recovery-message", _failures)
bind_behavior_scenario("projection.provenance-fields", lambda modules: _fields_scenario(modules, "provenance"))
bind_behavior_scenario("projection.column-fields", lambda modules: _fields_scenario(modules, "column"))
bind_behavior_scenario("projection.aggregate-fields", lambda modules: _fields_scenario(modules, "aggregate"))
bind_behavior_scenario("projection.no-legacy-policy", _no_legacy_projection)


def test_contract_helpers_do_not_use_registry_expected_class() -> None:
    assert "expected_class" not in _catalog_exact.__code__.co_names
    assert "expected_class" not in _roundtrip.__code__.co_names


@pytest.mark.parametrize(
    "case_id",
    (
        "lone_surrogate_byte_decode",
        "malformed_aggregate",
        "wrong_issuer_policy",
        "invalid_profile",
        "lease_failure",
        "artifact_integrity_failure",
        "model_to_authority_translation",
    ),
)
def test_all_model_and_route_translation_paths_drop_raw_exception_links(case_id: str, tmp_path) -> None:
    """Exercise translations whose source exception is otherwise easy to retain."""

    from tests.test_postgres_mssql_prepared_source_boundary import _prepared
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    authority, scope, connector, issuer, profile = issue_authority(modules)
    verified = scope.require_active(connector)
    malformed_selected = (
        b'{"database":{"canonical_name":"warehouse","oid":16384},'
        b'"dialect":"postgres","principals":{"effective":{"canonical_name":"dpone_reader","oid":17001},'
        b'"session":{"canonical_name":"dpone_login","oid":17002}},'
        b'"relation":{"namespace_oid":2200,"relation":"orders","relation_oid":22001,'
        b'"schema":"\\ud800"},"role":"source","system_identifier":"7272727272727272727",'
        b'"timeline_id":7,"topology_role":"primary","version":1}'
    )
    invalid_profile = replace(profile, contract_version="invalid")
    invalid_artifact = __import__("dpone.runtime.file_artifacts", fromlist=["FileExportArtifact"]).FileExportArtifact(
        str(tmp_path / "unsealed.csv"),
        ["column_1"],
        compressed=False,
        format="csv",
    )
    boundary, _boundary_connector, _runtime = _prepared(modules)

    route_error = modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1
    cases = {
        "lone_surrogate_byte_decode": (
            lambda: modules["models"].PostgresSelectedRelationAuthorityDocumentV1.from_authority_document_utf8(
                malformed_selected
            ),
            modules["models"].PostgresMssqlSourceSchemaModelErrorV1,
            "malformed_canonical_bytes",
        ),
        "malformed_aggregate": (
            lambda: modules["authority"].PostgresMssqlSelectedRelationSchemaAuthorityV1.from_canonical_bytes(
                authority.canonical_bytes[:-1]
            ),
            route_error,
            "malformed_canonical_bytes",
        ),
        "wrong_issuer_policy": (
            lambda: modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=object()),
            route_error,
            "exact_type_violation",
        ),
        "invalid_profile": (
            lambda: issuer.issue(
                connector=connector,
                verified_relation=verified,
                query_profile=invalid_profile,
            ),
            route_error,
            "exact_type_violation",
        ),
        "lease_failure": (
            lambda: issuer.issue(
                connector=connector,
                verified_relation=verified,
                query_profile=profile,
            ),
            route_error,
            "snapshot_lease_mismatch",
        ),
        "artifact_integrity_failure": (
            lambda: boundary.complete(invalid_artifact),
            route_error,
            "internal_invariant_violation",
        ),
        "model_to_authority_translation": (
            lambda: replace(
                authority,
                selected_source_document_utf8=malformed_selected,
                selected_source_authority_sha256=hashlib.sha256(malformed_selected).digest(),
            ),
            route_error,
            "malformed_canonical_bytes",
        ),
    }
    if case_id in {"lone_surrogate_byte_decode", "artifact_integrity_failure", "model_to_authority_translation"}:
        import asyncio

        cancellation = asyncio.CancelledError(f"caller cancelled during {case_id}")

        def cancel(*_args: object, **_kwargs: object) -> object:
            raise cancellation

        cancellation_operation = cases[case_id][0]
        cancellation_boundary = None
        cancellation_connector = None
        cancellation_lifecycle_receipt = None
        with pytest.MonkeyPatch.context() as monkeypatch:
            if case_id == "lone_surrogate_byte_decode":
                monkeypatch.setattr(modules["models"].json, "loads", cancel)
            elif case_id == "artifact_integrity_failure":
                cancellation_boundary, cancellation_connector, _cancellation_runtime = _prepared(modules)
                cancellation_lifecycle_receipt = cancellation_boundary.lifecycle.receipt
                assert cancellation_lifecycle_receipt is not None
                monkeypatch.setattr(type(invalid_artifact), "require_integrity_receipt", cancel)

                def cancel_artifact_integrity() -> object:
                    return cancellation_boundary.complete(invalid_artifact)

                cancellation_operation = cancel_artifact_integrity
            else:
                monkeypatch.setattr(
                    modules["models"].PostgresSelectedRelationAuthorityDocumentV1,
                    "from_authority_document_utf8",
                    classmethod(cancel),
                )
            caught = None
            try:
                cancellation_operation()
            except BaseException as caught_error:
                caught = caught_error
        assert caught is cancellation
        assert caught.__cause__ is caught.__context__ is None
        if cancellation_boundary is not None:
            assert cancellation_connector is not None
            assert cancellation_lifecycle_receipt is not None
            cancellation_receipt = cancellation_boundary.terminal_receipt
            assert cancellation_receipt is not None
            rollback_count = sum(event[0] == "rollback" for event in cancellation_connector.events)
            assert rollback_count == 1
            assert (
                cancellation_receipt.outcome,
                cancellation_receipt.cleanup_attempted,
                cancellation_receipt.cleanup_succeeded,
                cancellation_receipt.cleanup_error_reason,
                cancellation_receipt.connection_quarantined,
                cancellation_boundary.lifecycle.receipt is cancellation_lifecycle_receipt,
                cancellation_lifecycle_receipt.complete,
                cancellation_boundary.close_if_active(),
                cancellation_boundary.terminal_receipt is cancellation_receipt,
                sum(event[0] == "rollback" for event in cancellation_connector.events),
            ) == ("aborted", True, True, None, False, True, False, None, True, rollback_count)
    if case_id == "lease_failure":
        verified.snapshot_lease.lifecycle.complete()
    operation, expected_type, expected_reason = cases[case_id]
    try:
        error = _typed(operation)
        assert type(error) is expected_type
        assert getattr(error, "reason", "") == expected_reason
        assert error.__cause__ is None
        assert error.__context__ is None
    finally:
        scope.close_if_active()
        boundary.close_if_active()


_INITIAL_WITNESS_SQL = """WITH snapshot_witness AS MATERIALIZED (
    SELECT pg_catalog.txid_current_snapshot() AS snapshot_token
)
SELECT
    s.snapshot_token::text AS snapshot_token,
    pg_catalog.txid_snapshot_xmax(s.snapshot_token)::text::bigint
        AS visible_horizon,
    lock_witness.transaction_incarnation,
    pg_catalog.current_setting('transaction_isolation') AS isolation_level,
    pg_catalog.current_setting('transaction_read_only') AS read_only,
    pg_catalog.pg_backend_pid() AS backend_pid,
    pg_catalog.current_database() AS database_name,
    d.oid::bigint AS database_oid,
    CURRENT_USER AS effective_principal,
    effective_role.oid::bigint AS effective_principal_oid,
    SESSION_USER AS session_principal,
    session_role.oid::bigint AS session_principal_oid,
    pg_catalog.pg_is_in_recovery() AS in_recovery,
    n.oid::bigint AS namespace_oid,
    n.nspname AS schema_name,
    c.oid::bigint AS relation_oid,
    c.relname AS relation_name,
    lock_witness.lock_witness_count
FROM snapshot_witness AS s
JOIN pg_catalog.pg_database AS d
  ON d.datname = pg_catalog.current_database()
JOIN pg_catalog.pg_roles AS effective_role
  ON effective_role.rolname = CURRENT_USER
JOIN pg_catalog.pg_roles AS session_role
  ON session_role.rolname = SESSION_USER
LEFT JOIN pg_catalog.pg_namespace AS n ON n.nspname = %s
LEFT JOIN pg_catalog.pg_class AS c
  ON c.relnamespace = n.oid AND c.relname = %s
LEFT JOIN LATERAL (
    SELECT
        pg_catalog.count(*)::integer AS lock_witness_count,
        pg_catalog.min(l.virtualtransaction) AS transaction_incarnation
    FROM pg_catalog.pg_locks AS l
    WHERE l.pid = pg_catalog.pg_backend_pid()
      AND l.locktype = 'relation'
      AND l.relation = c.oid
      AND l.mode = 'AccessShareLock'
      AND l.granted
) AS lock_witness ON true"""

_PHYSICAL_IDENTITY_SQL = """SELECT
    (pg_catalog.pg_control_system()).system_identifier::text
        AS system_identifier,
    CASE
        WHEN pg_catalog.pg_is_in_recovery()
        THEN (pg_catalog.pg_control_checkpoint()).timeline_id::bigint
        ELSE ('x' || pg_catalog.substring(
            pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_lsn()),
            1,
            8
        ))::bit(32)::bigint
    END AS timeline_id"""

_ACTIVE_SCOPE_SQL = """SELECT
    pg_catalog.txid_current_snapshot()::text AS snapshot_token,
    pg_catalog.min(l.virtualtransaction) AS transaction_incarnation,
    pg_catalog.count(*)::integer AS lock_witness_count
FROM pg_catalog.pg_locks AS l
WHERE l.pid = pg_catalog.pg_backend_pid()
  AND l.locktype = 'relation'
  AND l.relation = %s
  AND l.mode = 'AccessShareLock'
  AND l.granted"""

_RELATION_PROFILE_SQL = """SELECT
    c.oid::bigint AS relation_oid,
    c.relnamespace::bigint AS namespace_oid,
    n.nspname AS schema_name,
    c.relname AS relation_name,
    c.relkind,
    c.relpersistence,
    c.relhassubclass
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE c.oid = %s AND c.relnamespace = %s"""

_COLUMN_CATALOG_SQL = """SELECT
    c.oid::bigint AS relation_oid,
    c.relnamespace::bigint AS namespace_oid,
    c.relkind,
    c.relpersistence,
    c.relhassubclass,
    a.attnum::integer AS attribute_number,
    a.attname AS column_name,
    a.atttypid::bigint AS type_oid,
    t.typnamespace::bigint AS type_namespace_oid,
    tn.nspname AS type_namespace_name,
    t.typname AS type_name,
    t.typtype AS type_kind,
    a.atttypmod::integer AS type_modifier,
    NOT a.attnotnull AS nullable,
    a.attcollation::bigint AS collation_oid,
    a.attgenerated AS generated_kind,
    a.attidentity AS identity_kind
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
JOIN pg_catalog.pg_type AS t ON t.oid = a.atttypid
JOIN pg_catalog.pg_namespace AS tn ON tn.oid = t.typnamespace
WHERE c.oid = %s
  AND c.relnamespace = %s
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
LIMIT 1025"""

_TEMPLATE_SHA256_GOLDENS = {
    1: bytes.fromhex("4b4af3caa7213e174c85610b5bdad930bc994a10000031cc190d2e125157568b"),
    2: bytes.fromhex("7c12e711cc6a48f460892769ecb5fdb71b765b587cc9798339690bd9fb47c9f3"),
}


def _fields(*values: tuple[str, str, bool]) -> list[dict[str, object]]:
    return [{"name": name, "python_type": kind, "nullable": nullable} for name, kind, nullable in values]


def _template_items(version: int) -> list[dict[str, object]]:
    none: list[dict[str, object]] = []
    items: list[dict[str, object]] = [
        {
            "statement_id": "set_transaction",
            "sql_template_text": "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "parameter_slots": none,
            "result_fields": none,
            "cardinality": "none",
        },
        {
            "statement_id": "set_lock_timeout",
            "sql_template_text": "SET LOCAL lock_timeout = '5000ms'",
            "parameter_slots": none,
            "result_fields": none,
            "cardinality": "none",
        },
        {
            "statement_id": "lock_relation",
            "sql_template_text": "LOCK TABLE ONLY {schema_identifier}.{relation_identifier} IN ACCESS SHARE MODE",
            "parameter_slots": none,
            "result_fields": none,
            "cardinality": "none",
        },
        {
            "statement_id": "initial_snapshot_witness",
            "sql_template_text": _INITIAL_WITNESS_SQL,
            "parameter_slots": [
                {"name": "selected_schema", "kind": "utf8_text"},
                {"name": "selected_relation", "kind": "utf8_text"},
            ],
            "result_fields": _fields(
                ("snapshot_token", "str", False),
                ("visible_horizon", "int", False),
                ("transaction_incarnation", "str", True),
                ("isolation_level", "str", False),
                ("read_only", "str", False),
                ("backend_pid", "int", False),
                ("database_name", "str", False),
                ("database_oid", "int", False),
                ("effective_principal", "str", False),
                ("effective_principal_oid", "int", False),
                ("session_principal", "str", False),
                ("session_principal_oid", "int", False),
                ("in_recovery", "bool", False),
                ("namespace_oid", "int", True),
                ("schema_name", "str", True),
                ("relation_oid", "int", True),
                ("relation_name", "str", True),
                ("lock_witness_count", "int", False),
            ),
            "cardinality": "exactly_one",
        },
    ]
    physical: dict[str, object] = {
        "statement_id": "v1_physical_identity",
        "sql_template_text": _PHYSICAL_IDENTITY_SQL,
        "parameter_slots": none,
        "result_fields": _fields(("system_identifier", "str", False), ("timeline_id", "int", False)),
        "cardinality": "exactly_one",
    }
    if version == 1:
        items.append(physical)
    items.extend(
        [
            {
                "statement_id": "active_scope_revalidation",
                "sql_template_text": _ACTIVE_SCOPE_SQL,
                "parameter_slots": [{"name": "selected_relation_oid", "kind": "uint32_decimal"}],
                "result_fields": _fields(
                    ("snapshot_token", "str", False),
                    ("transaction_incarnation", "str", True),
                    ("lock_witness_count", "int", False),
                ),
                "cardinality": "exactly_one",
            },
            {
                "statement_id": "relation_profile",
                "sql_template_text": _RELATION_PROFILE_SQL,
                "parameter_slots": [
                    {"name": "selected_relation_oid", "kind": "uint32_decimal"},
                    {"name": "selected_namespace_oid", "kind": "uint32_decimal"},
                ],
                "result_fields": _fields(
                    ("relation_oid", "int", False),
                    ("namespace_oid", "int", False),
                    ("schema_name", "str", False),
                    ("relation_name", "str", False),
                    ("relkind", "str", False),
                    ("relpersistence", "str", False),
                    ("relhassubclass", "bool", False),
                ),
                "cardinality": "exactly_one",
            },
            {
                "statement_id": "column_catalog",
                "sql_template_text": _COLUMN_CATALOG_SQL,
                "parameter_slots": [
                    {"name": "selected_relation_oid", "kind": "uint32_decimal"},
                    {"name": "selected_namespace_oid", "kind": "uint32_decimal"},
                ],
                "result_fields": _fields(
                    ("relation_oid", "int", False),
                    ("namespace_oid", "int", False),
                    ("relkind", "str", False),
                    ("relpersistence", "str", False),
                    ("relhassubclass", "bool", False),
                    ("attribute_number", "int", False),
                    ("column_name", "str", False),
                    ("type_oid", "int", False),
                    ("type_namespace_oid", "int", False),
                    ("type_namespace_name", "str", False),
                    ("type_name", "str", False),
                    ("type_kind", "str", False),
                    ("type_modifier", "int", False),
                    ("nullable", "bool", False),
                    ("collation_oid", "int", False),
                    ("generated_kind", "str", False),
                    ("identity_kind", "str", False),
                ),
                "cardinality": "zero_to_1025",
            },
        ]
    )
    return items


def _template_digest(version: int) -> bytes:
    document = {
        "contract_version": "dpone-postgres-mssql-source-schema-query-template-profile-1",
        "items": _template_items(version),
    }
    encoded = json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(b"dpone-postgres-mssql-source-schema-query-template-profile-v1\0" + encoded).digest()


def test_exact_v1_v2_query_template_authorities_match_independent_sql_and_shape_goldens() -> None:
    modules = {}
    for key, name in {
        "issuer": "dpone.runtime.sources.postgres_mssql_source_schema_issuer",
        "observation": "dpone.runtime.sources.postgres_mssql_source_schema_observation",
    }.items():
        try:
            modules[key] = importlib.import_module(name)
        except ModuleNotFoundError:
            pytest.fail(f"approved implementation missing: {name}", pytrace=False)
    from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier
    from tests.test_postgres_mssql_r1_source_schema_runtime import _load_config, _source_authority

    for version in (1, 2):
        selected = PostgresSourceAuthorityVerifier(_source_authority(version=version)).select_for(_load_config())
        profile = (
            modules["issuer"]
            .PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
            .bind_query_profile(selected_source_authority=selected)
        )
        observed = [item.to_template_document() for item in profile.ordered_items]
        assert observed == _template_items(version)
        expected = _template_digest(version)
        assert expected == _TEMPLATE_SHA256_GOLDENS[version]
        assert profile.query_template_profile_sha256 == expected
        assert getattr(modules["observation"], f"POSTGRES_MSSQL_QUERY_TEMPLATE_PROFILE_SHA256_V{version}") == expected


def test_template_golden_mutations_change_digest_and_cannot_preserve_shape() -> None:
    baseline = _template_items(1)
    for mutate in (
        lambda items: items[0].update(sql_template_text=items[0]["sql_template_text"] + " "),
        lambda items: items.reverse(),
        lambda items: items[3]["parameter_slots"][0].update(name="wrong"),
        lambda items: items[3]["result_fields"][0].update(python_type="int"),
        lambda items: items[3]["result_fields"][0].update(nullable=True),
        lambda items: items[3].update(cardinality="none"),
    ):
        candidate = json.loads(json.dumps(baseline))
        mutate(candidate)
        assert candidate != baseline
        document = {
            "contract_version": "dpone-postgres-mssql-source-schema-query-template-profile-1",
            "items": candidate,
        }
        encoded = json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
        assert hashlib.sha256(
            b"dpone-postgres-mssql-source-schema-query-template-profile-v1\0" + encoded
        ).digest() != _template_digest(1)
