"""Historical public identities survive cohesive implementation ownership changes."""

import importlib
import pickle

import pytest


@pytest.mark.parametrize(
    ("historical", "owner", "names"),
    [
        (
            "contracts.composition_snapshot",
            "contracts.composition_snapshot_subjects",
            "SnapshotTarget SnapshotLimits SnapshotGeneration SnapshotCatalogObservation SnapshotPublisherClosure",
        ),
        (
            "contracts.composition_attempt",
            "contracts.composition_persistence",
            "CompositionAttemptIdentity CompositionAttemptReceipt require_composition_attempt_scope require_composition_attempt_admission",
        ),
        (
            "contracts.composition_proof",
            "contracts.composition_persistence",
            "CompositionAttemptProof CompositionProofAuthority composition_attempt_epoch_subject",
        ),
        ("adapters.composition_mssql_gate_schema", "adapters.composition_mssql_catalog_types", "module_sha256"),
        (
            "contracts.composition_activation",
            "contracts.composition_identity",
            "CompositionAdmissionError require_digest require_text require_ordered_unique",
        ),
        (
            "contracts.nonproduction_scope",
            "contracts.nonproduction_document",
            "NonproductionAuthorityError exact_fields canonical_document document_sha256 parse_document digest text uuid_text repository sequence ordered",
        ),
        (
            "adapters.composition_mssql_layout",
            "adapters.composition_mssql_catalog_types",
            "CompositionColumn CompositionKey CompositionForeignKey CompositionCheck CompositionTable CompositionTrigger",
        ),
        (
            "adapters.composition_mssql_invariants",
            "adapters.composition_mssql_schema",
            "composition_invariant_trigger_sql",
        ),
        (
            "adapters.composition_mssql_gate_catalog",
            "adapters.composition_mssql_catalog",
            "require_composition_mssql_gate_schema",
        ),
        (
            "services.composition_activation_preparation",
            "services.composition_activation_coordinator",
            "CompositionActivationPreparation",
        ),
    ],
)
def test_historical_exports_preserve_identity_provenance_and_pickle(historical, owner, names):
    old = importlib.import_module("dpone." + historical)
    new = importlib.import_module("dpone." + owner)
    for name in names.split():
        value = getattr(old, name)
        assert value is getattr(new, name)
        assert value.__module__ == old.__name__
        assert pickle.loads(pickle.dumps(value)) is value


def test_catalog_values_round_trip_through_historical_pickle_locator():
    from dpone.adapters.composition_mssql_layout import COMPOSITION_TABLES

    for value in COMPOSITION_TABLES:
        assert pickle.loads(pickle.dumps(value)) == value


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        ("dpone_control", "4ba5f294030e7c2da8cb867adfbb2fcba45ba87df623fa24f9f7d86e858ae990"),
        ("tenant_control", "49a033f98df8a76c3254fbe8d19d1753bd396e8e2ff0a58d43c79c4d457ed23f"),
    ],
)
def test_core_ddl_and_invariant_sql_bytes_match_approved_base(schema, expected):
    # Generated from 5dd3346568595128a027ede3eb0858337945c97f before extraction.
    from hashlib import sha256

    from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema

    assert sha256(render_composition_mssql_schema(schema).encode("utf-8")).hexdigest() == expected


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        ("dpone_control", "79a1c7630442a7ee326801ca316a9455578a07b06e6c0cc4b59ab10ad4b0027f"),
        ("tenant_control", "941f9114be02b2d188da66c5228835b2e70f5f5730fc4fa8a75786b418da82c3"),
    ],
)
def test_login_gate_ddl_bytes_match_approved_base(schema, expected):
    from hashlib import sha256

    from dpone.adapters.composition_mssql_gate_schema import render_composition_mssql_login_gate

    sql = render_composition_mssql_login_gate(control_database="control", control_schema=schema)
    assert sha256(sql.encode("utf-8")).hexdigest() == expected


def test_durable_attempt_codec_and_pickle_round_trip_preserve_original_identity():
    from dpone.contracts.composition_persistence import decode_attempt_identity, encode_attempt_identity
    from tests.composition_snapshot_helpers import intent

    attempt = intent().attempt
    encoded = encode_attempt_identity(attempt)
    assert decode_attempt_identity(encoded, attempt.attempt_sha256) == attempt
    assert pickle.loads(pickle.dumps(attempt)) == attempt
    assert encode_attempt_identity(pickle.loads(pickle.dumps(attempt))) == encoded


def test_module_digest_preserves_sql_server_nvarchar_bytes():
    from hashlib import sha256

    from dpone.adapters.composition_mssql_gate_schema import module_sha256

    sql = "SELECT N'Привет 👋';\r\n"
    assert module_sha256(sql) == sha256(sql.encode("utf-16le")).digest()


def test_documented_source_and_plan_dataclass_types_resolve_without_extra_namespace():
    from typing import get_type_hints

    from dpone.contracts.composition_execution import CompositionExecutionPlan
    from dpone.contracts.composition_sources import CompositionSourceSnapshot
    from dpone.contracts.dbt_relation_writes import DbtRelationWrite
    from dpone.contracts.dbt_source_inventory_binding import DbtReleaseSources
    from dpone.contracts.release_composition_ordinary import OrdinaryReleaseCapture

    source_types = get_type_hints(CompositionSourceSnapshot)
    assert source_types["native"] is DbtReleaseSources
    assert source_types["ordinary"] is OrdinaryReleaseCapture
    plan_types = get_type_hints(CompositionExecutionPlan)
    assert plan_types["sources"] is CompositionSourceSnapshot
    assert plan_types["writes"] == tuple[DbtRelationWrite, ...]


def test_snapshot_field_types_and_value_pickle_resolve_through_historical_module():
    from typing import get_type_hints

    from dpone.contracts.composition_snapshot import (
        SnapshotCatalogObservation,
        SnapshotPublicationIntent,
        SnapshotPublicationRecord,
        SnapshotTarget,
    )
    from tests.composition_snapshot_helpers import intent, observation

    assert get_type_hints(SnapshotPublicationIntent)["target"] is SnapshotTarget
    assert get_type_hints(SnapshotCatalogObservation)["target"] is SnapshotTarget
    assert get_type_hints(SnapshotPublicationRecord)["intent"] is SnapshotPublicationIntent
    value = intent()
    for subject in (value.target, value.generation, value.limits, observation(value)):
        assert pickle.loads(pickle.dumps(subject)) == subject


@pytest.mark.parametrize(
    ("service", "digest", "reason"),
    [
        ("invalid", "invalid", "protected_service_id"),
        ("00000000-0000-0000-0000-000000000001", "invalid", "digest"),
    ],
)
def test_mutated_snapshot_guard_preserves_physical_validation_precedence(service, digest, reason):
    from dpone.contracts.composition_activation import CompositionAdmissionError
    from tests.composition_snapshot_helpers import target

    value = target()
    object.__setattr__(value, "service_id", service)
    object.__setattr__(value, "physical_subject_sha256", digest)
    with pytest.raises(CompositionAdmissionError) as caught:
        _ = value.guard_id
    assert caught.value.reason == reason


def test_snapshot_guard_checks_only_domain_tuple_and_preserves_zero_uuid_domain_semantics():
    from dpone.contracts.composition_physical import CompositionPhysicalDomain
    from tests.composition_snapshot_helpers import target

    value = target()
    object.__setattr__(value, "service_id", "00000000-0000-0000-0000-000000000000")
    object.__setattr__(value, "generation_table", "")
    assert (
        value.guard_id
        == CompositionPhysicalDomain("clickhouse", value.service_id, value.physical_subject_sha256).guard_id
    )
