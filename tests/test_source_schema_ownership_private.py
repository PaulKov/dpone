"""Boundary completion and schema authority ownership regressions."""

from types import SimpleNamespace

import pytest

import dpone.contracts.postgres_mssql_source_schema_authority as authority_module
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import PreparedPostgresSourceBoundary
from tests.test_postgres_mssql_r1_source_schema_runtime import FakeCatalogConnector, _policy


@pytest.mark.parametrize(
    "receipt",
    [
        None,
        SimpleNamespace(outcome="aborted", cleanup_succeeded=True),
        SimpleNamespace(outcome="completed", cleanup_succeeded=False),
        SimpleNamespace(outcome="completed", cleanup_succeeded=True),
    ],
)
def test_completed_admission_is_read_only(receipt):
    boundary = object.__new__(PreparedPostgresSourceBoundary)
    scope = SimpleNamespace(terminal_receipt=receipt)
    object.__setattr__(boundary, "_runtime_issued", True)
    object.__setattr__(boundary, "_scope", scope)
    if receipt is not None and receipt.outcome == "completed" and receipt.cleanup_succeeded:
        assert boundary.require_completed() is None
    else:
        with pytest.raises(authority_module.PostgresMssqlSourceSchemaAuthorityErrorV1) as error:
            boundary.require_completed()
        assert error.value.reason == "snapshot_cleanup_failed"
    assert scope.terminal_receipt is receipt


def test_column_derivation_and_exact_policy_admission():
    policy = _policy()
    assert authority_module.require_issuer_type_policy(policy) is policy
    with pytest.raises(authority_module.PostgresMssqlSourceSchemaAuthorityErrorV1) as error:
        authority_module.require_issuer_type_policy(object())
    assert error.value.reason == "exact_type_violation"
    column = authority_module.derive_observed_source_column(policy, FakeCatalogConnector._column(1), 1, b"a" * 32)
    assert column.name == "column_1"
    assert column.selected_source_authority_sha256 == b"a" * 32


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"type_namespace_oid": 12}, "column_type_identity_invalid"),
        ({"generated_kind": "s"}, "source_column_unsupported"),
        ({"type_oid": 23}, "type_policy_mismatch"),
        ({"column_name": ""}, "type_policy_mismatch"),
    ],
)
def test_column_derivation_errors(changes, reason):
    row = FakeCatalogConnector._column(1) | changes
    with pytest.raises(authority_module.PostgresMssqlSourceSchemaAuthorityErrorV1) as error:
        authority_module.derive_observed_source_column(_policy(), row, 1, b"a" * 32)
    assert error.value.reason == reason


def test_scalar_renderers_keep_runtime_export_identity():
    import dpone.adapters.postgres_mssql_source_schema_rendering as rendering
    import dpone.runtime.sources.postgres_mssql_source_schema_projection as projection

    for name in ("render_mssql_target_type", "render_postgres_declared_type", "render_transfer_representation"):
        assert getattr(projection, name) is getattr(rendering, name)


def test_issuer_model_error_uses_direct_reason_not_constructor_translation():
    from dpone.contracts.postgres_mssql_source_schema_models import PostgresMssqlSourceSchemaModelErrorV1

    failure = PostgresMssqlSourceSchemaModelErrorV1("type_policy_exact_type_required")
    assert authority_module.translate_issuer_failure(failure).reason == "internal_invariant_violation"
    assert authority_module.translate_model_reason(failure.reason).reason == "exact_type_violation"


def test_authority_decision_rejects_missing_and_duplicate_matches():
    authority = object.__new__(authority_module.PostgresMssqlSelectedRelationSchemaAuthorityV1)
    policy = _policy()
    decision = policy.ordered_decisions[0]
    for decisions in ((), (decision,), (decision, decision)):
        object.__setattr__(authority, "type_policy_authority", SimpleNamespace(ordered_decisions=decisions))
        if len(decisions) == 1:
            assert authority.decision_for(decision.source_shape) is decision
        else:
            with pytest.raises(authority_module.PostgresMssqlSourceSchemaAuthorityErrorV1) as error:
                authority.decision_for(decision.source_shape)
            assert error.value.reason == "type_policy_mismatch"


def test_selected_document_projection_preserves_authority_bytes():
    from tests.test_postgres_mssql_r1_source_schema_runtime import issue_authority
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    authority, scope, *_ = issue_authority(_feature_modules())
    try:
        before = authority.canonical_bytes
        selected = authority.selected_source_document
        assert selected.relation.relation_oid == authority.ordered_columns[0].relation_oid
        assert selected == type(selected).from_authority_document_utf8(authority.selected_source_document_utf8)
        assert authority.canonical_bytes == before
    finally:
        scope.close_if_active()
