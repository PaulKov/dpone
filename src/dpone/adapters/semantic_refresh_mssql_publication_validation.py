"""Closed request validation for transactional MSSQL publication state."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from dpone.adapters.semantic_refresh_mssql_publication_authority import MssqlPublicationCanonicalAuthority

from dpone.adapters.semantic_refresh_mssql_publication_values import (
    require_closed as _require_closed,
)
from dpone.adapters.semantic_refresh_mssql_publication_values import (
    require_digest as _require_digest,
)
from dpone.adapters.semantic_refresh_mssql_publication_values import (
    require_identifier as require_identifier,
)
from dpone.adapters.semantic_refresh_mssql_publication_values import (
    require_non_negative_int as _require_non_negative_int,
)
from dpone.adapters.semantic_refresh_mssql_publication_values import (
    require_positive_int as _require_positive_int,
)
from dpone.adapters.semantic_refresh_mssql_publication_values import (
    require_text as _require_text,
)
from dpone.adapters.semantic_refresh_mssql_publication_values import (
    require_uuid as _require_uuid,
)
from dpone.adapters.semantic_refresh_mssql_publication_values import (
    validate_authority_projection as _validate_authority_projection,
)

_TRANSITION_FIELDS = {
    "operation_id",
    "operation_plan_sha256",
    "workflow_execution_binding_sha256",
    "attempt_binding_sha256",
    "fence_epoch",
    "prepare_receipt_sha256",
    "target_uuid",
    "expected_journal_state",
    "next_journal_state",
}
_PRE_EXCHANGE_HEAD_FIELDS = frozenset(
    {
        "expected_target_uuid",
        "expected_target_generation",
        "expected_scope_revision",
        "expected_checkpoint_sha256",
    }
)
_PREPARED_DOCUMENT_FIELDS = {
    "prepare_plan_sha256",
    "prepare_plan_json",
    "prepared_receipt_sha256",
    "prepared_receipt_json",
    "artifact_manifest_key",
    "artifact_manifest_version",
    "artifact_manifest_sha256",
}


class SemanticRefreshMssqlPublicationError(RuntimeError):
    """Raised when exact journal/head publication cannot be proven."""


def assert_transition_authority(
    authority: MssqlPublicationCanonicalAuthority,
    cursor: Any,
    request: Mapping[str, object],
    journal: tuple[Any, ...],
    *,
    uuid_text: Callable[[object], str | None],
) -> None:
    """Match the requested transition to its durable journal and authority."""

    if (journal[0], journal[1], journal[2]) != (
        request["operation_plan_sha256"],
        request["attempt_binding_sha256"],
        request["fence_epoch"],
    ):
        raise SemanticRefreshMssqlPublicationError("journal identity differs from transition")
    protected_predecessors = (
        request["target_predecessor_generation_id"],
        request["scope_predecessor_operation_id"],
        request["predecessor_target_generation"],
        request["predecessor_target_uuid"],
        request["predecessor_target_operation_id"],
        request["predecessor_scope_revision"],
        request["predecessor_checkpoint_sha256"],
        request["predecessor_checkpoint_operation_id"],
        request["predecessor_checkpoint_version"],
    )
    durable_predecessors = (*journal[10:13], uuid_text(journal[13]), *journal[14:19])
    if durable_predecessors != protected_predecessors:
        raise SemanticRefreshMssqlPublicationError("journal predecessor authority differs")
    authority.assert_execution_and_guard(cursor, request, journal)
    authority.assert_authorized(cursor, request, journal)


def assert_publication_authority(
    authority: MssqlPublicationCanonicalAuthority,
    cursor: Any,
    request: Mapping[str, object],
    journal: tuple[Any, ...],
    *,
    uuid_text: Callable[[object], str | None],
) -> None:
    """Match terminal publication evidence to the durable transition authority."""

    assert_transition_authority(authority, cursor, request, journal, uuid_text=uuid_text)
    expected_uuid = (
        request["target_uuid"]
        if journal[3] in {"TARGET_COMMITTED", "COMMITTED_INCOMPLETE", "COMPLETE"}
        else request["expected_target_uuid"]
    )
    if (journal[4], uuid_text(journal[5])) != (
        request["prepare_receipt_sha256"],
        expected_uuid,
    ):
        raise SemanticRefreshMssqlPublicationError("COMMITTING receipt/target predecessor differs")


def validate_prepared_request(
    request: Mapping[str, object],
    *,
    protected: bool,
    authority_fields: frozenset[str],
) -> None:
    """Validate the exact PREPARING-to-PREPARED request."""

    legacy = _TRANSITION_FIELDS | {"workflow_plan_sha256"} | _PREPARED_DOCUMENT_FIELDS
    _require_closed(
        request,
        legacy=legacy,
        protected=legacy | authority_fields,
        required=protected,
    )
    if request["expected_journal_state"] != "PREPARING" or request["next_journal_state"] != "PREPARED":
        raise ValueError("prepared transition states are invalid")
    _validate_transition_identity(request)
    for field_name in (
        "prepare_plan_sha256",
        "prepared_receipt_sha256",
        "artifact_manifest_sha256",
    ):
        _require_digest(request[field_name], field_name)
    for field_name in (
        "prepare_plan_json",
        "prepared_receipt_json",
        "artifact_manifest_key",
        "artifact_manifest_version",
    ):
        _require_text(request[field_name], field_name)
    if request["prepared_receipt_sha256"] != request["prepare_receipt_sha256"]:
        raise ValueError("prepared receipt digest differs from transition")


def validate_committing_request(
    request: Mapping[str, object],
    *,
    protected: bool,
    authority_fields: frozenset[str],
) -> None:
    """Validate PREPARED-to-COMMITTING identity and predecessor heads."""

    _require_closed(
        request,
        legacy=_TRANSITION_FIELDS,
        protected=(_TRANSITION_FIELDS | authority_fields | _PRE_EXCHANGE_HEAD_FIELDS | {"workflow_plan_sha256"}),
        required=protected,
    )
    if request["expected_journal_state"] != "PREPARED" or request["next_journal_state"] != "COMMITTING":
        raise ValueError("committing transition states are invalid")
    _validate_transition_identity(request)


def validate_post_exchange_request(
    request: Mapping[str, object],
    *,
    protected: bool,
    incomplete: bool,
    authority_fields: frozenset[str],
) -> None:
    """Validate exact durable post-exchange evidence/state transitions."""

    fields = _TRANSITION_FIELDS | {
        "workflow_plan_sha256",
        "clickhouse_commit_receipt_sha256",
        "expected_target_uuid",
    }
    _require_closed(
        request,
        legacy=fields,
        protected=fields | authority_fields,
        required=protected,
    )
    expected = ("TARGET_COMMITTED", "COMMITTED_INCOMPLETE") if incomplete else ("COMMITTING", "TARGET_COMMITTED")
    if (request["expected_journal_state"], request["next_journal_state"]) != expected:
        raise ValueError("post-exchange transition states are invalid")
    _validate_transition_identity(request)
    _require_digest(request["workflow_plan_sha256"], "workflow_plan_sha256")
    _require_digest(request["clickhouse_commit_receipt_sha256"], "clickhouse_commit_receipt_sha256")
    _require_uuid(request["expected_target_uuid"], "expected_target_uuid")
    if request["target_uuid"] == request["expected_target_uuid"]:
        raise ValueError("post-exchange target UUID must be the proven successor")


def validate_commit_reconciliation_request(
    request: Mapping[str, object],
    *,
    protected: bool,
    commit_unknown: bool,
    authority_fields: frozenset[str],
) -> None:
    """Validate COMMITTING→COMMIT_UNKNOWN or proven-not-committed recovery."""

    fields = _TRANSITION_FIELDS | {"workflow_plan_sha256"}
    _require_closed(
        request,
        legacy=fields,
        protected=fields | authority_fields,
        required=protected,
    )
    expected = ("COMMITTING", "COMMIT_UNKNOWN") if commit_unknown else ("COMMIT_UNKNOWN", "PREPARED")
    if (request["expected_journal_state"], request["next_journal_state"]) != expected:
        raise ValueError("commit reconciliation transition states are invalid")
    _validate_transition_identity(request)
    _require_digest(request["workflow_plan_sha256"], "workflow_plan_sha256")


def validate_publication_request(
    request: Mapping[str, object],
    *,
    protected: bool,
    authority_fields: frozenset[str],
) -> None:
    """Validate one exact COMMITTING-to-COMPLETE multi-head request."""

    expected = {
        "operation_id",
        "operation_plan_sha256",
        "workflow_plan_sha256",
        "workflow_execution_binding_sha256",
        "attempt_binding_sha256",
        "fence_epoch",
        "database",
        "target_table",
        "scope_id",
        "prepare_receipt_sha256",
        "clickhouse_commit_receipt_sha256",
        "terminal_receipt_sha256",
        "target_uuid",
        "expected_target_uuid",
        "expected_target_generation",
        "target_generation",
        "target_generation_id",
        "expected_scope_revision",
        "scope_revision",
        "expected_checkpoint_sha256",
        "checkpoint_sha256",
        "target_mutation_outcome",
        "value_conversion_outcome",
        "expected_journal_state",
        "terminal_journal_state",
    }
    _require_closed(
        request,
        legacy=expected,
        protected=expected | authority_fields,
        required=protected,
    )
    _validate_transition_identity(request)
    for field_name in (
        "workflow_plan_sha256",
        "clickhouse_commit_receipt_sha256",
        "terminal_receipt_sha256",
        "target_generation_id",
        "checkpoint_sha256",
    ):
        _require_digest(request[field_name], field_name)
    if request["expected_checkpoint_sha256"] is not None:
        _require_digest(request["expected_checkpoint_sha256"], "expected_checkpoint_sha256")
    _require_uuid(request["expected_target_uuid"], "expected_target_uuid")
    for field_name in ("database", "target_table", "scope_id"):
        _require_text(request[field_name], field_name)
    for field_name in (
        "expected_target_generation",
        "target_generation",
        "expected_scope_revision",
        "scope_revision",
    ):
        _require_non_negative_int(request[field_name], field_name)
    expected_target_generation = cast(int, request["expected_target_generation"])
    expected_scope_revision = cast(int, request["expected_scope_revision"])
    mutation_outcome = request["target_mutation_outcome"]
    if mutation_outcome == "TARGET_COMMITTED":
        if request["target_generation"] != expected_target_generation + 1:
            raise ValueError("target generation must advance exactly once")
        if request["value_conversion_outcome"] != "CONFORMANT":
            raise ValueError("target commit conversion outcome is invalid")
        expected_journal_states = {"TARGET_COMMITTED", "COMMITTED_INCOMPLETE", "COMPLETE"}
    elif mutation_outcome == "NOT_REQUIRED_EMPTY_SCOPE":
        if (
            request["target_generation"] != expected_target_generation
            or request["target_generation_id"] != request["target_predecessor_generation_id"]
            or request["target_uuid"] != request["expected_target_uuid"]
            or request["value_conversion_outcome"] != "NOT_APPLICABLE_NO_DATA"
        ):
            raise ValueError("empty-scope target successor is invalid")
        expected_journal_states = {"PREPARED"}
    else:
        raise ValueError("target mutation outcome is invalid")
    if request["scope_revision"] != expected_scope_revision + 1:
        raise ValueError("scope revision must advance exactly once")
    if (
        request["expected_journal_state"] not in expected_journal_states
        or request["terminal_journal_state"] != "COMPLETE"
    ):
        raise ValueError("terminal journal transition is invalid")


def _validate_transition_identity(request: Mapping[str, object]) -> None:
    for field_name in (
        "operation_plan_sha256",
        "workflow_execution_binding_sha256",
        "attempt_binding_sha256",
        "prepare_receipt_sha256",
    ):
        _require_digest(request[field_name], field_name)
    _require_digest(request["operation_id"], "operation_id")
    _require_uuid(request["target_uuid"], "target_uuid")
    _require_positive_int(request["fence_epoch"], "fence_epoch")
    if "publication_authority_sha256" in request:
        _validate_authority_projection(request)
    if "expected_target_generation" in request:
        _require_uuid(request["expected_target_uuid"], "expected_target_uuid")
        _require_non_negative_int(request["expected_target_generation"], "expected_target_generation")
        _require_non_negative_int(request["expected_scope_revision"], "expected_scope_revision")
        if request["expected_checkpoint_sha256"] is not None:
            _require_digest(request["expected_checkpoint_sha256"], "expected_checkpoint_sha256")


__all__ = [
    "SemanticRefreshMssqlPublicationError",
    "assert_publication_authority",
    "assert_transition_authority",
    "require_identifier",
    "validate_committing_request",
    "validate_prepared_request",
    "validate_publication_request",
]
