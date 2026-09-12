"""Canonical selected-relation schema authority for PostgreSQL to MSSQL R1."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_codec import (
    canonical_bytes,
    decode_canonical_bytes,
    expect_bool,
    expect_bytes,
    expect_text,
    expect_tuple,
)
from dpone.contracts.postgres_mssql_source_schema_models import (
    PostgresMssqlObservedSourceColumnV1,
    PostgresMssqlSourceSchemaModelErrorV1,
    PostgresSelectedRelationAuthorityDocumentV1,
    require_type_policy,
)
from dpone.contracts.postgres_mssql_type_authority import PostgresMssqlTypePolicyAuthorityV1

if TYPE_CHECKING:
    from dpone.contracts.postgres_mssql_type_target_shapes import PostgresMssqlSourceScalarShapeV1


_AUTHORITY_DOMAIN = b"dpone-postgres-mssql-selected-relation-schema-authority-v1\0"
_AUTHORITY_VERSION = "dpone-postgres-mssql-selected-relation-schema-authority-1"
_OBSERVATION_PROFILE = "postgres-16-live-user-columns-only-relation-v1"


class PostgresMssqlSourceSchemaRecoveryV1(StrEnum):
    PERMANENT_INPUT_ERROR = "permanent_input_error"
    PERMANENT_CAPABILITY_ERROR = "permanent_capability_error"
    RETRYABLE_SOURCE = "retryable_source"
    OPERATOR_INTERVENTION = "operator_intervention"


_RECOVERY = {
    "wrong_domain": PostgresMssqlSourceSchemaRecoveryV1.PERMANENT_INPUT_ERROR,
    "wrong_version": PostgresMssqlSourceSchemaRecoveryV1.OPERATOR_INTERVENTION,
    "malformed_canonical_bytes": PostgresMssqlSourceSchemaRecoveryV1.PERMANENT_INPUT_ERROR,
    "exact_type_violation": PostgresMssqlSourceSchemaRecoveryV1.PERMANENT_INPUT_ERROR,
    "snapshot_lease_mismatch": PostgresMssqlSourceSchemaRecoveryV1.RETRYABLE_SOURCE,
    "source_authority_mismatch": PostgresMssqlSourceSchemaRecoveryV1.OPERATOR_INTERVENTION,
    "relation_lock_not_proven": PostgresMssqlSourceSchemaRecoveryV1.RETRYABLE_SOURCE,
    "relation_profile_unsupported": PostgresMssqlSourceSchemaRecoveryV1.PERMANENT_CAPABILITY_ERROR,
    "metadata_permission_denied": PostgresMssqlSourceSchemaRecoveryV1.OPERATOR_INTERVENTION,
    "catalog_observation_failed": PostgresMssqlSourceSchemaRecoveryV1.RETRYABLE_SOURCE,
    "column_count_invalid": PostgresMssqlSourceSchemaRecoveryV1.PERMANENT_CAPABILITY_ERROR,
    "column_ordinal_invalid": PostgresMssqlSourceSchemaRecoveryV1.OPERATOR_INTERVENTION,
    "column_identifier_unsupported": PostgresMssqlSourceSchemaRecoveryV1.PERMANENT_CAPABILITY_ERROR,
    "column_type_identity_invalid": PostgresMssqlSourceSchemaRecoveryV1.OPERATOR_INTERVENTION,
    "source_column_unsupported": PostgresMssqlSourceSchemaRecoveryV1.PERMANENT_CAPABILITY_ERROR,
    "type_policy_mismatch": PostgresMssqlSourceSchemaRecoveryV1.OPERATOR_INTERVENTION,
    "authority_splice": PostgresMssqlSourceSchemaRecoveryV1.OPERATOR_INTERVENTION,
    "snapshot_cleanup_failed": PostgresMssqlSourceSchemaRecoveryV1.RETRYABLE_SOURCE,
    "internal_invariant_violation": PostgresMssqlSourceSchemaRecoveryV1.OPERATOR_INTERVENTION,
}
_MESSAGES = {
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


class PostgresMssqlSourceSchemaAuthorityErrorV1(ValueError):
    """Stable route-owned source-schema failure."""

    def __init__(self, reason: str) -> None:
        if reason not in _RECOVERY:
            reason = "internal_invariant_violation"
        self.reason = reason
        self.recovery = _RECOVERY[reason]
        super().__init__(_MESSAGES[reason])

    @property
    def __cause__(self) -> None:
        return None

    @__cause__.setter
    def __cause__(self, value: object) -> None:
        del value

    @property
    def __context__(self) -> None:
        return None

    @__context__.setter
    def __context__(self, value: object) -> None:
        del value

    @classmethod
    def from_internal_reason(cls, reason: str) -> PostgresMssqlSourceSchemaAuthorityErrorV1:
        return cls(reason if reason in _RECOVERY else "internal_invariant_violation")


def translate_model_reason(reason: str) -> PostgresMssqlSourceSchemaAuthorityErrorV1:
    if reason == "type_policy_exact_type_required":
        return PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
    if reason in _RECOVERY:
        return PostgresMssqlSourceSchemaAuthorityErrorV1(reason)
    return PostgresMssqlSourceSchemaAuthorityErrorV1("malformed_canonical_bytes")


def translate_issuer_failure(error: Exception) -> PostgresMssqlSourceSchemaAuthorityErrorV1:
    """Preserve issuer errors separately from canonical constructor translation."""
    if isinstance(error, PostgresMssqlSourceSchemaModelErrorV1):
        return PostgresMssqlSourceSchemaAuthorityErrorV1(error.reason)
    reason_id = getattr(error, "reason_id", None)
    reason = "type_policy_mismatch" if reason_id == "policy_coverage_invalid" else "internal_invariant_violation"
    return PostgresMssqlSourceSchemaAuthorityErrorV1(reason)


def require_issuer_type_policy(value: object) -> PostgresMssqlTypePolicyAuthorityV1:
    """Admit issuer policy while preserving its exact-type error contract."""
    try:
        return require_type_policy(value)
    except PostgresMssqlSourceSchemaModelErrorV1:
        raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation") from None


def derive_observed_source_column(
    policy: PostgresMssqlTypePolicyAuthorityV1, row: dict[str, Any], ordinal: int, source_digest: bytes
) -> PostgresMssqlObservedSourceColumnV1:
    """Derive one catalog column through the already admitted exact type policy."""
    if row["type_namespace_oid"] != 11 or row["type_namespace_name"] != "pg_catalog":
        raise PostgresMssqlSourceSchemaAuthorityErrorV1("column_type_identity_invalid")
    if row["type_kind"] != "b" or row["generated_kind"] != "" or row["identity_kind"] not in {"", "a", "d"}:
        raise PostgresMssqlSourceSchemaAuthorityErrorV1("source_column_unsupported")
    try:
        shape = policy.resolve_catalog_shape(row["type_namespace_oid"], row["type_oid"], row["type_modifier"])
        reference = policy.source_column_ref(
            ordinal=ordinal,
            name=row["column_name"],
            nullable=row["nullable"],
            source_shape=shape,
        )
    except Exception:
        raise PostgresMssqlSourceSchemaAuthorityErrorV1("type_policy_mismatch") from None
    try:
        return PostgresMssqlObservedSourceColumnV1(
            "dpone-postgres-mssql-observed-source-column-1",
            source_digest,
            row["namespace_oid"],
            row["relation_oid"],
            ordinal,
            row["attribute_number"],
            row["column_name"],
            row["type_oid"],
            row["type_namespace_oid"],
            row["type_namespace_name"],
            row["type_name"],
            row["type_kind"],
            row["type_modifier"],
            row["nullable"],
            row["collation_oid"],
            row["generated_kind"],
            row["identity_kind"],
            shape,
            reference,
        )
    except PostgresMssqlSourceSchemaModelErrorV1 as error:
        raise PostgresMssqlSourceSchemaAuthorityErrorV1(error.reason) from None


@dataclass(frozen=True, slots=True)
class PostgresMssqlSelectedRelationSchemaAuthorityV1:
    """One complete source-owned relation and type-policy authority."""

    contract_version: str
    selected_source_document_utf8: bytes
    selected_source_authority_sha256: bytes
    observation_profile: str
    relation_kind: str
    relation_persistence: str
    relation_has_subclass: bool
    ordered_columns: tuple[PostgresMssqlObservedSourceColumnV1, ...]
    type_policy_authority: PostgresMssqlTypePolicyAuthorityV1

    def __post_init__(self) -> None:
        try:
            self._validate()
        except asyncio.CancelledError:
            raise
        except PostgresMssqlSourceSchemaAuthorityErrorV1:
            raise
        except PostgresMssqlSourceSchemaModelErrorV1 as exc:
            error = translate_model_reason(exc.reason)
            exc.__traceback__ = None
            raise error from None
        except Exception:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation") from None

    def _validate(self) -> None:
        if self.contract_version != _AUTHORITY_VERSION:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("wrong_version")
        if type(self.selected_source_document_utf8) is not bytes:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
        if type(self.selected_source_authority_sha256) is not bytes or len(self.selected_source_authority_sha256) != 32:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
        selected = PostgresSelectedRelationAuthorityDocumentV1.from_authority_document_utf8(
            self.selected_source_document_utf8
        )
        if hashlib.sha256(self.selected_source_document_utf8).digest() != self.selected_source_authority_sha256:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("authority_splice")
        if self.observation_profile != _OBSERVATION_PROFILE:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("wrong_version")
        if (
            self.relation_kind != "r"
            or self.relation_persistence != "p"
            or type(self.relation_has_subclass) is not bool
        ):
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("relation_profile_unsupported")
        if self.relation_has_subclass:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("relation_profile_unsupported")
        if type(self.ordered_columns) is not tuple or not 1 <= len(self.ordered_columns) <= 1024:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("column_count_invalid")
        policy = require_type_policy(self.type_policy_authority)
        prior_attribute = 0
        folded: set[str] = set()
        for ordinal, column in enumerate(self.ordered_columns, start=1):
            if type(column) is not PostgresMssqlObservedSourceColumnV1:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
            if column.projection_ordinal != ordinal or column.attribute_number <= prior_attribute:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("column_ordinal_invalid")
            if column.name.casefold() in folded:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("column_identifier_unsupported")
            folded.add(column.name.casefold())
            prior_attribute = column.attribute_number
            if (
                column.selected_source_authority_sha256 != self.selected_source_authority_sha256
                or column.namespace_oid != selected.relation.namespace_oid
                or column.relation_oid != selected.relation.relation_oid
                or column.source_column_ref.type_policy_digest != policy.digest
            ):
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("authority_splice")
            column.source_column_ref.validate_against_policy(policy)

    @property
    def selected_source_document(self) -> PostgresSelectedRelationAuthorityDocumentV1:
        """Decode the admitted selected relation document without changing its bytes."""
        return PostgresSelectedRelationAuthorityDocumentV1.from_authority_document_utf8(
            self.selected_source_document_utf8
        )

    def decision_for(
        self,
        source_shape: PostgresMssqlSourceScalarShapeV1,
    ) -> Any:
        """Require one canonical type decision for this source shape."""
        matches = tuple(
            item
            for item in self.type_policy_authority.ordered_decisions
            if item.source_shape.canonical_bytes == source_shape.canonical_bytes
        )
        if len(matches) != 1:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("type_policy_mismatch")
        return matches[0]

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _AUTHORITY_DOMAIN,
            (
                self.contract_version,
                self.selected_source_document_utf8,
                self.selected_source_authority_sha256,
                self.observation_profile,
                self.relation_kind,
                self.relation_persistence,
                self.relation_has_subclass,
                tuple(item.canonical_bytes for item in self.ordered_columns),
                self.type_policy_authority.canonical_bytes,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> PostgresMssqlSelectedRelationSchemaAuthorityV1:
        try:
            values = decode_canonical_bytes(payload, _AUTHORITY_DOMAIN, field_count=9)
            return cls(
                expect_text(values[0], "version"),
                expect_bytes(values[1], "selected document"),
                expect_bytes(values[2], "selected digest"),
                expect_text(values[3], "observation profile"),
                expect_text(values[4], "relation kind"),
                expect_text(values[5], "relation persistence"),
                expect_bool(values[6], "relation subclass"),
                tuple(
                    PostgresMssqlObservedSourceColumnV1.from_canonical_bytes(expect_bytes(item, "column"))
                    for item in expect_tuple(values[7], "columns")
                ),
                PostgresMssqlTypePolicyAuthorityV1.from_canonical_bytes(expect_bytes(values[8], "type policy")),
            )
        except asyncio.CancelledError:
            raise
        except PostgresMssqlSourceSchemaAuthorityErrorV1:
            raise
        except Exception as exc:
            reason = "wrong_domain" if "domain" in str(exc).lower() else "malformed_canonical_bytes"
            exc.__traceback__ = None
            raise PostgresMssqlSourceSchemaAuthorityErrorV1(reason) from None


__all__ = [
    "PostgresMssqlSelectedRelationSchemaAuthorityV1",
    "PostgresMssqlSourceSchemaAuthorityErrorV1",
    "PostgresMssqlSourceSchemaRecoveryV1",
    "translate_model_reason",
]
