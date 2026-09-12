"""Issue PostgreSQL-to-MSSQL schema authority from one verified snapshot."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from typing import Any

from dpone.runtime.sources.postgres_mssql_source_schema_observation import (
    PostgresMssqlRelationQueryProfileV1,
    build_relation_query_profile,
)
from dpone.runtime.sources.postgres_mssql_source_schema_types import (
    PostgresMssqlSelectedRelationSchemaAuthorityV1,
    PostgresMssqlSourceSchemaAuthorityErrorV1,
    derive_observed_source_column,
    require_issuer_type_policy,
    translate_issuer_failure,
)
from dpone.runtime.sources.postgres_verified_relation_snapshot import (
    PostgresVerifiedRelationObservationError,
    PostgresVerifiedRelationSnapshotV1,
    execute_profile_item,
    observe_relation_profile,
)


@dataclass(frozen=True, slots=True)
class PostgresMssqlRelationSchemaAuthorityIssuerV1:
    """Own the one exact type policy used to issue route authority."""

    type_policy_authority: Any
    _bound_query_profile: PostgresMssqlRelationQueryProfileV1 | None = field(
        init=False,
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "type_policy_authority", require_issuer_type_policy(self.type_policy_authority))

    def bind_query_profile(self, *, selected_source_authority: object) -> PostgresMssqlRelationQueryProfileV1:
        try:
            profile = build_relation_query_profile(selected_source_authority)
            object.__setattr__(self, "_bound_query_profile", profile)
            return profile
        except asyncio.CancelledError:
            raise
        except Exception:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation") from None

    def issue(
        self,
        *,
        connector: object,
        verified_relation: PostgresVerifiedRelationSnapshotV1,
        query_profile: PostgresMssqlRelationQueryProfileV1,
    ) -> PostgresMssqlSelectedRelationSchemaAuthorityV1:
        if type(verified_relation) is not PostgresVerifiedRelationSnapshotV1:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
        if (
            type(query_profile) is not PostgresMssqlRelationQueryProfileV1
            or query_profile is not self._bound_query_profile
        ):
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
        if query_profile.contract_version != "dpone-postgres-mssql-source-schema-query-profile-1":
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
        try:
            try:
                verified_relation.snapshot_lease.require_for(connector)
            except Exception:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("snapshot_lease_mismatch") from None
            selected = verified_relation.selected_source_authority
            profile = observe_relation_profile(connector, _item(query_profile, "relation_profile"), selected)
            rows = execute_profile_item(connector, _item(query_profile, "column_catalog"))
            if not rows:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("column_count_invalid")
            selected_digest = hashlib.sha256(selected.authority_document_utf8).digest()
            columns = tuple(
                derive_observed_source_column(self.type_policy_authority, row, ordinal, selected_digest)
                for ordinal, row in enumerate(rows, 1)
            )
            return PostgresMssqlSelectedRelationSchemaAuthorityV1(
                "dpone-postgres-mssql-selected-relation-schema-authority-1",
                selected.authority_document_utf8,
                selected_digest,
                "postgres-16-live-user-columns-only-relation-v1",
                profile.relation_kind,
                profile.relation_persistence,
                profile.relation_has_subclass,
                columns,
                self.type_policy_authority,
            )
        except asyncio.CancelledError:
            raise
        except PostgresMssqlSourceSchemaAuthorityErrorV1:
            raise
        except PostgresVerifiedRelationObservationError as error:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1(error.reason) from None
        except Exception as error:
            raise translate_issuer_failure(error) from None


def _item(profile: PostgresMssqlRelationQueryProfileV1, statement_id: str) -> Any:
    return next(item for item in profile.ordered_items if item.statement_id == statement_id)


__all__ = ["PostgresMssqlRelationSchemaAuthorityIssuerV1"]
