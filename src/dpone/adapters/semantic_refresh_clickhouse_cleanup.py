"""Authenticated idempotent cleanup of one COMPLETE ClickHouse publication attempt."""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

from dpone.adapters import semantic_refresh_clickhouse_http_queries as queries
from dpone.ports.semantic_refresh_clickhouse_authority import (
    SemanticRefreshClickHousePublicationAuthorityPort,
    clickhouse_operation_table_names,
)
from dpone.ports.semantic_refresh_clickhouse_resources import (
    ClickHouseFailedScratchCleanupReceipt,
    ClickHouseScratchRelationAbsence,
)
from dpone.ports.semantic_refresh_mssql_replacement import (
    MssqlFailedScratchCleanupAuthority,
)

if TYPE_CHECKING:
    from dpone.adapters.semantic_refresh_clickhouse_connection import (
        ClickHouseConnectionAuthorityVerifier,
    )

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class _ClickHouseCleanupClient(Protocol):
    def execute(self, statement: str) -> Any:
        """Execute one bounded query or command."""

    def execute_operation(self, statement: str, *, query_id: str) -> Any:
        """Execute one operation-bound command."""


class _RelationAuthority(Protocol):
    def __call__(self, database: str, table: str) -> dict[str, object]:
        """Return current schema/physical/engine authority."""


class ClickHouseOperationRelationRebuilder:
    """Rebuild only one deterministic relation already bound to an operation plan."""

    def __init__(self, *, client: _ClickHouseCleanupClient, authority: _RelationAuthority) -> None:
        self._client = client
        self._authority = authority

    def rebuild(
        self,
        names: queries.PlanNames,
        table: str,
        *,
        plan_sha256: str,
        query_id: str,
    ) -> None:
        """Drop only an exact owned predecessor and atomically tag its replacement."""

        existing_uuid = _optional_text(self._client, queries.uuid_query(names.database, table))
        expected_comment = f"dpone-semantic-refresh:{plan_sha256}"
        if existing_uuid is not None:
            if _required_text(self._client, queries.relation_comment_query(names.database, table)) != expected_comment:
                raise queries.ClickHouseHttpGatewayError(
                    "existing ClickHouse temporary relation is not owned by this operation"
                )
            _operation_command(
                self._client,
                f"DROP TABLE {queries.table(names.database, table)}",
                query_id=query_id,
            )
        _operation_command(
            self._client,
            f"CREATE TABLE {queries.table(names.database, table)} "
            f"AS {queries.table(names.database, names.target)} COMMENT {queries.literal(expected_comment)}",
            query_id=query_id,
        )
        target = self._authority(names.database, names.target)
        candidate = self._authority(names.database, table)
        if any(candidate[field] != target[field] for field in ("schema_sha256", "physical_sha256", "table_engine")):
            raise queries.ClickHouseHttpGatewayError("created ClickHouse relation differs from target authority")


class ClickHouseCompletedPublicationCleaner:
    """Drop exact staging only after COMPLETE while retaining the old target."""

    def __init__(
        self,
        *,
        client: _ClickHouseCleanupClient,
        authority: SemanticRefreshClickHousePublicationAuthorityPort,
        connection: ClickHouseConnectionAuthorityVerifier,
    ) -> None:
        self._client = client
        self._authority = authority
        self._connection = connection

    def cleanup(self, request: Mapping[str, object]) -> None:
        """Drop attempt-local staging and prove the old target remains retained."""

        protected = queries.load_authority(request, self._authority)
        self._connection.assert_current(protected.clickhouse_cluster_authority_id)
        staging, shadow = clickhouse_operation_table_names(protected.target_table, protected.operation_id)
        if (
            request.get("operation_plan_sha256") != protected.operation_plan_sha256
            or request.get("staging_table") != staging
            or request.get("shadow_table") != shadow
            or _DIGEST_RE.fullmatch(str(request.get("terminal_receipt_sha256"))) is None
        ):
            raise queries.ClickHouseHttpGatewayError("ClickHouse cleanup authority differs")
        expected_target_uuid = _uuid(request.get("expected_target_uuid"), "target")
        expected_staging_uuid = _uuid(request.get("expected_staging_uuid"), "staging")
        expected_retained_uuid = _uuid(request.get("expected_retained_uuid"), "retained")
        database = protected.database
        target_uuid = _required_text(self._client, queries.uuid_query(database, protected.target_table))
        if target_uuid != expected_target_uuid:
            raise queries.ClickHouseHttpGatewayError("ClickHouse cleanup target UUID differs")
        expected_comment = f"dpone-semantic-refresh:{protected.operation_plan_sha256}"
        if (
            _required_text(
                self._client,
                queries.relation_comment_query(database, protected.target_table),
            )
            != expected_comment
        ):
            raise queries.ClickHouseHttpGatewayError("ClickHouse cleanup terminal target owner differs")
        staging_present = self._owned_attempt_relation_is_present(
            database=database,
            table=staging,
            expected_uuid=expected_staging_uuid,
            expected_comment=expected_comment,
        )
        retained_present = self._retained_target_is_present(
            database=database,
            table=shadow,
            expected_uuid=expected_retained_uuid,
        )
        if staging_present:
            self._drop(
                database=database,
                table=staging,
                expected_uuid=expected_staging_uuid,
            )
        if not retained_present:
            raise queries.ClickHouseHttpGatewayError(
                "ClickHouse retained target is absent before its governed retention horizon"
            )

    def _owned_attempt_relation_is_present(
        self,
        *,
        database: str,
        table: str,
        expected_uuid: str,
        expected_comment: str,
    ) -> bool:
        observed_uuid = _optional_text(self._client, queries.uuid_query(database, table))
        if observed_uuid is None:
            return False
        if observed_uuid != expected_uuid:
            raise queries.ClickHouseHttpGatewayError("ClickHouse cleanup staging UUID differs")
        if _required_text(self._client, queries.relation_comment_query(database, table)) != expected_comment:
            raise queries.ClickHouseHttpGatewayError("ClickHouse cleanup staging owner differs")
        return True

    def _retained_target_is_present(self, *, database: str, table: str, expected_uuid: str) -> bool:
        observed_uuid = _optional_text(self._client, queries.uuid_query(database, table))
        if observed_uuid is None:
            return False
        if observed_uuid != expected_uuid:
            raise queries.ClickHouseHttpGatewayError("ClickHouse cleanup retained target UUID differs")
        return True

    def _drop(self, *, database: str, table: str, expected_uuid: str) -> None:
        _drop_exact(self._client, database, table, expected_uuid)


class ClickHouseFailedPrecommitScratchCleaner:
    """Drop only deterministic scratch relations authorized by durable FAILED_PRE_COMMIT."""

    def __init__(
        self,
        *,
        client: _ClickHouseCleanupClient,
        connection: ClickHouseConnectionAuthorityVerifier,
    ) -> None:
        self._client = client
        self._connection = connection

    def cleanup(self, authority: MssqlFailedScratchCleanupAuthority) -> ClickHouseFailedScratchCleanupReceipt:
        """Validate the unchanged target and complete relation closure before any DROP."""

        if not isinstance(authority, MssqlFailedScratchCleanupAuthority):
            raise TypeError("failed-precommit ClickHouse cleanup authority is invalid")
        cluster = _cluster_authority_id(authority)
        self._connection.assert_current(cluster)
        database = authority.protected_target_database
        if (
            _required_text(
                self._client,
                queries.uuid_query(database, authority.protected_target_table),
            )
            != authority.protected_target_uuid
        ):
            raise queries.ClickHouseHttpGatewayError("failed-precommit protected target UUID changed")
        expected_comment = f"dpone-semantic-refresh:{authority.operation_plan_sha256}"
        cleanup: list[tuple[str, str]] = []
        for relation in authority.relations:
            observed = _optional_text(
                self._client,
                queries.uuid_query(relation.database_name, relation.table_name),
            )
            if observed is None:
                continue
            if relation.observed_uuid is not None and relation.observed_uuid != observed:
                raise queries.ClickHouseHttpGatewayError("failed-precommit scratch UUID differs")
            if (
                _required_text(
                    self._client,
                    queries.relation_comment_query(relation.database_name, relation.table_name),
                )
                != expected_comment
            ):
                raise queries.ClickHouseHttpGatewayError("failed-precommit scratch owner differs")
            cleanup.append((relation.table_name, observed))
        for table, expected_uuid in cleanup:
            _drop_exact(self._client, database, table, expected_uuid)
        absences: list[ClickHouseScratchRelationAbsence] = []
        for relation in authority.relations:
            observed = _optional_text(
                self._client,
                queries.uuid_query(relation.database_name, relation.table_name),
            )
            if observed is not None:
                raise queries.ClickHouseHttpGatewayError("failed-precommit scratch absence is not proven")
            absences.append(
                ClickHouseScratchRelationAbsence(
                    kind=relation.relation_role.lower(),
                    name=relation.table_name,
                    expected_uuid=relation.observed_uuid,
                )
            )
        if (
            _required_text(
                self._client,
                queries.uuid_query(database, authority.protected_target_table),
            )
            != authority.protected_target_uuid
        ):
            raise queries.ClickHouseHttpGatewayError("failed-precommit protected target changed during cleanup")
        return ClickHouseFailedScratchCleanupReceipt(
            workflow_execution_id=authority.workflow_execution_id,
            workflow_execution_binding_sha256=authority.workflow_execution_binding_sha256,
            operation_id=authority.operation_id,
            operation_plan_sha256=authority.operation_plan_sha256,
            attempt_binding_sha256=authority.attempt_binding_sha256,
            fencing_epoch=authority.fencing_epoch,
            target_uuid=authority.protected_target_uuid,
            relations=tuple(absences),
        )


def _uuid(value: object, field: str) -> str:
    try:
        parsed = uuid.UUID(str(value))
    except (AttributeError, TypeError, ValueError) as exc:
        raise queries.ClickHouseHttpGatewayError(f"ClickHouse cleanup {field} UUID is invalid") from exc
    canonical = str(parsed)
    if canonical != value:
        raise queries.ClickHouseHttpGatewayError(f"ClickHouse cleanup {field} UUID is invalid")
    return canonical


def _command(client: _ClickHouseCleanupClient, statement: str) -> None:
    try:
        client.execute(statement)
    except Exception as exc:
        raise queries.ClickHouseHttpGatewayError("ClickHouse relation command outcome is unavailable") from exc


def _operation_command(
    client: _ClickHouseCleanupClient,
    statement: str,
    *,
    query_id: str,
) -> None:
    try:
        client.execute_operation(statement, query_id=query_id)
    except Exception as exc:
        raise queries.ClickHouseHttpGatewayError("ClickHouse operation command outcome is unavailable") from exc


def _drop_exact(client: _ClickHouseCleanupClient, database: str, table: str, expected_uuid: str) -> None:
    try:
        client.execute(f"DROP TABLE {queries.table(database, table)}")
    except Exception as exc:
        observed = _optional_text(client, queries.uuid_query(database, table))
        if observed is None:
            return
        if observed == expected_uuid:
            raise queries.ClickHouseHttpGatewayError("ClickHouse cleanup is proven not committed") from exc
        raise queries.ClickHouseHttpGatewayError("ClickHouse cleanup UUID outcome is ambiguous") from exc


def _cluster_authority_id(authority: MssqlFailedScratchCleanupAuthority) -> str:
    suffix = f"/{authority.protected_target_database}/{authority.protected_target_table}"
    value = authority.protected_target_authority_id
    if not value.startswith("clickhouse://") or not value.endswith(suffix):
        raise queries.ClickHouseHttpGatewayError("failed-precommit ClickHouse cluster authority is invalid")
    cluster = value[len("clickhouse://") : -len(suffix)]
    if not cluster:
        raise queries.ClickHouseHttpGatewayError("failed-precommit ClickHouse cluster authority is empty")
    return cluster


def _required_text(client: _ClickHouseCleanupClient, statement: str) -> str:
    value = _optional_text(client, statement)
    if value is None:
        raise queries.ClickHouseHttpGatewayError("ClickHouse relation authority is absent")
    return value


def _optional_text(client: _ClickHouseCleanupClient, statement: str) -> str | None:
    try:
        rows = client.execute(statement)
    except Exception as exc:
        raise queries.ClickHouseHttpGatewayError("ClickHouse relation authority is unavailable") from exc
    if rows == []:
        return None
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], tuple) or len(rows[0]) != 1:
        raise queries.ClickHouseHttpGatewayError("ClickHouse relation authority is ambiguous")
    value = rows[0][0]
    if not isinstance(value, str) or not value:
        raise queries.ClickHouseHttpGatewayError("ClickHouse relation authority is invalid")
    return value


__all__ = [
    "ClickHouseCompletedPublicationCleaner",
    "ClickHouseFailedPrecommitScratchCleaner",
    "ClickHouseOperationRelationRebuilder",
]
