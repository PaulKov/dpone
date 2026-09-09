"""Concrete bounded HTTP-query gateway for semantic-refresh ClickHouse publication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from dpone.adapters import semantic_refresh_clickhouse_http_queries as queries
from dpone.adapters.semantic_refresh_clickhouse_artifact_authority import (
    ClickHouseArtifactAuthorityReader,
)
from dpone.adapters.semantic_refresh_clickhouse_cleanup import (
    ClickHouseCompletedPublicationCleaner,
    ClickHouseOperationRelationRebuilder,
)
from dpone.adapters.semantic_refresh_clickhouse_connection import (
    ClickHouseConnectionAuthorityVerifier,
)
from dpone.adapters.semantic_refresh_clickhouse_http_authority import (
    ClickHouseHttpRelationAuthorityReader,
)
from dpone.adapters.semantic_refresh_clickhouse_http_response import ClickHouseHttpResponseReader
from dpone.adapters.semantic_refresh_clickhouse_operation import ClickHouseOperationExecutor
from dpone.adapters.semantic_refresh_clickhouse_resource_guard import (
    ClickHousePublicationResourceGuard,
)
from dpone.ports.semantic_refresh_clickhouse_connection import (
    ClickHouseClusterConnectionAuthority,
    SemanticRefreshClickHouseHttpClientPort,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_artifact_reader import (
        SemanticRefreshSealedArtifactReader,
        VersionPinnedSealedArtifact,
    )
    from dpone.ports.semantic_refresh_artifact_seal import (
        SemanticRefreshSealAuthorizationPort,
    )
    from dpone.ports.semantic_refresh_clickhouse_authority import (
        ClickHousePublicationAuthority,
        SemanticRefreshClickHousePublicationAuthorityPort,
    )

ClickHouseHttpGatewayError = queries.ClickHouseHttpGatewayError
_PlanNames = queries.PlanNames
_columns = queries.columns
_count = queries.count_query
_desired_count_query = queries.desired_count_query
_duplicate_groups = queries.duplicate_groups_query
_identifier = queries.identifier
_identifier_tuple = queries.identifier_tuple
_identifier_value = queries.identifier_value
_input_type = queries.input_type
_literal = queries.literal
_load_authority = queries.load_authority
_relation_identifier_value = queries.relation_identifier_value
_schema_query = queries.schema_query
_scope_count_query = queries.scope_count_query
_table = queries.table
_uuid_query = queries.uuid_query
_validate_plan = queries.validate_plan
_validate_sql = queries.validate_sql


class ClickHouseHttpSemanticRefreshGateway:
    """Execute the approved PREPARE/revalidation/exchange subset over HTTP."""

    def __init__(
        self,
        *,
        client: SemanticRefreshClickHouseHttpClientPort,
        authority: SemanticRefreshClickHousePublicationAuthorityPort,
        artifact_reader: SemanticRefreshSealedArtifactReader,
        seal_authorization: SemanticRefreshSealAuthorizationPort,
        connection_authority: ClickHouseClusterConnectionAuthority,
    ) -> None:
        if not isinstance(connection_authority, ClickHouseClusterConnectionAuthority):
            raise TypeError("ClickHouse connection authority is invalid")
        self._client = client
        self._authority = authority
        self._artifacts = ClickHouseArtifactAuthorityReader(
            artifact_reader=artifact_reader,
            seal_authorization=seal_authorization,
            error_type=ClickHouseHttpGatewayError,
        )
        self._connection = ClickHouseConnectionAuthorityVerifier(
            client=client,
            authority=connection_authority,
        )
        self._relation_authority_reader = ClickHouseHttpRelationAuthorityReader(
            client=client,
            connection=self._connection,
            connection_authority=connection_authority,
        )
        self._responses = ClickHouseHttpResponseReader(client)
        self._operations = ClickHouseOperationExecutor(client)
        self._cleaner = ClickHouseCompletedPublicationCleaner(
            client=client,
            authority=authority,
            connection=self._connection,
        )
        self._relations = ClickHouseOperationRelationRebuilder(
            client=client,
            authority=self._relation_authority,
        )
        self._resources = ClickHousePublicationResourceGuard(self._responses.scalar)

    def prepare(
        self,
        plan: Mapping[str, object],
        sql: Mapping[str, str],
    ) -> Mapping[str, object]:
        """Rebuild staging/shadow from the exact artifact and return observations."""

        names, authority = _validate_plan(plan, self._authority)
        self._connection.assert_current(authority.clickhouse_cluster_authority_id)
        statements = _validate_sql(sql)
        artifact = self._artifacts.read(plan, authority)
        if not artifact.manifest.chunks:
            raise ClickHouseHttpGatewayError("empty sealed artifact requires the empty-scope path")
        self._resources.assert_pre_mutation(plan, names)
        self._relations.rebuild(
            names,
            names.staging,
            plan_sha256=str(plan["operation_plan_sha256"]),
            query_id=self._operations.query_id(plan, "rebuild_staging"),
        )
        self._load_staging(plan, names, artifact)
        self._relations.rebuild(
            names,
            names.shadow,
            plan_sha256=str(plan["operation_plan_sha256"]),
            query_id=self._operations.query_id(plan, "rebuild_shadow"),
        )
        self._operations.execute(
            statements["populate_shadow_retained"],
            plan,
            "populate_shadow",
        )
        self._resources.assert_current(plan, names, boundary="after retained-target shadow population")
        self._operations.execute(statements["append_staging"], plan, "append_staging")
        self._resources.assert_current(plan, names, boundary="after staging append")
        return self._observe(plan, statements, names, authority)

    def revalidate(
        self,
        plan: Mapping[str, object],
        sql: Mapping[str, str],
    ) -> Mapping[str, object]:
        """Rerun every exact engine observation without another mutation."""

        names, authority = _validate_plan(plan, self._authority)
        self._connection.assert_current(authority.clickhouse_cluster_authority_id)
        self._artifacts.read(plan, authority)
        return self._observe(plan, _validate_sql(sql), names, authority)

    def inspect_empty_scope(self, plan: Mapping[str, object]) -> Mapping[str, object]:
        """Verify a canonical empty artifact and current empty target scope without mutation."""

        names, authority = _validate_plan(plan, self._authority)
        self._connection.assert_current(authority.clickhouse_cluster_authority_id)
        artifact = self._artifacts.read(plan, authority)
        if artifact.manifest.chunks or artifact.manifest.total_rows != 0 or artifact.manifest.total_bytes != 0:
            raise ClickHouseHttpGatewayError("empty-scope publication requires a canonical empty manifest")
        relation = self._relation_authority(names.database, names.target)
        return {
            "target_uuid": self._responses.text(_uuid_query(names.database, names.target)),
            "target_scope_rows": self._responses.scalar(
                _scope_count_query(
                    names.database,
                    names.target,
                    authority.event_time_column,
                    authority.scope_start,
                    authority.scope_end,
                )
            ),
            "schema_sha256": relation["schema_sha256"],
            "physical_sha256": relation["physical_sha256"],
            "guard_operation_id": authority.operation_id,
            "guard_attempt_binding_sha256": authority.attempt_binding_sha256,
            "guard_fence_epoch": authority.fencing_epoch,
            "database_engine": self._responses.text(
                f"SELECT engine FROM system.databases WHERE name = {_literal(names.database)}"
            ),
            "table_engine": relation["table_engine"],
            "shard_count": relation["shard_count"],
            "replica_count": relation["replica_count"],
        }

    def inspect_uuid_map(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Read actual target/shadow UUID ownership from system.tables."""

        database = _identifier_value(request.get("database"), "database")
        target = _relation_identifier_value(request.get("target_table"), "target_table")
        shadow = _relation_identifier_value(request.get("shadow_table"), "shadow_table")
        authority = _load_authority(request, self._authority)
        self._connection.assert_current(authority.clickhouse_cluster_authority_id)
        return {
            "target_uuid": self._responses.text(_uuid_query(database, target)),
            "shadow_uuid": self._responses.optional_text(_uuid_query(database, shadow)),
        }

    def inspect_target_authority(self, plan: Mapping[str, object]) -> Mapping[str, object]:
        """Recheck the complete physical authority after EXCHANGE and before COMPLETE."""

        names, authority = _validate_plan(plan, self._authority)
        relation = self._relation_authority(names.database, names.target)
        return {
            "target_uuid": self._responses.text(_uuid_query(names.database, names.target)),
            "schema_sha256": relation["schema_sha256"],
            "physical_sha256": relation["physical_sha256"],
            "guard_operation_id": authority.operation_id,
            "guard_attempt_binding_sha256": authority.attempt_binding_sha256,
            "guard_fence_epoch": authority.fencing_epoch,
            "database_engine": relation["database_engine"],
            "table_engine": relation["table_engine"],
            "shard_count": relation["shard_count"],
            "replica_count": relation["replica_count"],
        }

    def exchange(self, request: Mapping[str, object]) -> None:
        """Execute one guarded atomic name exchange; caller reconciles acknowledgement loss."""

        authority = _load_authority(request, self._authority)
        self._connection.assert_current(authority.clickhouse_cluster_authority_id)
        database = _identifier_value(request.get("database"), "database")
        target = _relation_identifier_value(request.get("target_table"), "target_table")
        shadow = _relation_identifier_value(request.get("shadow_table"), "shadow_table")
        observed = {
            "target_uuid": self._responses.text(_uuid_query(database, target)),
            "shadow_uuid": self._responses.text(_uuid_query(database, shadow)),
        }
        expected = {
            "target_uuid": request.get("expected_target_uuid"),
            "shadow_uuid": request.get("expected_shadow_uuid"),
        }
        if observed != expected:
            raise ClickHouseHttpGatewayError("ClickHouse exchange UUID guard differs")
        self._operations.execute(
            f"EXCHANGE TABLES {_table(database, target)} AND {_table(database, shadow)}",
            request,
            "exchange",
        )

    def cleanup_retained(self, request: Mapping[str, object]) -> None:
        """After COMPLETE, drop exact staging while retaining the old target."""

        self._cleaner.cleanup(request)

    def inspect_relation_authority(self, *, database: str, table: str) -> Mapping[str, object]:
        """Observe canonical schema, physical, engine, and single-node topology evidence."""

        names = _PlanNames(
            database=_identifier_value(database, "database"),
            target=_relation_identifier_value(table, "table"),
            staging="unused_staging",
            shadow="unused_shadow",
        )
        return self._relation_authority(names.database, names.target)

    def _observe(
        self,
        plan: Mapping[str, object],
        sql: Mapping[str, str],
        names: _PlanNames,
        authority: ClickHousePublicationAuthority,
    ) -> dict[str, object]:
        keys = _identifier_tuple(plan.get("effective_key_columns"), "effective_key_columns")
        null_predicate = " OR ".join(f"{_identifier(column)} IS NULL" for column in keys)
        grouped_keys = ", ".join(_identifier(column) for column in keys)
        desired = _desired_count_query(plan, names)
        relation = self._relation_authority(names.database, names.target)
        staging_bytes, shadow_bytes, retained_backup_bytes = self._resources.resource_bytes(names)
        return {
            "target_uuid": self._responses.text(_uuid_query(names.database, names.target)),
            "staging_uuid": self._responses.text(_uuid_query(names.database, names.staging)),
            "shadow_uuid": self._responses.text(_uuid_query(names.database, names.shadow)),
            "staging_rows": self._responses.scalar(_count(names.database, names.staging)),
            "target_scope_rows": self._responses.scalar(
                _scope_count_query(
                    names.database,
                    names.target,
                    authority.event_time_column,
                    authority.scope_start,
                    authority.scope_end,
                )
            ),
            "shadow_rows": self._responses.scalar(_count(names.database, names.shadow)),
            "desired_rows": self._responses.scalar(desired),
            "staging_null_key_rows": self._responses.scalar(
                f"SELECT count() FROM {_table(names.database, names.staging)} WHERE {null_predicate}"
            ),
            "staging_duplicate_key_groups": self._responses.scalar(
                _duplicate_groups(names.database, names.staging, grouped_keys)
            ),
            "target_null_key_rows": self._responses.scalar(
                f"SELECT count() FROM {_table(names.database, names.target)} WHERE {null_predicate}"
            ),
            "target_duplicate_key_groups": self._responses.scalar(
                _duplicate_groups(names.database, names.target, grouped_keys)
            ),
            "forward_difference_groups": self._responses.scalar(sql["forward_multiset_difference"]),
            "reverse_difference_groups": self._responses.scalar(sql["reverse_multiset_difference"]),
            "schema_sha256": relation["schema_sha256"],
            "physical_sha256": relation["physical_sha256"],
            "staging_bytes": staging_bytes,
            "shadow_bytes": shadow_bytes,
            "retained_backup_bytes": retained_backup_bytes,
            "total_transient_bytes": staging_bytes + shadow_bytes + retained_backup_bytes,
            "guard_operation_id": authority.operation_id,
            "guard_attempt_binding_sha256": authority.attempt_binding_sha256,
            "guard_fence_epoch": authority.fencing_epoch,
            "database_engine": self._responses.text(
                f"SELECT engine FROM system.databases WHERE name = {_literal(names.database)}"
            ),
            "table_engine": relation["table_engine"],
            "shard_count": relation["shard_count"],
            "replica_count": relation["replica_count"],
        }

    def _relation_authority(self, database: str, table: str) -> dict[str, object]:
        return self._relation_authority_reader.load(database=database, table=table)

    def _load_staging(
        self,
        plan: Mapping[str, object],
        names: _PlanNames,
        artifact: VersionPinnedSealedArtifact,
    ) -> None:
        columns = _identifier_tuple(plan.get("business_columns"), "business_columns")
        schema_rows = self._responses.rows(_schema_query(names.database, names.target), columns=6)
        type_by_column = {str(row[0]): str(row[1]) for row in schema_rows}
        if any(column not in type_by_column for column in columns):
            raise ClickHouseHttpGatewayError("artifact business projection differs from target schema")
        input_fields = ", ".join(f"{column} {_input_type(type_by_column[column])}" for column in columns)
        input_fields += ", __dpone_seal_ordinal Int64"
        query = (
            f"INSERT INTO {_table(names.database, names.staging)} ({_columns(columns)}) "
            f"SELECT {_columns(columns)} FROM input({_literal(input_fields)}) FORMAT Parquet"
        )
        for index, (descriptor, content) in enumerate(
            zip(artifact.manifest.chunks, artifact.chunk_bytes, strict=True),
            start=1,
        ):
            if not content.startswith(b"PAR1") or not content.endswith(b"PAR1"):
                raise ClickHouseHttpGatewayError("sealed artifact chunk is not Parquet")
            try:
                self._client.insert_parquet_operation(
                    query,
                    content,
                    query_id=self._operations.query_id(plan, f"load_staging_{index}"),
                )
            except Exception as exc:
                raise ClickHouseHttpGatewayError("ClickHouse artifact load outcome is unavailable") from exc
            self._resources.assert_current(plan, names, boundary="after sealed artifact chunk load")
            if descriptor.row_count < 0:  # canonical contract already enforces; defensive adapter boundary
                raise ClickHouseHttpGatewayError("sealed artifact row count is invalid")
        if self._responses.scalar(_count(names.database, names.staging)) != artifact.manifest.total_rows:
            raise ClickHouseHttpGatewayError("ClickHouse staging rows differ from sealed manifest")


__all__ = [
    "ClickHouseHttpGatewayError",
    "ClickHouseHttpSemanticRefreshGateway",
]
