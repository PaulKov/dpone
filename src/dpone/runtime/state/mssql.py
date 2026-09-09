"""MSSQL-backed runtime state storage."""

from __future__ import annotations

from typing import Any

from dpone.contracts.mssql_object_name import MSSQLObjectName, mssql_ensure_schema_statement
from dpone.ports.source_state_storage import (
    CheckpointCommitOutcome,
    MssqlStateLocation,
    SourceStateKey,
)
from dpone.runtime.state.mssql_database_authority_binding import (
    MssqlDatabaseAuthorityBindingMixin,
)
from dpone.runtime.state.mssql_operational import MSSQLLoadAuditStorage, MSSQLRunStateStorage
from dpone.runtime.state.xmin_storage import XMinState

MSSQL_XMIN_STATE_CAPABILITY = "mssql_xmin_state_v1"


class MSSQLXMinStateStorage(MssqlDatabaseAuthorityBindingMixin):
    """Stores PostgreSQL XMin checkpoints in SQL Server."""

    mssql_state_capability = MSSQL_XMIN_STATE_CAPABILITY

    def __init__(
        self,
        connector,
        schema: str = "etl_state",
        table: str = "etl_xmin_state",
        *,
        database: str | None = None,
        receipt_table: str = "dpone_commit_receipt",
        repair_authority_table: str = "dpone_repair_authority",
        repair_consumption_table: str = "dpone_repair_authority_consumption",
        run_table: str = "etl_run_state",
        audit_table: str | None = None,
        atomicity: str = "after_target",
        provisioning: str = "runtime",
    ) -> None:
        self.connector = connector
        self.database = database
        self.schema = schema
        self.table = table
        self.receipt_table = receipt_table
        self.repair_authority_table = repair_authority_table
        self.repair_consumption_table = repair_consumption_table
        self.run_table = run_table
        self.audit_table = audit_table
        self.atomicity = atomicity
        self.provisioning = provisioning
        self._transaction_connector = None
        self._table_created = False
        self._initialize_database_authority_binding()

    @property
    def fq_table(self) -> str:
        return _qualified_name(self.connector, self.database, self.schema, self.table)

    def bind_transaction_connector(self, target_connector) -> MSSQLXMinStateStorage:
        """Bind the target executor used by ``target_atomic`` finalization."""

        self._transaction_connector = target_connector
        return self

    def _transactional(self, *, connector=None):
        from dpone.runtime.state.mssql_transactional import MssqlTransactionalStateService

        if not self.database:
            raise ValueError("target_atomic MSSQL state requires an explicit database")
        return MssqlTransactionalStateService(
            connector or self.connector,
            MssqlStateLocation(
                self.database,
                self.schema,
                self.table,
                self.receipt_table,
                self.repair_authority_table,
                self.repair_consumption_table,
            ),
        )

    def load_state_by_key(self, key: SourceStateKey) -> XMinState | None:
        self._ensure_table_exists()
        return self._transactional().load_state_by_key(key)

    def compare_and_set_with_receipt(
        self,
        *,
        key: SourceStateKey,
        expected: XMinState | None,
        candidate: XMinState,
        load_id: str,
        snapshot_token: str,
        publication_receipt_id: str | None = None,
        executor: Any | None = None,
    ) -> CheckpointCommitOutcome:
        self._ensure_table_exists()
        executor = executor or self._transaction_connector
        if executor is None:
            raise ValueError("target_atomic MSSQL state has no bound transaction connector")
        return self._transactional(connector=executor).compare_and_set_with_receipt(
            executor=executor,
            key=key,
            expected=expected,
            candidate=candidate,
            load_id=load_id,
            snapshot_token=snapshot_token,
            publication_receipt_id=publication_receipt_id,
        )

    def assert_target_authority(self, *, executor: Any, key: SourceStateKey) -> None:
        """Prove that this state identity exclusively owns its physical target."""

        self._ensure_table_exists()
        self._transactional(connector=executor).assert_target_authority(executor=executor, key=key)

    def assert_physical_target_identity(self, *, executor: Any, key: SourceStateKey) -> None:
        """Re-resolve the target-local registry binding under the applock."""

        from dpone.runtime.state.mssql_target_identity import assert_mssql_physical_target_identity

        assert_mssql_physical_target_identity(
            executor,
            database=key.target_database,
            schema=key.target_schema,
            table=key.target_table,
            expected=key.target_identity,
        )

    def assert_or_transfer_target_authority(self, *, executor: Any, key: SourceStateKey, authority: Any) -> None:
        """Assert target ownership or apply an exact repair-authority transfer."""

        self._ensure_table_exists()
        self._transactional(connector=executor).assert_or_transfer_target_authority(
            executor=executor,
            key=key,
            authority=authority,
        )

    def admit_repair_authority(self, **kwargs: Any) -> Any | None:
        """Validate and lock one invocation-selected authority in target transaction."""

        self._ensure_table_exists()
        executor = kwargs.get("executor") or self._transaction_connector
        if executor is None:
            raise ValueError("target_atomic MSSQL state has no bound transaction connector")
        return self._transactional(connector=executor).admit_repair_authority(**kwargs)

    def consume_repair_authority(self, **kwargs: Any) -> None:
        """Persist unique consumption evidence in the caller-owned transaction."""

        self._ensure_table_exists()
        executor = kwargs.get("executor") or self._transaction_connector
        if executor is None:
            raise ValueError("target_atomic MSSQL state has no bound transaction connector")
        self._transactional(connector=executor).consume_repair_authority(**kwargs)

    def preview_repair_authority(self, **kwargs: Any) -> Any:
        """Validate immutable repair authority before opening the source snapshot."""

        self._ensure_table_exists()
        return self._transactional().preview_repair_authority(executor=self.connector, **kwargs)

    def probe_receipt(
        self,
        *,
        key: SourceStateKey,
        load_id: str,
        executor: Any | None = None,
    ) -> CheckpointCommitOutcome | None:
        self._ensure_table_exists()
        return self._transactional(connector=executor).probe_receipt(key=key, load_id=load_id)

    def bind_seed_publication_receipt(
        self,
        *,
        executor: Any,
        key: SourceStateKey,
        load_id: str,
        receipt_id: str,
        candidate_xmin: int,
        snapshot_token: str,
        publication_receipt_id: str,
    ) -> CheckpointCommitOutcome:
        """Bind a legacy initial receipt while the caller holds both target locks."""

        self._ensure_table_exists()
        return self._transactional(connector=executor).bind_seed_publication_receipt(
            executor=executor,
            key=key,
            load_id=load_id,
            receipt_id=receipt_id,
            candidate_xmin=candidate_xmin,
            snapshot_token=snapshot_token,
            publication_receipt_id=publication_receipt_id,
        )

    def create_state_table(self) -> None:
        if self._table_created:
            return
        if self.atomicity == "target_atomic" or self.provisioning == "external":
            from dpone.runtime.state.mssql_transactional import preflight_external_state_tables

            preflight_external_state_tables(
                self.connector,
                database=self.database,
                schema=self.schema,
                state_table=self.table,
                receipt_table=self.receipt_table,
                repair_authority_table=self.repair_authority_table,
                repair_consumption_table=self.repair_consumption_table,
            )
            self._table_created = True
            return
        self.connector.execute_query(*_ensure_schema_statement(self.database, self.schema))
        self.connector.execute_query(
            f"""
            IF OBJECT_ID(N'{self.schema}.{self.table}', N'U') IS NULL
            CREATE TABLE {self.fq_table} (
                source_schema nvarchar(256) NOT NULL,
                source_table nvarchar(256) NOT NULL,
                xmin_value bigint NOT NULL,
                is_initial bit NOT NULL,
                wraparound_detected bit NOT NULL,
                frozen_xid bigint NULL,
                __dpone__loaded_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                __dpone__updated_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                CONSTRAINT pk_{self.table}_source PRIMARY KEY (source_schema, source_table)
            )
            """
        )
        self._table_created = True

    def preflight_atomic_catalog(self) -> None:
        """Validate all target-atomic external state objects before source I/O."""

        if self.atomicity != "target_atomic" or self.provisioning != "external":
            raise RuntimeError("mssql_atomic_state_external_provisioning_required")
        if not self.database or not self.audit_table:
            raise RuntimeError("mssql_atomic_state_catalog_location_incomplete")
        from dpone.runtime.state.mssql_atomic_catalog import (
            MssqlAtomicStateTables,
            require_external_atomic_state_catalog,
        )

        require_external_atomic_state_catalog(
            self.connector,
            database=self.database,
            schema=self.schema,
            tables=MssqlAtomicStateTables(
                state=self.table,
                receipt=self.receipt_table,
                repair_authority=self.repair_authority_table,
                repair_consumption=self.repair_consumption_table,
                run=self.run_table,
                audit=self.audit_table,
            ),
        )

    def _ensure_table_exists(self) -> None:
        self.create_state_table()

    def save_state(self, source_schema: str, source_table: str, xmin_state: XMinState) -> None:
        self._ensure_table_exists()
        self.connector.execute_query(
            f"""
            MERGE {self.fq_table} AS target
            USING (SELECT ? AS source_schema, ? AS source_table) AS source
            ON target.source_schema = source.source_schema AND target.source_table = source.source_table
            WHEN MATCHED THEN UPDATE SET
                xmin_value = ?, is_initial = ?, wraparound_detected = ?, frozen_xid = ?, __dpone__updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (
                source_schema, source_table, xmin_value, is_initial, wraparound_detected, frozen_xid, __dpone__loaded_at, __dpone__updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME());
            """,
            (
                source_schema,
                source_table,
                xmin_state.xmin_value,
                int(xmin_state.is_initial),
                int(xmin_state.wraparound_detected),
                xmin_state.frozen_xid,
                source_schema,
                source_table,
                xmin_state.xmin_value,
                int(xmin_state.is_initial),
                int(xmin_state.wraparound_detected),
                xmin_state.frozen_xid,
            ),
        )

    def load_state(self, source_schema: str, source_table: str) -> XMinState | None:
        self._ensure_table_exists()
        rows = self.connector.get_records(
            f"""
            SELECT TOP (1) xmin_value, __dpone__loaded_at, is_initial, wraparound_detected, frozen_xid
            FROM {self.fq_table}
            WHERE source_schema = ? AND source_table = ?
            """,
            (source_schema, source_table),
            as_dict=True,
        )
        if not rows:
            return None
        row = rows[0]
        return XMinState(
            xmin_value=int(row["xmin_value"]),
            timestamp=row["__dpone__loaded_at"],
            is_initial=bool(row["is_initial"]),
            wraparound_detected=bool(row["wraparound_detected"]),
            frozen_xid=row.get("frozen_xid"),
        )

    def delete_state(self, source_schema: str, source_table: str) -> None:
        self._ensure_table_exists()
        self.connector.execute_query(
            f"DELETE FROM {self.fq_table} WHERE source_schema = ? AND source_table = ?",
            (source_schema, source_table),
        )


def _qualified_name(connector: Any, database: str | None, schema: str, table: str) -> str:
    try:
        return str(connector.qualified_name(schema, table, database=database))
    except TypeError:
        label = f"{database}.{schema}" if database else schema
        return str(connector.qualified_name(label, table))


def _ensure_schema_statement(database: str | None, schema: str) -> tuple[str, tuple[str, ...]]:
    name = MSSQLObjectName.from_parts(database=database, schema=schema, table="__dpone_schema_probe")
    return mssql_ensure_schema_statement(name.schema_label)


def is_mssql_xmin_state_storage(storage: Any) -> bool:
    """Return whether a storage owns the six-object XMin catalog."""

    return getattr(storage, "mssql_state_capability", None) == MSSQL_XMIN_STATE_CAPABILITY


__all__ = [
    "MSSQLLoadAuditStorage",
    "MSSQLRunStateStorage",
    "MSSQLXMinStateStorage",
    "MSSQL_XMIN_STATE_CAPABILITY",
    "is_mssql_xmin_state_storage",
]
