"""Independent native BCP attempts with durable, typed content verification.

A worker owns one connector. The injected mutation scope fences and settles SQL
ownership; it must not return while a previous BCP writer remains active.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkPlan, NativeChunkReceipt
from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.runtime.consumed_payload_evidence import ConsumedPayloadEvidence, canonical_source_provenance_sha256
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.mssql_native_chunks_observations import NativeDeliverySession, delivery_session
from dpone.runtime.native_wire_models import stable_hash

if TYPE_CHECKING:
    from dpone.ports.native_delivery_observer import NativeDeliveryObserver


def native_attempt_table_name(plan: NativeChunkPlan, attempt_id: str) -> str:
    """Derive the same isolated physical object across importer and composition."""
    return "dpone_native_" + sha256(repr((asdict(plan), attempt_id)).encode()).hexdigest()[:40]


def native_stage_allocated_bytes(connector: Any, database: str) -> int:
    """Observe native-stage allocation through one caller-owned target session."""
    quoted_database = connector.quote_identifier(database)
    rows = connector.get_records(
        f"SELECT COALESCE(SUM(p.reserved_page_count), 0) * 8192 FROM "
        f"{quoted_database}.sys.dm_db_partition_stats p "
        f"JOIN {quoted_database}.sys.tables t ON t.object_id=p.object_id WHERE t.name LIKE 'dpone[_]native[_]%'"
    )
    if not rows or type(rows[0][0]) is not int or rows[0][0] < 0:
        raise ValueError("mssql_native.allocation_unavailable")
    return int(rows[0][0])


class MssqlNativeChunkImporter:
    """Import isolated, predictably named tables; counts never use shared deltas."""

    def __init__(
        self,
        connector: Any,
        *,
        database: str,
        schema: str,
        columns: Sequence[Any],
        encode_row: Callable[[Any], bytes],
        assert_lease: Callable[[Any], None],
        mutation_scope: Callable[..., Any],
        options_factory: Callable[..., Any],
        target_digest_contract: Any = None,
        target_local_attempt: Any = None,
        persisted_hash_layout: bool = False,
        observer: NativeDeliveryObserver | NativeDeliverySession | None = None,
    ) -> None:
        self.connector = connector
        self.observations = delivery_session(observer)
        self.database = database
        self.schema = schema
        self.columns = tuple(columns)
        self._encode = encode_row
        self._assert_lease = assert_lease
        self._mutation_scope = mutation_scope
        self._options_factory = options_factory
        self._target_digest_contract = target_digest_contract
        self._target_local_attempt = target_local_attempt
        self._persisted_hash_layout = persisted_hash_layout
        self._types = tuple(
            normalize_mssql_physical_type(column.source_type.removesuffix(" nullable")) for column in columns
        )
        if any(dtype.startswith(("varchar", "char(")) for dtype in self._types):
            raise ValueError("mssql_native.utf8_collation_authority_required")
        if not database or not schema or not self.columns:
            raise ValueError("mssql_native.stage_authority_required")

    def table_name(self, plan: NativeChunkPlan, attempt_id: str) -> str:
        """Derive an owned identifier from the complete invocation and attempt."""

        return native_attempt_table_name(plan, attempt_id)

    def qualified(self, table: str) -> str:
        return str(self.connector.qualified_name(self.schema, table, database=self.database))

    def import_file(
        self, plan: NativeChunkPlan, file: EncodedNativeFile, attempt_id: str, lease: Any
    ) -> NativeChunkReceipt:
        self._assert_lease(lease)
        table = self.table_name(plan, attempt_id)
        artifact = FileExportArtifact(
            str(file.path),
            tuple(column.name for column in self.columns),
            format="mssql-native",
            rows_exported=file.rows,
        )
        integrity = artifact.require_integrity_receipt()
        if integrity.sha256 != file.file_sha256 or integrity.size_bytes != file.encoded_bytes:
            raise ValueError("mssql_native.file_identity_mismatch")
        if self._target_local_attempt is not None:
            return self._target_local_attempt.import_file(self, plan, file, attempt_id, lease, artifact)
        with self._mutation_scope(plan, attempt_id, lease):
            self._assert_lease(lease)
            self._create_owned_stage(plan, attempt_id, table)
            object_id = self._object_id(table)
            with TemporaryDirectory(prefix="dpone-native-rejects-", dir=file.path.parent) as directory:
                rejects = Path(directory) / "rejects.txt"
                options = self._options_factory(
                    file_format="native",
                    error_file=str(rejects),
                    trust_server_certificate=getattr(self.connector, "trust_server_certificate", "no") == "yes",
                )
                if options.file_format != "native" or options.error_file != str(rejects):
                    raise ValueError("mssql_native.native_options_required")
                copied = 0
                if file.rows:
                    with self.observations.recorder("importer").phase(
                        "bcp",
                        ordinal=file.ordinal,
                        attempt_id=attempt_id,
                        rows=file.rows,
                        encoded_bytes=file.encoded_bytes,
                    ):
                        copied = self.connector.bcp_import(
                            self.schema, table, str(file.path), options=options, database=self.database
                        )
                if type(copied) is not int or copied != file.rows:
                    raise ValueError("mssql_native.vendor_count_mismatch")
                if rejects.exists() and rejects.stat().st_size:
                    raise ValueError("mssql_native.rejects_not_empty")
            if artifact.require_integrity_receipt() != integrity:
                raise ValueError("mssql_native.file_identity_changed")
            with self.observations.recorder("importer").phase(
                "raw_verify", reason="import", ordinal=file.ordinal, attempt_id=attempt_id, rows=file.rows
            ):
                typed_sum = self._verify_contents(table, file.rows, file.typed_digest)
            self._assert_lease(lease)
            return self._receipt(plan, file, attempt_id, object_id, typed_sum, artifact)

    def _create_owned_stage(self, plan: NativeChunkPlan, attempt_id: str, table: str) -> None:
        ddl = ", ".join(
            f"{self.connector.quote_identifier(column.name)} {dtype} {'NULL' if column.nullable else 'NOT NULL'}"
            for column, dtype in zip(self.columns, self._types, strict=True)
        )
        if self._persisted_hash_layout:
            ddl += ", [__dpone__native_row_hash] binary(32) NOT NULL, [__dpone__mutation_version] rowversion NOT NULL"
        self.connector.begin()
        try:
            self.connector.execute_query(f"CREATE TABLE {self.qualified(table)} ({ddl})")
            database = self.connector.quote_identifier(self.database)
            self.connector.execute_query(
                f"EXEC {database}.sys.sp_addextendedproperty @name=N'dpone_native_owner', @value=?, "
                "@level0type=N'SCHEMA', @level0name=?, @level1type=N'TABLE', @level1name=?",
                (self._ownership(plan, attempt_id)["binding"], self.schema, table),
            )
        except BaseException:
            try:
                self.connector.rollback()
            except Exception:
                self.connector.close()
            raise
        self.connector.commit_transaction()

    def _receipt(
        self,
        plan: NativeChunkPlan,
        file: EncodedNativeFile,
        attempt_id: str,
        object_id: int,
        typed_sum: int,
        artifact: FileExportArtifact,
        mutation_watermark: int | None = None,
    ) -> NativeChunkReceipt:
        schema = tuple((column.name, dtype) for column, dtype in zip(self.columns, self._types, strict=True))
        evidence = ConsumedPayloadEvidence.empty().append_verified_file(
            artifact,
            validated_schema=schema,
            source_provenance_sha256=canonical_source_provenance_sha256(
                relation_dialect=None, relation_schema=None, relation_metadata=None, fallback_schema=schema
            ),
            actual_raw_rows=file.rows,
            order_key=f"native:{file.ordinal:020d}",
        )
        part = evidence.parts[0].to_payload()
        part["native_typed_sum"] = typed_sum
        part["native_object_id"] = object_id
        part["native_plan_binding"] = stable_hash(asdict(plan))
        if mutation_watermark is not None:
            part["native_mutation_watermark"] = mutation_watermark
            part["native_stage_layout"] = "mssql-native-persisted-hash-v2"
        return NativeChunkReceipt(
            file.ordinal,
            attempt_id,
            self.qualified(self.table_name(plan, attempt_id)),
            file.rows,
            file.encoded_bytes,
            file.file_sha256,
            file.typed_digest,
            part,
        )

    def inspect(self, plan: NativeChunkPlan, receipt: NativeChunkReceipt, lease: Any) -> NativeChunkReceipt:
        self._assert_lease(lease)
        table = self.table_name(plan, receipt.attempt_id)
        if receipt.stage_id != self.qualified(table):
            raise ValueError("mssql_native.stage_identity_mismatch")
        if self._target_local_attempt is not None:
            return self._target_local_attempt.inspect(self, plan, receipt, lease)
        with self._mutation_scope(plan, receipt.attempt_id, lease):
            from dpone.runtime.sinks.mssql_native_prepared_owner import require_prepared_owner

            require_prepared_owner(self.connector, self._ownership(plan, receipt.attempt_id))
            part = receipt.consumed_part_evidence
            if (
                part.get("native_plan_binding") != stable_hash(asdict(plan))
                or part.get("native_object_id") != self._object_id(table)
                or part.get("artifact_sha256") != receipt.file_sha256
                or part.get("declared_rows") != receipt.rows
                or part.get("artifact_size_bytes") != receipt.encoded_bytes
            ):
                raise ValueError("mssql_native.stage_identity_mismatch")
            total = self._verify_contents(table, receipt.rows, receipt.typed_digest)
            if part.get("native_typed_sum") != total:
                raise ValueError("mssql_native.typed_digest_mismatch")
            self._assert_lease(lease)
        return receipt

    def settle(self, plan: NativeChunkPlan, attempt_id: str, lease: Any) -> None:
        if self._target_local_attempt is not None:
            raise ValueError("mssql_native.target_local_settlement_requires_proof")
        self._assert_lease(lease)
        with self._mutation_scope(plan, attempt_id, lease):
            from dpone.runtime.sinks.mssql_native_prepared_owner import require_prepared_owner

            table = self.table_name(plan, attempt_id)
            exists = self.connector.get_records("SELECT OBJECT_ID(?)", (self.qualified(table),))
            if exists and exists[0][0] is not None:
                require_prepared_owner(self.connector, self._ownership(plan, attempt_id))
            self.connector.execute_query(f"DROP TABLE IF EXISTS {self.qualified(self.table_name(plan, attempt_id))}")
            self._assert_lease(lease)

    def settle_published(self, plan: NativeChunkPlan, receipt: NativeChunkReceipt, lease: Any) -> None:
        """Delegate v2 stage removal to its publication and exact-owner proof."""
        if self._target_local_attempt is None:
            raise ValueError("mssql_native.target_local_cleanup_required")
        self._target_local_attempt.settle_published(self, plan, receipt, lease)

    def drop_exact_owned(self, plan: NativeChunkPlan, receipt: NativeChunkReceipt, lease: Any) -> None:
        """Execute exact-stage retirement under an outer durable journal proof."""
        if self._target_local_attempt is None:
            raise ValueError("mssql_native.target_local_retirement_required")
        self._target_local_attempt.drop_exact_owned(self, plan, receipt, lease)

    def recover_positive(
        self, plan: NativeChunkPlan, file: EncodedNativeFile, attempt_id: str, lease: Any
    ) -> NativeChunkReceipt:
        """Observe a durable positive BCP terminal without another launch."""
        if self._target_local_attempt is None:
            raise ValueError("mssql_native.target_local_recovery_required")
        return self._target_local_attempt.recover_positive(self, plan, file, attempt_id, lease)

    def allocated_bytes(self) -> int:
        """Observe reserved pages of all native staging tables in this database."""
        return native_stage_allocated_bytes(self.connector, self.database)

    def digest_rows(self, rows: Iterable[Any]) -> tuple[int, str]:
        """Canonical native re-encoding verifies duplicates without ordering rows."""

        count, digest, _total = self._digest_rows(rows)
        return count, digest

    def _digest_rows(self, rows: Iterable[Any]) -> tuple[int, str, int]:
        from dpone.runtime.mssql_native_chunks_files import native_multiset_digest

        count, total = 0, 0
        for row in rows:
            count += 1
            total = (total + int.from_bytes(sha256(self._encode(row)).digest(), "big")) % (1 << 256)
        return count, native_multiset_digest(count, total), total

    def _verify_contents(self, table: str, expected_rows: int, expected_digest: str) -> int:
        actual = self.connector.fetch_schema_columns(self.schema, table, database=self.database)
        expected = self._expected_stage_schema()
        if tuple(self._normalize_stage_column(column) for column in actual) != expected:
            raise ValueError("mssql_native.stage_schema_changed")
        if self._target_digest_contract is not None:
            digest, _row = self._target_digest(table, SimpleNamespace(rows=expected_rows, typed_digest=expected_digest))
            return digest.typed_sum
        count = self.connector.get_records(f"SELECT COUNT_BIG(*) FROM {self.qualified(table)}")
        if not count or type(count[0][0]) is not int or count[0][0] != expected_rows:
            raise ValueError("mssql_native.server_count_mismatch")
        columns = ", ".join(self.connector.quote_identifier(column.name) for column in self.columns)
        rows, digest, total = self._digest_rows(
            self.connector.get_records_iterator(f"SELECT {columns} FROM {self.qualified(table)}")
        )
        if rows != expected_rows or digest != expected_digest:
            raise ValueError("mssql_native.typed_digest_mismatch")
        return total

    def _target_digest(self, table: str, file: Any) -> tuple[Any, tuple[Any, ...]]:
        observed, row = self._target_digest_observation(table, file.rows)
        if observed.rows != file.rows or observed.typed_digest != file.typed_digest:
            raise ValueError("mssql_native.typed_digest_mismatch")
        return observed, row

    def _target_digest_observation(self, table: str, expected_rows: int) -> tuple[Any, tuple[Any, ...]]:
        """Return bounded aggregate evidence without interpreting content equality."""
        if self._persisted_hash_layout:
            from dpone.runtime.sinks.mssql_native_persisted_hash import (
                build_initial_persisted_hash_sql,
                decode_persisted_hash_observation,
            )

            query = build_initial_persisted_hash_sql(self.qualified(table), expected_rows)
            aggregate = self.connector.get_records(query)
            if len(aggregate) != 1:
                raise ValueError("mssql_native.persisted_hash_row_shape")
            row = tuple(aggregate[0])
            return decode_persisted_hash_observation(row, expected_rows=expected_rows), row
        from dpone.runtime.sinks.mssql_native_target_digest import build_target_digest_sql, decode_target_digest_row

        query = build_target_digest_sql(self.qualified(table), self._target_digest_contract, expected_rows)
        aggregate = self.connector.get_records(query)
        if len(aggregate) != 1:
            raise ValueError("mssql_native.target_digest_row_shape")
        row = tuple(aggregate[0])
        observed = decode_target_digest_row(row, expected_rows=expected_rows)
        return observed, row

    def _assert_stage_identity(self, plan: NativeChunkPlan, attempt_id: str, table: str, object_id: int) -> None:
        from dpone.runtime.sinks.mssql_native_prepared_owner import require_prepared_owner

        require_prepared_owner(self.connector, self._ownership(plan, attempt_id))
        if self._object_id(table) != object_id:
            raise ValueError("mssql_native.stage_identity_mismatch")
        actual = self.connector.fetch_schema_columns(self.schema, table, database=self.database)
        expected = self._expected_stage_schema()
        if tuple(self._normalize_stage_column(column) for column in actual) != expected:
            raise ValueError("mssql_native.stage_schema_changed")

    def _expected_stage_schema(self) -> tuple[tuple[str, str, bool], ...]:
        business = tuple(
            (column.name, dtype, column.nullable) for column, dtype in zip(self.columns, self._types, strict=True)
        )
        if not self._persisted_hash_layout:
            return business
        return (
            *business,
            ("__dpone__native_row_hash", "binary(32)", False),
            ("__dpone__mutation_version", "timestamp", False),
        )

    @staticmethod
    def _normalize_stage_column(column: Any) -> tuple[str, str, bool]:
        dtype = str(column.dtype).strip().lower()
        if column.name == "__dpone__mutation_version" and dtype in {"timestamp", "rowversion"}:
            return column.name, "timestamp", column.nullable
        return column.name, normalize_mssql_physical_type(column.dtype), column.nullable

    def _repeat_mutation_watermark(self, table: str, expected_rows: int) -> Any:
        from dpone.runtime.sinks.mssql_native_persisted_hash import build_repeat_watermark_sql, decode_repeat_watermark

        rows = self.connector.get_records(build_repeat_watermark_sql(self.qualified(table), expected_rows))
        if len(rows) != 1:
            raise ValueError("mssql_native.persisted_hash_row_shape")
        return decode_repeat_watermark(tuple(rows[0]), expected_rows=expected_rows)

    def _ownership(self, plan: NativeChunkPlan, attempt_id: str) -> dict[str, str]:
        return {
            "database": self.database,
            "schema": self.schema,
            "table": self.table_name(plan, attempt_id),
            "binding": sha256(repr((asdict(plan), attempt_id)).encode()).hexdigest(),
        }

    def _object_id(self, table: str) -> int:
        rows = self.connector.get_records("SELECT OBJECT_ID(?)", (self.qualified(table),))
        if not rows or type(rows[0][0]) is not int or rows[0][0] < 1:
            raise ValueError("mssql_native.object_identity_unavailable")
        return int(rows[0][0])
