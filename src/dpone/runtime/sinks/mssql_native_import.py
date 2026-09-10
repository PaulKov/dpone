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
from typing import Any

from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkPlan, NativeChunkReceipt
from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.runtime.consumed_payload_evidence import ConsumedPayloadEvidence, canonical_source_provenance_sha256
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.native_wire_models import stable_hash


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
    ) -> None:
        self.connector = connector
        self.database = database
        self.schema = schema
        self.columns = tuple(columns)
        self._encode = encode_row
        self._assert_lease = assert_lease
        self._mutation_scope = mutation_scope
        self._options_factory = options_factory
        self._types = tuple(
            normalize_mssql_physical_type(column.source_type.removesuffix(" nullable")) for column in columns
        )
        if any(dtype.startswith(("varchar", "char(")) for dtype in self._types):
            raise ValueError("mssql_native.utf8_collation_authority_required")
        if not database or not schema or not self.columns:
            raise ValueError("mssql_native.stage_authority_required")

    def table_name(self, plan: NativeChunkPlan, attempt_id: str) -> str:
        """Derive an owned identifier from the complete invocation and attempt."""

        return "dpone_native_" + sha256(repr((asdict(plan), attempt_id)).encode()).hexdigest()[:40]

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
        with self._mutation_scope(plan, attempt_id, lease):
            self._assert_lease(lease)
            ddl = ", ".join(
                f"{self.connector.quote_identifier(column.name)} {dtype} {'NULL' if column.nullable else 'NOT NULL'}"
                for column, dtype in zip(self.columns, self._types, strict=True)
            )
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
                copied = (
                    0
                    if file.rows == 0
                    else self.connector.bcp_import(
                        self.schema, table, str(file.path), options=options, database=self.database
                    )
                )
                if type(copied) is not int or copied != file.rows:
                    raise ValueError("mssql_native.vendor_count_mismatch")
                if rejects.exists() and rejects.stat().st_size:
                    raise ValueError("mssql_native.rejects_not_empty")
            if artifact.require_integrity_receipt() != integrity:
                raise ValueError("mssql_native.file_identity_changed")
            typed_sum = self._verify_contents(table, file.rows, file.typed_digest)
            self._assert_lease(lease)
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
            return NativeChunkReceipt(
                file.ordinal,
                attempt_id,
                self.qualified(table),
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
        self._assert_lease(lease)
        with self._mutation_scope(plan, attempt_id, lease):
            from dpone.runtime.sinks.mssql_native_prepared_owner import require_prepared_owner

            table = self.table_name(plan, attempt_id)
            exists = self.connector.get_records("SELECT OBJECT_ID(?)", (self.qualified(table),))
            if exists and exists[0][0] is not None:
                require_prepared_owner(self.connector, self._ownership(plan, attempt_id))
            self.connector.execute_query(f"DROP TABLE IF EXISTS {self.qualified(self.table_name(plan, attempt_id))}")
            self._assert_lease(lease)

    def allocated_bytes(self) -> int:
        """Observe reserved pages of all native staging tables in this database."""

        database = self.connector.quote_identifier(self.database)
        rows = self.connector.get_records(
            f"SELECT COALESCE(SUM(p.reserved_page_count), 0) * 8192 FROM {database}.sys.dm_db_partition_stats p "
            f"JOIN {database}.sys.tables t ON t.object_id=p.object_id WHERE t.name LIKE 'dpone[_]native[_]%'"
        )
        if not rows or type(rows[0][0]) is not int or rows[0][0] < 0:
            raise ValueError("mssql_native.allocation_unavailable")
        return int(rows[0][0])

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
        expected = tuple(
            (column.name, dtype, column.nullable) for column, dtype in zip(self.columns, self._types, strict=True)
        )
        if (
            tuple((column.name, normalize_mssql_physical_type(column.dtype), column.nullable) for column in actual)
            != expected
        ):
            raise ValueError("mssql_native.stage_schema_changed")
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
