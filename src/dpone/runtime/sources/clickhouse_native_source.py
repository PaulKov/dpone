"""Dispatch plain and raw query snapshots for bounded native MSSQL extraction.

The injected connector owns a dedicated native-driver session. No table count,
header probe, offset restart or independent partition query is executed.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager
from typing import Any, NoReturn
from uuid import uuid4

from dpone.contracts.clickhouse_raw_snapshot import ClickHouseRawSnapshotProfileV1, raw_source_query_binding_version
from dpone.manifest.clickhouse_raw_snapshot_policy import native_source_snapshot_policy
from dpone.manifest.mssql_native_policy import validate_native_config
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.sources.clickhouse_plain_snapshot import prepare_plain_snapshot
from dpone.runtime.sources.clickhouse_raw_snapshot import RawSnapshot
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.streaming_rows import StreamingRowsArtifact
from dpone.type_system.source_sink.provenance import SourceRelationDialect


class NativeQueryArtifact(StreamingRowsArtifact):
    """Owned source iterator whose EOF is distinct from verified staging."""

    def __init__(
        self,
        rows: Iterator[Mapping[str, object]],
        *,
        query_id: str,
        cleanup: Any,
        source_relation_uuid: str | None = None,
    ) -> None:
        super().__init__(rows, cleanup_callback=cleanup)
        self.source_query_id = query_id
        self.source_relation_uuid = source_relation_uuid
        self._native_started = False

    def iter_native_rows(self) -> Iterator[Mapping[str, object]]:
        """Consume once, including existing payload iterator transformations."""
        if self._native_started:
            raise ValueError("mssql_native.source_reextract_required")
        self._native_started = True
        count = 0
        try:
            for row in self._iterator:
                yield row
                count += 1
            self.rows_exported = self.row_count = count
            if self.extraction_lifecycle is not None:
                self.extraction_lifecycle.complete()
        finally:
            primary = sys.exc_info()[1]
            close = getattr(self._iterator, "close", None)
            if callable(close):
                try:
                    close()
                except BaseException as error:
                    if primary is None:
                        raise
                    primary.add_note(f"native source cleanup failed: {type(error).__name__}")


class RawQueryArtifact(NativeQueryArtifact):
    """Raw-only descriptor attributes; the EOF member is assigned after validation."""

    raw_snapshot_profile: ClickHouseRawSnapshotProfileV1
    raw_snapshot_eof: Any
    source_query_binding: str


class ClickHouseNativeSource:
    """Metadata admission followed by exactly one explicitly identified SELECT."""

    def __init__(self, connector: Any, *, schema_guard_factory: Any = None) -> None:
        self.connector = connector
        self.schema_guard_factory = schema_guard_factory
        self._raw_snapshot: RawSnapshot | None = None

    def snapshot_profile(self, config: Any) -> ClickHouseRawSnapshotProfileV1 | None:
        """Freeze explicit raw authority; legacy planning performs no source I/O."""
        if native_source_snapshot_policy(config).mode != "exact_raw_rows":
            return None
        validate_native_config(config)
        if getattr(self.connector, "driver", None) != "native":
            raise ValueError("mssql_native.native_clickhouse_driver_required")
        try:
            if self._raw_snapshot is None:
                self._raw_snapshot = RawSnapshot(self.connector, config)
            return self._raw_snapshot.acquire_profile(config)
        except BaseException as primary:
            try:
                self.connector.connection.disconnect()
            except BaseException:
                primary.add_note("mssql_native.source_disconnect_failed")
            if isinstance(primary, ValueError) and str(primary).startswith("mssql_native."):
                raise
            if isinstance(primary, (GeneratorExit, KeyboardInterrupt, SystemExit)):
                raise
            raise ValueError("mssql_native.source_read_profile_unsupported") from None

    def extract(
        self,
        config: Any,
        *,
        query_id: str | None = None,
        expected_profile: ClickHouseRawSnapshotProfileV1 | None = None,
        source_query_binding: str | None = None,
    ) -> ExtractResult:
        validate_native_config(config)
        if native_source_snapshot_policy(config).mode == "exact_raw_rows":
            return self._extract_raw(config, query_id, expected_profile, source_query_binding)
        if expected_profile is not None or source_query_binding is not None:
            raise ValueError("mssql_native.source_snapshot_binding_invalid")
        if getattr(self.connector, "driver", None) != "native":
            raise ValueError("mssql_native.native_clickhouse_driver_required")
        if self.schema_guard_factory is None:
            raise ValueError("mssql_native.source_ddl_guard_required")
        guard = self.schema_guard_factory(config)
        guard.__enter__()
        try:
            return self._extract_guarded(config, guard, query_id)
        except BaseException as primary:
            try:
                self.connector.connection.disconnect()
            except BaseException as cleanup_error:
                primary.add_note(f"native source disconnect failed: {type(cleanup_error).__name__}")
            try:
                guard.__exit__(*sys.exc_info())
            except BaseException as cleanup_error:
                primary.add_note(f"native source guard cleanup failed: {type(cleanup_error).__name__}")
            raise

    def _extract_raw(
        self, config: Any, query_id: str | None, expected: ClickHouseRawSnapshotProfileV1 | None, binding: str | None
    ) -> ExtractResult:
        profile = self.snapshot_profile(config)
        if profile is None or (expected is not None and profile != expected):
            self._reject_raw("mssql_native.source_snapshot_profile_changed")
        try:
            if binding is None or raw_source_query_binding_version(binding) != 1:
                raise ValueError
        except ValueError:
            self._reject_raw("mssql_native.source_snapshot_binding_invalid")
        snapshot = self._raw_snapshot
        assert snapshot is not None
        vendor_id = query_id or "dpone-native-" + uuid4().hex
        artifact: RawQueryArtifact

        def rows() -> Iterator[Mapping[str, object]]:
            yield from snapshot.rows(config, profile, vendor_id)
            if snapshot.eof is None:
                raise ValueError("mssql_native.source_provenance_incomplete")
            artifact.raw_snapshot_eof = snapshot.eof

        artifact = RawQueryArtifact(
            rows(),
            query_id=vendor_id,
            cleanup=self.connector.connection.disconnect,
            source_relation_uuid=profile.relation_uuid,
        )
        artifact.raw_snapshot_profile = profile
        artifact.source_query_binding = binding
        lifecycle = ExtractionLifecycleAuthority()
        lifecycle.acquire_snapshot(snapshot_authority="clickhouse.raw-query-snapshot.v1", source_token=profile.sha256)
        artifact.bind_extraction_lifecycle(lifecycle)
        schema = tuple((name, dtype) for name, dtype, _ in profile.ordered_schema)
        return ExtractResult(
            artifact, schema, relation_schema=schema, relation_dialect=SourceRelationDialect.CLICKHOUSE
        )

    def _reject_raw(self, code: str) -> NoReturn:
        primary = ValueError(code)
        try:
            self.connector.connection.disconnect()
        except BaseException:
            primary.add_note("mssql_native.source_disconnect_failed")
        raise primary from None

    def _extract_guarded(self, config: Any, guard: AbstractContextManager[Any], query_id: str | None) -> ExtractResult:
        snapshot = prepare_plain_snapshot(self.connector, config, query_id)
        schema, source_uuid = snapshot.schema, snapshot.source_uuid
        lifecycle = ExtractionLifecycleAuthority()
        lifecycle.acquire_snapshot(
            snapshot_authority="clickhouse.query-snapshot.v1",
            source_token=source_uuid,
        )

        def cleanup() -> None:
            try:
                guard.__exit__(None, None, None)
            finally:
                primary = sys.exc_info()[1]
                try:
                    self.connector.connection.disconnect()
                except BaseException as cleanup_error:
                    if primary is None:
                        raise
                    primary.add_note(f"native source disconnect failed: {type(cleanup_error).__name__}")

        artifact = NativeQueryArtifact(
            snapshot.rows,
            query_id=snapshot.query_id,
            cleanup=cleanup,
            source_relation_uuid=source_uuid,
        )
        artifact.bind_extraction_lifecycle(lifecycle)
        return ExtractResult(
            artifact, schema, relation_schema=schema, relation_dialect=SourceRelationDialect.CLICKHOUSE
        )
