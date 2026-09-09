from __future__ import annotations

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from dpone.runtime.bulk_options import ClickHouseBulkOptionsResolver
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.clickhouse_bulk_path import resolve_clickhouse_bulk_path
from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.direct_ingest import (
    ClickHouseDirectNativeCredentials,
    ClickHouseDirectNativeOptions,
    ClickHouseDirectNativeRunner,
    DirectIngestDecision,
    DirectIngestResolver,
    DirectIngestRouteRequest,
)
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse_nullability_policy import ClickHouseNullInsertPolicy
from dpone.runtime.sinks.clickhouse_tsv_formats import CLICKHOUSE_TSV_ARTIFACT_FORMATS
from dpone.runtime.support.clickhouse_bulk import (
    ClickHouseClientCredentials,
    ClickHouseClientOptions,
    ClickHouseClientRunner,
    ClickHouseHttpBulkRunner,
    ClickHouseHttpCredentials,
    ClickHouseHttpOptions,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class ClickHouseBulkMixin:
    connector: Any
    _client_runner_cls: type[ClickHouseClientRunner]
    _http_runner_cls: type[ClickHouseHttpBulkRunner]
    _direct_native_runner_cls: type[ClickHouseDirectNativeRunner] = ClickHouseDirectNativeRunner

    if TYPE_CHECKING:

        def _table(self, load_config: LoadConfig) -> str: ...

        def _count(self, load_config: LoadConfig) -> int: ...

    def _should_use_client_bulk(self, load_config: LoadConfig, artifact: FileExportArtifact) -> bool:
        if artifact.format not in CLICKHOUSE_TSV_ARTIFACT_FORMATS:
            return False
        return resolve_clickhouse_bulk_path(getattr(load_config, "options", {}) or {}) == "client"

    def _should_use_http_bulk(self, load_config: LoadConfig, artifact: FileExportArtifact) -> bool:
        if artifact.format not in CLICKHOUSE_TSV_ARTIFACT_FORMATS:
            return False
        return resolve_clickhouse_bulk_path(getattr(load_config, "options", {}) or {}) == "http"

    def _should_use_http_stream(self, load_config: LoadConfig, artifact: ByteStreamArtifact) -> bool:
        if artifact.format not in {*CLICKHOUSE_TSV_ARTIFACT_FORMATS, "clickhouse-rowbinary", "clickhouse-native"}:
            return False
        return resolve_clickhouse_bulk_path(getattr(load_config, "options", {}) or {}) == "http"

    def _should_use_client_stream(self, load_config: LoadConfig, artifact: ByteStreamArtifact) -> bool:
        if artifact.format not in {*CLICKHOUSE_TSV_ARTIFACT_FORMATS, "clickhouse-rowbinary", "clickhouse-native"}:
            return False
        return resolve_clickhouse_bulk_path(getattr(load_config, "options", {}) or {}) in {"client", "native_tcp"}

    def _insert_file_with_client(
        self,
        load_config: LoadConfig,
        artifact: FileExportArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        runner = self._build_client_runner(load_config, artifact=artifact)
        runner.insert_file(self._table(load_config), [column for column, _ in schema], artifact.file_path)
        return int(artifact.estimated_rows or 0)

    def _insert_file_with_http(
        self,
        load_config: LoadConfig,
        artifact: FileExportArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        runner = self._build_http_runner(load_config, artifact=artifact)
        runner.insert_file(self._table(load_config), [column for column, _ in schema], artifact.file_path)
        return int(artifact.estimated_rows or 0)

    def _insert_stream_with_http(
        self,
        load_config: LoadConfig,
        artifact: ByteStreamArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        runner = self._build_http_runner(load_config, artifact=artifact)
        runner.insert_stream(self._table(load_config), [column for column, _ in schema], artifact.iter_bytes())
        if artifact.estimated_rows is not None:
            return int(artifact.estimated_rows)
        return int(self._count(load_config))

    def _insert_stream_with_client(
        self,
        load_config: LoadConfig,
        artifact: ByteStreamArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        decision = self._direct_ingest_decision(load_config, artifact)
        if decision is not None and decision.selected_backend == "direct":
            direct_runner = self._build_direct_native_runner(load_config)
            direct_runner.insert_stream(
                self._table(load_config), [column for column, _ in schema], artifact.iter_bytes()
            )
            if artifact.estimated_rows is not None:
                return int(artifact.estimated_rows)
            return int(self._count(load_config))
        if decision is not None and decision.blockers:
            raise RuntimeError(",".join(decision.blockers))
        client_runner = self._build_client_runner(load_config, artifact=artifact)
        client_runner.insert_stream(self._table(load_config), [column for column, _ in schema], artifact.iter_bytes())
        if artifact.estimated_rows is not None:
            return int(artifact.estimated_rows)
        return int(self._count(load_config))

    def _should_use_direct_native_stream(self, load_config: LoadConfig, artifact: ByteStreamArtifact) -> bool:
        decision = self._direct_ingest_decision(load_config, artifact)
        if decision is None:
            return False
        if decision.selected_backend == "direct":
            return True
        if decision.blockers:
            raise RuntimeError(",".join(decision.blockers))
        return False

    def _direct_ingest_decision(
        self,
        load_config: LoadConfig,
        artifact: ByteStreamArtifact,
    ) -> DirectIngestDecision | None:
        if resolve_clickhouse_bulk_path(getattr(load_config, "options", {}) or {}) != "native_tcp":
            return None
        bulk = ClickHouseBulkOptionsResolver.resolve(getattr(load_config, "options", {}) or {})
        route_certified = bool(getattr(getattr(artifact, "bulk_wire_contract", None), "route_certified", False))
        certification_mode = _direct_ingest_certification_mode(load_config)
        decision = DirectIngestResolver().decide(
            DirectIngestRouteRequest(
                bulk_options=bulk,
                input_format=_artifact_input_format(artifact),
                route_certified=route_certified,
                certification_mode=certification_mode,
            )
        )
        publish_runtime_decision(
            decision,
            decision_id="clickhouse.direct_ingest",
            phase="load",
            component="clickhouse_sink",
            category="backend_selection",
            fallback_allowed=decision.requested_backend == "auto" and not decision.blockers,
            provider="dpone-native-accel",
            details={
                "input_format": _artifact_input_format(artifact),
                "certification_mode": certification_mode,
                "route_certified": route_certified,
                "bulk_path": "native_tcp",
                "artifact_format": artifact.format,
            },
        )
        return decision

    def _build_client_runner(
        self,
        load_config: LoadConfig,
        *,
        artifact: FileExportArtifact | ByteStreamArtifact | None = None,
    ) -> ClickHouseClientRunner:
        options = getattr(load_config, "options", {}) or {}
        bulk = ClickHouseBulkOptionsResolver.resolve(options)
        client_command = bulk.client.command or os.getenv("DPONE_CLICKHOUSE_CLIENT") or "clickhouse-client"
        password = bulk.client.password
        native_tcp_mode = resolve_clickhouse_bulk_path(options) == "native_tcp"
        client_settings = ClickHouseNullInsertPolicy.from_load_config(load_config).merge_format_settings(
            bulk.insert_settings
        )
        if native_tcp_mode and bulk.native_tcp.compression in {"lz4", "zstd"}:
            client_settings.setdefault("compression", 1)
            client_settings.setdefault("network_compression_method", bulk.native_tcp.compression)
        credentials = ClickHouseClientCredentials(
            host=str((bulk.native_tcp.host if native_tcp_mode else None) or bulk.client.host or self.connector.host),
            port=int((bulk.native_tcp.port if native_tcp_mode else None) or bulk.client.port or self.connector.port),
            database=str(bulk.client.database or self.connector.database),
            user=str(bulk.client.user or self.connector.user),
            password=str(password if password is not None else self.connector.password),
            secure=bool(
                bulk.native_tcp.secure
                if native_tcp_mode
                else self.connector.secure
                if bulk.client.secure is None
                else bulk.client.secure
            ),
        )
        client_options = ClickHouseClientOptions(
            client_command=str(client_command),
            timeout_seconds=bulk.native_tcp.timeout_seconds if native_tcp_mode else bulk.client.timeout_seconds,
            max_insert_block_size=options.get("clickhouse_max_insert_block_size"),
            settings=client_settings,
            query_id=bulk.query_id,
            insert_deduplication_token=bulk.insert_deduplication_token,
        )
        contract = getattr(artifact, "bulk_wire_contract", None) if artifact is not None else None
        if contract is not None:
            client_options = ClickHouseClientOptions.from_bulk_wire_contract(contract, base=client_options)
        return self._client_runner_cls(credentials, client_options)

    def _build_direct_native_runner(self, load_config: LoadConfig) -> ClickHouseDirectNativeRunner:
        options = getattr(load_config, "options", {}) or {}
        bulk = ClickHouseBulkOptionsResolver.resolve(options)
        settings = ClickHouseNullInsertPolicy.from_load_config(load_config).merge_format_settings(bulk.insert_settings)
        compression = "lz4" if bulk.native_tcp.compression == "auto" else bulk.native_tcp.compression
        credentials = ClickHouseDirectNativeCredentials(
            host=str(bulk.native_tcp.host or bulk.client.host or self.connector.host),
            port=int(bulk.native_tcp.port or bulk.client.port or self.connector.port),
            database=str(bulk.client.database or self.connector.database),
            user=str(bulk.client.user or self.connector.user),
            password=str(bulk.client.password if bulk.client.password is not None else self.connector.password),
            secure=bool(bulk.native_tcp.secure),
        )
        direct_options = ClickHouseDirectNativeOptions(
            compression=compression,
            timeout_seconds=int(bulk.native_tcp.query_timeout_seconds or bulk.native_tcp.timeout_seconds or 3600),
            query_id=bulk.query_id,
            settings=settings,
        )
        return self._direct_native_runner_cls(credentials=credentials, options=direct_options)

    def _build_http_runner(
        self,
        load_config: LoadConfig,
        *,
        artifact: Any | None = None,
    ) -> ClickHouseHttpBulkRunner:
        options = getattr(load_config, "options", {}) or {}
        bulk = ClickHouseBulkOptionsResolver.resolve(options)
        password = bulk.http.password
        credentials = ClickHouseHttpCredentials(
            host=str(bulk.http.host or self.connector.host),
            port=int(bulk.http.port or 8123),
            database=str(bulk.http.database or self.connector.database),
            user=str(bulk.http.user or self.connector.user),
            password=str(password if password is not None else self.connector.password),
            secure=bool(bulk.http.secure),
        )
        http_options = ClickHouseHttpOptions(
            timeout_seconds=int(bulk.http.timeout_seconds),
            chunk_size=int(bulk.http.chunk_size),
            settings=ClickHouseNullInsertPolicy.from_load_config(load_config).merge_format_settings(
                bulk.insert_settings
            ),
            query_id=bulk.query_id,
            insert_deduplication_token=bulk.insert_deduplication_token,
        )
        contract = getattr(artifact, "bulk_wire_contract", None) if artifact is not None else None
        if contract is not None:
            http_options = ClickHouseHttpOptions.from_bulk_wire_contract(contract, base=http_options)
        return self._http_runner_cls(credentials, http_options)

    @staticmethod
    def _clickhouse_insert_settings(options: dict[str, Any]) -> dict[str, Any]:
        bulk = ClickHouseBulkOptionsResolver.resolve(options)
        return ClickHouseNullInsertPolicy.from_options(options).merge_format_settings(bulk.insert_settings)


def _artifact_input_format(artifact: ByteStreamArtifact) -> str:
    contract = getattr(artifact, "bulk_wire_contract", None)
    if contract is not None and getattr(contract, "input_format", None):
        return str(contract.input_format)
    return "Native" if artifact.format == "clickhouse-native" else str(artifact.format)


def _direct_ingest_certification_mode(load_config: LoadConfig) -> str:
    options = getattr(load_config, "options", {}) or {}
    native_transfer = options.get("native_transfer") if isinstance(options, dict) else None
    execution = native_transfer.get("execution") if isinstance(native_transfer, dict) else None
    certification = execution.get("certification") if isinstance(execution, dict) else None
    if isinstance(certification, dict):
        return str(certification.get("mode") or "certified_only")
    return "certified_only"
