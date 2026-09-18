"""Invocation-scoped sealing for replayable external ClickHouse artifacts."""

from __future__ import annotations

import csv
import hashlib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.config.load_strategy import MAX_SOURCE_BYTE_BUDGET, SOURCE_BYTE_BUDGET_OPTION
from dpone.ports.clickhouse_external_replication import (
    ArtifactIdentity,
    ExternalContractError,
    digest_payload,
)
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sinks.clickhouse_external_replication_member_driver import (
    canonical_rows_digest,
    canonical_rows_json,
    canonical_schema_digest,
)
from dpone.runtime.sinks.clickhouse_row_values import ClickHouseRowValueCoercer
from dpone.runtime.sinks.clickhouse_tsv_formats import CLICKHOUSE_TSV_ARTIFACT_FORMATS
from dpone.runtime.sinks.load_payload import LoadPayload


class ClickHouseExternalArtifactSource:
    """Seal one bounded payload into a path-free, replayable row snapshot."""

    def __init__(self, *, sink: Any, load_config: Any, payload: LoadPayload, maximum_rows: int) -> None:
        if isinstance(maximum_rows, bool) or maximum_rows <= 0:
            raise ValueError("clickhouse_external_artifact.maximum_rows_must_be_positive")
        self._sink = sink
        self._load_config = load_config
        self._payload = payload
        self._maximum_rows = maximum_rows
        options = getattr(load_config, "options", {}) or {}
        self._maximum_bytes = int(options.get(SOURCE_BYTE_BUDGET_OPTION, MAX_SOURCE_BYTE_BUDGET))
        self._sealed_payload, self._identity = self._seal()
        sealed = self._sealed_payload.artifact
        if not isinstance(sealed, InMemoryRowsArtifact):
            raise AssertionError("external artifact seal must produce in-memory rows")
        self._sealed_rows = tuple(dict(row) for row in sealed._rows)
        self._binding_id = digest_payload(
            {"version": 1, "artifact": self._identity.sha256, "schema": self._identity.schema_digest}
        )

    @property
    def binding_id(self) -> str:
        return self._binding_id

    @property
    def identity(self) -> ArtifactIdentity:
        return self._identity

    def revalidate(self, expected: ArtifactIdentity) -> None:
        if expected != self._identity or self._seal()[1] != expected:
            raise ExternalContractError("ARTIFACT_CHANGED", "sealed artifact identity changed")

    def open_replay(self) -> LoadPayload:
        rows = [dict(row) for row in self._sealed_rows]
        return self._sealed_payload.rebind(artifact=InMemoryRowsArtifact(rows))

    def _seal(self) -> tuple[LoadPayload, ArtifactIdentity]:
        mapped_schema = self._mapped_schema()
        columns = tuple(column for column, _ in mapped_schema)
        rows = self._typed_rows(columns, tuple(dtype for _, dtype in mapped_schema))
        if len(rows) > self._maximum_rows:
            raise ExternalContractError("CONTENT_BUDGET_EXCEEDED", "artifact row budget exceeded")
        schema_rows = tuple((name, dtype, "", "", index) for index, (name, dtype) in enumerate(mapped_schema, 1))
        wire_digest = canonical_rows_digest(rows)
        canonical = canonical_rows_json(rows).encode("utf-8")
        if len(canonical) > self._maximum_bytes:
            raise ExternalContractError("CONTENT_BUDGET_EXCEEDED", "artifact byte budget exceeded")
        identity = ArtifactIdentity(
            sha256=hashlib.sha256(canonical).hexdigest(),
            byte_size=len(canonical),
            row_count=len(rows),
            schema_digest=canonical_schema_digest(schema_rows),
            wire_digest=wire_digest,
        )
        identity.validate()
        replay_rows = [dict(zip(columns, row, strict=True)) for row in rows]
        return self._payload.rebind(artifact=InMemoryRowsArtifact(replay_rows)), identity

    def _mapped_schema(self) -> tuple[tuple[str, str], ...]:
        mapper = getattr(self._sink._payload_ingestion, "_clickhouse_schema", None)
        if not callable(mapper):
            raise ExternalContractError("ARTIFACT_UNSUPPORTED", "ClickHouse schema mapping is unavailable")
        return tuple(mapper(self._load_config, self._payload.schema))

    def _typed_rows(self, columns: Sequence[str], types: Sequence[str]) -> list[tuple[Any, ...]]:
        artifact = self._payload.artifact
        coercer = ClickHouseRowValueCoercer()
        if isinstance(artifact, InMemoryRowsArtifact):
            return [coercer.coerce_row(tuple(row.get(column) for column in columns), types) for row in artifact._rows]
        if isinstance(artifact, FileExportArtifact):
            return self._file_rows(artifact)
        raise ExternalContractError("ARTIFACT_UNSUPPORTED", "external mode requires a replayable row or file artifact")

    def _file_rows(self, artifact: FileExportArtifact) -> list[tuple[Any, ...]]:
        if artifact.compressed or artifact.bulk_text_codec is not None:
            raise ExternalContractError("ARTIFACT_UNSUPPORTED", "encoded or compressed files are unsupported")
        file_format = str(artifact.format or "csv").strip().lower()
        if file_format not in {"csv", *CLICKHOUSE_TSV_ARTIFACT_FORMATS}:
            raise ExternalContractError("ARTIFACT_UNSUPPORTED", "file format is unsupported")
        receipt = artifact.require_integrity_receipt()
        delimiter = "\t" if file_format in CLICKHOUSE_TSV_ARTIFACT_FORMATS else ","
        rows: list[tuple[Any, ...]] = []
        with Path(artifact.file_path).open(encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter=delimiter)
            for index, row in enumerate(reader):
                if index == 0 and artifact.has_header:
                    continue
                rows.append(self._sink._coerce_file_row(row, self._payload.schema))
                if len(rows) > self._maximum_rows:
                    raise ExternalContractError("CONTENT_BUDGET_EXCEEDED", "artifact row budget exceeded")
        if receipt.rows_exported != len(rows):
            raise ExternalContractError("ARTIFACT_CHANGED", "artifact row count changed")
        return rows

    def __repr__(self) -> str:
        return "ClickHouseExternalArtifactSource(sealed=True)"


def external_content_row_budget(load_config: Any) -> int:
    """Resolve the explicit bounded proof budget with a conservative default."""

    options = getattr(load_config, "options", {}) or {}
    physical = options.get("physical_design") if isinstance(options, Mapping) else None
    storage = physical.get("storage") if isinstance(physical, Mapping) else None
    clickhouse = storage.get("clickhouse") if isinstance(storage, Mapping) else None
    cluster = clickhouse.get("cluster") if isinstance(clickhouse, Mapping) else None
    value = cluster.get("external_content_row_budget", 100_000) if isinstance(cluster, Mapping) else 100_000
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ExternalContractError("CONTENT_BUDGET_EXCEEDED", "external content row budget is invalid")
    return value


__all__ = ["ClickHouseExternalArtifactSource", "external_content_row_budget"]
