"""Invocation-scoped sealing for replayable external ClickHouse artifacts."""

from __future__ import annotations

import csv
import hashlib
import json
import os
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
from dpone.runtime.support.bulk_text_codec import is_bulk_text_type
from dpone.runtime.support.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec


def _artifact_identity(
    replay_schema: Sequence[tuple[str, str]],
    identity_schema: Sequence[tuple[str, str]],
    rows: Sequence[Sequence[Any]],
) -> ArtifactIdentity:
    schema_rows = tuple((name, dtype, "", "", index) for index, (name, dtype) in enumerate(identity_schema, 1))
    canonical = json.dumps(
        {
            "rows": json.loads(canonical_rows_json(rows)),
            "schema": list(replay_schema),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    identity = ArtifactIdentity(
        sha256=hashlib.sha256(canonical).hexdigest(),
        byte_size=len(canonical),
        row_count=len(rows),
        schema_digest=canonical_schema_digest(schema_rows),
        wire_digest=canonical_rows_digest(rows),
    )
    identity.validate()
    return identity


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
        self._sealed_payload, self._identity, self._identity_schema = self._seal()
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
        resealed = self._identity if self._sink is None else self._seal()[1]
        if expected != self._identity or resealed != expected:
            raise ExternalContractError("ARTIFACT_CHANGED", "sealed artifact identity changed")

    def open_replay(self) -> LoadPayload:
        rows = [dict(row) for row in self._sealed_rows]
        return self._sealed_payload.rebind(artifact=InMemoryRowsArtifact(rows))

    def persist(self, root: Path) -> None:
        """Atomically retain the sealed typed artifact for process restart."""

        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        target = root / f"{self.binding_id}.json"
        columns = tuple(name for name, _ in self._sealed_payload.schema)
        typed_rows = [tuple(row[column] for column in columns) for row in self._sealed_rows]
        document = {
            "version": 1,
            "binding_id": self.binding_id,
            "identity": {
                "sha256": self.identity.sha256,
                "byte_size": self.identity.byte_size,
                "row_count": self.identity.row_count,
                "schema_digest": self.identity.schema_digest,
                "wire_digest": self.identity.wire_digest,
            },
            "schema": list(self._sealed_payload.schema),
            "identity_schema": list(self._identity_schema),
            "rows": json.loads(canonical_rows_json(typed_rows)),
        }
        encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        temporary = root / f".{self.binding_id}.{os.getpid()}.tmp"
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            try:
                os.link(temporary, target)
            except FileExistsError:
                reopened = self.reopen(root=root, binding_id=self.binding_id, expected=self.identity)
                reopened.revalidate(self.identity)
            directory_fd = os.open(root, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)

    @classmethod
    def reopen(cls, *, root: Path, binding_id: str, expected: ArtifactIdentity) -> ClickHouseExternalArtifactSource:
        """Reopen an exact retained artifact without consulting the source."""

        path = root / f"{binding_id}.json"
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ExternalContractError("ARTIFACT_UNAVAILABLE", "retained artifact is unavailable") from exc
        if not isinstance(document, Mapping) or document.get("version") != 1:
            raise ExternalContractError("ARTIFACT_CHANGED", "retained artifact envelope changed")
        try:
            identity = ArtifactIdentity(**document.get("identity", {}))
            schema = tuple((str(name), str(dtype)) for name, dtype in document.get("schema", ()))
            identity_schema = tuple((str(name), str(dtype)) for name, dtype in document.get("identity_schema", ()))
            rows = [tuple(_decode_canonical_value(value) for value in row) for row in document.get("rows", ())]
            observed = _artifact_identity(schema, identity_schema, rows)
            expected_binding = digest_payload(
                {"version": 1, "artifact": observed.sha256, "schema": observed.schema_digest}
            )
        except (TypeError, ValueError) as exc:
            raise ExternalContractError("ARTIFACT_CHANGED", "retained artifact content changed") from exc
        if (
            document.get("binding_id") != binding_id
            or expected_binding != binding_id
            or identity != expected
            or observed != expected
        ):
            raise ExternalContractError("ARTIFACT_CHANGED", "retained artifact content changed")
        instance = object.__new__(cls)
        instance._sink = None
        instance._load_config = None
        instance._payload = LoadPayload(artifact=InMemoryRowsArtifact([]), schema=schema)
        instance._maximum_rows = expected.row_count
        instance._maximum_bytes = expected.byte_size
        replay_rows = [dict(zip((name for name, _ in schema), row, strict=True)) for row in rows]
        instance._sealed_payload = instance._payload.rebind(artifact=InMemoryRowsArtifact(replay_rows))
        instance._sealed_rows = tuple(replay_rows)
        instance._identity = identity
        instance._identity_schema = identity_schema
        instance._binding_id = binding_id
        return instance

    def release(self, root: Path) -> None:
        """Release only the exact content-addressed retained object."""

        (root / f"{self.binding_id}.json").unlink(missing_ok=True)

    def _seal(self) -> tuple[LoadPayload, ArtifactIdentity, tuple[tuple[str, str], ...]]:
        mapped_schema = self._mapped_schema()
        columns = tuple(column for column, _ in mapped_schema)
        rows = self._typed_rows(columns, tuple(dtype for _, dtype in mapped_schema))
        if len(rows) > self._maximum_rows:
            raise ExternalContractError("CONTENT_BUDGET_EXCEEDED", "artifact row budget exceeded")
        identity = _artifact_identity(self._payload.schema, mapped_schema, rows)
        if identity.byte_size > self._maximum_bytes:
            raise ExternalContractError("CONTENT_BUDGET_EXCEEDED", "artifact byte budget exceeded")
        replay_rows = [dict(zip(columns, row, strict=True)) for row in rows]
        return self._payload.rebind(artifact=InMemoryRowsArtifact(replay_rows)), identity, mapped_schema

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
            return [coercer.coerce_row(row, types) for row in self._file_rows(artifact)]
        raise ExternalContractError("ARTIFACT_UNSUPPORTED", "external mode requires a replayable row or file artifact")

    def _file_rows(self, artifact: FileExportArtifact) -> list[tuple[Any, ...]]:
        if artifact.compressed:
            raise ExternalContractError("ARTIFACT_UNSUPPORTED", "compressed files are unsupported")
        codec = artifact.bulk_text_codec
        if codec is not None and not isinstance(codec, ClickHouseTabSeparatedCodec):
            raise ExternalContractError("ARTIFACT_UNSUPPORTED", "file codec is unsupported")
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
                if codec is None:
                    if file_format == "mssql-delimited":
                        raise ExternalContractError(
                            "ARTIFACT_UNSUPPORTED", "raw MSSQL delimited files are not lossless"
                        )
                    rows.append(self._sink._coerce_file_row(row, self._payload.schema))
                else:
                    rows.append(self._decode_codec_row(codec, row))
                if len(rows) > self._maximum_rows:
                    raise ExternalContractError("CONTENT_BUDGET_EXCEEDED", "artifact row budget exceeded")
        if receipt.rows_exported != len(rows):
            raise ExternalContractError("ARTIFACT_CHANGED", "artifact row count changed")
        return rows

    def _decode_codec_row(
        self,
        codec: ClickHouseTabSeparatedCodec,
        row: Sequence[str],
    ) -> tuple[Any, ...]:
        if len(row) != len(self._payload.schema):
            raise ExternalContractError("ARTIFACT_CHANGED", "artifact row width changed")
        decoded: list[Any] = []
        for value, (_, source_type) in zip(row, self._payload.schema, strict=True):
            item = codec.decode_wire_value(value, source_type=source_type)
            normalized = str(source_type).strip().lower()
            if item is None or not isinstance(item, str):
                decoded.append(item)
            elif is_bulk_text_type(source_type) or codec.preserves_text_value(normalized):
                decoded.append(item)
            else:
                decoded.append(self._sink._coerce_file_value(item, source_type))
        return tuple(decoded)

    def __repr__(self) -> str:
        return "ClickHouseExternalArtifactSource(sealed=True)"


def external_artifact_store_root(load_config: Any) -> Path:
    """Resolve the explicitly configured durable retention root."""

    options = getattr(load_config, "options", {}) or {}
    configured = options.get("external_artifact_store_path") if isinstance(options, Mapping) else None
    environment = os.environ.get("DPONE_EXTERNAL_ARTIFACT_STORE")
    value = str(configured or environment or "").strip()
    if not value:
        raise ExternalContractError(
            "ARTIFACT_UNAVAILABLE",
            "external replication requires an explicit durable artifact store",
        )
    root = Path(value).expanduser()
    if not root.is_absolute():
        raise ExternalContractError("ARTIFACT_UNAVAILABLE", "artifact store path must be absolute")
    return root


def _decode_canonical_value(value: Any) -> Any:
    if not isinstance(value, list) or len(value) != 2:
        raise ExternalContractError("ARTIFACT_CHANGED", "retained canonical value is invalid")
    tag, payload = value
    if tag == "null":
        return None
    if tag == "bool":
        return bool(payload)
    if tag == "int":
        return int(payload)
    if tag == "decimal":
        from decimal import Decimal

        return Decimal(str(payload))
    if tag == "float":
        return float.fromhex(str(payload))
    if tag == "string":
        return str(payload)
    if tag == "bytes":
        return bytes.fromhex(str(payload))
    if tag == "datetime":
        from datetime import datetime

        return datetime.fromisoformat(str(payload))
    if tag == "date":
        from datetime import date

        return date.fromisoformat(str(payload))
    if tag == "time":
        from datetime import time

        return time.fromisoformat(str(payload))
    if tag == "uuid":
        from uuid import UUID

        return UUID(str(payload))
    if tag == "sequence":
        return tuple(_decode_canonical_value(item) for item in payload)
    if tag == "mapping":
        return {_decode_canonical_value(key): _decode_canonical_value(item) for key, item in payload}
    raise ExternalContractError("ARTIFACT_CHANGED", "retained canonical type is unsupported")


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


__all__ = [
    "ClickHouseExternalArtifactSource",
    "external_artifact_store_root",
    "external_content_row_budget",
]
