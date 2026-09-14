"""Exact logical preparation into a bounded, separately identified RowBinary file."""

from __future__ import annotations

import hashlib
import io
import os
import re
import struct
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import BinaryIO

from dpone.config.load_config import LoadConfig
from dpone.runtime.artifact_integrity import ArtifactIntegrityError, FileIdentity
from dpone.runtime.clickhouse_binary_encoding import encode_clickhouse_value, unwrap_nullable
from dpone.runtime.clickhouse_file_stage_contract import (
    CHUNK_BYTES,
    SYNC_SETTINGS,
    ClickHouseFileJournalResource,
    ClickHouseFilePlan,
    ClickHouseValidatedFilePolicy,
    FileConsumptionError,
    canonical_json,
    require_transport_profile,
)
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec, BulkTextFileReadError, iter_rows
from dpone.runtime.etl.validated_file_artifact import FileValidationAttempt, FileValidationBinding
from dpone.runtime.file_artifact_authority import FileVerificationBudget
from dpone.runtime.process_io import add_exception_note
from dpone.runtime.sinks.clickhouse_physical_types import ClickHousePhysicalColumnTypeResolver
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypeMapper, MssqlClickHouseTypePolicy


def config_digest(config: LoadConfig) -> str:
    """Bind effective caller configuration without persisting credentials/options."""
    return hashlib.sha256(
        canonical_json({"schema": config.target_schema, "table": config.target_table, "options": config.options})
    ).hexdigest()


def make_plan(
    config: LoadConfig, schema: Sequence[tuple[str, str]], *, resolver: ClickHousePhysicalColumnTypeResolver
) -> ClickHouseFilePlan:
    """Pure finite capability/type admission, with exact positional identities."""
    mode, timeout = require_transport_profile(config.options or {})
    policy = MssqlClickHouseTypePolicy.from_config((config.options or {}).get("type_fidelity"))
    mapper = MssqlClickHouseTypeMapper(policy)
    source = tuple((str(name), str(dtype)) for name, dtype in schema)
    if not source or len({name.casefold() for name, _ in source}) != len(source):
        raise FileConsumptionError("schema_changed")
    targets = tuple(
        (name, resolver.resolve(load_config=config, type_mapper=mapper, column=name, source_type=dtype))
        for name, dtype in source
    )
    for (_, dtype), (_, target) in zip(source, targets, strict=True):
        _require_type_pair(dtype, target)
    return ClickHouseFilePlan(
        source,
        targets,
        mode,
        policy.binary_encoding,
        timeout,
        config_digest(config),
        config.target_schema,
        config.target_table,
    )


def _require_type_pair(source: str, target: str) -> None:
    base = re.sub(r"\s+nullable$", "", source.strip().lower())
    _, inner = unwrap_nullable(target)
    expected = {"tinyint": "UInt8", "smallint": "Int16", "int": "Int32", "bigint": "Int64", "bit": "Bool"}.get(base)
    if re.fullmatch(r"(?:char|varchar|nchar|nvarchar|text|ntext|json|xml|string)(?:\([^)]*\))?", base):
        expected = "String"
    elif re.fullmatch(r"(?:binary|varbinary|image)(?:\([^)]*\))?", base):
        # Match the producer's binary recognition, including its nullable spelling limit.
        if source.strip().lower().split("(", 1)[0] not in {"binary", "varbinary", "image"}:
            raise FileConsumptionError("source_type_unsupported")
        expected = "String"
    elif decimal := re.fullmatch(r"(?:decimal|numeric)\(\s*(\d+)\s*,\s*(\d+)\s*\)", base):
        precision, scale = map(int, decimal.groups())
        if not 1 <= precision <= 76 or not 0 <= scale <= precision:
            raise FileConsumptionError("source_type_unsupported")
        expected = f"Decimal({precision},{scale})"
    if expected is None:
        raise FileConsumptionError("source_type_unsupported")
    if inner.replace(" ", "") != expected:
        raise FileConsumptionError("target_representation_unsupported")


def _target_value(value: object, source_type: str) -> object:
    if value is None or isinstance(value, bytes):
        return value
    source = re.sub(r"\s+nullable$", "", source_type.strip().lower())
    if source in {"tinyint", "smallint", "int", "bigint"}:
        if not isinstance(value, str) or not re.fullmatch(r"[+-]?[0-9]+", value):
            raise ValueError("integer representation")
        return int(value)
    if source == "bit":
        if value not in {"0", "1"}:
            raise ValueError("bit representation")
        return value == "1"
    if source.startswith(("decimal(", "numeric(")):
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError("finite decimal required")
        return result
    return value


@dataclass(frozen=True, slots=True)
class PreparedClickHouseFile:
    """A sealed owned stream; neither a source receipt nor commit authority."""

    binding: FileValidationBinding
    plan: ClickHouseFilePlan
    stream: BinaryIO
    rows: int
    size_bytes: int
    sha256: str
    identity: FileIdentity
    semantic_id: str

    def verify_transport(self, journal: ClickHouseFileJournalResource, *, check_deadline: Callable[[], None]) -> None:
        """Rehash the held file and verify that its sealed pathname still agrees."""
        journal.require_identity()
        path = journal.directory / "transport.rowbinary"
        if (
            FileIdentity.from_stat(path.lstat()) != self.identity
            or FileIdentity.from_stat(os.fstat(self.stream.fileno())) != self.identity
        ):
            raise FileConsumptionError("transport_changed", phase="verifying")
        self.stream.seek(0)
        digest = hashlib.sha256()
        size = 0
        while True:
            check_deadline()
            chunk = self.stream.read(CHUNK_BYTES)
            check_deadline()
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
        if size != self.size_bytes or digest.hexdigest() != self.sha256:
            raise FileConsumptionError("transport_changed", phase="verifying")
        if (
            FileIdentity.from_stat(path.lstat()) != self.identity
            or FileIdentity.from_stat(os.fstat(self.stream.fileno())) != self.identity
        ):
            raise FileConsumptionError("transport_changed", phase="verifying")
        self.stream.seek(0)


class _ObservedSource(io.RawIOBase):
    """Observe actual descriptor reads beneath the canonical buffered parser."""

    def __init__(self, stream: BinaryIO, maximum: int, check: Callable[[], None]) -> None:
        self.stream, self.maximum, self.check = stream, maximum, check
        self.digest = hashlib.sha256()
        self.size = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: object) -> int:
        self.check()
        view = memoryview(buffer)  # type: ignore[arg-type]
        data = self.stream.read(len(view))
        self.check()
        self.size += len(data)
        if self.size > self.maximum:
            raise FileConsumptionError("resource_limit", phase="preparing")
        self.digest.update(data)
        view[: len(data)] = data
        return len(data)


class ClickHouseValidatedFilePreparer:
    """Prepare the complete finite input before staging can mutate a target."""

    def __init__(self, *, clock: Callable[[], float]) -> None:
        self.clock = clock

    def prepare(
        self,
        attempt: FileValidationAttempt,
        plan: ClickHouseFilePlan,
        policy: ClickHouseValidatedFilePolicy,
        journal: ClickHouseFileJournalResource,
        *,
        verification_budget: FileVerificationBudget | None = None,
    ) -> PreparedClickHouseFile:
        binding = attempt.binding
        artifact, receipt = binding.artifact, binding.receipt
        if binding.source_schema != plan.source_schema:
            raise FileConsumptionError("schema_changed")
        if (
            artifact.bulk_text_codec != BulkTextCodec()
            or artifact.format != "mssql-delimited"
            or artifact.compressed
            or artifact.has_header
        ):
            raise FileConsumptionError("codec_profile_unsupported")
        if receipt.artifact_size_bytes > policy.max_source_bytes:
            raise FileConsumptionError("resource_limit")
        budget = verification_budget or FileVerificationBudget(
            self.clock, self.clock() + policy.preparation_timeout_seconds, policy.max_source_bytes
        )

        def check() -> None:
            budget.check()

        journal.record(
            phase="preparing",
            outcome="running",
            source_identity=asdict(receipt),
            requested_mode=plan.mode,
            resolved_mode=plan.mode,
        )
        stream = journal.open_partial()
        try:
            rows, size, digest = self._encode(attempt, plan, policy, stream, journal, check)
            check()
            attempt.verify_unchanged(verification_budget=budget)
            check()
            journal.seal(stream)
            check()
            stable = {
                "preparation_version": 1,
                "source_receipt_sha256": hashlib.sha256(canonical_json(asdict(receipt))).hexdigest(),
                "source_sha256": receipt.artifact_sha256,
                "source_size_bytes": receipt.artifact_size_bytes,
                "source_wire_sha256": receipt.wire_contract_sha256,
                "source_contract_sha256": receipt.contract_sha256,
                "source_schema": plan.source_schema,
                "codec": {
                    "id": artifact.bulk_text_codec.codec_id,
                    "version": artifact.bulk_text_codec.codec_version,
                    **asdict(artifact.bulk_text_codec),
                },
                "target_schema": plan.target_schema,
                "binary_encoding": plan.binary_encoding,
                "mode": plan.mode,
                "input_format": "RowBinary",
                "settings": dict(SYNC_SETTINGS),
                "transport_sha256": digest,
                "transport_size_bytes": size,
                "rows_prepared": rows,
            }
            semantic = hashlib.sha256(canonical_json(stable)).hexdigest()
            journal.record(
                phase="prepared",
                outcome="running",
                semantic_id=semantic,
                derived_identity=stable,
                rows_prepared=rows,
                cleanup={"source": "unchanged", "spool": "owned", "staging": "not_created"},
            )
            check()
            return PreparedClickHouseFile(
                binding, plan, stream, rows, size, digest, FileIdentity.from_stat(os.fstat(stream.fileno())), semantic
            )
        except BaseException as error:
            try:
                stream.close()
                journal.release_spool()
            except BaseException as cleanup_error:
                add_exception_note(error, f"preparation cleanup failed:{type(cleanup_error).__name__}")
            raise

    def _encode(
        self,
        attempt: FileValidationAttempt,
        plan: ClickHouseFilePlan,
        policy: ClickHouseValidatedFilePolicy,
        target: BinaryIO,
        journal: ClickHouseFileJournalResource,
        check: Callable[[], None],
    ) -> tuple[int, int, str]:
        artifact = attempt.binding.artifact
        receipt = attempt.binding.receipt
        check()
        descriptor = os.open(artifact.file_path, os.O_RDONLY | os.O_NOFOLLOW)
        rows, size = 0, 0
        digest = hashlib.sha256()
        encoding_policy = MssqlClickHouseTypePolicy.from_config({"binary_encoding": plan.binary_encoding})
        with os.fdopen(descriptor, "rb") as source:
            before = FileIdentity.from_stat(os.fstat(source.fileno()))
            if artifact.integrity_receipt is None or before != artifact.integrity_receipt.identity:
                raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")
            observed = _ObservedSource(source, policy.max_source_bytes, check)
            with io.BufferedReader(observed) as buffered:
                try:
                    for row in iter_rows(
                        buffered, plan.source_schema, artifact.bulk_text_codec, max_record_bytes=policy.max_record_bytes
                    ):
                        for value, (name, source_type), (_target_name, target_type) in zip(
                            row, plan.source_schema, plan.target_schema, strict=True
                        ):
                            try:
                                encoded = encode_clickhouse_value(
                                    _target_value(value, source_type), target_type, source_type, encoding_policy
                                )
                            except (ValueError, ArithmeticError, struct.error):
                                raise FileConsumptionError(
                                    "value_not_representable", phase="preparing", column=name, row_ordinal=rows + 1
                                ) from None
                            journal.reserve(len(encoded))
                            target.write(encoded)
                            digest.update(encoded)
                            size += len(encoded)
                        rows += 1
                except BulkTextFileReadError as exc:
                    blocker = (
                        "resource_limit"
                        if isinstance(exc, BulkTextFileReadError) and exc.blocker == "record_limit"
                        else "value_not_representable"
                    )
                    raise FileConsumptionError(blocker, phase="preparing", row_ordinal=rows + 1) from None
            if (
                observed.size != receipt.artifact_size_bytes
                or observed.digest.hexdigest() != receipt.artifact_sha256
                or FileIdentity.from_stat(os.fstat(source.fileno())) != before
            ):
                raise ArtifactIntegrityError("artifact_integrity.sha256_mismatch")
        if rows != receipt.rows_validated:
            raise FileConsumptionError("staging_count_mismatch", phase="preparing")
        target.flush()
        return rows, size, digest.hexdigest()
