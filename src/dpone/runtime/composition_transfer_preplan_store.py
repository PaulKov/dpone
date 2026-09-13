"""Exclusive supervisor originals for independently observed transfer preplans.

The external document digest must be pinned by the composition SQL binding.
Files are never replaced, repaired or regenerated here. A partial write blocks
reuse; recovery must inspect the retained original. This store grants no permit.
"""

from __future__ import annotations

import os
import stat
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID

from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_mssql_binding import stable_operation_document
from dpone.contracts.composition_persistence import decode_attempt_identity, encode_attempt_identity
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.runtime.consumed_payload_evidence import canonical_source_provenance_sha256
from dpone.runtime.etl.mssql_schema_preplan_codec import decode_mssql_schema_preplan
from dpone.runtime.etl.mssql_schema_preplan_support import schema_columns_sha256, source_columns
from dpone.runtime.sinks.mssql_target_mutation_codec import binary_digest
from dpone.runtime.sources.strategies.postgres.postgres_schema_metadata import PostgresFetchedSchema
from dpone.runtime.state.mssql_target_identity_models import MssqlPhysicalTargetIdentity
from dpone.runtime.support.postgres_mssql_projection_models import (
    PostgresMssqlColumnProjection,
    PostgresMssqlSchemaProjection,
)
from dpone.runtime.support.postgres_mssql_retained_catalog import RetainedMssqlTargetColumn
from dpone.type_system.source_sink.provenance import SourceColumnProvenance

MAX_TRANSFER_PREPLAN_BYTES = 4 * 1024 * 1024
_FIELDS = {
    "schema",
    "attempt_original",
    "operation_original",
    "write",
    "plan_sha256",
    "manifest_sha256",
    "preplan",
    "source_identity",
    "source_projection",
    "source_provenance_sha256",
    "target_identity",
    "route_fingerprint",
    "connection_observation",
}


def _model(kind: Any, value: Any) -> Any:
    if type(value) is not dict or set(value) != {field.name for field in fields(kind)}:
        raise ValueError
    return kind(**value)


def decode_source_projection(value: Any) -> PostgresFetchedSchema:
    """Rebuild the full existing projection, rejecting open nested structures."""
    if type(value) is not dict or set(value) != {field.name for field in fields(PostgresFetchedSchema)}:
        raise ValueError
    raw = dict(value)
    for name in ("relation_schema", "projected_schema"):
        if type(raw[name]) is not list or any(
            type(pair) is not list or len(pair) != 2 or any(type(item) is not str for item in pair)
            for pair in raw[name]
        ):
            raise ValueError
        raw[name] = tuple(tuple(pair) for pair in raw[name])
    raw["relation_metadata"] = tuple(_model(SourceColumnProvenance, item) for item in raw["relation_metadata"])
    projection = raw["target_projection"]
    if projection is not None:
        if type(projection) is not dict or set(projection) != {"columns", "retained_target_columns"}:
            raise ValueError
        raw["target_projection"] = PostgresMssqlSchemaProjection(
            tuple(_model(PostgresMssqlColumnProjection, item) for item in projection["columns"]),
            tuple(_model(RetainedMssqlTargetColumn, item) for item in projection["retained_target_columns"]),
        )
    result = PostgresFetchedSchema(**raw)
    if canonical_json_bytes(asdict(result)) != canonical_json_bytes(value):
        raise ValueError
    return result


def projection_provenance(projection: PostgresFetchedSchema) -> str:
    return canonical_source_provenance_sha256(
        relation_dialect="postgres",
        relation_schema=projection.relation_schema,
        relation_metadata=projection.relation_metadata,
        fallback_schema=projection.projected_schema,
    )


def _operation(value: Any) -> MssqlTransactionOperation:
    raw = dict(value)
    attempt = dict(raw["attempt"])
    request = dict(attempt["request"])
    request["invocation"] = _model(InvocationIdentity, request["invocation"])
    for key in ("target_identity", "route_fingerprint"):
        request[key] = binary_digest(request[key])
    attempt["request"] = _model(MssqlAttemptRequest, request)
    raw["attempt"] = _model(MssqlTransactionAttempt, attempt)
    for key in ("operation_key", "scope_hash", "owner_digest"):
        raw[key] = binary_digest(raw[key])
    raw["lease_expires_at_utc"] = None
    result = _model(MssqlTransactionOperation, raw)
    if stable_operation_document(result) != canonical_json_bytes(value):
        raise ValueError
    return result


@dataclass(frozen=True, slots=True)
class RetainedTransferPreplanReference:
    """Exact detached bytes whose digest can be bound in a protected SQL original."""

    document: bytes

    @property
    def document_sha256(self) -> bytes:
        return sha256(self.document).digest()

    @property
    def body(self) -> dict[str, Any]:
        return strict_json_object(self.document)

    @property
    def preplan(self) -> Any:
        original = canonical_json_bytes(self.body["preplan"])
        return decode_mssql_schema_preplan(original, sha256(original).digest())

    @property
    def mutation_plan_sha256(self) -> bytes:
        return self.preplan.target_mutation_plan.digest


def decode_transfer_preplan(
    document: bytes, expected_sha256: bytes, *, attempt: Any
) -> RetainedTransferPreplanReference:
    """Check complete envelope integrity, structure and internal identity links."""
    try:
        if (
            type(document) is not bytes
            or not 0 < len(document) <= MAX_TRANSFER_PREPLAN_BYTES
            or type(expected_sha256) is not bytes
            or len(expected_sha256) != 32
            or sha256(document).digest() != expected_sha256
        ):
            raise ValueError
        body = strict_json_object(document)
        if (
            set(body) != _FIELDS
            or body["schema"] != "dpone.composition-transfer-preplan.v1"
            or canonical_json_bytes(body) != document
        ):
            raise ValueError
        original = canonical_json_bytes(body["attempt_original"])
        decode_attempt_identity(original, attempt.attempt_sha256)
        if original != encode_attempt_identity(attempt) or body["plan_sha256"] != attempt.plan_sha256:
            raise ValueError
        require_digest(body["manifest_sha256"])
        write = _model(DbtRelationWrite, body["write"])
        if write.kind != "transfer" or write.connector != "mssql" or write.role != "target":
            raise ValueError
        identity = SourcePhysicalIdentity(**body["source_identity"])
        if identity.version not in {2, 3} or identity.to_dict() != body["source_identity"]:
            raise ValueError
        projection = decode_source_projection(body["source_projection"])
        if body["source_provenance_sha256"] != projection_provenance(projection):
            raise ValueError
        target_raw = dict(body["target_identity"])
        target_raw["binding_id"] = UUID(target_raw["binding_id"])
        target = _model(MssqlPhysicalTargetIdentity, target_raw)
        target_document = {**asdict(target), "binding_id": str(target.binding_id)}
        if target_document != body["target_identity"]:
            raise ValueError
        reference = RetainedTransferPreplanReference(document)
        preplan = reference.preplan
        mutation = preplan.target_mutation_plan
        if preplan.source_schema_sha256 != schema_columns_sha256(source_columns(projection)):
            raise ValueError
        request = _operation(body["operation_original"]).attempt.request
        route = binary_digest(body["route_fingerprint"])
        if (
            request.target_identity != target.digest
            or mutation.target_identity != target.digest
            or request.route_fingerprint != route
            or (write.database, write.schema, write.relation)
            != (mutation.target_database, mutation.target_schema, mutation.target_table)
            or (request.target_database, request.target_schema, request.target_table)
            != (write.database, write.schema, write.relation)
            or (target.database_name, target.schema_name, target.table_name)
            != (write.database, write.schema, write.relation)
            or request.strategy != "full_refresh"
        ):
            raise ValueError
        if type(body["connection_observation"]) is not dict or not body["connection_observation"]:
            raise ValueError
        return reference
    except Exception:
        raise CompositionAdmissionError("transfer_preplan_original") from None


class CompositionTransferPreplanStore:
    """Retain under a separate preplans directory, never the payload attempt path."""

    def __init__(self, payload_root: Path) -> None:
        info = payload_root.lstat()
        self._require_private(info, directory=True)
        self._root = payload_root
        self._root_identity = (info.st_dev, info.st_ino)
        with self._directory():
            pass

    @staticmethod
    def _require_private(info: os.stat_result, *, directory: bool) -> None:
        valid_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
        if (
            not valid_type
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o077
            or (not directory and info.st_nlink != 1)
        ):
            raise CompositionAdmissionError("transfer_preplan_private_path")

    @contextmanager
    def _directory(self):
        root = os.open(self._root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            info = os.fstat(root)
            self._require_private(info, directory=True)
            if (info.st_dev, info.st_ino) != self._root_identity:
                raise CompositionAdmissionError("transfer_preplan_private_path")
            try:
                os.mkdir("preplans", mode=0o700, dir_fd=root)
                os.fsync(root)
            except FileExistsError:
                pass
            folder = os.open("preplans", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root)
            try:
                self._require_private(os.fstat(folder), directory=True)
                yield folder
            finally:
                os.close(folder)
        finally:
            os.close(root)

    def capture(self, attempt: Any, document: bytes) -> RetainedTransferPreplanReference:
        reference = decode_transfer_preplan(document, sha256(document).digest(), attempt=attempt)
        name = attempt.attempt_sha256[7:] + ".json"
        with self._directory() as folder:
            fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=folder)
            with os.fdopen(fd, "wb") as output:
                output.write(document)
                output.flush()
                os.fsync(output.fileno())
            os.fsync(folder)
        return self.load(attempt, reference.document_sha256)

    def load(self, attempt: Any, expected_sha256: bytes) -> RetainedTransferPreplanReference:
        attempt.__post_init__()
        with self._directory() as folder:
            fd = os.open(attempt.attempt_sha256[7:] + ".json", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=folder)
            with os.fdopen(fd, "rb") as source:
                info = os.fstat(source.fileno())
                self._require_private(info, directory=False)
                if info.st_size > MAX_TRANSFER_PREPLAN_BYTES:
                    raise CompositionAdmissionError("transfer_preplan_budget")
                document = source.read(MAX_TRANSFER_PREPLAN_BYTES + 1)
        return decode_transfer_preplan(document, expected_sha256, attempt=attempt)
