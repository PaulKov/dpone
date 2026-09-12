"""Exact typed/canonical scan attestation for MSSQL R1 V3 stages."""

from __future__ import annotations

import hashlib
import re
import struct
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import cast
from uuid import UUID

from dpone.contracts.mssql_r1_v3_stage_evidence import (
    R1StageArtifactKindV1,
    R1StageCellStateV1,
    R1TypedStageCellV1,
    R1TypedStageRowV1,
    R1TypedStageScanEvidenceV1,
    artifact_digest_for_rows,
)
from dpone.contracts.mssql_r1_v3_staging import (
    R1OpenStagePlanV1,
    R1SealedStageManifestV1,
    canonical_bytes,
    require_identifier,
)
from dpone.contracts.postgres_mssql_hash_policy import (
    R1Column,
    R1HashContractError,
    R1ScalarKind,
    R1ScalarType,
    canonical_key_payload,
    canonical_row_bytes,
)

from .mssql_r1_v3_stage_observation import (
    bytes_value,
    coordinate_values,
    query_all,
    query_one,
    stage_observation_sql,
    stage_scan_sql,
    transaction_handle,
)

_SCAN_DOMAIN = b"dpone-r1-stage-scan-contract-v1\0"
_ROW_DOMAIN = b"dpone-r1-row-hash-v1\0"
_EPOCH = datetime(2000, 1, 1)
_DAY_EPOCH = date(2000, 1, 1)
_LOGICAL_TYPE = re.compile(r"^pg\.(.+)-mssql\.(.+)\.v1$")


class MssqlR1V3StageAttestationError(RuntimeError):
    """A physical stage cannot prove its exact typed/canonical authority."""


@dataclass(frozen=True, slots=True)
class MssqlR1V3StageScan:
    evidence: R1TypedStageScanEvidenceV1
    rows: tuple[R1TypedStageRowV1, ...]


class MssqlR1V3ScalarCodec:
    """Decode and re-encode the closed R1 logical scalar representation."""

    def specification(self, logical_type_id: str) -> R1ScalarType:
        match = _LOGICAL_TYPE.fullmatch(logical_type_id)
        if match is None:
            raise MssqlR1V3StageAttestationError("unsupported logical type identity")
        source = match.group(1)
        aliases = {
            "bool": "bool",
            "bit1": "bit(1)",
            "int2": "int2",
            "int4": "int4",
            "int8": "int8",
            "float4": "float4",
            "float8": "float8",
            "uuid": "uuid",
            "date": "date",
            "time": "time",
            "timestamp": "timestamp",
            "timestamptz": "timestamptz",
            "text": "text",
            "varchar": "varchar",
            "bytea": "bytea",
        }
        source = aliases.get(source, source)
        try:
            specification = R1ScalarType.from_postgres(source)
        except R1HashContractError as exc:
            raise MssqlR1V3StageAttestationError("unsupported logical type identity") from exc
        expected_target = {
            "float8": "float",
            "time": "time",
            "timestamp": "datetime2",
            "timestamptz": "datetimeoffset",
            "text": "nvarchar",
            "varchar": "nvarchar",
            "bytea": "varbinary",
        }.get(source, specification.target_type.lower())
        observed_target = match.group(2).lower()
        if expected_target != observed_target:
            raise MssqlR1V3StageAttestationError("logical type target mapping differs from type policy")
        return specification

    def encode(self, logical_type_id: str, value: object) -> bytes:
        specification = self.specification(logical_type_id)
        encoded = canonical_row_bytes((R1Column(1, "value", specification, True),), (value,))
        offset = len(_ROW_DOMAIN) + 4 + 1
        state = encoded[offset]
        length = int.from_bytes(encoded[offset + 1 : offset + 9], "big")
        payload = encoded[offset + 9 :]
        if len(payload) != length:
            raise MssqlR1V3StageAttestationError("canonical scalar framing is invalid")
        return b"" if state == 0 else payload

    def decode(self, logical_type_id: str, payload: bytes, *, is_null: bool = False) -> object:
        if is_null:
            if payload:
                raise MssqlR1V3StageAttestationError("NULL scalar payload is not empty")
            return None
        spec = self.specification(logical_type_id)
        try:
            decoded = self._decode_value(spec, payload)
            if self.encode(logical_type_id, decoded) != payload:
                raise ValueError("non-canonical scalar")
            return decoded
        except (OverflowError, ValueError, UnicodeError, R1HashContractError) as exc:
            raise MssqlR1V3StageAttestationError("canonical scalar payload is invalid") from exc

    @staticmethod
    def _decode_value(spec: R1ScalarType, payload: bytes) -> object:  # noqa: PLR0911
        kind = spec.kind
        if kind in {R1ScalarKind.INT2, R1ScalarKind.INT4, R1ScalarKind.INT8}:
            if not payload:
                raise ValueError("empty integer")
            return int.from_bytes(payload, "big", signed=True)
        if kind in {R1ScalarKind.BOOL, R1ScalarKind.BIT1}:
            if payload not in {b"\x00", b"\x01"}:
                raise ValueError("invalid bit")
            return payload == b"\x01"
        if kind is R1ScalarKind.UUID:
            return UUID(bytes=payload)
        if kind is R1ScalarKind.TEXT:
            return payload.decode("utf-8")
        if kind is R1ScalarKind.BYTEA:
            return payload
        if kind is R1ScalarKind.FLOAT4:
            return struct.unpack(">f", payload)[0]
        if kind is R1ScalarKind.FLOAT8:
            return struct.unpack(">d", payload)[0]
        if kind is R1ScalarKind.NUMERIC:
            if len(payload) < 6 or payload[0] not in (0, 1):
                raise ValueError("invalid numeric")
            scale = int.from_bytes(payload[1:5], "big", signed=True)
            coefficient = int.from_bytes(payload[5:], "big")
            decimal_value = Decimal(coefficient).scaleb(-scale)
            return -decimal_value if payload[0] else decimal_value
        if kind is R1ScalarKind.DATE:
            return _DAY_EPOCH + timedelta(days=int.from_bytes(payload, "big", signed=True))
        if kind is R1ScalarKind.TIME:
            micros = int.from_bytes(payload, "big")
            hour, remainder = divmod(micros, 3_600_000_000)
            minute, remainder = divmod(remainder, 60_000_000)
            second, microsecond = divmod(remainder, 1_000_000)
            return time(hour, minute, second, microsecond)
        if kind in {R1ScalarKind.TIMESTAMP, R1ScalarKind.TIMESTAMPTZ}:
            timestamp_value = _EPOCH + timedelta(microseconds=int.from_bytes(payload, "big", signed=True))
            return (
                timestamp_value if kind is R1ScalarKind.TIMESTAMP else timestamp_value.replace(tzinfo=timezone.utc)  # noqa: UP017
            )
        raise ValueError("unsupported scalar")


class MssqlR1V3SealedStageAttestor:
    """Compute stage evidence from physical typed rows on the injected handle."""

    def __init__(
        self, authority_schema: str = "dpone_authority", *, scalar_codec: MssqlR1V3ScalarCodec | None = None
    ) -> None:
        self._authority_schema = require_identifier(authority_schema, "authority_schema")
        self._codec = scalar_codec or MssqlR1V3ScalarCodec()

    def attest(self, transaction: object, manifest: R1SealedStageManifestV1) -> R1TypedStageScanEvidenceV1:
        handle = transaction_handle(transaction)
        stored = query_one(
            handle,
            stage_observation_sql(self._authority_schema),
            (manifest.artifact_id,),
        )
        if stored is None or len(stored) != 19 or str(stored[1]) != "SEALED":
            raise MssqlR1V3StageAttestationError("sealed stage authority is unavailable")
        try:
            plan = R1OpenStagePlanV1.from_canonical_bytes(bytes_value(stored[0]))
            persisted = R1SealedStageManifestV1.from_canonical_bytes(bytes_value(stored[8]))
        except (TypeError, ValueError) as exc:
            raise MssqlR1V3StageAttestationError("sealed stage authority is malformed") from exc
        if (
            persisted != manifest
            or bytes_value(stored[9]) != persisted.manifest_digest
            or not manifest.matches_open_plan(plan)
        ):
            raise MssqlR1V3StageAttestationError("sealed stage authority differs from manifest")
        scan = self.scan_open(
            handle,
            plan,
            manifest.object_uuid,
            manifest.object_id,
            manifest.physical_token,
            manifest.target_local_schema,
            manifest.target_local_object,
        )
        if (
            scan.evidence.artifact_digest,
            scan.evidence.observed_row_count,
            scan.evidence.observed_payload_bytes,
        ) != (manifest.artifact_digest, manifest.observed_row_count, manifest.observed_payload_bytes):
            raise MssqlR1V3StageAttestationError("fresh rows differ from sealed artifact authority")
        if scan.evidence != manifest.typed_scan_evidence:
            raise MssqlR1V3StageAttestationError("fresh typed scan differs from sealed evidence")
        return scan.evidence

    def scan_open(
        self,
        transaction: object,
        plan: R1OpenStagePlanV1,
        object_uuid: UUID,
        object_id: int,
        physical_token: UUID,
        schema_name: str,
        object_name: str,
    ) -> MssqlR1V3StageScan:
        handle = transaction_handle(transaction)
        schema = require_identifier(schema_name, "stage_schema")
        table = require_identifier(object_name, "stage_object")
        proof = query_one(handle, stage_observation_sql(self._authority_schema), (plan.artifact_id,))
        expected_stage = (
            plan.canonical_bytes,
            plan.owner_epoch,
            str(object_uuid),
            object_id,
            str(physical_token),
            schema,
            table,
        )
        if (
            proof is None
            or len(proof) != 19
            or (
                bytes_value(proof[0]),
                int(str(proof[2])),
                str(proof[3]),
                int(str(proof[4])),
                str(proof[5]),
                str(proof[6]),
                str(proof[7]),
            )
            != expected_stage
        ):
            raise MssqlR1V3StageAttestationError("stage registry differs from its exact OPEN plan")
        expected_proof = (
            object_id,
            str(object_uuid),
            str(physical_token),
            plan.exact_stage_ddl_digest,
            plan.schema_digest,
            plan.catalog_contract_digest,
            plan.permission_contract_digest,
            plan.type_policy_digest,
        )
        try:
            coordinates = coordinate_values(proof[11:])
        except (TypeError, ValueError) as exc:
            raise MssqlR1V3StageAttestationError("stage physical proof has an invalid shape") from exc
        if coordinates != expected_proof:
            raise MssqlR1V3StageAttestationError("stage physical identity or protected contract differs")
        rows = query_all(
            handle,
            stage_scan_sql(self._authority_schema),
            (plan.artifact_id, plan.owner_epoch),
        )
        typed = tuple(self._typed_row(plan, row) for row in rows)
        keys = tuple(item.canonical_key_payload for item in typed)
        if keys != tuple(sorted(set(keys))):
            raise MssqlR1V3StageAttestationError("canonical stage keys are not unique and ordered")
        digest, payload_bytes = artifact_digest_for_rows(plan.artifact_kind, plan.schema_digest, typed)
        count = len(typed)
        evidence = R1TypedStageScanEvidenceV1(
            plan.artifact_kind,
            digest,
            count,
            payload_bytes,
            count,
            count,
            0 if plan.artifact_kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS else count,
            count,
            _scan_contract_digest(plan),
        )
        return MssqlR1V3StageScan(evidence, typed)

    def _typed_row(self, plan: R1OpenStagePlanV1, raw: tuple[object, ...]) -> R1TypedStageRowV1:
        column_count = len(plan.ordered_business_columns)
        expected = column_count + (1 if plan.artifact_kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS else 3)
        if len(raw) != expected:
            raise MssqlR1V3StageAttestationError("stage scan row has an invalid field count")
        values = raw[:column_count]
        key = bytes_value(raw[column_count])
        complete = plan.artifact_kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS
        row_payload = None if complete else bytes_value(raw[column_count + 1])
        stored_hash = None if complete else bytes_value(raw[column_count + 2])
        specs = tuple(self._codec.specification(column.logical_type_id) for column in plan.ordered_business_columns)
        columns = tuple(
            R1Column(column.target_ordinal, column.target_column, spec, column.nullable)
            for column, spec in zip(plan.ordered_business_columns, specs, strict=True)
        )
        try:
            expected_row = None if complete else canonical_row_bytes(columns, values)
            key_index = next(i for i, column in enumerate(plan.ordered_business_columns) if column.is_business_key)
            expected_key = canonical_key_payload(specs[key_index], values[key_index])
        except R1HashContractError as exc:
            raise MssqlR1V3StageAttestationError("typed stage value violates the registered type policy") from exc
        if expected_key != key:
            raise MssqlR1V3StageAttestationError("typed business key differs from canonical key")
        if expected_row != row_payload:
            raise MssqlR1V3StageAttestationError("typed business values differ from canonical row")
        if not complete and stored_hash != hashlib.sha256(cast(bytes, row_payload)).digest():
            raise MssqlR1V3StageAttestationError("stored row hash differs from canonical row")
        cells = tuple(
            R1TypedStageCellV1(
                column.target_ordinal,
                column.logical_type_id,
                R1StageCellStateV1.NULL if value is None else R1StageCellStateV1.VALUE,
                self._codec.encode(column.logical_type_id, value),
            )
            for column, value in zip(plan.ordered_business_columns, values, strict=True)
        )
        for payload, maximum, label in (
            (key, plan.maximum_key_bytes, "key"),
            (b"" if row_payload is None else row_payload, plan.maximum_row_bytes, "row"),
        ):
            if len(payload) > maximum:
                raise MssqlR1V3StageAttestationError(f"canonical {label} exceeds the sealed bound")
        return R1TypedStageRowV1(plan.artifact_kind, cells, key, row_payload, stored_hash)


def _scan_contract_digest(plan: R1OpenStagePlanV1) -> bytes:
    return hashlib.sha256(
        canonical_bytes(
            _SCAN_DOMAIN,
            (
                plan.artifact_kind,
                plan.schema_digest,
                plan.type_policy_digest,
                tuple(item.canonical_values for item in plan.ordered_business_columns),
                plan.canonical_key_payload_column,
                plan.canonical_row_payload_column,
                plan.canonical_row_hash_column,
                plan.maximum_key_bytes,
                plan.maximum_row_bytes,
            ),
        )
    ).digest()


__all__ = [
    "MssqlR1V3ScalarCodec",
    "MssqlR1V3SealedStageAttestor",
    "MssqlR1V3StageAttestationError",
    "MssqlR1V3StageScan",
]
