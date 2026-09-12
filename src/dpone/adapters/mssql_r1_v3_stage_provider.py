"""Crash-safe OPEN/load/seal provider for MSSQL R1 V3 staging."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from dpone.contracts.mssql_r1_v3_stage_evidence import artifact_digest_for_rows
from dpone.contracts.mssql_r1_v3_staging import (
    R1SealedStageManifestV1,
    require_identifier,
)

from .mssql_r1_v3_open_recovery import (
    MssqlR1V3OpenStageRecoveryAdapter,
    MssqlR1V3OpenStageRecoveryBackend,
    bind_open_stage_recovery,
    recovery_artifact_ids_are_disjoint,
)
from .mssql_r1_v3_stage_attestor import MssqlR1V3ScalarCodec, MssqlR1V3SealedStageAttestor
from .mssql_r1_v3_stage_observation import (
    bytes_value,
    chunk_admission_sql,
    chunk_complete_sql,
    chunk_observation_sql,
    column_temp_table_sql,
    execute,
    fresh_query,
    matches_completed_chunk_observation,
    matches_open_stage_observation,
    probe_sealed_stage_fresh,
    query_one,
    rollback_quietly,
    stage_insert_sql,
    stage_observation_sql,
)
from .mssql_r1_v3_stage_schema import render_stage_object_plan, stage_sql_type

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_control import MssqlOpenStageRecoveryReceiptV1
    from dpone.contracts.mssql_r1_v3_control_proof import MssqlOpenStageRecoveryFreshProofV1
    from dpone.contracts.mssql_r1_v3_stage_evidence import R1BoundedStageChunkV1
    from dpone.contracts.mssql_r1_v3_staging import R1OpenStagePlanV1
    from dpone.ports.mssql_target_authority_v2 import MssqlTransactionSessionFactoryPort


class MssqlR1V3StageProviderError(RuntimeError):
    """The exact stage lifecycle cannot be proved."""


class MssqlR1V3StageProvider:
    """Own target sessions for register, bounded load, renew and seal actions."""

    def __init__(
        self,
        session_factory: MssqlTransactionSessionFactoryPort | object,
        authority_schema: str = "dpone_authority",
        stage_schema: str = "dpone_stage",
        *,
        attestor: MssqlR1V3SealedStageAttestor | None = None,
        scalar_codec: MssqlR1V3ScalarCodec | None = None,
        recovery: MssqlR1V3OpenStageRecoveryAdapter | None = None,
        recovery_backend: MssqlR1V3OpenStageRecoveryBackend | None = None,
    ) -> None:
        self._sessions = session_factory
        self._authority_schema = require_identifier(authority_schema, "authority_schema")
        self._stage_schema = require_identifier(stage_schema, "stage_schema")
        self._codec = scalar_codec or MssqlR1V3ScalarCodec()
        self._attestor = attestor or MssqlR1V3SealedStageAttestor(authority_schema, scalar_codec=self._codec)
        self._recovery = bind_open_stage_recovery(recovery, recovery_backend, self._stage_schema)

    def register_open(self, plan: R1OpenStagePlanV1) -> None:
        self._require_exact_ddl(plan)
        try:
            self._register_open_once(plan)
        except Exception as exc:
            if self._probe_open_fresh(plan):
                return
            if isinstance(exc, MssqlR1V3StageProviderError):
                raise
            raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_OPEN_STAGE_OUTCOME_UNKNOWN") from exc

    def write(self, plan: R1OpenStagePlanV1, chunk: R1BoundedStageChunkV1) -> None:
        self._require_exact_ddl(plan)
        if (chunk.artifact_kind, chunk.ordered_business_columns) != (
            plan.artifact_kind,
            plan.ordered_business_columns,
        ):
            raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_STAGE_CHUNK_AUTHORITY_MISMATCH")
        if not self._renew_once(plan.artifact_id, plan.owner_epoch):
            raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_STAGE_LEASE_LOST")
        self._write_once(plan, chunk)

    def renew(self, artifact_id: UUID, owner_epoch: int) -> bool:
        return self._renew_once(artifact_id, owner_epoch)

    def seal(self, plan: R1OpenStagePlanV1) -> R1SealedStageManifestV1:
        self._require_exact_ddl(plan)
        try:
            return self._seal_once(plan)
        except Exception as exc:
            sealed = self._probe_sealed_fresh(plan)
            if sealed is not None:
                return sealed
            if isinstance(exc, MssqlR1V3StageProviderError):
                raise
            raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_STAGE_SEAL_OUTCOME_UNKNOWN") from exc

    def recover_expired_open(
        self,
        operation_key: bytes,
        effect_key: bytes,
        old_artifact_set_digest: bytes,
    ) -> MssqlOpenStageRecoveryReceiptV1:
        if self._recovery is None:
            raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_OPEN_RECOVERY_BACKEND_REQUIRED")
        receipt = self._recovery.recover_expired_open(operation_key, effect_key, old_artifact_set_digest)
        self._require_disjoint_recovery_receipt(receipt)
        return receipt

    def probe_recovery_fresh(self, recovery_effect_key: bytes) -> MssqlOpenStageRecoveryFreshProofV1 | None:
        if self._recovery is None:
            raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_OPEN_RECOVERY_BACKEND_REQUIRED")
        proof = self._recovery.probe_recovery_fresh(recovery_effect_key)
        if proof is not None:
            self._require_disjoint_recovery_receipt(proof.receipt)
        return proof

    def _register_open_once(self, plan: R1OpenStagePlanV1) -> None:
        rendered = render_stage_object_plan(plan, self._stage_schema)
        session = self._open_session()
        handle: object | None = None
        dispatched = False
        try:
            handle = session.begin()
            execute(handle, column_temp_table_sql())
            for column in plan.ordered_business_columns:
                execute(
                    handle,
                    "INSERT INTO #dpone_stage_columns_v3 VALUES (?,?,?,?,?,?,?,?);",
                    (
                        column.source_ordinal,
                        column.stage_column,
                        column.target_ordinal,
                        column.target_column,
                        column.logical_type_id,
                        stage_sql_type(column.logical_type_id),
                        column.nullable,
                        column.is_business_key,
                    ),
                )
            row = query_one(
                handle,
                f"EXEC [{self._authority_schema}].[dpone_open_stage_v3] " + ",".join("?" for _ in range(19)) + ";",
                (
                    plan.artifact_id,
                    plan.effect_key,
                    plan.target_binding_uuid,
                    plan.owner_epoch,
                    plan.artifact_kind.value,
                    plan.canonical_bytes,
                    plan.digest,
                    plan.exact_stage_ddl_digest,
                    rendered.object_name,
                    plan.server_lease_seconds,
                    plan.canonical_key_payload_column,
                    plan.canonical_row_payload_column,
                    plan.canonical_row_hash_column,
                    plan.maximum_key_bytes,
                    plan.maximum_row_bytes,
                    plan.schema_digest,
                    plan.catalog_contract_digest,
                    plan.permission_contract_digest,
                    plan.type_policy_digest,
                ),
            )
            if row is None or len(row) != 5:
                raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_OPEN_STAGE_READBACK_FAILED")
            if (str(row[3]), str(row[4])) != (rendered.schema_name, rendered.object_name):
                raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_OPEN_STAGE_IDENTITY_CONFLICT")
            observed = query_one(handle, stage_observation_sql(self._authority_schema), (plan.artifact_id,))
            if not matches_open_stage_observation(observed, plan, rendered.schema_name, rendered.object_name):
                raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_OPEN_STAGE_READBACK_FAILED")
            dispatched = True
            session.commit(handle)
        except Exception:
            if handle is not None and not dispatched:
                rollback_quietly(session, handle)
            raise
        finally:
            session.close()

    def _write_once(self, plan: R1OpenStagePlanV1, chunk: R1BoundedStageChunkV1) -> None:
        chunk_digest, _payload_bytes = artifact_digest_for_rows(plan.artifact_kind, plan.schema_digest, chunk.rows)
        session = self._open_session()
        handle: object | None = None
        dispatched = False
        try:
            handle = session.begin()
            row = query_one(
                handle,
                chunk_admission_sql(self._authority_schema),
                (plan.artifact_id, plan.owner_epoch, chunk.chunk_sequence, chunk_digest, len(chunk.rows)),
            )
            if row is None:
                raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_STAGE_CHUNK_BLOCKED")
            if bool(row[0]):
                rendered = render_stage_object_plan(plan, self._stage_schema)
                insert_sql = stage_insert_sql(plan, rendered.schema_name, rendered.object_name)
                for typed_row in chunk.rows:
                    values = tuple(
                        self._codec.decode(
                            cell.logical_type_id,
                            cell.canonical_scalar_bytes,
                            is_null=cell.value_state.value == "null",
                        )
                        for cell in typed_row.cells
                    )
                    values += (typed_row.canonical_key_payload,)
                    if typed_row.canonical_row_payload is not None:
                        values += (typed_row.canonical_row_payload, typed_row.canonical_row_hash)
                    execute(handle, insert_sql, values)
                execute(
                    handle,
                    chunk_complete_sql(self._authority_schema),
                    (plan.artifact_id, plan.owner_epoch, chunk.chunk_sequence, chunk_digest),
                )
            dispatched = True
            session.commit(handle)
        except Exception as exc:
            if handle is not None and not dispatched:
                rollback_quietly(session, handle)
            if dispatched:
                if self._probe_chunk_fresh(plan.artifact_id, chunk.chunk_sequence, chunk_digest):
                    return
                raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_STAGE_CHUNK_OUTCOME_UNKNOWN") from exc
            if isinstance(exc, MssqlR1V3StageProviderError):
                raise
            raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_STAGE_CHUNK_FAILED") from exc
        finally:
            session.close()

    def _renew_once(self, artifact_id: UUID, owner_epoch: int) -> bool:
        session = self._open_session()
        handle: object | None = None
        try:
            handle = session.begin()
            row = query_one(
                handle,
                f"EXEC [{self._authority_schema}].[dpone_renew_stage_v3] ?,?;",
                (artifact_id, owner_epoch),
            )
            renewed = row is not None and bool(row[0])
            session.commit(handle)
            return renewed
        except Exception:
            if handle is not None:
                rollback_quietly(session, handle)
            return False
        finally:
            session.close()

    def _seal_once(self, plan: R1OpenStagePlanV1) -> R1SealedStageManifestV1:
        session = self._open_session()
        handle: object | None = None
        dispatched = False
        try:
            handle = session.begin()
            row = query_one(handle, stage_observation_sql(self._authority_schema), (plan.artifact_id,))
            if (
                row is None
                or len(row) != 19
                or bytes_value(row[0]) != plan.canonical_bytes
                or str(row[1]) != "OPEN"
                or int(str(row[2])) != plan.owner_epoch
            ):
                raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_OPEN_STAGE_AUTHORITY_CONFLICT")
            object_uuid, object_id, token, schema_name, object_name = (
                UUID(str(row[3])),
                int(str(row[4])),
                UUID(str(row[5])),
                str(row[6]),
                str(row[7]),
            )
            rendered = render_stage_object_plan(plan, self._stage_schema)
            if (schema_name, object_name) != (rendered.schema_name, rendered.object_name):
                raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_OPEN_STAGE_IDENTITY_CONFLICT")
            scan = self._attestor.scan_open(
                handle,
                plan,
                object_uuid,
                object_id,
                token,
                schema_name,
                object_name,
            )
            evidence = scan.evidence
            manifest = R1SealedStageManifestV1(
                plan.artifact_id,
                plan.artifact_kind,
                plan.target_binding_uuid,
                plan.effect_key,
                plan.schema_digest,
                plan.catalog_contract_digest,
                plan.permission_contract_digest,
                plan.type_policy_digest,
                object_uuid,
                object_id,
                token,
                schema_name,
                object_name,
                plan.ordered_business_columns,
                plan.canonical_key_payload_column,
                plan.canonical_row_payload_column,
                plan.canonical_row_hash_column,
                plan.maximum_key_bytes,
                plan.maximum_row_bytes,
                evidence.observed_row_count,
                evidence.observed_payload_bytes,
                evidence.artifact_digest,
                evidence,
                None if plan.canonical_row_hash_column is None else "sha256_canonical_row_v1",
                plan.digest,
                plan.exact_stage_ddl_digest,
            )
            execute(
                handle,
                f"EXEC [{self._authority_schema}].[dpone_seal_stage_v3] ?,?,?,?;",
                (plan.artifact_id, plan.owner_epoch, manifest.canonical_bytes, manifest.manifest_digest),
            )
            persisted = query_one(handle, stage_observation_sql(self._authority_schema), (plan.artifact_id,))
            if (
                persisted is None
                or len(persisted) != 19
                or str(persisted[1]) != "SEALED"
                or bytes_value(persisted[8]) != manifest.canonical_bytes
                or bytes_value(persisted[9]) != manifest.manifest_digest
            ):
                raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_STAGE_SEAL_READBACK_FAILED")
            dispatched = True
            session.commit(handle)
            return manifest
        except Exception:
            if handle is not None and not dispatched:
                rollback_quietly(session, handle)
            raise
        finally:
            session.close()

    def _probe_open_fresh(self, plan: R1OpenStagePlanV1) -> bool:
        row = fresh_query(self._open_session, stage_observation_sql(self._authority_schema), (plan.artifact_id,))
        rendered = render_stage_object_plan(plan, self._stage_schema)
        return matches_open_stage_observation(row, plan, rendered.schema_name, rendered.object_name)

    def _probe_sealed_fresh(self, plan: R1OpenStagePlanV1) -> R1SealedStageManifestV1 | None:
        return probe_sealed_stage_fresh(self._open_session, self._authority_schema, plan, self._attestor)

    def _probe_chunk_fresh(self, artifact_id: UUID, sequence: int, digest: bytes) -> bool:
        row = fresh_query(self._open_session, chunk_observation_sql(self._authority_schema), (artifact_id, sequence))
        return matches_completed_chunk_observation(row, digest)

    def _require_exact_ddl(self, plan: R1OpenStagePlanV1) -> None:
        if render_stage_object_plan(plan, self._stage_schema).ddl_digest != plan.exact_stage_ddl_digest:
            raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_STAGE_DDL_AUTHORITY_MISMATCH")

    @staticmethod
    def _require_disjoint_recovery_receipt(receipt: MssqlOpenStageRecoveryReceiptV1) -> None:
        if not recovery_artifact_ids_are_disjoint(receipt):
            raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_OPEN_RECOVERY_ARTIFACT_ID_CONFLICT")

    def _open_session(self) -> Any:
        opener = getattr(self._sessions, "open", None)
        if not callable(opener):
            raise MssqlR1V3StageProviderError("DPONE_POSTGRES_MSSQL_FRESH_SESSION_REQUIRED")
        return opener()


__all__ = ["MssqlR1V3StageProvider", "MssqlR1V3StageProviderError"]
