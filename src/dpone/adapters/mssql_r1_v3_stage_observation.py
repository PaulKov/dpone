"""Shared DB-API observation boundary for MSSQL R1 V3 staging adapters."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, cast
from uuid import UUID

from dpone.contracts.mssql_r1_v3_staging import R1SealedStageManifestV1

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_staging import R1OpenStagePlanV1


class SealedStageAttestor(Protocol):
    def attest(self, transaction: object, manifest: R1SealedStageManifestV1) -> object: ...


def column_temp_table_sql() -> str:
    return """CREATE TABLE #dpone_stage_columns_v3(source_ordinal int,stage_column sysname,target_ordinal int,
target_column sysname,logical_type_id varchar(128),target_sql_type varchar(64),nullable bit,is_business_key bit);"""


def chunk_admission_sql(schema: str) -> str:
    return f"EXEC [{schema}].[dpone_begin_stage_chunk_v3] ?,?,?,?,?;"


def chunk_complete_sql(schema: str) -> str:
    return f"EXEC [{schema}].[dpone_complete_stage_chunk_v3] ?,?,?,?;"


def stage_observation_sql(schema: str) -> str:
    return f"EXEC [{schema}].[dpone_observe_stage_v3] ?;"


def chunk_observation_sql(schema: str) -> str:
    return f"EXEC [{schema}].[dpone_observe_stage_chunk_v3] ?,?;"


def stage_scan_sql(schema: str) -> str:
    return f"EXEC [{schema}].[dpone_scan_stage_v3] ?,?;"


def stage_insert_sql(plan: R1OpenStagePlanV1, schema_name: str, object_name: str) -> str:
    columns = tuple(item.stage_column for item in plan.ordered_business_columns) + (plan.canonical_key_payload_column,)
    if plan.canonical_row_payload_column is not None:
        columns += (plan.canonical_row_payload_column, cast(str, plan.canonical_row_hash_column))
    return (
        f"INSERT INTO [{schema_name}].[{object_name}] "
        f"({','.join(f'[{item}]' for item in columns)}) VALUES ({','.join('?' for _ in columns)});"
    )


def coordinate_values(row: tuple[object, ...]) -> tuple[object, ...]:
    if len(row) != 8:
        raise ValueError("stage physical proof has an invalid shape")
    return (
        int(str(row[0])),
        str(row[1]),
        str(row[2]),
        *(bytes_value(value) for value in row[3:8]),
    )


def matches_open_stage_observation(
    row: tuple[object, ...] | None,
    plan: R1OpenStagePlanV1,
    schema_name: str,
    object_name: str,
) -> bool:
    """Validate fresh OPEN registry, physical properties and plan authority."""

    try:
        if row is None or len(row) != 19:
            return False
        object_uuid, object_id, token = UUID(str(row[3])), int(str(row[4])), UUID(str(row[5]))
        physical = (int(str(row[11])), UUID(str(row[12])), UUID(str(row[13])))
        observed = (bytes_value(row[0]), str(row[1]), int(str(row[2])), str(row[6]), str(row[7]))
        digests = tuple(bytes_value(value) for value in row[14:19])
    except (TypeError, ValueError):
        return False
    return (
        object_id > 0
        and (object_id, object_uuid, token) == physical
        and observed == (plan.canonical_bytes, "OPEN", plan.owner_epoch, schema_name, object_name)
        and digests
        == (
            plan.exact_stage_ddl_digest,
            plan.schema_digest,
            plan.catalog_contract_digest,
            plan.permission_contract_digest,
            plan.type_policy_digest,
        )
    )


def matches_completed_chunk_observation(row: tuple[object, ...] | None, digest: bytes) -> bool:
    try:
        return row is not None and len(row) == 2 and (bytes_value(row[0]), str(row[1])) == (digest, "COMPLETE")
    except (TypeError, ValueError):
        return False


def transaction_handle(transaction: object) -> object:
    return getattr(transaction, "handle", transaction)


def execute(handle: object, sql: str, parameters: tuple[object, ...] = ()) -> object:
    return cast(Any, handle).execute(sql, parameters)


def query_one(handle: object, sql: str, parameters: tuple[object, ...]) -> tuple[object, ...] | None:
    result = execute(handle, sql, parameters)
    row = cast(Any, result if hasattr(result, "fetchone") else handle).fetchone()
    return None if row is None else tuple(row)


def query_all(handle: object, sql: str, parameters: tuple[object, ...]) -> tuple[tuple[object, ...], ...]:
    result = execute(handle, sql, parameters)
    return tuple(tuple(row) for row in cast(Any, result if hasattr(result, "fetchall") else handle).fetchall())


def bytes_value(value: object) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError("SQL binary value is required")


def rollback_quietly(session: Any, handle: object) -> None:
    try:
        session.rollback(handle)
    except Exception:
        pass


def fresh_query(
    open_session: Callable[[], Any],
    sql: str,
    parameters: tuple[object, ...],
) -> tuple[object, ...] | None:
    """Read authority through a new transaction, returning no proof on read failure."""

    session = open_session()
    handle: object | None = None
    try:
        handle = session.begin()
        row = query_one(handle, sql, parameters)
        session.commit(handle)
        return row
    except Exception:
        if handle is not None:
            rollback_quietly(session, handle)
        return None
    finally:
        session.close()


def probe_sealed_stage_fresh(
    open_session: Callable[[], Any],
    authority_schema: str,
    plan: R1OpenStagePlanV1,
    attestor: SealedStageAttestor,
) -> R1SealedStageManifestV1 | None:
    """Accept a sealed result only after fresh physical and typed attestation."""

    session = open_session()
    handle: object | None = None
    try:
        handle = session.begin()
        row = query_one(handle, stage_observation_sql(authority_schema), (plan.artifact_id,))
        if row is None or len(row) != 19 or str(row[1]) != "SEALED":
            raise ValueError("sealed stage authority is unavailable")
        manifest = R1SealedStageManifestV1.from_canonical_bytes(bytes_value(row[8]))
        if bytes_value(row[9]) != manifest.manifest_digest or not manifest.matches_open_plan(plan):
            raise ValueError("sealed stage authority differs from plan")
        attestor.attest(handle, manifest)
        session.commit(handle)
        return manifest
    except Exception:
        if handle is not None:
            rollback_quietly(session, handle)
        return None
    finally:
        session.close()


__all__ = [
    "bytes_value",
    "chunk_admission_sql",
    "chunk_complete_sql",
    "chunk_observation_sql",
    "column_temp_table_sql",
    "coordinate_values",
    "execute",
    "fresh_query",
    "matches_completed_chunk_observation",
    "matches_open_stage_observation",
    "query_all",
    "query_one",
    "probe_sealed_stage_fresh",
    "rollback_quietly",
    "stage_observation_sql",
    "stage_insert_sql",
    "stage_scan_sql",
    "transaction_handle",
]
