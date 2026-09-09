"""Target-global generation authority for MSSQL shadow publication."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from dpone.runtime.support.mssql_object_name import (
    MSSQLObjectName,
    quote_mssql_identifier,
)

MssqlPublicationTarget = MSSQLObjectName

PUBLICATION_RECEIPT_PROPERTY = "dpone_backfill_publication_receipt"
GENERATION_ID_PROPERTY = "dpone_backfill_generation_id"
XMIN_STATE_KEY_PROPERTY = "dpone_backfill_xmin_state_key_sha256"
XMIN_SEED_LOAD_PROPERTY = "dpone_backfill_xmin_seed_load_id"
XMIN_RECEIPT_PROPERTY = "dpone_backfill_xmin_receipt_id"
PUBLICATION_OBJECT_PROPERTIES = (
    GENERATION_ID_PROPERTY,
    PUBLICATION_RECEIPT_PROPERTY,
    XMIN_STATE_KEY_PROPERTY,
    XMIN_SEED_LOAD_PROPERTY,
    XMIN_RECEIPT_PROPERTY,
)


@dataclass(frozen=True, slots=True)
class MssqlPublicationGeneration:
    """One live or shadow SQL object plus its durable publication head."""

    object_id: int
    create_token: str
    publication_receipt_id: str | None
    xmin_state_key_sha256: str | None
    xmin_seed_load_id: str | None
    xmin_receipt_id: str | None

    def __post_init__(self) -> None:
        if self.object_id < 1 or not _is_uuid(self.create_token):
            raise RuntimeError("mssql_backfill_publication.generation_invalid")
        pending = (self.xmin_state_key_sha256, self.xmin_seed_load_id)
        if (pending[0] is None) != (pending[1] is None):
            raise RuntimeError("mssql_backfill_publication.generation_invalid")
        if self.publication_receipt_id is None and any((*pending, self.xmin_receipt_id)):
            raise RuntimeError("mssql_backfill_publication.generation_invalid")
        if self.xmin_receipt_id is not None and pending[0] is None:
            raise RuntimeError("mssql_backfill_publication.generation_invalid")

    @property
    def xmin_handoff_pending(self) -> bool:
        return self.xmin_state_key_sha256 is not None and self.xmin_receipt_id is None


def read_publication_generation(
    connector: Any,
    target: MssqlPublicationTarget,
) -> MssqlPublicationGeneration:
    """Read the exact object id and publication-head properties for one table."""

    rows = _read_generation_rows(connector, target)
    if len(rows) != 1:
        raise RuntimeError(f"mssql_backfill_publication.table_missing:{target.dataset}")
    return _generation_from_row(rows[0])


def ensure_publication_generation_token(
    connector: Any,
    target: MssqlPublicationTarget,
) -> MssqlPublicationGeneration:
    """Bind and return a UUID generation while the caller holds publication lock."""

    rows = _read_generation_rows(connector, target)
    if len(rows) != 1:
        raise RuntimeError(f"mssql_backfill_publication.table_missing:{target.dataset}")
    row = rows[0]
    object_id = int(row.get("object_id") or 0)
    token = _optional_text(row.get("create_token"))
    if token is not None:
        return _generation_from_row(row)
    token = str(uuid.uuid4())
    _add_property(connector, target, GENERATION_ID_PROPERTY, token)
    rebound = _read_generation_rows(connector, target)
    if len(rebound) != 1 or int(rebound[0].get("object_id") or 0) != object_id:
        raise RuntimeError("mssql_backfill_publication.generation_bind_changed")
    generation = _generation_from_row(rebound[0])
    if generation.create_token != token:
        raise RuntimeError("mssql_backfill_publication.generation_bind_changed")
    return generation


def _read_generation_rows(
    connector: Any,
    target: MssqlPublicationTarget,
) -> list[dict[str, Any]]:
    prefix = f"{quote_mssql_identifier(target.database)}." if target.database else ""
    rows = connector.get_records(
        "SELECT t.object_id, "
        "MAX(CASE WHEN ep.name=? THEN CONVERT(nvarchar(36), ep.value) END) AS create_token, "
        "MAX(CASE WHEN ep.name=? THEN CONVERT(nvarchar(256), ep.value) END) AS publication_receipt_id, "
        "MAX(CASE WHEN ep.name=? THEN CONVERT(nvarchar(256), ep.value) END) AS xmin_state_key_sha256, "
        "MAX(CASE WHEN ep.name=? THEN CONVERT(nvarchar(256), ep.value) END) AS xmin_seed_load_id, "
        "MAX(CASE WHEN ep.name=? THEN CONVERT(nvarchar(256), ep.value) END) AS xmin_receipt_id "
        f"FROM {prefix}sys.tables AS t "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id=t.schema_id "
        f"LEFT JOIN {prefix}sys.extended_properties AS ep "
        "ON ep.class=1 AND ep.major_id=t.object_id AND ep.minor_id=0 "
        "WHERE s.name=? AND t.name=? GROUP BY t.object_id",
        (
            GENERATION_ID_PROPERTY,
            PUBLICATION_RECEIPT_PROPERTY,
            XMIN_STATE_KEY_PROPERTY,
            XMIN_SEED_LOAD_PROPERTY,
            XMIN_RECEIPT_PROPERTY,
            target.schema,
            target.table,
        ),
        as_dict=True,
    )
    return list(rows or ())


def _generation_from_row(row: dict[str, Any]) -> MssqlPublicationGeneration:
    return MssqlPublicationGeneration(
        object_id=int(row.get("object_id") or 0),
        create_token=str(row.get("create_token") or ""),
        publication_receipt_id=_optional_text(row.get("publication_receipt_id")),
        xmin_state_key_sha256=_optional_text(row.get("xmin_state_key_sha256")),
        xmin_seed_load_id=_optional_text(row.get("xmin_seed_load_id")),
        xmin_receipt_id=_optional_text(row.get("xmin_receipt_id")),
    )


def acquire_publication_lock(
    connector: Any,
    target: MssqlPublicationTarget,
    *,
    phase: str,
    timeout_ms: int = 300_000,
) -> None:
    """Acquire the target-wide transaction lock shared by cutover and handoff."""

    if not phase:
        raise ValueError("publication lock phase is required for diagnostics")
    procedure = f"{target.execution_scope_prefix}sys.sp_getapplock"
    rows = connector.get_records(
        f"DECLARE @result int; EXEC @result = {procedure} @Resource=?, @LockMode='Exclusive', "
        "@LockOwner='Transaction', @LockTimeout=?; SELECT @result AS lock_result",
        (_publication_lock_resource(connector, target), timeout_ms),
        as_dict=True,
    )
    if len(rows or ()) != 1 or int(rows[0].get("lock_result", -999)) < 0:
        raise RuntimeError("mssql_backfill_publication.lock_unavailable")


def bind_publication_generation_tokens(
    connector: Any,
    authority: MssqlPublicationTarget,
    *targets: MssqlPublicationTarget,
) -> None:
    """Bind missing UUID generations under the target-global lock."""

    try:
        for target in targets:
            read_publication_generation(connector, target)
    except RuntimeError as exc:
        if str(exc) != "mssql_backfill_publication.generation_invalid":
            raise
    else:
        return
    commit_attempted = False
    connector.begin()
    try:
        connector.execute_query("SET XACT_ABORT ON")
        connector.execute_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        acquire_publication_lock(connector, authority, phase="generation-bind")
        for target in targets:
            ensure_publication_generation_token(connector, target)
        commit_attempted = True
        connector.commit_transaction()
    except BaseException as exc:
        if not commit_attempted:
            connector.rollback()
            raise
        closer = getattr(connector, "close", None)
        if callable(closer):
            closer()
        raise RuntimeError("mssql_backfill_publication.generation_bind_commit_outcome_unknown") from exc


def publish_generation_head(
    connector: Any,
    target: MssqlPublicationTarget,
    *,
    publication_receipt_id: str,
    xmin_state_key_sha256: str | None,
    xmin_seed_load_id: str | None,
    xmin_receipt_id: str | None = None,
) -> None:
    """Create the immutable live head, including a pending XMin authority."""

    _add_property(connector, target, PUBLICATION_RECEIPT_PROPERTY, publication_receipt_id)
    if xmin_state_key_sha256 is None and xmin_seed_load_id is None:
        if xmin_receipt_id is not None:
            raise RuntimeError("mssql_backfill_publication.xmin_authority_incomplete")
        return
    if not xmin_state_key_sha256 or not xmin_seed_load_id:
        raise RuntimeError("mssql_backfill_publication.xmin_authority_incomplete")
    _add_property(connector, target, XMIN_STATE_KEY_PROPERTY, xmin_state_key_sha256)
    _add_property(connector, target, XMIN_SEED_LOAD_PROPERTY, xmin_seed_load_id)
    if xmin_receipt_id is not None:
        _add_property(connector, target, XMIN_RECEIPT_PROPERTY, xmin_receipt_id)


def require_xmin_publication_head(
    connector: Any,
    target: MssqlPublicationTarget,
    *,
    publication_receipt_id: str,
    state_key_sha256: str,
    seed_load_id: str,
    xmin_receipt_id: str | None,
) -> MssqlPublicationGeneration:
    """Prove that the live relation still owns this exact handoff."""

    generation = read_publication_generation(connector, target)
    exact = (
        generation.publication_receipt_id == publication_receipt_id
        and generation.xmin_state_key_sha256 == state_key_sha256
        and generation.xmin_seed_load_id == seed_load_id
    )
    if not exact:
        raise RuntimeError("postgres_xmin_handoff.publication_authority_changed")
    if xmin_receipt_id is None:
        if generation.xmin_receipt_id is not None:
            raise RuntimeError("postgres_xmin_handoff.seed_state_conflict")
    elif generation.xmin_receipt_id != xmin_receipt_id:
        raise RuntimeError("postgres_xmin_handoff.seed_state_conflict")
    return generation


def require_recoverable_xmin_publication_head(
    connector: Any,
    target: MssqlPublicationTarget,
    *,
    publication_receipt_id: str,
    state_key_sha256: str,
    seed_load_id: str,
    xmin_receipt_id: str,
) -> MssqlPublicationGeneration:
    """Accept only the exact legacy pending head or its already-closed form."""

    generation = read_publication_generation(connector, target)
    exact_authority = (
        generation.publication_receipt_id == publication_receipt_id
        and generation.xmin_state_key_sha256 == state_key_sha256
        and generation.xmin_seed_load_id == seed_load_id
    )
    if not exact_authority:
        raise RuntimeError("postgres_xmin_handoff.publication_authority_changed")
    if generation.xmin_receipt_id not in (None, xmin_receipt_id):
        raise RuntimeError("postgres_xmin_handoff.seed_state_conflict")
    return generation


def complete_xmin_publication_head(
    connector: Any,
    target: MssqlPublicationTarget,
    *,
    publication_receipt_id: str,
    state_key_sha256: str,
    seed_load_id: str,
    xmin_receipt_id: str,
) -> None:
    """Close a pending live head in the same transaction as checkpoint CAS."""

    require_xmin_publication_head(
        connector,
        target,
        publication_receipt_id=publication_receipt_id,
        state_key_sha256=state_key_sha256,
        seed_load_id=seed_load_id,
        xmin_receipt_id=None,
    )
    _add_property(connector, target, XMIN_RECEIPT_PROPERTY, xmin_receipt_id)


def _add_property(
    connector: Any,
    target: MssqlPublicationTarget,
    name: str,
    value: str,
) -> None:
    connector.execute_query(
        f"EXEC {target.execution_scope_prefix}sys.sp_addextendedproperty "
        "@name=?, @value=?, @level0type=N'SCHEMA', @level0name=?, "
        "@level1type=N'TABLE', @level1name=?",
        (name, value, target.schema, target.table),
    )


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _is_uuid(value: Any) -> bool:
    try:
        parsed = uuid.UUID(str(value or ""))
    except (AttributeError, ValueError):
        return False
    return str(parsed) == str(value).lower()


def _publication_lock_resource(connector: Any, target: MssqlPublicationTarget) -> str:
    prefix = f"{quote_mssql_identifier(target.database)}." if target.database else ""
    rows = connector.get_records(
        "SELECT CONVERT(nvarchar(36), r.binding_id) AS binding_id "
        f"FROM {prefix}[dbo].[dpone_target_identity] AS r "
        "WHERE r.schema_name=CONVERT(nvarchar(128), ?) "
        "AND r.table_name=CONVERT(nvarchar(128), ?)",
        (target.schema, target.table),
        as_dict=True,
    )
    if len(rows or ()) != 1:
        raise RuntimeError("mssql_backfill_publication.target_binding_unavailable")
    try:
        binding_id = uuid.UUID(str(rows[0].get("binding_id") or ""))
    except (AttributeError, ValueError) as exc:
        raise RuntimeError("mssql_backfill_publication.target_binding_unavailable") from exc
    return f"dpone:backfill-publication:{binding_id}"


__all__ = [
    "MssqlPublicationGeneration",
    "GENERATION_ID_PROPERTY",
    "PUBLICATION_RECEIPT_PROPERTY",
    "PUBLICATION_OBJECT_PROPERTIES",
    "XMIN_RECEIPT_PROPERTY",
    "XMIN_SEED_LOAD_PROPERTY",
    "XMIN_STATE_KEY_PROPERTY",
    "acquire_publication_lock",
    "bind_publication_generation_tokens",
    "complete_xmin_publication_head",
    "ensure_publication_generation_token",
    "publish_generation_head",
    "read_publication_generation",
    "require_recoverable_xmin_publication_head",
    "require_xmin_publication_head",
]
