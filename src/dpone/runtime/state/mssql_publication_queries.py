"""Bound SQL batches for one indexed authority slot plus its immutable events."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any
from uuid import UUID

from dpone.adapters.mssql_publication_catalog_ddl import EVENT_TABLE, SLOT_TABLE, quote_identifier
from dpone.ports.mssql_publication import (
    PublicationAuthorityBinding,
    native_publication_provenance,
    publication_slot_key,
)

if TYPE_CHECKING:
    from dpone.ports.clickhouse_cluster_publication import contracts as c

_COLUMNS = "slot_key,binding_digest,revision,operation_id,phase,payload,payload_sha256,write_id"
_MATCH = (
    "e.binding_digest=s.binding_digest AND e.operation_id=s.operation_id AND e.phase=s.phase "
    "AND e.payload=s.payload AND DATALENGTH(e.payload)=DATALENGTH(s.payload) "
    "AND e.payload_sha256=s.payload_sha256 AND e.write_id=s.write_id"
)
_CHAIN = "((s.revision=1 AND e.previous_sha256 IS NULL) OR (s.revision>1 AND p.payload_sha256=e.previous_sha256))"


def read_statement(binding: PublicationAuthorityBinding, *, won: str = "0", include_root: bool = False) -> str:
    """Read current/event/predecessor in one statement; caller validates flags."""
    slot, events = publication_tables(binding)
    root_columns = (
        ",0,r.revision,r.payload,r.payload_sha256,r.write_id,r.binding_digest,"
        "CASE WHEN r.revision=1 THEN 1 ELSE 0 END,"
        "CASE WHEN r.previous_sha256 IS NULL THEN 1 ELSE 0 END,"
        "r.origin,r.provenance,r.provenance_sha256,r.operation_id,r.phase"
        if include_root
        else ""
    )
    root_join = (
        f"LEFT JOIN {events} r WITH (HOLDLOCK) ON r.slot_key=s.slot_key AND r.revision=1 " if include_root else ""
    )
    return (
        f"SELECT {won},s.revision,s.payload,s.payload_sha256,s.write_id,s.binding_digest,"
        f"CASE WHEN {_MATCH} THEN 1 ELSE 0 END,CASE WHEN {_CHAIN} THEN 1 ELSE 0 END,"
        f"e.origin,e.provenance,e.provenance_sha256,s.operation_id,s.phase{root_columns} "
        f"FROM {slot} s WITH (HOLDLOCK) LEFT JOIN {events} e WITH (HOLDLOCK) "
        "ON e.slot_key=s.slot_key AND e.revision=s.revision "
        f"LEFT JOIN {events} p WITH (HOLDLOCK) ON p.slot_key=s.slot_key AND p.revision=s.revision-1 "
        f"{root_join}"
        "WHERE s.slot_key=@slot;"
    )


def operation_read_statement(binding: PublicationAuthorityBinding) -> str:
    """Bounded immutable-root read; retained orphan history is never absence."""
    slot, events = publication_tables(binding)
    return (
        "DECLARE @slot char(64)=?;\n"
        f"IF NOT EXISTS (SELECT 1 FROM {slot} WITH (HOLDLOCK) WHERE slot_key=@slot) "
        f"AND EXISTS (SELECT 1 FROM {events} WITH (HOLDLOCK) WHERE slot_key=@slot) "
        "THROW 51075, 'publication history without current slot', 1;\n" + read_statement(binding, include_root=True)
    )


def mutation_statement(binding: PublicationAuthorityBinding) -> str:
    """A serializable key-range lock handles absent-create and exact CAS alike.

    Commit is owned by the DBAPI transaction boundary, not an OUTPUT row. Event
    uniqueness and its append-only trigger are required by catalog admission.
    """
    slot, events = publication_tables(binding)
    return f"""
DECLARE @slot char(64)=?, @binding binary(32)=?, @expected_revision bigint=?,
 @expected_payload varbinary(max)=?, @expected_hash binary(32)=?,
 @operation nvarchar(128)=?, @phase varchar(32)=?, @payload varbinary(max)=?,
 @hash binary(32)=?, @write uniqueidentifier=?, @provenance varbinary(max)=?, @provenance_hash binary(32)=?;
DECLARE @actual_revision bigint, @actual_payload varbinary(max), @actual_hash binary(32),
 @actual_binding binary(32), @won bit=0;
SELECT @actual_revision=revision,@actual_payload=payload,@actual_hash=payload_sha256,@actual_binding=binding_digest
 FROM {slot} WITH (UPDLOCK,HOLDLOCK) WHERE slot_key=@slot;
IF @actual_revision IS NULL AND EXISTS (SELECT 1 FROM {events} WITH (HOLDLOCK) WHERE slot_key=@slot)
 THROW 51075, 'publication history without current slot', 1;
IF @actual_revision IS NOT NULL AND NOT EXISTS (
 SELECT 1 FROM {events} WITH (HOLDLOCK) WHERE slot_key=@slot AND revision=1
 AND binding_digest=@binding AND previous_sha256 IS NULL)
 THROW 51076, 'publication root history differs', 1;
IF EXISTS (SELECT 1 FROM {events} WITH (HOLDLOCK) WHERE slot_key=@slot AND revision=1
 AND origin='legacy_retired' AND operation_id=@operation)
 THROW 51077, 'retired publication operation cannot be reused', 1;
IF @actual_revision IS NOT NULL AND NOT EXISTS (
 SELECT 1 FROM {slot} s JOIN {events} e WITH (HOLDLOCK) ON e.slot_key=s.slot_key AND e.revision=s.revision
 LEFT JOIN {events} p WITH (HOLDLOCK) ON p.slot_key=s.slot_key AND p.revision=s.revision-1
 WHERE s.slot_key=@slot AND {_MATCH} AND {_CHAIN}
 AND ((e.origin='native' AND e.provenance=@provenance
 AND DATALENGTH(e.provenance)=DATALENGTH(@provenance) AND e.provenance_sha256=@provenance_hash)
 OR (s.revision=1 AND e.origin='legacy_retired' AND e.phase='RETIRED_UNPUBLISHED'
 AND @expected_revision=1 AND @phase='PREPARED' AND e.operation_id<>@operation)))
 THROW 51072, 'publication history differs', 1;
IF (@expected_revision IS NULL AND @actual_revision IS NULL) OR
 (@expected_revision=@actual_revision AND @expected_payload=@actual_payload
  AND DATALENGTH(@expected_payload)=DATALENGTH(@actual_payload)
  AND @expected_hash=@actual_hash AND @binding=@actual_binding)
BEGIN
 DECLARE @next bigint=COALESCE(@actual_revision,0)+1;
 IF @actual_revision IS NULL
  INSERT INTO {slot} ({_COLUMNS}) VALUES (@slot,@binding,@next,@operation,@phase,@payload,@hash,@write);
 ELSE
  UPDATE {slot} SET revision=@next,operation_id=@operation,phase=@phase,
   payload=@payload,payload_sha256=@hash,write_id=@write WHERE slot_key=@slot AND revision=@actual_revision;
 IF @@ROWCOUNT<>1 THROW 51073, 'publication slot write count differs', 1;
 INSERT INTO {events} ({_COLUMNS},previous_sha256,origin,provenance,provenance_sha256,created_utc)
  SELECT {_COLUMNS},@actual_hash,'native',@provenance,@provenance_hash,SYSUTCDATETIME() FROM {slot} WHERE slot_key=@slot;
 IF @@ROWCOUNT<>1 THROW 51074, 'publication event write count differs', 1;
 SET @won=1;
END;
{read_statement(binding, won="@won", include_root=True)}
"""


def mutation_params(
    binding: PublicationAuthorityBinding,
    digest: str,
    current: c.VersionedAuthorityRecord | None,
    desired: c.AuthorityRecord,
) -> tuple[Any, ...]:
    provenance = native_publication_provenance(digest)
    return (
        publication_slot_key(binding, desired.target_key),
        bytes.fromhex(digest),
        current.version if current else None,
        current.record.payload.encode() if current else None,
        bytes.fromhex(current.record.payload_sha256) if current else None,
        desired.operation_id,
        desired.phase.value,
        desired.payload.encode(),
        bytes.fromhex(desired.payload_sha256),
        str(UUID(hex=desired.authority_write_id or "")),
        provenance,
        hashlib.sha256(provenance).digest(),
    )


def publication_tables(binding: PublicationAuthorityBinding) -> tuple[str, str]:
    """Quote the same admitted catalog location for native and migration SQL."""
    prefix = f"{quote_identifier(binding.database)}.{quote_identifier(binding.schema)}"
    return f"{prefix}.[{SLOT_TABLE}]", f"{prefix}.[{EVENT_TABLE}]"
