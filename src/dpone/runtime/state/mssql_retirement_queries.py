"""Absent-only retirement SQL; no native mutation or ClickHouse effect path."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any
from uuid import UUID

from dpone.ports.mssql_publication import PublicationAuthorityBinding, publication_slot_key
from dpone.runtime.state.mssql_publication_queries import publication_tables, read_statement

if TYPE_CHECKING:
    from dpone.ports.clickhouse_cluster_publication import contracts as c


def retirement_read_statement(binding: PublicationAuthorityBinding, *, won: str = "0") -> str:
    """Orphaned history is unknown, never a verified empty destination slot."""
    slot, events = publication_tables(binding)
    return (
        read_statement(binding, won=won).removesuffix(";")
        + " UNION ALL SELECT 0,0,NULL,NULL,NULL,NULL,0,0,NULL,NULL,NULL,NULL,NULL "
        + f"WHERE NOT EXISTS (SELECT 1 FROM {slot} WITH (HOLDLOCK) WHERE slot_key=@slot) "
        + f"AND EXISTS (SELECT 1 FROM {events} WITH (HOLDLOCK) WHERE slot_key=@slot);"
    )


def retirement_statement(binding: PublicationAuthorityBinding) -> str:
    """Insert slot and immutable event under the same absent-key range lock."""
    slot, events = publication_tables(binding)
    columns = "slot_key,binding_digest,revision,operation_id,phase,payload,payload_sha256,write_id"
    return f"""
DECLARE @slot char(64)=?, @binding binary(32)=?, @operation nvarchar(128)=?,
 @payload varbinary(max)=?, @hash binary(32)=?, @write uniqueidentifier=?,
 @provenance varbinary(max)=?, @provenance_hash binary(32)=?;
DECLARE @actual_revision bigint, @won bit=0;
SELECT @actual_revision=revision FROM {slot} WITH (UPDLOCK,HOLDLOCK) WHERE slot_key=@slot;
IF @actual_revision IS NULL
BEGIN
 IF EXISTS (SELECT 1 FROM {events} WITH (UPDLOCK,HOLDLOCK) WHERE slot_key=@slot)
  THROW 51075, 'publication retirement history exists', 1;
 INSERT INTO {slot} ({columns})
  VALUES (@slot,@binding,1,@operation,'RETIRED_UNPUBLISHED',@payload,@hash,@write);
 IF @@ROWCOUNT<>1 THROW 51073, 'publication slot write count differs', 1;
 INSERT INTO {events} ({columns},previous_sha256,origin,provenance,provenance_sha256,created_utc)
  SELECT {columns},NULL,'legacy_retired',@provenance,@provenance_hash,SYSUTCDATETIME()
  FROM {slot} WHERE slot_key=@slot;
 IF @@ROWCOUNT<>1 THROW 51074, 'publication event write count differs', 1;
 SET @won=1;
END;
{retirement_read_statement(binding, won="@won")}
"""


def retirement_params(
    binding: PublicationAuthorityBinding, digest: str, record: c.AuthorityRecord, provenance: bytes
) -> tuple[Any, ...]:
    return (
        publication_slot_key(binding, record.target_key),
        bytes.fromhex(digest),
        record.operation_id,
        record.payload.encode(),
        bytes.fromhex(record.payload_sha256),
        str(UUID(hex=record.authority_write_id or "")),
        provenance,
        hashlib.sha256(provenance).digest(),
    )
