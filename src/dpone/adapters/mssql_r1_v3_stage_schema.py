"""Closed SQL Server staging schema and DDL renderer for MSSQL R1 V3.

The runtime never submits SQL text to an owner procedure.  It submits typed
column metadata; the installed procedure renders only the closed templates
represented here.  Keeping the Python renderer deterministic lets planning
bind ``exact_stage_ddl_digest`` before any target I/O.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dpone.contracts.mssql_r1_v3_stage_evidence import R1StageArtifactKindV1
from dpone.contracts.mssql_r1_v3_staging import (
    canonical_bytes,
    require_identifier,
)
from dpone.contracts.postgres_mssql_hash_policy import R1HashContractError, R1ScalarType

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_staging import R1OpenStagePlanV1


_PLAN_SET_DOMAIN = b"dpone-r1-open-stage-plan-set-v1\0"
_PLAN_ORDER = (
    R1StageArtifactKindV1.BATCH_PAYLOAD,
    R1StageArtifactKindV1.XMIN_DELTA,
    R1StageArtifactKindV1.XMIN_COMPLETE_KEYS,
)
_PARAMETERIZED = re.compile(
    r"^pg\.(numeric\(\d+,-?\d+\)|varchar\(\d+\)|time\(\d\)|timestamp\(\d\)|timestamptz\(\d\))"
    r"-mssql\.(decimal\(\d+,\d+\)|nvarchar\(\d+\)|time\(\d\)|datetime2\(\d\)|datetimeoffset\(\d\))\.v1$"
)
_SIMPLE_TYPES = {
    "pg.bool-mssql.bit.v1": "bit",
    "pg.bit1-mssql.bit.v1": "bit",
    "pg.int2-mssql.smallint.v1": "smallint",
    "pg.int4-mssql.int.v1": "int",
    "pg.int8-mssql.bigint.v1": "bigint",
    "pg.float4-mssql.real.v1": "real",
    "pg.float8-mssql.float.v1": "float(53)",
    "pg.uuid-mssql.uniqueidentifier.v1": "uniqueidentifier",
    "pg.date-mssql.date.v1": "date",
    "pg.time-mssql.time.v1": "time(6)",
    "pg.timestamp-mssql.datetime2.v1": "datetime2(6)",
    "pg.timestamptz-mssql.datetimeoffset.v1": "datetimeoffset(6)",
    "pg.text-mssql.nvarchar.v1": "nvarchar(max)",
    "pg.varchar-mssql.nvarchar.v1": "nvarchar(max)",
    "pg.bytea-mssql.varbinary.v1": "varbinary(max)",
}


@dataclass(frozen=True, slots=True)
class MssqlR1V3StageObjectPlan:
    schema_name: str
    object_name: str
    create_table_sql: str
    ddl_digest: bytes


def render_stage_object_plan(
    plan: R1OpenStagePlanV1,
    stage_schema: str = "dpone_stage",
) -> MssqlR1V3StageObjectPlan:
    """Render the exact closed stage shape without trusting SQL from callers."""

    schema = require_identifier(stage_schema, "stage_schema")
    object_name = f"a_{plan.artifact_id.hex}"
    business = tuple(
        (column.stage_column, stage_sql_type(column.logical_type_id), column.nullable, column.is_business_key)
        for column in plan.ordered_business_columns
    )
    system = [(plan.canonical_key_payload_column, "varbinary(max)", False)]
    if plan.artifact_kind is not R1StageArtifactKindV1.XMIN_COMPLETE_KEYS:
        system.extend(
            (
                (str(plan.canonical_row_payload_column), "varbinary(max)", False),
                (str(plan.canonical_row_hash_column), "binary(32)", False),
            )
        )
    column_sql = [
        f"[{name}] {target_type}{' NULL' if nullable else ' NOT NULL'}" for name, target_type, nullable, *_ in business
    ]
    column_sql.extend(f"[{name}] {target_type} NOT NULL" for name, target_type, _nullable in system)
    key_columns = tuple(name for name, _target_type, _nullable, is_key in business if is_key)
    column_sql.append(f"CONSTRAINT [uq_{plan.artifact_id.hex}] UNIQUE ([{key_columns[0]}])")
    sql = f"CREATE TABLE [{schema}].[{object_name}](" + ",".join(column_sql) + ");"
    digest = hashlib.sha256(sql.encode("utf-16-le")).digest()
    return MssqlR1V3StageObjectPlan(schema, object_name, sql, digest)


def open_stage_plan_set_digest(plans: tuple[R1OpenStagePlanV1, ...]) -> bytes:
    """Hash one Batch or XMin OPEN plan set in the normative kind order."""

    kinds = tuple(item.artifact_kind for item in plans)
    if kinds not in {
        (R1StageArtifactKindV1.BATCH_PAYLOAD,),
        (R1StageArtifactKindV1.XMIN_DELTA, R1StageArtifactKindV1.XMIN_COMPLETE_KEYS),
    }:
        raise ValueError("OPEN stage plan set has an invalid kind/order")
    if len({item.artifact_id for item in plans}) != len(plans):
        raise ValueError("OPEN stage plan set contains duplicate artifacts")
    return hashlib.sha256(canonical_bytes(_PLAN_SET_DOMAIN, tuple(item.canonical_bytes for item in plans))).digest()


def render_mssql_r1_v3_stage_batches(
    authority_schema: str = "dpone_authority",
    stage_schema: str = "dpone_stage",
    *,
    runtime_principal: str = "dpone_r1_runtime",
    loader_principal: str = "dpone_r1_loader",
) -> tuple[str, ...]:
    """Return additive owner-side staging DDL without client-only ``GO``."""

    authority = require_identifier(authority_schema, "authority_schema")
    stage = require_identifier(stage_schema, "stage_schema")
    runtime = require_identifier(runtime_principal, "runtime_principal")
    loader = require_identifier(loader_principal, "loader_principal")
    return (
        f"IF SCHEMA_ID(N'{stage}') IS NULL EXEC(N'CREATE SCHEMA [{stage}] AUTHORIZATION dbo;');",
        _artifact_table(authority),
        _chunk_table(authority),
        _open_procedure(authority, stage, runtime, loader),
        _renew_procedure(authority),
        _begin_chunk_procedure(authority),
        _complete_chunk_procedure(authority),
        _observe_stage_procedure(authority),
        _observe_chunk_procedure(authority),
        _scan_stage_procedure(authority),
        _seal_procedure(authority, runtime, loader),
        f"GRANT EXECUTE ON SCHEMA::[{authority}] TO [{runtime}];",
    )


def _artifact_table(schema: str) -> str:
    return f"""IF OBJECT_ID(N'[{schema}].[dpone_staging_artifact_v3]',N'U') IS NULL
CREATE TABLE [{schema}].[dpone_staging_artifact_v3](
 artifact_id uniqueidentifier NOT NULL PRIMARY KEY,effect_key binary(32) NOT NULL,
 artifact_kind varchar(32) NOT NULL,target_binding_uuid uniqueidentifier NOT NULL,owner_epoch bigint NOT NULL,
 open_plan_payload varbinary(max) NOT NULL,open_plan_digest binary(32) NOT NULL,exact_stage_ddl_digest binary(32) NOT NULL,
 object_uuid uniqueidentifier NULL,object_id int NULL,physical_token uniqueidentifier NULL,
 stage_schema sysname NOT NULL,stage_object sysname NOT NULL,state varchar(16) NOT NULL,lease_seconds int NOT NULL,
 lease_expires_at datetime2(7) NULL,manifest_payload varbinary(max) NULL,manifest_digest binary(32) NULL,
 abandoned_at datetime2(7) NULL,
 CHECK(artifact_kind IN('batch_payload','xmin_delta','xmin_complete_keys')),CHECK(owner_epoch>0),
 CHECK(lease_seconds BETWEEN 1 AND 3600),CHECK(state IN('PLANNED','OPEN','SEALED','ABANDONED','CONSUMED')),
 CHECK((state='PLANNED' AND object_uuid IS NULL AND object_id IS NULL AND physical_token IS NULL)
  OR (state='OPEN' AND object_uuid IS NOT NULL AND object_id IS NOT NULL AND physical_token IS NOT NULL
   AND lease_expires_at IS NOT NULL AND manifest_payload IS NULL AND manifest_digest IS NULL)
  OR (state IN('SEALED','CONSUMED') AND object_uuid IS NOT NULL AND object_id IS NOT NULL
   AND physical_token IS NOT NULL AND lease_expires_at IS NULL AND manifest_payload IS NOT NULL AND manifest_digest IS NOT NULL)
  OR (state='ABANDONED' AND object_uuid IS NOT NULL AND object_id IS NOT NULL AND physical_token IS NOT NULL
   AND lease_expires_at IS NULL AND abandoned_at IS NOT NULL)));"""


def _chunk_table(schema: str) -> str:
    return f"""IF OBJECT_ID(N'[{schema}].[dpone_staging_chunk_v3]',N'U') IS NULL
CREATE TABLE [{schema}].[dpone_staging_chunk_v3](
 artifact_id uniqueidentifier NOT NULL,chunk_sequence bigint NOT NULL,chunk_digest binary(32) NOT NULL,
 row_count bigint NOT NULL,state varchar(12) NOT NULL,committed_at datetime2(7) NULL,
 PRIMARY KEY(artifact_id,chunk_sequence),CHECK(chunk_sequence>=0),CHECK(row_count>0),
 CHECK((state='LOADING' AND committed_at IS NULL) OR (state='COMPLETE' AND committed_at IS NOT NULL)),
 CONSTRAINT fk_dpone_staging_chunk_v3_artifact FOREIGN KEY(artifact_id)
 REFERENCES [{schema}].[dpone_staging_artifact_v3](artifact_id) ON DELETE NO ACTION);"""


def _open_guard() -> str:
    return """IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 THROW 51201,'DPONE_POSTGRES_MSSQL_TRANSACTION_REQUIRED',1;
DECLARE @lock_result int;
EXEC @lock_result=sys.sp_getapplock @Resource=CONCAT('dpone:r1:artifact:',CONVERT(varchar(36),@artifact_id)),@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=0;
IF @lock_result<0 THROW 51202,'DPONE_POSTGRES_MSSQL_STAGE_BUSY',1;"""


def _artifact_guard() -> str:
    return """IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 THROW 51201,'DPONE_POSTGRES_MSSQL_TRANSACTION_REQUIRED',1;
DECLARE @lock_result int;
EXEC @lock_result=sys.sp_getapplock @Resource=CONCAT('dpone:r1:artifact:',CONVERT(varchar(36),@artifact_id)),@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=0;
IF @lock_result<0 THROW 51202,'DPONE_POSTGRES_MSSQL_STAGE_BUSY',1;"""


def _open_procedure(schema: str, stage: str, runtime: str, loader: str) -> str:
    # The procedure deliberately accepts typed metadata, never executable DDL.
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[dpone_open_stage_v3]
 @artifact_id uniqueidentifier,@effect_key binary(32),@target_binding_uuid uniqueidentifier,@owner_epoch bigint,
 @artifact_kind varchar(32),@open_plan_payload varbinary(max),@open_plan_digest binary(32),
 @exact_stage_ddl_digest binary(32),@stage_object sysname,@lease_seconds int,@canonical_key sysname,
 @canonical_row sysname,@canonical_hash sysname,@maximum_key_bytes int,@maximum_row_bytes bigint,
 @schema_digest binary(32),@catalog_contract_digest binary(32),@permission_contract_digest binary(32),
 @type_policy_digest binary(32)
WITH EXECUTE AS OWNER AS BEGIN SET NOCOUNT ON; {_open_guard()}
IF @lease_seconds NOT BETWEEN 1 AND 3600 THROW 51203,'DPONE_POSTGRES_MSSQL_STAGE_PLAN_INVALID',1;
IF HASHBYTES('SHA2_256',@open_plan_payload)<>@open_plan_digest
 THROW 51203,'DPONE_POSTGRES_MSSQL_STAGE_PLAN_INVALID',1;
IF @stage_object<>CONCAT('a_',REPLACE(CONVERT(varchar(36),@artifact_id),'-',''))
 THROW 51203,'DPONE_POSTGRES_MSSQL_STAGE_PLAN_INVALID',1;
IF OBJECT_ID('tempdb..#dpone_stage_columns_v3') IS NULL THROW 51203,'DPONE_POSTGRES_MSSQL_STAGE_PLAN_INVALID',1;
IF @artifact_kind NOT IN('batch_payload','xmin_delta','xmin_complete_keys')
 THROW 51203,'DPONE_POSTGRES_MSSQL_STAGE_PLAN_INVALID',1;
IF EXISTS(SELECT 1 FROM #dpone_stage_columns_v3 WHERE logical_type_id NOT IN
 ('pg.bool-mssql.bit.v1','pg.bit1-mssql.bit.v1','pg.int2-mssql.smallint.v1','pg.int4-mssql.int.v1',
  'pg.int8-mssql.bigint.v1','pg.float4-mssql.real.v1','pg.float8-mssql.float.v1',
  'pg.uuid-mssql.uniqueidentifier.v1','pg.date-mssql.date.v1','pg.time-mssql.time.v1',
  'pg.timestamp-mssql.datetime2.v1','pg.timestamptz-mssql.datetimeoffset.v1',
  'pg.text-mssql.nvarchar.v1','pg.varchar-mssql.nvarchar.v1','pg.bytea-mssql.varbinary.v1')
 AND NOT(logical_type_id NOT LIKE '%[^a-z0-9(),.-]%' AND target_sql_type NOT LIKE '%[^a-z0-9(),]%'
  AND ((logical_type_id LIKE 'pg.numeric(%)-mssql.decimal(%,%).v1' AND target_sql_type LIKE 'decimal(%,%)')
   OR (logical_type_id LIKE 'pg.varchar(%)-mssql.nvarchar(%).v1' AND target_sql_type LIKE 'nvarchar(%)')
   OR (logical_type_id LIKE 'pg.time(%)-mssql.time(%).v1' AND target_sql_type LIKE 'time(%)')
   OR (logical_type_id LIKE 'pg.timestamp(%)-mssql.datetime2(%).v1' AND target_sql_type LIKE 'datetime2(%)')
   OR (logical_type_id LIKE 'pg.timestamptz(%)-mssql.datetimeoffset(%).v1' AND target_sql_type LIKE 'datetimeoffset(%)'))))
 THROW 51203,'DPONE_POSTGRES_MSSQL_STAGE_PLAN_INVALID',1;
IF EXISTS(SELECT 1 FROM #dpone_stage_columns_v3 WHERE target_sql_type<>CASE logical_type_id
 WHEN 'pg.bool-mssql.bit.v1' THEN 'bit' WHEN 'pg.bit1-mssql.bit.v1' THEN 'bit'
 WHEN 'pg.int2-mssql.smallint.v1' THEN 'smallint' WHEN 'pg.int4-mssql.int.v1' THEN 'int'
 WHEN 'pg.int8-mssql.bigint.v1' THEN 'bigint' WHEN 'pg.float4-mssql.real.v1' THEN 'real'
 WHEN 'pg.float8-mssql.float.v1' THEN 'float(53)' WHEN 'pg.uuid-mssql.uniqueidentifier.v1' THEN 'uniqueidentifier'
 WHEN 'pg.date-mssql.date.v1' THEN 'date' WHEN 'pg.time-mssql.time.v1' THEN 'time(6)'
 WHEN 'pg.timestamp-mssql.datetime2.v1' THEN 'datetime2(6)'
 WHEN 'pg.timestamptz-mssql.datetimeoffset.v1' THEN 'datetimeoffset(6)'
 WHEN 'pg.text-mssql.nvarchar.v1' THEN 'nvarchar(max)' WHEN 'pg.varchar-mssql.nvarchar.v1' THEN 'nvarchar(max)'
 WHEN 'pg.bytea-mssql.varbinary.v1' THEN 'varbinary(max)'
 ELSE SUBSTRING(logical_type_id,CHARINDEX('-mssql.',logical_type_id)+7,
  LEN(logical_type_id)-CHARINDEX('-mssql.',logical_type_id)-9) END)
 THROW 51203,'DPONE_POSTGRES_MSSQL_STAGE_PLAN_INVALID',1;
IF NOT EXISTS(SELECT 1 FROM #dpone_stage_columns_v3)
 OR (SELECT COUNT_BIG(*) FROM #dpone_stage_columns_v3 WHERE is_business_key=1 AND nullable=0)<>1
 OR EXISTS(SELECT 1 FROM #dpone_stage_columns_v3 WHERE source_ordinal<>target_ordinal OR source_ordinal<1)
 OR (SELECT MAX(source_ordinal) FROM #dpone_stage_columns_v3)<>(SELECT COUNT_BIG(*) FROM #dpone_stage_columns_v3)
 OR EXISTS(SELECT 1 FROM #dpone_stage_columns_v3 GROUP BY stage_column HAVING COUNT_BIG(*)>1)
 OR EXISTS(SELECT 1 FROM #dpone_stage_columns_v3 GROUP BY target_column HAVING COUNT_BIG(*)>1)
 THROW 51203,'DPONE_POSTGRES_MSSQL_STAGE_PLAN_INVALID',1;
DECLARE @object_uuid uniqueidentifier=NEWID(),@physical_token uniqueidentifier=NEWID(),@sql nvarchar(max),
 @columns nvarchar(max),@key_column sysname;
IF EXISTS(SELECT 1 FROM [{schema}].[dpone_staging_artifact_v3] WITH(UPDLOCK,HOLDLOCK)
 WHERE artifact_id=@artifact_id AND state='OPEN' AND effect_key=@effect_key AND target_binding_uuid=@target_binding_uuid
  AND owner_epoch=@owner_epoch AND open_plan_digest=@open_plan_digest AND exact_stage_ddl_digest=@exact_stage_ddl_digest)
BEGIN SELECT object_uuid,object_id,physical_token,stage_schema,stage_object
 FROM [{schema}].[dpone_staging_artifact_v3] WHERE artifact_id=@artifact_id; RETURN; END;
IF EXISTS(SELECT 1 FROM [{schema}].[dpone_staging_artifact_v3] WITH(UPDLOCK,HOLDLOCK)
 WHERE artifact_id=@artifact_id AND NOT(state='PLANNED' AND effect_key=@effect_key AND target_binding_uuid=@target_binding_uuid
  AND owner_epoch=@owner_epoch AND open_plan_digest=@open_plan_digest AND exact_stage_ddl_digest=@exact_stage_ddl_digest))
 THROW 51209,'DPONE_POSTGRES_MSSQL_STAGE_IDENTITY_CONFLICT',1;
SELECT @columns=STRING_AGG(CONVERT(nvarchar(max),QUOTENAME(stage_column)+N' '+target_sql_type+
 CASE WHEN nullable=1 THEN N' NULL' ELSE N' NOT NULL' END),N',' ) WITHIN GROUP(ORDER BY target_ordinal),
 @key_column=MAX(CASE WHEN is_business_key=1 THEN stage_column END) FROM #dpone_stage_columns_v3;
IF @artifact_kind='xmin_complete_keys' AND (SELECT COUNT_BIG(*) FROM #dpone_stage_columns_v3)<>1
 THROW 51203,'DPONE_POSTGRES_MSSQL_STAGE_PLAN_INVALID',1;
SET @sql=N'CREATE TABLE [{stage}].'+QUOTENAME(@stage_object)+N'('+@columns+N','+QUOTENAME(@canonical_key)
 +N' varbinary(max) NOT NULL'
 +CASE WHEN @artifact_kind='xmin_complete_keys' THEN N'' ELSE N','+QUOTENAME(@canonical_row)+N' varbinary('
  +N'max) NOT NULL,'
  +QUOTENAME(@canonical_hash)+N' binary(32) NOT NULL' END
 +N',CONSTRAINT '+QUOTENAME(CONCAT('uq_',REPLACE(CONVERT(varchar(36),@artifact_id),'-','')))+N' UNIQUE ('+QUOTENAME(@key_column)+N'));';
IF HASHBYTES('SHA2_256',CONVERT(varbinary(max),@sql))<>@exact_stage_ddl_digest
 THROW 51203,'DPONE_POSTGRES_MSSQL_STAGE_DDL_AUTHORITY_MISMATCH',1;
EXEC sys.sp_executesql @sql;
DECLARE @object_id int=OBJECT_ID(N'[{stage}].'+QUOTENAME(@stage_object));
EXEC sys.sp_addextendedproperty @name=N'dpone.staging_object_uuid',@value=@object_uuid,@level0type=N'SCHEMA',@level0name=N'{stage}',@level1type=N'TABLE',@level1name=@stage_object;
EXEC sys.sp_addextendedproperty @name=N'dpone.staging_physical_token',@value=@physical_token,@level0type=N'SCHEMA',@level0name=N'{stage}',@level1type=N'TABLE',@level1name=@stage_object;
EXEC sys.sp_addextendedproperty @name=N'dpone.open_plan_digest',@value=@open_plan_digest,@level0type=N'SCHEMA',@level0name=N'{stage}',@level1type=N'TABLE',@level1name=@stage_object;
EXEC sys.sp_addextendedproperty @name=N'dpone.stage_ddl_digest',@value=@exact_stage_ddl_digest,@level0type=N'SCHEMA',@level0name=N'{stage}',@level1type=N'TABLE',@level1name=@stage_object;
EXEC sys.sp_addextendedproperty @name=N'dpone.schema_digest',@value=@schema_digest,@level0type=N'SCHEMA',@level0name=N'{stage}',@level1type=N'TABLE',@level1name=@stage_object;
EXEC sys.sp_addextendedproperty @name=N'dpone.catalog_contract_digest',@value=@catalog_contract_digest,@level0type=N'SCHEMA',@level0name=N'{stage}',@level1type=N'TABLE',@level1name=@stage_object;
EXEC sys.sp_addextendedproperty @name=N'dpone.permission_contract_digest',@value=@permission_contract_digest,@level0type=N'SCHEMA',@level0name=N'{stage}',@level1type=N'TABLE',@level1name=@stage_object;
EXEC sys.sp_addextendedproperty @name=N'dpone.type_policy_digest',@value=@type_policy_digest,@level0type=N'SCHEMA',@level0name=N'{stage}',@level1type=N'TABLE',@level1name=@stage_object;
IF EXISTS(SELECT 1 FROM [{schema}].[dpone_staging_artifact_v3] WHERE artifact_id=@artifact_id AND state='PLANNED')
 UPDATE [{schema}].[dpone_staging_artifact_v3] SET object_uuid=@object_uuid,object_id=@object_id,physical_token=@physical_token,
  state='OPEN',lease_seconds=@lease_seconds,lease_expires_at=DATEADD(SECOND,@lease_seconds,SYSUTCDATETIME())
 WHERE artifact_id=@artifact_id AND state='PLANNED';
ELSE INSERT INTO [{schema}].[dpone_staging_artifact_v3]
(artifact_id,effect_key,artifact_kind,target_binding_uuid,owner_epoch,open_plan_payload,open_plan_digest,
 exact_stage_ddl_digest,object_uuid,object_id,physical_token,stage_schema,stage_object,state,lease_seconds,lease_expires_at)
SELECT @artifact_id,@effect_key,@artifact_kind,@target_binding_uuid,@owner_epoch,@open_plan_payload,@open_plan_digest,
 @exact_stage_ddl_digest,@object_uuid,@object_id,@physical_token,N'{stage}',@stage_object,
 'OPEN',@lease_seconds,DATEADD(SECOND,@lease_seconds,SYSUTCDATETIME());
SET @sql=N'GRANT INSERT ON OBJECT::[{stage}].'+QUOTENAME(@stage_object)+N' TO [{runtime}];'
 +N'GRANT INSERT ON OBJECT::[{stage}].'+QUOTENAME(@stage_object)+N' TO [{loader}]'; EXEC sys.sp_executesql @sql;
SELECT @object_uuid,@object_id,@physical_token,N'{stage}',@stage_object;
END;"""


def _renew_procedure(schema: str) -> str:
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[dpone_renew_stage_v3]
 @artifact_id uniqueidentifier,@owner_epoch bigint WITH EXECUTE AS OWNER AS BEGIN SET NOCOUNT ON; {_artifact_guard()}
UPDATE [{schema}].[dpone_staging_artifact_v3] WITH(UPDLOCK,HOLDLOCK)
 SET lease_expires_at=DATEADD(SECOND,lease_seconds,SYSUTCDATETIME())
 WHERE artifact_id=@artifact_id AND owner_epoch=@owner_epoch AND state='OPEN' AND lease_expires_at>SYSUTCDATETIME();
SELECT CONVERT(bit,CASE WHEN @@ROWCOUNT=1 THEN 1 ELSE 0 END); END;"""


def _begin_chunk_procedure(schema: str) -> str:
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[dpone_begin_stage_chunk_v3]
 @artifact_id uniqueidentifier,@owner_epoch bigint,@chunk_sequence bigint,@chunk_digest binary(32),@row_count bigint
WITH EXECUTE AS OWNER AS BEGIN SET NOCOUNT ON; {_artifact_guard()}
IF NOT EXISTS(SELECT 1 FROM [{schema}].[dpone_staging_artifact_v3] WITH(UPDLOCK,HOLDLOCK)
 WHERE artifact_id=@artifact_id AND owner_epoch=@owner_epoch AND state='OPEN' AND lease_expires_at>SYSUTCDATETIME())
 THROW 51205,'DPONE_POSTGRES_MSSQL_STAGE_LEASE_LOST',1;
IF EXISTS(SELECT 1 FROM [{schema}].[dpone_staging_chunk_v3] WITH(UPDLOCK,HOLDLOCK) WHERE artifact_id=@artifact_id
 AND chunk_sequence=@chunk_sequence AND (chunk_digest<>@chunk_digest OR row_count<>@row_count))
 THROW 51208,'DPONE_POSTGRES_MSSQL_STAGE_CHUNK_CONFLICT',1;
IF EXISTS(SELECT 1 FROM [{schema}].[dpone_staging_chunk_v3] WITH(UPDLOCK,HOLDLOCK) WHERE artifact_id=@artifact_id
 AND chunk_sequence=@chunk_sequence AND state='COMPLETE') BEGIN SELECT CONVERT(bit,0); RETURN; END;
INSERT INTO [{schema}].[dpone_staging_chunk_v3](artifact_id,chunk_sequence,chunk_digest,row_count,state)
VALUES(@artifact_id,@chunk_sequence,@chunk_digest,@row_count,'LOADING'); SELECT CONVERT(bit,1); END;"""


def _complete_chunk_procedure(schema: str) -> str:
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[dpone_complete_stage_chunk_v3]
 @artifact_id uniqueidentifier,@owner_epoch bigint,@chunk_sequence bigint,@chunk_digest binary(32)
WITH EXECUTE AS OWNER AS BEGIN SET NOCOUNT ON; {_artifact_guard()}
IF NOT EXISTS(SELECT 1 FROM [{schema}].[dpone_staging_artifact_v3] WITH(UPDLOCK,HOLDLOCK)
 WHERE artifact_id=@artifact_id AND owner_epoch=@owner_epoch AND state='OPEN' AND lease_expires_at>SYSUTCDATETIME())
 THROW 51205,'DPONE_POSTGRES_MSSQL_STAGE_LEASE_LOST',1;
UPDATE [{schema}].[dpone_staging_chunk_v3] SET state='COMPLETE',committed_at=SYSUTCDATETIME()
 WHERE artifact_id=@artifact_id AND chunk_sequence=@chunk_sequence AND chunk_digest=@chunk_digest AND state='LOADING';
IF @@ROWCOUNT<>1 THROW 51208,'DPONE_POSTGRES_MSSQL_STAGE_CHUNK_CONFLICT',1; END;"""


def _observe_stage_procedure(schema: str) -> str:
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[dpone_observe_stage_v3] @artifact_id uniqueidentifier
WITH EXECUTE AS OWNER AS BEGIN SET NOCOUNT ON; {_artifact_guard()}
SELECT a.open_plan_payload,a.state,a.owner_epoch,a.object_uuid,a.object_id,a.physical_token,a.stage_schema,a.stage_object,
 a.manifest_payload,a.manifest_digest,a.lease_expires_at,t.object_id,
 TRY_CONVERT(uniqueidentifier,(SELECT p.value FROM sys.extended_properties p WHERE p.class=1 AND p.major_id=t.object_id AND p.minor_id=0 AND p.name=N'dpone.staging_object_uuid')),
 TRY_CONVERT(uniqueidentifier,(SELECT p.value FROM sys.extended_properties p WHERE p.class=1 AND p.major_id=t.object_id AND p.minor_id=0 AND p.name=N'dpone.staging_physical_token')),
 TRY_CONVERT(varbinary(32),(SELECT p.value FROM sys.extended_properties p WHERE p.class=1 AND p.major_id=t.object_id AND p.minor_id=0 AND p.name=N'dpone.stage_ddl_digest')),
 TRY_CONVERT(varbinary(32),(SELECT p.value FROM sys.extended_properties p WHERE p.class=1 AND p.major_id=t.object_id AND p.minor_id=0 AND p.name=N'dpone.schema_digest')),
 TRY_CONVERT(varbinary(32),(SELECT p.value FROM sys.extended_properties p WHERE p.class=1 AND p.major_id=t.object_id AND p.minor_id=0 AND p.name=N'dpone.catalog_contract_digest')),
 TRY_CONVERT(varbinary(32),(SELECT p.value FROM sys.extended_properties p WHERE p.class=1 AND p.major_id=t.object_id AND p.minor_id=0 AND p.name=N'dpone.permission_contract_digest')),
 TRY_CONVERT(varbinary(32),(SELECT p.value FROM sys.extended_properties p WHERE p.class=1 AND p.major_id=t.object_id AND p.minor_id=0 AND p.name=N'dpone.type_policy_digest'))
FROM [{schema}].[dpone_staging_artifact_v3] a WITH(HOLDLOCK)
LEFT JOIN sys.schemas s ON s.name=a.stage_schema LEFT JOIN sys.tables t ON t.schema_id=s.schema_id AND t.name=a.stage_object
WHERE a.artifact_id=@artifact_id; END;"""


def _observe_chunk_procedure(schema: str) -> str:
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[dpone_observe_stage_chunk_v3]
 @artifact_id uniqueidentifier,@chunk_sequence bigint WITH EXECUTE AS OWNER AS BEGIN SET NOCOUNT ON; {_artifact_guard()}
SELECT chunk_digest,state FROM [{schema}].[dpone_staging_chunk_v3] WITH(HOLDLOCK)
 WHERE artifact_id=@artifact_id AND chunk_sequence=@chunk_sequence; END;"""


def _scan_stage_procedure(schema: str) -> str:
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[dpone_scan_stage_v3]
 @artifact_id uniqueidentifier,@owner_epoch bigint WITH EXECUTE AS OWNER AS BEGIN SET NOCOUNT ON; {_artifact_guard()}
DECLARE @stage_schema sysname,@stage_object sysname,@object_id int,@artifact_kind varchar(32),@key_column sysname,@sql nvarchar(max);
SELECT @stage_schema=stage_schema,@stage_object=stage_object,@object_id=object_id,@artifact_kind=artifact_kind
 FROM [{schema}].[dpone_staging_artifact_v3] WITH(UPDLOCK,HOLDLOCK) WHERE artifact_id=@artifact_id
 AND owner_epoch=@owner_epoch AND (state='SEALED' OR (state='OPEN' AND lease_expires_at>SYSUTCDATETIME()));
IF @object_id IS NULL THROW 51205,'DPONE_POSTGRES_MSSQL_STAGE_LEASE_LOST',1;
SELECT @key_column=name FROM sys.columns WHERE object_id=@object_id AND
 column_id=(SELECT MAX(column_id)-CASE WHEN @artifact_kind='xmin_complete_keys' THEN 0 ELSE 2 END FROM sys.columns WHERE object_id=@object_id);
IF @key_column IS NULL THROW 51204,'DPONE_POSTGRES_MSSQL_STAGE_MANIFEST_INVALID',1;
SET @sql=N'SELECT * FROM '+QUOTENAME(@stage_schema)+N'.'+QUOTENAME(@stage_object)
 +N' WITH(TABLOCKX,HOLDLOCK) ORDER BY '+QUOTENAME(@key_column)+N';'; EXEC sys.sp_executesql @sql; END;"""


def _seal_procedure(schema: str, runtime: str, loader: str) -> str:
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[dpone_seal_stage_v3]
 @artifact_id uniqueidentifier,@owner_epoch bigint,@manifest_payload varbinary(max),@manifest_digest binary(32)
WITH EXECUTE AS OWNER AS BEGIN SET NOCOUNT ON; {_artifact_guard()}
DECLARE @object_id int,@stage_schema sysname,@stage_object sysname,@sql nvarchar(max);
IF HASHBYTES('SHA2_256',@manifest_payload)<>@manifest_digest
 THROW 51204,'DPONE_POSTGRES_MSSQL_STAGE_MANIFEST_INVALID',1;
SELECT @object_id=object_id,@stage_schema=stage_schema,@stage_object=stage_object
 FROM [{schema}].[dpone_staging_artifact_v3] WITH(UPDLOCK,HOLDLOCK)
 WHERE artifact_id=@artifact_id AND owner_epoch=@owner_epoch AND state='OPEN' AND lease_expires_at>SYSUTCDATETIME();
IF @object_id IS NULL THROW 51205,'DPONE_POSTGRES_MSSQL_STAGE_LEASE_LOST',1;
SET @sql=N'SELECT COUNT_BIG(*) FROM '+QUOTENAME(@stage_schema)+N'.'+QUOTENAME(@stage_object)+N' WITH(TABLOCKX,HOLDLOCK);'; EXEC sys.sp_executesql @sql;
SET @sql=N'DENY INSERT,UPDATE,DELETE ON OBJECT::'+QUOTENAME(@stage_schema)+N'.'+QUOTENAME(@stage_object)+N' TO [{runtime}];'
 +N'DENY INSERT,UPDATE,DELETE ON OBJECT::'+QUOTENAME(@stage_schema)+N'.'+QUOTENAME(@stage_object)+N' TO [{loader}];'; EXEC sys.sp_executesql @sql;
UPDATE [{schema}].[dpone_staging_artifact_v3] SET state='SEALED',manifest_payload=@manifest_payload,
 manifest_digest=@manifest_digest,lease_expires_at=NULL WHERE artifact_id=@artifact_id AND state='OPEN';
END;"""


def stage_sql_type(logical_type_id: str) -> str:
    if logical_type_id in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[logical_type_id]
    match = _PARAMETERIZED.fullmatch(logical_type_id)
    if match is not None:
        try:
            expected = R1ScalarType.from_postgres(match.group(1)).target_type.lower().replace("float", "float(53)")
        except R1HashContractError as exc:
            raise ValueError(f"unsupported logical type: {logical_type_id}") from exc
        if match.group(2) == expected:
            return expected
    raise ValueError(f"unsupported logical type: {logical_type_id}")


__all__ = [
    "MssqlR1V3StageObjectPlan",
    "open_stage_plan_set_digest",
    "render_mssql_r1_v3_stage_batches",
    "render_stage_object_plan",
    "stage_sql_type",
]
