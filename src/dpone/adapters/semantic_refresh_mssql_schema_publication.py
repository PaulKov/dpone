"""DDL for MSSQL publication heads, evidence, and termination authority."""

from __future__ import annotations


def render_publication_schema(schema: str, schema_version: int) -> str:
    """Render the publication/recovery half of the locked schema migration."""

    def table(name: str) -> str:
        return f"[{schema}].[{name}]"

    return f"""
IF OBJECT_ID(N'{table("semantic_refresh_activation_authorities")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_activation_authorities")} (
    deployment_id varchar(71) NOT NULL,
    release_id varchar(71) NOT NULL,
    plan_bundle_sha256 varchar(71) NOT NULL,
    authority_store_ref nvarchar(512) NOT NULL,
    authority_receipt_json nvarchar(max) NOT NULL,
    activation_authority_receipt_sha256 varchar(71) NOT NULL UNIQUE,
    persisted_at datetime2(6) NOT NULL,
    status nvarchar(32) NOT NULL,
    CONSTRAINT [pk_{schema}_sr_activation_authorities]
        PRIMARY KEY (deployment_id, plan_bundle_sha256)
);

DECLARE @dpone_legacy_activation_authority_pk sysname;
DECLARE @dpone_drop_activation_authority_pk_sql nvarchar(max);
SELECT @dpone_legacy_activation_authority_pk = key_constraint.name
FROM sys.key_constraints AS key_constraint
JOIN sys.index_columns AS index_column
  ON index_column.object_id = key_constraint.parent_object_id
 AND index_column.index_id = key_constraint.unique_index_id
JOIN sys.columns AS column_definition
  ON column_definition.object_id = index_column.object_id
 AND column_definition.column_id = index_column.column_id
WHERE key_constraint.parent_object_id = OBJECT_ID(
    N'{table("semantic_refresh_activation_authorities")}', N'U'
)
  AND key_constraint.type = N'PK'
GROUP BY key_constraint.name
HAVING COUNT_BIG(*) = 1
   AND MAX(CASE WHEN index_column.key_ordinal = 1
                     AND column_definition.name = N'deployment_id'
                THEN 1 ELSE 0 END) = 1;

IF @dpone_legacy_activation_authority_pk IS NOT NULL
BEGIN
    SET @dpone_drop_activation_authority_pk_sql =
        N'ALTER TABLE {table("semantic_refresh_activation_authorities")} DROP CONSTRAINT '
        + QUOTENAME(@dpone_legacy_activation_authority_pk);
    EXEC sys.sp_executesql @dpone_drop_activation_authority_pk_sql;
    ALTER TABLE {table("semantic_refresh_activation_authorities")}
    ADD CONSTRAINT [pk_{schema}_sr_activation_authorities]
        PRIMARY KEY (deployment_id, plan_bundle_sha256);
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.key_constraints AS key_constraint
    JOIN sys.index_columns AS index_column
      ON index_column.object_id = key_constraint.parent_object_id
     AND index_column.index_id = key_constraint.unique_index_id
    JOIN sys.columns AS column_definition
      ON column_definition.object_id = index_column.object_id
     AND column_definition.column_id = index_column.column_id
    WHERE key_constraint.parent_object_id = OBJECT_ID(
        N'{table("semantic_refresh_activation_authorities")}', N'U'
    )
      AND key_constraint.type = N'PK'
    GROUP BY key_constraint.name
    HAVING COUNT_BIG(*) = 2
       AND MAX(CASE WHEN index_column.key_ordinal = 1
                         AND column_definition.name = N'deployment_id'
                    THEN 1 ELSE 0 END) = 1
       AND MAX(CASE WHEN index_column.key_ordinal = 2
                         AND column_definition.name = N'plan_bundle_sha256'
                    THEN 1 ELSE 0 END) = 1
)
    THROW 51115, 'DPONE_SEMANTIC_REFRESH_ACTIVATION_AUTHORITY_KEY_MIGRATION_FAILED', 1;

IF OBJECT_ID(N'{table("semantic_refresh_route_authorities")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_route_authorities")} (
    deployment_id varchar(71) NOT NULL PRIMARY KEY,
    route_certification_receipt_sha256 varchar(71) NOT NULL,
    receipt_json nvarchar(max) NOT NULL,
    status nvarchar(32) NOT NULL,
    effective_from datetime2(6) NULL,
    expires_at datetime2(6) NULL,
    revoked_receipt_sha256 varchar(71) NULL
);

IF OBJECT_ID(N'{table("semantic_refresh_runtime_assurance_authorities")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_runtime_assurance_authorities")} (
    deployment_id varchar(71) NOT NULL,
    model_unique_id nvarchar(512) NOT NULL,
    assurance_kind nvarchar(32) NOT NULL,
    runtime_assurance_receipt_sha256 varchar(71) NOT NULL,
    subject_json nvarchar(max) NOT NULL,
    receipt_json nvarchar(max) NOT NULL,
    status nvarchar(32) NOT NULL,
    effective_from datetime2(6) NULL,
    expires_at datetime2(6) NULL,
    revoked_receipt_sha256 varchar(71) NULL,
    CONSTRAINT [pk_{schema}_sr_runtime_authorities]
        PRIMARY KEY (deployment_id, model_unique_id, assurance_kind)
);

IF OBJECT_ID(N'{table("semantic_refresh_target_heads")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_target_heads")} (
    database_name nvarchar(128) NOT NULL,
    target_table nvarchar(128) NOT NULL,
    target_generation bigint NOT NULL,
    target_generation_id varchar(71) NOT NULL,
    target_uuid uniqueidentifier NOT NULL,
    operation_id nvarchar(512) NOT NULL,
    CONSTRAINT [pk_{schema}_sr_target_heads] PRIMARY KEY (database_name, target_table)
);

IF COL_LENGTH(N'{table("semantic_refresh_target_heads")}', N'target_generation_id') IS NULL
BEGIN
IF EXISTS (SELECT 1 FROM {table("semantic_refresh_target_heads")} WITH (UPDLOCK, HOLDLOCK))
    THROW 51105, 'DPONE_SEMANTIC_REFRESH_LEGACY_TARGET_HEAD_REQUIRES_EXPLICIT_MIGRATION', 1;
ALTER TABLE {table("semantic_refresh_target_heads")}
ADD target_generation_id varchar(71) NULL;
END;
ALTER TABLE {table("semantic_refresh_target_heads")}
ALTER COLUMN target_generation_id varchar(71) NOT NULL;

IF OBJECT_ID(N'{table("semantic_refresh_scope_heads")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_scope_heads")} (
    database_name nvarchar(128) NOT NULL,
    target_table nvarchar(128) NOT NULL,
    scope_id nvarchar(512) NOT NULL,
    scope_revision bigint NOT NULL,
    operation_id nvarchar(512) NOT NULL,
    CONSTRAINT [pk_{schema}_sr_scope_heads] PRIMARY KEY (database_name, target_table, scope_id)
);

IF OBJECT_ID(N'{table("semantic_refresh_checkpoints")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_checkpoints")} (
    database_name nvarchar(128) NOT NULL,
    target_table nvarchar(128) NOT NULL,
    scope_id nvarchar(512) NOT NULL,
    checkpoint_sha256 varchar(71) NOT NULL,
    checkpoint_version bigint NOT NULL,
    operation_id nvarchar(512) NOT NULL,
    CONSTRAINT [pk_{schema}_sr_checkpoints] PRIMARY KEY (database_name, target_table, scope_id)
);

IF COL_LENGTH(N'{table("semantic_refresh_checkpoints")}', N'checkpoint_version') IS NULL
BEGIN
IF EXISTS (SELECT 1 FROM {table("semantic_refresh_checkpoints")} WITH (UPDLOCK, HOLDLOCK))
    THROW 51107, 'DPONE_SEMANTIC_REFRESH_LEGACY_CHECKPOINT_REQUIRES_EXPLICIT_MIGRATION', 1;
ALTER TABLE {table("semantic_refresh_checkpoints")} ADD checkpoint_version bigint NULL;
END;
ALTER TABLE {table("semantic_refresh_checkpoints")}
ALTER COLUMN checkpoint_version bigint NOT NULL;

IF OBJECT_ID(N'{table("semantic_refresh_target_owners")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_target_owners")} (
    target_authority_id nvarchar(512) NOT NULL PRIMARY KEY,
    model_unique_id nvarchar(512) NOT NULL,
    deployment_id varchar(71) NOT NULL,
    owner_generation bigint NOT NULL,
    status nvarchar(32) NOT NULL
);

IF OBJECT_ID(N'{table("semantic_refresh_receipts")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_receipts")} (
    operation_id nvarchar(512) NOT NULL PRIMARY KEY,
    operation_plan_sha256 varchar(71) NOT NULL,
    attempt_binding_sha256 varchar(71) NOT NULL,
    fencing_epoch bigint NOT NULL,
    before_image_relation nvarchar(776) NOT NULL,
    before_image_sha256 varchar(71) NOT NULL,
    after_image_relation nvarchar(776) NOT NULL,
    after_image_sha256 varchar(71) NOT NULL,
    updated_count bigint NOT NULL,
    inserted_count bigint NOT NULL,
    build_receipt_sha256 varchar(71) NOT NULL
);

IF COL_LENGTH(N'{table("semantic_refresh_receipts")}', N'build_receipt_sha256') IS NULL
BEGIN
IF EXISTS (SELECT 1 FROM {table("semantic_refresh_receipts")} WITH (UPDLOCK, HOLDLOCK))
    THROW 51113, 'DPONE_SEMANTIC_REFRESH_LEGACY_BUILD_RECEIPT_REQUIRES_EXPLICIT_MIGRATION', 1;
ALTER TABLE {table("semantic_refresh_receipts")} ADD build_receipt_sha256 varchar(71) NULL;
END;
ALTER TABLE {table("semantic_refresh_receipts")} ALTER COLUMN build_receipt_sha256 varchar(71) NOT NULL;

IF OBJECT_ID(N'{table("semantic_refresh_attempt_continuations")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_attempt_continuations")} (
    operation_id varchar(71) NOT NULL,
    task_id nvarchar(250) NOT NULL,
    try_number int NOT NULL,
    pod_uid uniqueidentifier NOT NULL,
    original_attempt_binding_sha256 varchar(71) NOT NULL,
    receipt_json nvarchar(max) NOT NULL,
    continuation_receipt_sha256 varchar(71) NOT NULL UNIQUE,
    status nvarchar(32) NOT NULL,
    verified_at datetime2(6) NOT NULL,
    CONSTRAINT [pk_{schema}_sr_attempt_continuations]
        PRIMARY KEY (operation_id, task_id, try_number, pod_uid)
);

IF OBJECT_ID(N'{table("semantic_refresh_attempt_terminations")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_attempt_terminations")} (
    attempt_binding_sha256 varchar(71) NOT NULL PRIMARY KEY,
    workflow_execution_id nvarchar(512) NOT NULL,
    workflow_execution_binding_sha256 varchar(71) NOT NULL,
    operation_ids_json nvarchar(max) NOT NULL,
    operation_set_sha256 varchar(71) NOT NULL,
    dag_id nvarchar(250) NOT NULL,
    run_id nvarchar(250) NOT NULL,
    task_id nvarchar(250) NOT NULL,
    map_index int NOT NULL,
    try_number int NOT NULL,
    cluster_id nvarchar(250) NOT NULL,
    namespace nvarchar(250) NOT NULL,
    pod_name nvarchar(253) NOT NULL,
    pod_uid uniqueidentifier NOT NULL,
    pod_resource_version nvarchar(128) NOT NULL,
    terminal_phase nvarchar(16) NOT NULL,
    container_terminations_json nvarchar(max) NOT NULL,
    observed_at_utc datetime2(6) NOT NULL,
    observer_authority nvarchar(512) NOT NULL,
    observer_policy_sha256 varchar(71) NOT NULL,
    observer_attestation_sha256 varchar(71) NOT NULL,
    observer_signature_sha256 varchar(71) NOT NULL,
    verification_status nvarchar(32) NOT NULL,
    termination_receipt_sha256 varchar(71) NOT NULL UNIQUE
);

IF OBJECT_ID(N'{table("semantic_refresh_termination_observation_authorities")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_termination_observation_authorities")} (
    attempt_binding_sha256 varchar(71) NOT NULL PRIMARY KEY,
    workflow_execution_id nvarchar(512) NOT NULL,
    workflow_execution_binding_sha256 varchar(71) NOT NULL,
    operation_ids_json nvarchar(max) NOT NULL,
    operation_set_sha256 varchar(71) NOT NULL,
    dag_id nvarchar(250) NOT NULL,
    run_id nvarchar(250) NOT NULL,
    task_id nvarchar(250) NOT NULL,
    map_index int NOT NULL,
    try_number int NOT NULL,
    cluster_id nvarchar(250) NOT NULL,
    namespace nvarchar(250) NOT NULL,
    pod_name nvarchar(253) NOT NULL,
    pod_uid uniqueidentifier NOT NULL,
    observer_authority nvarchar(512) NOT NULL,
    observer_policy_sha256 varchar(71) NOT NULL,
    observer_attestation_sha256 varchar(71) NOT NULL,
    observation_authority_sha256 varchar(71) NOT NULL UNIQUE,
    status nvarchar(32) NOT NULL,
    created_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_sr_termination_authority_created] DEFAULT SYSUTCDATETIME()
);

IF COL_LENGTH(N'{table("semantic_refresh_attempt_terminations")}', N'workflow_execution_id') IS NULL
    THROW 51108, 'DPONE_SEMANTIC_REFRESH_LEGACY_TERMINATION_STATE_REQUIRES_EXPLICIT_MIGRATION', 1;

IF OBJECT_ID(N'{table("semantic_refresh_mssql_session_outcomes")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_mssql_session_outcomes")} (
    operation_id nvarchar(512) NOT NULL PRIMARY KEY,
    operation_plan_sha256 varchar(71) NOT NULL,
    attempt_binding_sha256 varchar(71) NOT NULL UNIQUE,
    fencing_epoch bigint NOT NULL,
    transaction_disposition nvarchar(32) NOT NULL,
    controller_proves_not_invoked bit NOT NULL,
    evidence_sha256 varchar(71) NOT NULL UNIQUE,
    recorded_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_sr_session_outcome_created] DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID(N'{table("semantic_refresh_seal_authorizations")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_seal_authorizations")} (
    operation_id varchar(71) NOT NULL PRIMARY KEY,
    workflow_execution_binding_sha256 varchar(71) NOT NULL,
    attempt_binding_sha256 varchar(71) NOT NULL,
    fencing_epoch bigint NOT NULL,
    journal_version bigint NOT NULL,
    seal_authorization_receipt_sha256 varchar(71) NOT NULL UNIQUE,
    seal_authorization_receipt_json nvarchar(max) NOT NULL,
    created_at datetime2(6) NOT NULL
);

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'prepare_plan_sha256') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD prepare_plan_sha256 varchar(71) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'prepare_plan_json') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD prepare_plan_json nvarchar(max) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'prepared_receipt_json') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD prepared_receipt_json nvarchar(max) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'artifact_manifest_key') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD artifact_manifest_key nvarchar(1024) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'artifact_manifest_version') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD artifact_manifest_version nvarchar(512) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'artifact_manifest_sha256') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD artifact_manifest_sha256 varchar(71) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'terminal_target_generation') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD terminal_target_generation bigint NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'terminal_target_generation_id') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD terminal_target_generation_id varchar(71) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'terminal_scope_revision') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD terminal_scope_revision bigint NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'terminal_checkpoint_sha256') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD terminal_checkpoint_sha256 varchar(71) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'terminal_checkpoint_version') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD terminal_checkpoint_version bigint NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'terminal_target_mutation_outcome') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD terminal_target_mutation_outcome nvarchar(64) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'terminal_value_conversion_outcome') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD terminal_value_conversion_outcome nvarchar(64) NULL;

IF NOT EXISTS (
    SELECT 1 FROM {table("semantic_refresh_schema_versions")} WITH (UPDLOCK, HOLDLOCK)
    WHERE component = N'semantic_refresh_v2'
)
INSERT INTO {table("semantic_refresh_schema_versions")} (component, schema_version)
VALUES (N'semantic_refresh_v2', {schema_version});

UPDATE {table("semantic_refresh_schema_versions")}
SET schema_version = {schema_version}
WHERE component = N'semantic_refresh_v2'
  AND schema_version < {schema_version};
""".strip()


__all__ = ["render_publication_schema"]
