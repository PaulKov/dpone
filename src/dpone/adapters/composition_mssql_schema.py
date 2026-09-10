"""External, additive SQL Server ledger DDL for composition occurrences.

An administrator reviews and executes this batch on a protected control database,
then provisions the authority identity and every SQL Server/ClickHouse domain.
It deliberately supplies no credentials, enrollment, permission policy or writer
gate. Runtime adapters never execute it, repair it, or migrate native-v2 tables.
Reapplying against existing tables fails instead of silently adopting them.
"""

from __future__ import annotations

import re

COMPOSITION_MSSQL_SCHEMA_VERSION = 1
COMPOSITION_MSSQL_LEDGER_LOCK = "dpone:composition-control:v1"


def require_control_schema(value: str) -> str:
    """Validate a bounded SQL identifier before interpolating any object name."""
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", value) is None:
        raise ValueError("control_schema must be a simple SQL identifier of at most 128 characters")
    return value


def render_composition_mssql_schema(control_schema: str = "dpone_control") -> str:
    """Render tables only; trusted provisioning must enroll and protect them.

    Documents are canonical UTF-8 bytes, never NVARCHAR encodings. Authority,
    domain identity, historical requests, attempts and proofs are append-only
    except for explicit state/ownership updates by protected controllers. Worker
    principals must have no permission to write any of these tables.
    """
    schema = require_control_schema(control_schema)

    def table(name: str) -> str:
        return f"[{schema}].[composition_{name}]"

    return f"""SET XACT_ABORT ON;
BEGIN TRANSACTION;
IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');

CREATE TABLE {table("authority")} (
    singleton tinyint NOT NULL PRIMARY KEY CHECK (singleton = 1),
    schema_version int NOT NULL CHECK (schema_version = {COMPOSITION_MSSQL_SCHEMA_VERSION}),
    service_id uniqueidentifier NOT NULL
);

CREATE TABLE {table("activations")} (
    activation_id uniqueidentifier NOT NULL PRIMARY KEY,
    request_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE,
    request_document varbinary(max) NOT NULL CHECK (DATALENGTH(request_document) BETWEEN 1 AND 8388608),
    state varchar(16) COLLATE Latin1_General_100_BIN2 NOT NULL
        CHECK (state IN ('PREPARED', 'ACTIVE', 'RETIRING', 'RETIRED'))
);

CREATE TABLE {table("domains")} (
    guard_id varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
    connector varchar(16) COLLATE Latin1_General_100_BIN2 NOT NULL CHECK (connector IN ('mssql', 'clickhouse')),
    service_id uniqueidentifier NOT NULL,
    physical_subject_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL,
    fencing_epoch bigint NOT NULL CHECK (fencing_epoch >= 0),
    owner_activation_id uniqueidentifier NULL REFERENCES {table("activations")}(activation_id),
    CHECK (owner_activation_id IS NULL OR fencing_epoch > 0),
    UNIQUE (connector, service_id, physical_subject_sha256)
);

CREATE TABLE {table("activation_domains")} (
    activation_id uniqueidentifier NOT NULL REFERENCES {table("activations")}(activation_id),
    guard_id varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL REFERENCES {table("domains")}(guard_id),
    resource_document varbinary(max) NOT NULL CHECK (DATALENGTH(resource_document) BETWEEN 1 AND 8388608),
    fencing_epoch bigint NOT NULL CHECK (fencing_epoch > 0),
    PRIMARY KEY (activation_id, guard_id)
);

CREATE INDEX composition_activation_domains_guard
    ON {table("activation_domains")} (guard_id, activation_id);

CREATE TABLE {table("attempts")} (
    attempt_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
    activation_id uniqueidentifier NOT NULL REFERENCES {table("activations")}(activation_id),
    activation_request_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL
        REFERENCES {table("activations")}(request_sha256),
    guard_epochs_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL,
    attempt_document varbinary(max) NOT NULL CHECK (DATALENGTH(attempt_document) BETWEEN 1 AND 8388608),
    state varchar(16) COLLATE Latin1_General_100_BIN2 NOT NULL
        CHECK (state IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'COMMIT_UNKNOWN')),
    closed_gates_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NULL,
    quiescence_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NULL,
    outcome_evidence_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NULL,
    CHECK (state IN ('RUNNING', 'COMMIT_UNKNOWN') OR (
        closed_gates_sha256 IS NOT NULL AND quiescence_sha256 IS NOT NULL AND outcome_evidence_sha256 IS NOT NULL
    ))
);

CREATE TABLE {table("attempt_domains")} (
    attempt_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL REFERENCES {table("attempts")}(attempt_sha256),
    guard_id varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL REFERENCES {table("domains")}(guard_id),
    fencing_epoch bigint NOT NULL CHECK (fencing_epoch > 0),
    PRIMARY KEY (attempt_sha256, guard_id)
);

CREATE TABLE {table("issued_authorities")} (
    attempt_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL REFERENCES {table("attempts")}(attempt_sha256),
    connector varchar(16) COLLATE Latin1_General_100_BIN2 NOT NULL CHECK (connector IN ('mssql', 'clickhouse')),
    service_id uniqueidentifier NOT NULL,
    principal_id varchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    PRIMARY KEY (attempt_sha256, connector, service_id, principal_id),
    UNIQUE (connector, service_id, principal_id)
);

CREATE TABLE {table("proofs")} (
    attempt_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL REFERENCES {table("attempts")}(attempt_sha256),
    kind varchar(16) COLLATE Latin1_General_100_BIN2 NOT NULL CHECK (kind IN ('CLOSED_GATES', 'QUIESCENCE', 'OUTCOME')),
    proof_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL,
    activation_request_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL
        REFERENCES {table("activations")}(request_sha256),
    guard_epochs_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL,
    proof_document varbinary(max) NOT NULL CHECK (DATALENGTH(proof_document) BETWEEN 1 AND 8388608),
    PRIMARY KEY (attempt_sha256, kind, proof_sha256)
);
COMMIT TRANSACTION;
"""
