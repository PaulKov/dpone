"""Offline additive DDL for the protected workspace execution witness.

This renderer neither installs nor grants access. Existing objects deliberately
cause CREATE to fail: the separately approved installer must attest exact catalog
shape before deciding that a migration is already installed. Merely finding a
table with the expected name is not sufficient schema authority.
"""

from __future__ import annotations

from dpone.adapters.dbt_workspace_mssql_gateway_security import (
    WORKSPACE_GATEWAY_SESSION_OPTIONS,
    workspace_gateway_identifier,
)


def render_workspace_handover_schema(control_schema: str) -> str:
    """Render the two witness tables and single-pending uniqueness constraint."""
    schema = workspace_gateway_identifier(control_schema)
    return f"""{WORKSPACE_GATEWAY_SESSION_OPTIONS}
SET XACT_ABORT ON;
BEGIN TRANSACTION;
CREATE TABLE [{schema}].[dbt_workspace_channels] (
    channel_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
    channel_json nvarchar(max) NOT NULL,
    revision bigint NOT NULL,
    current_activation_id uniqueidentifier NULL,
    pending_activation_id uniqueidentifier NULL,
    registration_id uniqueidentifier NOT NULL UNIQUE,
    registration_input_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL,
    registration_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL,
    registration_json nvarchar(max) NOT NULL,
    CONSTRAINT [ck_workspace_channel_revision] CHECK (revision >= 0),
    CONSTRAINT [ck_workspace_channel_json]
        CHECK (DATALENGTH(channel_json) <= 65536 AND ISJSON(channel_json) = 1),
    CONSTRAINT [ck_workspace_registration_json]
        CHECK (DATALENGTH(registration_json) <= 67108864 AND ISJSON(registration_json) = 1)
);
CREATE TABLE [{schema}].[dbt_workspace_handover_claims] (
    channel_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL,
    activation_id uniqueidentifier NOT NULL,
    claim_revision bigint NOT NULL,
    claim_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NOT NULL,
    claim_json nvarchar(max) NOT NULL,
    request_sha256 varchar(71) COLLATE Latin1_General_100_BIN2 NULL,
    request_json nvarchar(max) NULL,
    completed bit NOT NULL CONSTRAINT [df_workspace_handover_completed] DEFAULT (0),
    CONSTRAINT [pk_workspace_handover_claim] PRIMARY KEY (channel_sha256, activation_id),
    CONSTRAINT [uq_workspace_handover_claim_uuid] UNIQUE (activation_id),
    CONSTRAINT [uq_workspace_handover_claim_revision] UNIQUE (channel_sha256, claim_revision),
    CONSTRAINT [fk_workspace_handover_channel] FOREIGN KEY (channel_sha256)
        REFERENCES [{schema}].[dbt_workspace_channels] (channel_sha256),
    CONSTRAINT [ck_workspace_handover_claim_revision] CHECK (claim_revision > 0),
    CONSTRAINT [ck_workspace_handover_claim_json]
        CHECK (DATALENGTH(claim_json) <= 524288 AND ISJSON(claim_json) = 1),
    CONSTRAINT [ck_workspace_handover_request_pair]
        CHECK ((request_sha256 IS NULL AND request_json IS NULL)
            OR (request_sha256 IS NOT NULL AND request_json IS NOT NULL)),
    CONSTRAINT [ck_workspace_handover_request_json]
        CHECK (request_json IS NULL OR
            (DATALENGTH(request_json) <= 33554432 AND ISJSON(request_json) = 1))
);
CREATE UNIQUE INDEX [uq_workspace_handover_pending]
ON [{schema}].[dbt_workspace_handover_claims] (channel_sha256)
WHERE completed = 0;
COMMIT TRANSACTION;"""
