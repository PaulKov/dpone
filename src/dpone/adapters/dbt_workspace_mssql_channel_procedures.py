"""Fixed channel readback procedures; render only, never provision at runtime."""

from __future__ import annotations

from dpone.adapters.dbt_workspace_mssql_gateway_security import workspace_gateway_identifier
from dpone.adapters.dbt_workspace_mssql_gateway_transaction import workspace_gateway_transaction
from dpone.adapters.dbt_workspace_mssql_gateway_validation import workspace_document_validation

_CHANNEL_FIELDS = {
    name: (1,)
    for name in (
        "schema",
        "desired_state_uri",
        "registry_scope_id",
        "environment",
        "source_project",
        "source_ref",
        "channel_sha256",
    )
}


def render_workspace_channel_read(control_schema: str) -> str:
    """Read one coherent witness and original lifecycle snapshot without mutation.

    The internal JSON result includes full stored registration/claim/request payloads
    and protected occurrence rows. It is not a new public wire schema. The adapter
    must validate closed framing and reconstruct the existing typed readback before
    returning any authority to the application. Live-held guard predicates are also
    returned for exact validation, never synthesized from request resources.
    """
    schema = workspace_gateway_identifier(control_schema)
    validation = workspace_document_validation(
        schema,
        variable="channel",
        fields=_CHANNEL_FIELDS,
        schema_name="dpone.dbt-workspace-channel.v1",
        digest_field="channel_sha256",
        maximum_bytes=32 * 1024,
    )
    body = f"""
    IF NOT EXISTS (
        SELECT 1 FROM [{schema}].[dbt_workspace_channels] WITH (HOLDLOCK)
        WHERE channel_sha256 = @channel_sha256
    ) THROW 51005, 'workspace channel is unregistered', 1;
    IF NOT EXISTS (
        SELECT 1 FROM [{schema}].[dbt_workspace_channels] WITH (HOLDLOCK)
        WHERE channel_sha256 = @channel_sha256
          AND CONVERT(varbinary(max), channel_json) = CONVERT(varbinary(max), @channel_canonical)
    ) THROW 51000, 'workspace registered channel differs', 1;
    SELECT @snapshot = (
        SELECT channel.channel_json, channel.revision,
            LOWER(CONVERT(varchar(36), channel.current_activation_id)) AS current_activation_id,
            LOWER(CONVERT(varchar(36), channel.pending_activation_id)) AS pending_activation_id, channel.registration_json,
            current_claim.claim_json AS current_claim_json,
            current_claim.request_json AS current_request_json,
            current_claim.completed AS current_completed,
            pending_claim.claim_json AS pending_claim_json,
            pending_claim.request_json AS pending_request_json,
            pending_claim.completed AS pending_completed,
            JSON_QUERY({_lifecycle_snapshot(schema, "channel.current_activation_id")}) AS current_lifecycle,
            JSON_QUERY({_lifecycle_snapshot(schema, "channel.pending_activation_id")}) AS pending_lifecycle
        FROM [{schema}].[dbt_workspace_channels] AS channel WITH (HOLDLOCK)
        LEFT JOIN [{schema}].[dbt_workspace_handover_claims] AS current_claim WITH (HOLDLOCK)
          ON current_claim.channel_sha256 = channel.channel_sha256
          AND current_claim.activation_id = channel.current_activation_id
        LEFT JOIN [{schema}].[dbt_workspace_handover_claims] AS pending_claim WITH (HOLDLOCK)
          ON pending_claim.channel_sha256 = channel.channel_sha256
          AND pending_claim.activation_id = channel.pending_activation_id
        WHERE channel.channel_sha256 = @channel_sha256
        FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
    );
    IF @snapshot IS NULL THROW 51000, 'workspace channel snapshot is missing', 1;
""".strip()
    transaction = workspace_gateway_transaction(schema, body, channel=True, read_only=True)
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[workspace_channel_read]
    @channel nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    {validation}
    DECLARE @channel_sha256 varchar(71) = @channel_digest, @snapshot nvarchar(max);
    {transaction}
    SELECT @snapshot AS snapshot_json;
END;"""


def _lifecycle_snapshot(schema: str, identity: str) -> str:
    return f"""(
        SELECT LOWER(CONVERT(varchar(36), activation.activation_id)) AS activation_id,
            activation.request_sha256, activation.environment,
            activation.release_id, activation.deployment_id, activation.previous_deployment_id,
            activation.source_inventory_sha256, activation.runtime_context_sha256, activation.state,
            JSON_QUERY((
                SELECT TOP (8193) ownership.guard_id, ownership.resource_sha256, ownership.fencing_epoch,
                    guard.fencing_epoch AS live_epoch, guard.owner_id, guard.workflow_id,
                    guard.operation_id, guard.status,
                    JSON_QUERY((
                        SELECT TOP (8193) subject.write_subject_sha256
                        FROM [{schema}].[dbt_workspace_activation_write_subjects] AS subject WITH (HOLDLOCK)
                        WHERE subject.activation_id = activation.activation_id AND subject.guard_id = ownership.guard_id
                        ORDER BY subject.write_subject_sha256 FOR JSON PATH, INCLUDE_NULL_VALUES
                    )) AS write_subjects
                FROM [{schema}].[dbt_workspace_activation_guards] AS ownership WITH (HOLDLOCK)
                LEFT JOIN [{schema}].[semantic_refresh_guards] AS guard WITH (HOLDLOCK)
                  ON guard.resource_id = ownership.guard_id
                WHERE ownership.activation_id = activation.activation_id
                ORDER BY ownership.guard_id FOR JSON PATH, INCLUDE_NULL_VALUES
            )) AS guards
        FROM [{schema}].[dbt_workspace_activations] AS activation WITH (HOLDLOCK)
        WHERE activation.activation_id = {identity}
        FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
    )"""
