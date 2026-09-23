"""Private SQL validation of the complete original workspace activation request.

The existing fingerprint normalizes backslashes in string values. Durable replay
must still retain the unnormalized original request/guard identifiers. These are
separate outputs, not permission to rewrite historical physical observations.
This renderer does not install procedures or reserve any guard.
"""

from __future__ import annotations

from dpone.adapters.dbt_workspace_mssql_gateway_security import workspace_gateway_identifier
from dpone.adapters.dbt_workspace_mssql_gateway_validation import (
    workspace_document_validation,
    workspace_object_validation,
)

_REQUEST_FIELDS = {
    **{
        name: (1,)
        for name in (
            "schema",
            "activation_id",
            "environment",
            "release_id",
            "deployment_id",
            "source_inventory_sha256",
            "runtime_context_sha256",
            "request_sha256",
        )
    },
    "previous_deployment_id": (0, 1),
    "write_subjects": (4,),
    "resources": (4,),
}
_RESOURCE_FIELDS = {
    **{
        name: (1,)
        for name in (
            "guard_id",
            "connector",
            "service_authority_sha256",
            "target_authority_sha256",
            "observation_sha256",
        )
    },
    "write_subjects": (4,),
}


def _invalid_digest(value: str) -> str:
    return f"""({value} IS NULL OR DATALENGTH({value}) <> 142
        OR CONVERT(varbinary(max), LEFT({value}, 7)) <> CONVERT(varbinary(max), N'sha256:')
        OR SUBSTRING({value}, 8, 64) COLLATE Latin1_General_100_BIN2
           LIKE N'%[^0-9a-f]%' COLLATE Latin1_General_100_BIN2)"""


def render_workspace_request_validation(control_schema: str) -> str:
    """Render the closed original request validator, callable only by gateways.

    Only workspace request/resource shapes use slash-normalized hashing. Their
    closed arrays contain no ``id``-bearing object collections, so the separate
    legacy artifact-list normalization rule is inapplicable. Resource ordering uses
    UTF-8 bytes to match Python Unicode-code-point ordering, not database collation.
    """
    schema = workspace_gateway_identifier(control_schema)
    framing = workspace_document_validation(
        schema,
        variable="request",
        fields=_REQUEST_FIELDS,
        schema_name="dpone.dbt-workspace-activation-request.v1",
        digest_field="request_sha256",
        maximum_bytes=16 * 1024 * 1024,
        ensure_ascii=False,
        normalize_slashes=True,
    )
    resource_shape = workspace_object_validation(variable="resource", fields=_RESOURCE_FIELDS)
    digests = " OR ".join(
        _invalid_digest(f"JSON_VALUE(@request, N'$.{name}')")
        for name in (
            "release_id",
            "deployment_id",
            "source_inventory_sha256",
            "runtime_context_sha256",
        )
    )
    resource_digests = " OR ".join(
        _invalid_digest(f"JSON_VALUE(@resource, N'$.{name}')")
        for name in (
            "service_authority_sha256",
            "target_authority_sha256",
            "observation_sha256",
        )
    )
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[workspace_request_require]
    @request nvarchar(max), @canonical nvarchar(max) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    {framing}
    DECLARE @raw_digest varchar(71), @utf8 varbinary(max);
    EXEC [{schema}].[workspace_json_utf8_sha256] @request, @raw_digest OUTPUT, @utf8 OUTPUT;
    IF DATALENGTH(@utf8) > 16777216 THROW 51000, 'workspace request raw byte bound differs', 1;
    EXEC [{schema}].[workspace_json_utf8_sha256] @request_canonical, @raw_digest OUTPUT, @utf8 OUTPUT;
    IF DATALENGTH(@utf8) > 16777216 THROW 51000, 'workspace request canonical byte bound differs', 1;
    DECLARE @identity nvarchar(4000) = JSON_VALUE(@request, N'$.activation_id'),
        @environment nvarchar(4000) = JSON_VALUE(@request, N'$.environment'),
        @previous nvarchar(max);
    SELECT @previous = value FROM OPENJSON(@request) WHERE [key] = N'previous_deployment_id';
    IF @identity IS NULL OR DATALENGTH(@identity) <> 72 OR TRY_CONVERT(uniqueidentifier, @identity) IS NULL
       OR CONVERT(varbinary(max), @identity) <>
          CONVERT(varbinary(max), LOWER(CONVERT(nvarchar(36), TRY_CONVERT(uniqueidentifier, @identity))))
        THROW 51000, 'workspace request UUID differs', 1;
    IF @environment IS NULL OR DATALENGTH(@environment) NOT BETWEEN 2 AND 126
       OR LEFT(@environment, 1) COLLATE Latin1_General_100_BIN2 NOT LIKE N'[a-z0-9]' COLLATE Latin1_General_100_BIN2
       OR @environment COLLATE Latin1_General_100_BIN2 LIKE N'%[^a-z0-9_-]%' COLLATE Latin1_General_100_BIN2
        THROW 51000, 'workspace request environment differs', 1;
    IF {digests} OR (@previous IS NOT NULL AND {_invalid_digest("@previous")})
        THROW 51000, 'workspace request digest coordinate differs', 1;
    DECLARE @subjects TABLE (ordinal int NOT NULL PRIMARY KEY, subject nvarchar(max));
    INSERT @subjects SELECT CONVERT(int, [key]), value FROM OPENJSON(@request, N'$.write_subjects');
    IF (SELECT COUNT_BIG(*) FROM @subjects) NOT BETWEEN 1 AND 8192 OR EXISTS (
        SELECT 1 FROM OPENJSON(@request, N'$.write_subjects') WHERE type <> 1 OR {_invalid_digest("value")}
    ) THROW 51000, 'workspace request write subjects differ', 1;
    IF EXISTS (
        SELECT 1 FROM (SELECT subject, LAG(subject) OVER (ORDER BY ordinal) AS previous FROM @subjects) AS ordered
        WHERE previous IS NOT NULL AND subject COLLATE Latin1_General_100_BIN2 <= previous COLLATE Latin1_General_100_BIN2
    ) THROW 51000, 'workspace request write order differs', 1;
    DECLARE @resources TABLE (ordinal int NOT NULL PRIMARY KEY, body nvarchar(max));
    INSERT @resources SELECT CONVERT(int, [key]), value FROM OPENJSON(@request, N'$.resources');
    IF (SELECT COUNT_BIG(*) FROM @resources) NOT BETWEEN 1 AND 8192 OR EXISTS (
        SELECT 1 FROM OPENJSON(@request, N'$.resources') WHERE type <> 5
    ) THROW 51000, 'workspace request resources differ', 1;
    DECLARE @partition TABLE (subject nvarchar(max));
    DECLARE @resource_subjects TABLE (ordinal int NOT NULL PRIMARY KEY, subject nvarchar(max));
    DECLARE @ordinal int = 0, @resource nvarchar(max), @guard nvarchar(4000), @connector nvarchar(4000),
        @last_guard varbinary(2048) = NULL, @guard_bytes varbinary(max), @position int;
    WHILE @ordinal < (SELECT COUNT_BIG(*) FROM @resources)
    BEGIN
        SELECT @resource = body FROM @resources WHERE ordinal = @ordinal;
        {resource_shape}
        SET @guard = JSON_VALUE(@resource, N'$.guard_id');
        SET @connector = JSON_VALUE(@resource, N'$.connector');
        -- Existing protected guard columns hold at most 512 UTF-16 units.
        IF @guard IS NULL OR DATALENGTH(@guard) NOT BETWEEN 2 AND 1024
            THROW 51000, 'workspace request physical guard storage bound differs', 1;
        SET @position = 1;
        WHILE @position <= DATALENGTH(@guard) / 2
        BEGIN
            IF UNICODE(SUBSTRING(@guard COLLATE Latin1_General_100_BIN2, @position, 1)) = 0
                THROW 51000, 'workspace request physical guard contains NUL', 1;
            SET @position += 1;
        END;
        IF @connector IS NULL OR CONVERT(varbinary(max), @connector) NOT IN (
            CONVERT(varbinary(max), N'mssql'), CONVERT(varbinary(max), N'clickhouse'), CONVERT(varbinary(max), N'postgres')
        ) OR {resource_digests}
            THROW 51000, 'workspace request physical resource differs', 1;
        EXEC [{schema}].[workspace_json_utf8_sha256] @guard, @raw_digest OUTPUT, @guard_bytes OUTPUT;
        IF @last_guard IS NOT NULL AND @guard_bytes <= @last_guard
            THROW 51000, 'workspace request resource order differs', 1;
        SET @last_guard = @guard_bytes;
        DELETE FROM @resource_subjects;
        INSERT @resource_subjects SELECT CONVERT(int, [key]), value FROM OPENJSON(@resource, N'$.write_subjects');
        IF (SELECT COUNT_BIG(*) FROM @resource_subjects) NOT BETWEEN 1 AND 8192 OR EXISTS (
            SELECT 1 FROM OPENJSON(@resource, N'$.write_subjects') WHERE type <> 1 OR {_invalid_digest("value")}
        ) THROW 51000, 'workspace request resource subjects differ', 1;
        IF EXISTS (
            SELECT 1 FROM (SELECT subject, LAG(subject) OVER (ORDER BY ordinal) AS previous FROM @resource_subjects) AS ordered
            WHERE previous IS NOT NULL AND subject COLLATE Latin1_General_100_BIN2 <= previous COLLATE Latin1_General_100_BIN2
        ) THROW 51000, 'workspace request resource write order differs', 1;
        INSERT @partition SELECT subject FROM @resource_subjects;
        IF (SELECT COUNT_BIG(*) FROM @partition) > 8192
            THROW 51000, 'workspace request resource partition bound differs', 1;
        SET @ordinal += 1;
    END;
    IF (SELECT COUNT_BIG(*) FROM @subjects) <> (SELECT COUNT_BIG(*) FROM @partition) OR EXISTS (
        SELECT 1 FROM @partition GROUP BY subject COLLATE Latin1_General_100_BIN2 HAVING COUNT_BIG(*) <> 1
    ) OR EXISTS (
        SELECT 1 FROM @subjects AS expected FULL JOIN @partition AS actual
          ON CONVERT(varbinary(max), expected.subject) = CONVERT(varbinary(max), actual.subject)
        WHERE expected.subject IS NULL OR actual.subject IS NULL
    ) THROW 51000, 'workspace request resource partition differs', 1;
    SET @canonical = @request_canonical;
END;"""
