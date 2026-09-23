"""Render private, bounded JSON/UTF-8 primitives for the SQL workspace gateway.

These fixed procedures run through the same-owner gateway chain, never through
runtime EXECUTE grants. SQL Server 2016 SP1+ and compatibility level 130 are
required. No UTF-8 collation, CLR, dynamic SQL or Python-supplied digest is used.
Rendering is not execution or certification; live SQL/Python parity is required
before installation in an approved environment.
"""

from __future__ import annotations

from dpone.adapters.dbt_workspace_mssql_gateway_security import workspace_gateway_identifier


def render_workspace_gateway_json(control_schema: str) -> tuple[str, ...]:
    """Return independently executable CREATE OR ALTER batches, without GO."""
    schema = workspace_gateway_identifier(control_schema)
    return (_escape(schema), _utf8_sha256(schema), _canonical(schema))


def _units(source: str) -> str:
    # A bounded tally works on UTF-16 code units even under a default SC collation.
    return f"""
;WITH digits(n) AS (
    SELECT n FROM (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d(n)
), positions(n) AS (
    SELECT TOP (CONVERT(int, DATALENGTH({source}) / 2))
        CONVERT(int, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))
    FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d
    CROSS JOIN digits e CROSS JOIN digits f CROSS JOIN digits g CROSS JOIN digits h
), units AS (
    SELECT n, UNICODE(SUBSTRING({source} COLLATE Latin1_General_100_BIN2, n, 1)) AS u,
        UNICODE(SUBSTRING({source} COLLATE Latin1_General_100_BIN2, n - 1, 1)) AS p,
        UNICODE(SUBSTRING({source} COLLATE Latin1_General_100_BIN2, n + 1, 1)) AS q
    FROM positions
)
""".strip()


def _unicode_check(source: str) -> str:
    return f"""
{_units(source)}
SELECT @invalid = COUNT_BIG(*) FROM units
WHERE (u BETWEEN 55296 AND 56319 AND (q IS NULL OR q NOT BETWEEN 56320 AND 57343))
   OR (u BETWEEN 56320 AND 57343 AND (p IS NULL OR p NOT BETWEEN 55296 AND 56319));
IF @invalid <> 0 THROW 51000, 'workspace JSON surrogate is invalid', 1;
""".strip()


def _escape(schema: str) -> str:
    return rf"""CREATE OR ALTER PROCEDURE [{schema}].[workspace_json_escape]
    @value nvarchar(max), @result nvarchar(max) OUTPUT, @ensure_ascii bit = 1
AS
BEGIN
    SET NOCOUNT ON;
    IF @value IS NULL OR DATALENGTH(@value) > 67108864 OR @ensure_ascii IS NULL
        THROW 51000, 'workspace JSON string bound differs', 1;
    IF @value COLLATE Latin1_General_100_BIN2 NOT LIKE N'%[^ -~]%' COLLATE Latin1_General_100_BIN2
    BEGIN
        SET @result = N'"' + REPLACE(STRING_ESCAPE(@value, 'json'), N'\/', N'/') + N'"';
        RETURN;
    END;
    DECLARE @invalid bigint;
    {_unicode_check("@value")}
    -- STRING_ESCAPE differs from Python only for slash and non-ASCII characters.
    DECLARE @escaped nvarchar(max) = REPLACE(STRING_ESCAPE(@value, 'json'), N'\/', N'/');
    IF DATALENGTH(@escaped) > 67108864
        THROW 51000, 'workspace JSON escaped string bound differs', 1;
    IF @ensure_ascii = 0
    BEGIN
        SET @result = N'"' + @escaped + N'"';
        RETURN;
    END;
    {_units("@escaped")}
    SELECT @result = N'"' + COALESCE((
        SELECT CASE WHEN u >= 127 THEN N'\u' + LOWER(CONVERT(varchar(4), CONVERT(binary(2), u), 2))
                    ELSE SUBSTRING(@escaped COLLATE Latin1_General_100_BIN2, n, 1) END AS [text()]
        FROM units ORDER BY n FOR XML PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), N'') + N'"';
END;"""


def _utf8_sha256(schema: str) -> str:
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[workspace_json_utf8_sha256]
    @value nvarchar(max), @result varchar(71) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    IF @value IS NULL OR DATALENGTH(@value) > 67108864
        THROW 51000, 'workspace UTF-8 input bound differs', 1;
    DECLARE @invalid bigint, @encoded varchar(max), @bytes varbinary(max);
    IF @value COLLATE Latin1_General_100_BIN2 NOT LIKE N'%[^ -~]%' COLLATE Latin1_General_100_BIN2
    BEGIN
        SET @bytes = CONVERT(varbinary(max), CONVERT(varchar(max), @value));
        SET @result = 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', @bytes), 2));
        RETURN;
    END;
    {_unicode_check("@value")}
    {_units("@value")}, points AS (
        SELECT n, CASE WHEN u BETWEEN 55296 AND 56319
            THEN 65536 + (u - 55296) * 1024 + (q - 56320) ELSE u END AS point
        FROM units WHERE u NOT BETWEEN 56320 AND 57343
    )
    SELECT @encoded = COALESCE((
        SELECT CASE
            WHEN point < 128 THEN CONVERT(varchar(2), CONVERT(binary(1), point), 2)
            WHEN point < 2048 THEN
                CONVERT(varchar(2), CONVERT(binary(1), 192 + point / 64), 2) +
                CONVERT(varchar(2), CONVERT(binary(1), 128 + point % 64), 2)
            WHEN point < 65536 THEN
                CONVERT(varchar(2), CONVERT(binary(1), 224 + point / 4096), 2) +
                CONVERT(varchar(2), CONVERT(binary(1), 128 + (point / 64) % 64), 2) +
                CONVERT(varchar(2), CONVERT(binary(1), 128 + point % 64), 2)
            ELSE
                CONVERT(varchar(2), CONVERT(binary(1), 240 + point / 262144), 2) +
                CONVERT(varchar(2), CONVERT(binary(1), 128 + (point / 4096) % 64), 2) +
                CONVERT(varchar(2), CONVERT(binary(1), 128 + (point / 64) % 64), 2) +
                CONVERT(varchar(2), CONVERT(binary(1), 128 + point % 64), 2)
            END AS [text()]
        FROM points ORDER BY n FOR XML PATH(''), TYPE
    ).value('.', 'varchar(max)'), '');
    SET @bytes = CONVERT(varbinary(max), @encoded, 2);
    IF DATALENGTH(@bytes) > 33554432
        THROW 51000, 'workspace UTF-8 output bound differs', 1;
    SET @result = 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', @bytes), 2));
END;"""


def _canonical(schema: str) -> str:
    return f"""CREATE OR ALTER PROCEDURE [{schema}].[workspace_json_canonical]
    @document nvarchar(max), @result nvarchar(max) OUTPUT, @depth int = 0, @ensure_ascii bit = 1
AS
BEGIN
    SET NOCOUNT ON;
    IF @document IS NULL OR DATALENGTH(@document) > 67108864 OR ISJSON(@document) <> 1
       OR @depth IS NULL OR @depth < 0 OR @depth > 16 OR @ensure_ascii IS NULL
        THROW 51000, 'workspace JSON document bound differs', 1;
    DECLARE @first nchar(1) = LEFT(LTRIM(REPLACE(REPLACE(REPLACE(
        @document, NCHAR(9), N' '), NCHAR(10), N' '), NCHAR(13), N' ')), 1);
    IF @first NOT IN (N'{{', N'[')
        THROW 51000, 'workspace JSON root is not a collection', 1;
    DECLARE @members TABLE (
        ordinal int IDENTITY(1,1), [key] nvarchar(4000) COLLATE Latin1_General_100_BIN2,
        value nvarchar(max), kind int
    );
    INSERT @members ([key], value, kind) SELECT [key], value, type FROM OPENJSON(@document);
    IF (SELECT COUNT_BIG(*) FROM @members) > 8192
        THROW 51000, 'workspace JSON member bound differs', 1;
    IF EXISTS (SELECT [key] FROM @members GROUP BY [key] HAVING COUNT_BIG(*) <> 1)
        THROW 51000, 'workspace JSON duplicate key', 1;
    IF @first = N'{{' AND EXISTS (
        SELECT 1 FROM @members WHERE DATALENGTH([key]) NOT BETWEEN 2 AND 256
            OR [key] COLLATE Latin1_General_100_BIN2 LIKE N'%[^a-zA-Z0-9_]%' COLLATE Latin1_General_100_BIN2
    ) THROW 51000, 'workspace JSON property differs', 1;
    DECLARE @key nvarchar(4000), @value nvarchar(max), @kind int, @piece nvarchar(max),
        @quoted nvarchar(max), @separator nvarchar(1) = N'', @next_depth int = @depth + 1;
    SET @result = @first;
    DECLARE members CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], value, kind FROM @members
        ORDER BY CASE WHEN @first = N'[' THEN TRY_CONVERT(int, [key]) ELSE 0 END, [key];
    OPEN members;
    FETCH NEXT FROM members INTO @key, @value, @kind;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @piece = NULL;
        IF @kind = 0 SET @piece = N'null';
        ELSE IF @kind = 1 EXEC [{schema}].[workspace_json_escape] @value, @piece OUTPUT, @ensure_ascii;
        ELSE IF @kind = 2
        BEGIN
            IF TRY_CONVERT(bigint, @value) IS NULL
               OR @value COLLATE Latin1_General_100_BIN2 LIKE N'%[^0-9-]%' COLLATE Latin1_General_100_BIN2
                THROW 51000, 'workspace JSON number is not an integer', 1;
            SET @piece = CONVERT(nvarchar(20), CONVERT(bigint, @value));
        END
        ELSE IF @kind = 3 SET @piece = @value;
        ELSE IF @kind IN (4, 5)
            EXEC [{schema}].[workspace_json_canonical] @value, @piece OUTPUT, @next_depth, @ensure_ascii;
        IF @piece IS NULL THROW 51000, 'workspace JSON member type differs', 1;
        SET @result = @result + @separator;
        IF @first = N'{{'
        BEGIN
            EXEC [{schema}].[workspace_json_escape] @key, @quoted OUTPUT;
            SET @result = @result + @quoted + N':';
        END;
        SET @result = @result + @piece;
        IF DATALENGTH(@result) > 67108864
            THROW 51000, 'workspace JSON canonical output bound differs', 1;
        SET @separator = N',';
        FETCH NEXT FROM members INTO @key, @value, @kind;
    END;
    CLOSE members;
    DEALLOCATE members;
    SET @result = @result + CASE WHEN @first = N'{{' THEN N'}}' ELSE N']' END;
END;"""
