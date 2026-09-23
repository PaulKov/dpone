"""Generate closed-document validation inside fixed owner-chained procedures.

The generated SQL recomputes canonical JSON and its digest independently of
Python. Field/type maps are installer-time constants, never runtime parameters.
This validates document framing, not domain predicates: each gateway operation
must additionally validate its exact coordinates, subjects, state and ownership.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from dpone.adapters.dbt_workspace_mssql_gateway_security import workspace_gateway_identifier


def workspace_document_validation(
    control_schema: str,
    *,
    variable: str,
    fields: Mapping[str, tuple[int, ...]],
    schema_name: str,
    digest_field: str,
    maximum_bytes: int,
    ensure_ascii: bool = True,
    normalize_slashes: bool = False,
) -> str:
    """Validate one witness object and declare its canonical/digest SQL locals.

    ``variable`` names an existing ``nvarchar(max)`` parameter without its ``@``.
    OPENJSON type codes describe every required field, including explicit nulls.
    The fragment declares ``@<variable>_canonical`` for durable storage and preserves
    the original variable for exact caller comparisons. Fixed procedure renderers
    must use a different variable name per document in the same SQL scope.
    """
    schema = workspace_gateway_identifier(control_schema)
    name = workspace_gateway_identifier(variable)
    digest_name = workspace_gateway_identifier(digest_field)
    if (
        re.fullmatch(r"[a-z][a-z0-9.-]{0,127}", schema_name) is None
        or type(maximum_bytes) is not int
        or not 1 <= maximum_bytes <= 32 * 1024 * 1024
        or len(name) > 80
        or type(ensure_ascii) is not bool
        or type(normalize_slashes) is not bool
        or not fields
        or fields.get("schema") != (1,)
        or fields.get(digest_name) != (1,)
    ):
        raise ValueError("workspace gateway document contract is invalid")
    shape = workspace_object_validation(variable=name, fields=fields)
    return f"""IF @{name} IS NULL OR DATALENGTH(@{name}) > {maximum_bytes * 2} OR ISJSON(@{name}) <> 1
    THROW 51000, 'workspace document bound differs', 1;
{shape}
IF JSON_VALUE(@{name}, N'$.schema') IS NULL
   OR CONVERT(varbinary(max), JSON_VALUE(@{name}, N'$.schema'))
   <> CONVERT(varbinary(max), N'{schema_name}')
    THROW 51000, 'workspace document schema differs', 1;
DECLARE @{name}_canonical nvarchar(max), @{name}_body nvarchar(max),
    @{name}_digest varchar(71), @{name}_claimed_digest nvarchar(4000);
EXEC [{schema}].[workspace_json_canonical] @{name}, @{name}_canonical OUTPUT, @ensure_ascii = {int(ensure_ascii)};
IF DATALENGTH(@{name}_canonical) > {maximum_bytes * 2}
    THROW 51000, 'workspace canonical document bound differs', 1;
SET @{name}_claimed_digest = JSON_VALUE(@{name}, N'$.{digest_name}');
SET @{name}_body = JSON_MODIFY(@{name}, N'$.{digest_name}', NULL);
EXEC [{schema}].[workspace_json_canonical] @{name}_body, @{name}_body OUTPUT,
    @ensure_ascii = {int(ensure_ascii)}, @normalize_slashes = {int(normalize_slashes)};
EXEC [{schema}].[workspace_json_utf8_sha256] @{name}_body, @{name}_digest OUTPUT;
IF @{name}_claimed_digest IS NULL OR CONVERT(varbinary(max), @{name}_claimed_digest)
   <> CONVERT(varbinary(max), CONVERT(nvarchar(71), @{name}_digest))
    THROW 51000, 'workspace document digest differs', 1;"""


def workspace_object_validation(*, variable: str, fields: Mapping[str, tuple[int, ...]]) -> str:
    """Check exact property membership/types for a nested already-parsed object."""
    name = workspace_gateway_identifier(variable)
    if len(name) > 80 or not fields:
        raise ValueError("workspace gateway object contract is invalid")
    rows = []
    for field, kinds in sorted(fields.items()):
        workspace_gateway_identifier(field)
        if not kinds or any(type(kind) is not int or kind not in range(6) for kind in kinds):
            raise ValueError("workspace gateway document type is invalid")
        rows.append(f"(N'{field}', {sum(1 << kind for kind in set(kinds))})")
    declarations = ", ".join(rows)
    return f"""IF (SELECT COUNT_BIG(*) FROM OPENJSON(@{name})) <> {len(fields)} OR EXISTS (
    SELECT 1 FROM OPENJSON(@{name}) AS actual
    FULL JOIN (VALUES {declarations}) AS expected(name, type_mask)
      ON CONVERT(varbinary(max), actual.[key]) = CONVERT(varbinary(max), expected.name)
    WHERE actual.[key] IS NULL OR expected.name IS NULL
       OR (expected.type_mask & CONVERT(int, POWER(2, actual.type))) = 0
) OR EXISTS (
    SELECT 1 FROM OPENJSON(@{name}) GROUP BY [key] COLLATE Latin1_General_100_BIN2
    HAVING COUNT_BIG(*) <> 1
)
    THROW 51000, 'workspace document fields differ', 1;"""
