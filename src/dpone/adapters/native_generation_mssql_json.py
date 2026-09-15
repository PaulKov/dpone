"""Exact SQL JSON rendering shared by fixed native generation procedures."""

from __future__ import annotations


def utf8(expression: str) -> str:
    return f"CONVERT(varbinary(max), CONVERT(varchar(max), {expression} COLLATE Latin1_General_100_BIN2_UTF8))"


def decode_bytes(source: str, target: str) -> str:
    return f"""DECLARE {target}_utf8 TABLE(payload varchar(max) COLLATE Latin1_General_100_BIN2_UTF8);
INSERT INTO {target}_utf8(payload) VALUES ({source});
DECLARE {target} nvarchar(max)=(SELECT CONVERT(nvarchar(max),payload) FROM {target}_utf8);"""


def scalar(document: str, path: str) -> str:
    return f"(SELECT value FROM OPENJSON({document}) WITH (value nvarchar(max) '{path}'))"


def canonical_reference(document: str, path: str) -> str:
    """Encode a reference with JSON escapes matching the canonical UTF-8 codec."""
    locator = scalar(document, path + ".locator")
    digest = scalar(document, path + ".sha256")
    return (
        f'(CONVERT(nvarchar(max),N\'{{"locator":"\')'
        f"+REPLACE(STRING_ESCAPE({locator},'json'),NCHAR(92)+N'/',N'/')"
        f"+N'\",\"sha256\":\"'+REPLACE(STRING_ESCAPE({digest},'json'),NCHAR(92)+N'/',N'/')+N'\"}}')"
    )


def shape(expression: str, fields: dict[str, int]) -> str:
    expected = ",".join(f"(N'{name}',{kind})" for name, kind in fields.items())
    return f"""IF ISJSON({expression},OBJECT)<>1
 OR (SELECT COUNT(*) FROM OPENJSON({expression}))<>{len(fields)}
 OR EXISTS (SELECT 1 FROM (VALUES {expected}) e(name,kind)
 WHERE NOT EXISTS (SELECT 1 FROM OPENJSON({expression}) j WHERE j.[key]=e.name AND j.type=e.kind))
 THROW 51301, 'DPONE_NATIVE_GENERATION_JSON_SHAPE_INVALID', 1;"""
