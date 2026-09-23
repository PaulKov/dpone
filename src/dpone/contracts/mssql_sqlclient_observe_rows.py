"""Lossless, position-specific SQL catalog rows for the finite OBSERVE wire.

Encoding validates original driver scalars before transformation. This is wire
validation only; the existing catalog parsers remain responsible for admission.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

_ERROR = "mssql_native.sqlclient_observe_rows_invalid"
_SCHEMAS = {
    "WRITER_ADMISSION": "SSSSISUYISYIS",
    "PRINCIPALS": "ISYSS",
    "PERMISSIONS": "IIIIISSS",
    "MEMBER": "IISSSD",
    "OBJECT_PROPERTIES": "ISDSSIIII",
    "FEATURES": "IIIIIIIIIIII",
    "COLUMNS": "ISSBIIIsIBB",
    "BATCH_MEMBERS": "IISSSD",
    "BATCH_OBJECTS": "ISDSSIIII",
    "BATCH_FEATURES": "IIIIIIIIIIIII",
    "BATCH_COLUMNS": "IISSBIIIsIBB",
}


def _schema(opcode: str, row: list[Any] | tuple[Any, ...]) -> str:
    if opcode != "OWN_INCARNATION":
        try:
            return _SCHEMAS[opcode]
        except (KeyError, TypeError):
            raise ValueError(_ERROR) from None
    if len(row) != 68:
        raise ValueError(_ERROR)
    # Cardinality gates precede interpretation of MAX-projected values.
    for index in (19, 28, 39, 43, 45, 62, 66, 67):
        if type(row[index]) is not int or row[index] != (0 if index >= 66 else 1):
            raise ValueError(_ERROR)
    if type(row[51]) is not int or row[51] not in (1, 2):
        raise ValueError(_ERROR)
    schema = ["S"] * 68
    for index in (
        0,
        1,
        6,
        8,
        9,
        10,
        11,
        12,
        19,
        21,
        28,
        29,
        31,
        32,
        33,
        38,
        39,
        40,
        43,
        45,
        46,
        50,
        51,
        52,
        62,
        63,
        66,
        67,
    ):
        schema[index] = "I"
    for index in (13, 14):
        if row[index] is not None and (type(row[index]) is not int or row[index] not in (0, 1)):
            raise ValueError(_ERROR)
        schema[index] = "i"
    for index, kind in ((3, "Y"), (5, "Y"), (20, "U"), (44, "U"), (22, "D"), (30, "D"), (23, "N")):
        schema[index] = kind
    schema[57:62] = "NNNNN" if row[51] == 1 else "ISSSS"
    return "".join(schema)


def _scalar(kind: str, value: Any, *, decoding: bool) -> Any:
    if kind == "N" or kind in ("s", "i") and value is None:
        if value is not None:
            raise ValueError(_ERROR)
        return None
    kind = kind.upper()
    expected: Any = {"I": int, "S": str, "B": bool, "Y": bytes, "U": UUID, "D": datetime}[kind]
    if decoding and kind in ("Y", "U", "D"):
        if type(value) is not str:
            raise ValueError(_ERROR)
        try:
            parser: Any = {"Y": bytes.fromhex, "U": UUID, "D": datetime.fromisoformat}[kind]
            parsed = parser(value)
            if _scalar(kind, parsed, decoding=False) != value:
                raise ValueError(_ERROR)
            return parsed
        except (TypeError, ValueError):
            raise ValueError(_ERROR) from None
    if type(value) is not expected:
        raise ValueError(_ERROR)
    if kind == "I" and not -(2**31) <= value <= 2**31 - 1:
        raise ValueError(_ERROR)
    if kind == "Y":
        if not 1 <= len(value) <= 85:
            raise ValueError(_ERROR)
        return value.hex()
    if kind == "U":
        if type(value.int) is not int or not 0 < value.int < 2**128:
            raise ValueError(_ERROR)
        return str(value)
    if kind == "D":
        if value.tzinfo is not None:
            raise ValueError(_ERROR)
        return value.isoformat(timespec="microseconds")
    return value


def _rows(opcode: str, rows: Any, *, decoding: bool) -> list[Any]:
    if type(opcode) is not str or type(rows) not in (list, tuple):
        raise ValueError(_ERROR)
    result = []
    for row in rows:
        if type(row) not in ((list,) if decoding else (tuple, list)):
            raise ValueError(_ERROR)
        schema = _schema(opcode, row)
        if len(row) != len(schema):
            raise ValueError(_ERROR)
        values = [_scalar(kind, value, decoding=decoding) for kind, value in zip(schema, row, strict=True)]
        result.append(tuple(values) if decoding else values)
    return result


def encode_rows(opcode: str, rows: Any) -> list[Any]:
    """Project exact original driver rows to ordinary position-specific arrays."""
    return _rows(opcode, rows, decoding=False)


def decode_rows(opcode: str, rows: Any) -> list[Any]:
    """Restore bytes, UUID and naive datetime without modifying SQL strings."""
    return _rows(opcode, rows, decoding=True)


# Additive preparation projections keep their own strict scalar grammar.
ERROR = "mssql_native.sqlclient_preparation_rows_invalid"
OWNERSHIP_LABELS = (
    "server_role_member",
    "database_role_member",
    "database_owner",
    "server_principal_owner",
    "database_principal_owner",
    "schema_owner",
    "object_owner",
    "type",
    "xml_schema",
    "assembly",
    "certificate",
    "asymmetric_key",
    "symmetric_key",
    "fulltext_catalog",
    "fulltext_stoplist",
    "search_property_list",
    "message_type",
    "service_contract",
    "service",
    "remote_binding",
    "route",
    "database_credential",
    "endpoint",
    "external_library",
    "external_language",
)
OPCODE_LIMITS = {
    "PREP_ENV": 1,
    "PREP_OWNERSHIP_COUNTS": 25,
    "PREP_SERVER_PERMISSIONS": 4096,
    "PREP_EFFECTIVE": 1,
    "PREP_STAGE_SECURITY": 1,
    "PREP_PERMISSION_IDENTITIES": 1,
}
# Lowercase s means nullable text. C is a nonnegative signed64 COUNT_BIG.
SCHEMAS = {
    "PREP_ENV": "VEIIISISIBBISYSBISYSSY",
    "PREP_OWNERSHIP_COUNTS": "SC",
    "PREP_SERVER_PERMISSIONS": "IIIISSSSs",
    "PREP_STAGE_SECURITY": "IC",
    "subject": "SYISISBBS",
    "token": "IYSSS",
    "permission": "ssS",
}
# Fixed ENV order: version, edition, engine, major, compatibility, collation,
# database id/name, containment, trustworthy, chaining, login id/name/SID/type,
# disabled, user id/name/SID/type/authentication, database owner SID.


def _preparation_scalar(kind: str, value: Any, *, decode: bool) -> Any:
    if kind in ("s", "b") and value is None:
        return None
    if kind in ("S", "s", "V", "E"):
        if type(value) is not str or len(value) > {"V": 64, "E": 256}.get(kind, 128):
            raise ValueError(ERROR)
        value.encode("utf-8", errors="strict")
    elif kind == "Y":
        if decode:
            if type(value) is not str:
                raise ValueError(ERROR)
            restored = bytes.fromhex(value)
            if restored.hex() != value:
                raise ValueError(ERROR)
            value = restored
        if type(value) is not bytes or not 1 <= len(value) <= 85:
            raise ValueError(ERROR)
    elif (
        type(value) is not int
        or not {
            "I": -(2**31) <= value <= 2**31 - 1,
            "C": 0 <= value <= 2**63 - 1,
            "B": value in (0, 1),
            "b": value in (0, 1),
        }[kind]
    ):
        raise ValueError(ERROR)
    return value


def _row(schema: str, row: Any, *, decode: bool) -> tuple[Any, ...]:
    if type(row) not in (list, tuple) or len(row) != len(schema):
        raise ValueError(ERROR)
    return tuple(_preparation_scalar(kind, value, decode=decode) for kind, value in zip(schema, row, strict=True))


def effective(value: Any, *, decode: bool = False) -> dict[str, Any]:
    """Six fully terminated segments, including unchanged 68-field restoration."""

    keys = {"subject", "login_token", "user_token", "server_permissions", "database_permissions", "restored_management"}
    if type(value) is not dict or set(value) != keys:
        raise ValueError(ERROR)
    output = {"subject": _row(SCHEMAS["subject"], value["subject"], decode=decode)}
    for key, schema, limit in (
        ("login_token", "token", 8),
        ("user_token", "token", 8),
        ("server_permissions", "permission", 128),
        ("database_permissions", "permission", 128),
    ):
        rows = value[key]
        if type(rows) not in (list, tuple) or len(rows) > limit:
            raise ValueError(ERROR)
        output[key] = tuple(_row(SCHEMAS[schema], row, decode=decode) for row in rows)
    restored = value["restored_management"]
    output["restored_management"] = (
        decode_rows("OWN_INCARNATION", [restored])[0]
        if decode
        else decode_rows("OWN_INCARNATION", encode_rows("OWN_INCARNATION", [restored]))[0]
    )
    return output


def validate_rows(opcode: str, rows: Any, *, decode: bool = False) -> list[Any]:
    """Preserve positive ownership counts as evidence; policy rejects them later."""
    if opcode not in OPCODE_LIMITS or type(rows) not in (list, tuple) or len(rows) > OPCODE_LIMITS[opcode]:
        raise ValueError(ERROR)
    if opcode != "PREP_SERVER_PERMISSIONS" and len(rows) != OPCODE_LIMITS[opcode]:
        raise ValueError(ERROR)
    if opcode == "PREP_EFFECTIVE":
        return [effective(rows[0], decode=decode)]
    if opcode == "PREP_PERMISSION_IDENTITIES":
        return [permission_identities(rows[0], decode=decode)]
    result = [_row(SCHEMAS[opcode], row, decode=decode) for row in rows]
    if opcode == "PREP_OWNERSHIP_COUNTS" and tuple(row[0] for row in result) != OWNERSHIP_LABELS:
        raise ValueError(ERROR)
    return result


def encode_preparation_rows(opcode: str, rows: Any) -> list[Any]:
    """Reuse historical restoration encoding; new SQL counts alone use int64."""

    valid = validate_rows(opcode, rows)

    def wire(value: Any) -> Any:
        if type(value) is bytes:
            return value.hex()
        if type(value) in (tuple, list):
            return [wire(item) for item in value]
        if type(value) is dict:
            return {key: wire(item) for key, item in value.items()}
        return value

    if opcode == "PREP_EFFECTIVE":
        result = dict(valid[0])
        restored = result.pop("restored_management")
        body = wire(result)
        body["restored_management"] = encode_rows("OWN_INCARNATION", [restored])[0]
        return [body]
    return wire(valid)


def permission_identities(value: Any, *, decode: bool = False) -> dict[str, Any]:
    """Bound all raw resolver facts before semantic interpretation or admission."""
    from dpone.contracts.strict_json import canonical_json_bytes

    schemas = {
        "server_principals": ("ISYSB", 8194),
        "server_endpoints": ("ISSSSBB", 4096),
        "database_principals": ("ISYSS", 8194),
        "database_securables": ("IIISsssb", 4096),
    }
    if type(value) is not dict or set(value) != set(schemas):
        raise ValueError(ERROR)
    result = {}
    encoded = {}
    for key, (schema, maximum) in schemas.items():
        rows = value[key]
        if type(rows) not in (list, tuple) or len(rows) > maximum:
            raise ValueError(ERROR)
        result[key] = tuple(_row(schema, row, decode=decode) for row in rows)
        identifiers = [row[:3] if key == "database_securables" else row[:1] for row in result[key]]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError(ERROR)
        encoded[key] = [[item.hex() if type(item) is bytes else item for item in row] for row in result[key]]
    if len(canonical_json_bytes(encoded)) > 1024 * 1024:
        raise ValueError(ERROR)
    return result
