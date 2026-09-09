"""Optional driver and connection-option helpers for the MSSQL connector."""

from __future__ import annotations

from typing import Any

_ODBC_OPTION_ALIASES = {
    "multisubnetfailover": "MultiSubnetFailover",
    "applicationintent": "ApplicationIntent",
    "transparentnetworkipresolution": "TransparentNetworkIPResolution",
}
_BOOLEAN_ODBC_OPTIONS = {"MultiSubnetFailover", "TransparentNetworkIPResolution"}


def build_mssql_connection_string(
    *,
    driver: str,
    host: str,
    port: int,
    database: str,
    application_name: str,
    encrypt: str,
    trust_server_certificate: str,
    odbc_options: dict[str, str],
    user: str | None,
    password: str | None,
) -> str:
    """Build the exact pyodbc connection string from normalized settings."""

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={host},{port}",
        f"DATABASE={database}",
        f"APP={application_name}",
        f"Encrypt={encrypt}",
        f"TrustServerCertificate={trust_server_certificate}",
    ]
    parts.extend(f"{key}={value}" for key, value in odbc_options.items())
    if user:
        parts += [f"UID={user}", f"PWD={password or ''}"]
    else:
        parts.append("Trusted_Connection=yes")
    return ";".join(parts)


def require_pyodbc():
    try:
        import pyodbc
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency guard
        raise ModuleNotFoundError("pyodbc is required for MSSQL runtime. Install the 'mssql' extra.") from exc
    return pyodbc


def first_rowset(cursor: Any) -> tuple[list[Any], list[str]]:
    """Return the first row-bearing result set from a SQL Server batch."""

    while True:
        if cursor.description is not None:
            columns = [str(column[0]) for column in cursor.description]
            return list(cursor.fetchall()), columns
        nextset = getattr(cursor, "nextset", None)
        if nextset is None or not nextset():
            return [], []


def normalize_odbc_options(options: dict[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for raw_key, raw_value in options.items():
        option_name = _ODBC_OPTION_ALIASES.get(_odbc_option_token(raw_key))
        if option_name is None or raw_value in (None, ""):
            continue
        normalized[option_name] = _odbc_option_value(option_name, raw_value)
    return normalized


def _odbc_option_token(value: object) -> str:
    return "".join(character for character in str(value or "").lower() if character.isalnum())


def _odbc_option_value(option_name: str, value: object) -> str:
    if option_name in _BOOLEAN_ODBC_OPTIONS:
        return "Yes" if _bool_option(value) else "No"
    text = str(value).strip()
    if ";" in text or "\x00" in text:
        raise ValueError(f"Unsafe ODBC option value for {option_name}")
    return text


def _bool_option(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}
