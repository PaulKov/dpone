"""Shared canonical MSSQL AIP-60 Asset URI parser/codec.

Runtime authority for closed projection URIs (GitOps builder, deployment
projection validator, JSON Schema companion tests, Airflow provider).

Accepted path shapes (port always explicit in the closed form):

- ``mssql://{host}:{port}/{database}/{schema}/{table}`` (3 segments)
- ``mssql://{host}:{port}/{instance}/{database}/{schema}/{table}`` (4 segments)

Rules enforced by ``require_canonical_mssql_asset_uri``:

1. scheme is exactly ``mssql`` (case-sensitive on the wire form)
2. no userinfo / query / fragment
3. explicit port in ``1..65535``
4. 3 or 4 non-empty path segments
5. decoded segments are length-bounded
6. percent-decode then re-encode equals the on-wire segments
7. host and named instance are lowercase
8. ``canonicalize(parse(uri)) == uri``
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote, unquote, urlsplit

_MSSQL_SCHEME = "mssql"
_DEFAULT_PORT = 1433
MAX_MSSQL_ASSET_URI_CHARS = 2048
MAX_MSSQL_PATH_SEGMENT_CHARS = 128
MAX_MSSQL_HOST_CHARS = 253


@dataclass(frozen=True, slots=True)
class ParsedMssqlAssetUri:
    """Structured coordinates for one canonical MSSQL Asset URI."""

    host: str
    port: int
    instance: str | None
    database: str
    schema: str
    table: str

    def path_segments(self) -> tuple[str, ...]:
        if self.instance is not None:
            return (self.instance, self.database, self.schema, self.table)
        return (self.database, self.schema, self.table)


def quote_mssql_path_segment(value: str) -> str:
    """Percent-encode one path segment (no reserved characters left unescaped)."""

    return quote(value, safe="")


def canonicalize_mssql_asset_uri_parts(
    *,
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    instance: str | None = None,
) -> str:
    """Build one closed canonical ``mssql://`` URI from validated parts."""

    normalized_host = _normalize_host(host)
    normalized_instance = _normalize_instance(instance)
    if not normalized_host:
        raise ValueError("mssql Asset URI host is required")
    if not 1 <= int(port) <= 65535:
        raise ValueError("mssql Asset URI port must be an integer 1..65535")
    segments = {
        "database": database,
        "schema": schema,
        "table": table,
    }
    if normalized_instance is not None:
        segments = {"instance": normalized_instance, **segments}
    encoded: list[str] = []
    for name, raw in segments.items():
        text = (raw or "").strip()
        if not text:
            raise ValueError(f"mssql Asset URI requires {name}")
        if len(text) > MAX_MSSQL_PATH_SEGMENT_CHARS:
            raise ValueError(f"mssql Asset URI {name} exceeds {MAX_MSSQL_PATH_SEGMENT_CHARS} characters")
        encoded.append(quote_mssql_path_segment(text))
    uri = f"{_MSSQL_SCHEME}://{normalized_host}:{int(port)}/{'/'.join(encoded)}"
    if len(uri) > MAX_MSSQL_ASSET_URI_CHARS:
        raise ValueError(f"mssql Asset URI exceeds {MAX_MSSQL_ASSET_URI_CHARS} characters")
    return uri


def parse_mssql_asset_uri(uri: str, *, require_explicit_port: bool = True) -> ParsedMssqlAssetUri:
    """Parse one MSSQL Asset URI into structured coordinates.

    When ``require_explicit_port`` is true (closed projection / provider), the
    on-wire form must include ``:port``. Authoring helpers may pass false and
    default missing ports to ``1433``.
    """

    text = (uri or "").strip()
    if not text:
        raise ValueError("mssql Asset URI is empty")
    if len(text) > MAX_MSSQL_ASSET_URI_CHARS:
        raise ValueError(f"mssql Asset URI exceeds {MAX_MSSQL_ASSET_URI_CHARS} characters")
    if text.split("://", 1)[0] != _MSSQL_SCHEME:
        raise ValueError("mssql Asset URI scheme must be exactly 'mssql'")
    parsed = urlsplit(text)
    if parsed.scheme != _MSSQL_SCHEME:
        raise ValueError("mssql Asset URI scheme must be exactly 'mssql'")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("mssql Asset URI must not embed credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("mssql Asset URI must not include query or fragment")
    host = (parsed.hostname or "").strip()
    if not host or any(ch in host for ch in ("/", "?", "#", "@", ":", " ")):
        raise ValueError("mssql Asset URI host must be a bare hostname")
    if len(host) > MAX_MSSQL_HOST_CHARS:
        raise ValueError(f"mssql Asset URI host exceeds {MAX_MSSQL_HOST_CHARS} characters")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("mssql Asset URI port must be an integer 1..65535") from exc
    if port is None:
        if require_explicit_port:
            raise ValueError("mssql Asset URI requires an explicit port 1..65535")
        port = _DEFAULT_PORT
    if not 1 <= port <= 65535:
        raise ValueError("mssql Asset URI port must be an integer 1..65535")
    raw_segments = [part for part in parsed.path.split("/") if part]
    if len(raw_segments) not in {3, 4}:
        raise ValueError(
            "mssql Asset URI path must be host:port/database/schema/table or host:port/instance/database/schema/table"
        )
    decoded: list[str] = []
    for segment in raw_segments:
        value = unquote(segment, errors="strict")
        if not value or len(value) > MAX_MSSQL_PATH_SEGMENT_CHARS:
            raise ValueError("mssql Asset URI path segment is empty or too long")
        if quote_mssql_path_segment(value) != segment:
            raise ValueError("mssql Asset URI path segments must be canonical percent-encoding")
        decoded.append(value)
    if len(decoded) == 4:
        instance, database, schema, table = decoded
    else:
        instance, database, schema, table = None, decoded[0], decoded[1], decoded[2]
    return ParsedMssqlAssetUri(
        host=_normalize_host(host),
        port=port,
        instance=_normalize_instance(instance),
        database=database,
        schema=schema,
        table=table,
    )


def canonicalize_mssql_asset_uri(uri: str, *, require_explicit_port: bool = True) -> str:
    """Parse then re-emit the closed canonical form."""

    parsed = parse_mssql_asset_uri(uri, require_explicit_port=require_explicit_port)
    return canonicalize_mssql_asset_uri_parts(
        host=parsed.host,
        port=parsed.port,
        instance=parsed.instance,
        database=parsed.database,
        schema=parsed.schema,
        table=parsed.table,
    )


def require_canonical_mssql_asset_uri(uri: str) -> str:
    """Fail closed unless ``uri`` is already the closed canonical form."""

    text = (uri or "").strip()
    canonical = canonicalize_mssql_asset_uri(text, require_explicit_port=True)
    if canonical != text:
        raise ValueError("mssql Asset URI is not in closed canonical form")
    return canonical


def is_canonical_mssql_asset_uri(uri: str) -> bool:
    """Return whether ``uri`` is already the closed canonical form."""

    try:
        require_canonical_mssql_asset_uri(uri)
    except ValueError:
        return False
    return True


def _normalize_host(host: str) -> str:
    text = host.strip().rstrip(".").lower()
    if not text:
        return text
    try:
        return text.encode("idna").decode("ascii")
    except UnicodeError:
        return text


def _normalize_instance(instance: str | None) -> str | None:
    if instance is None:
        return None
    text = instance.strip()
    return text.lower() if text else None


__all__ = [
    "MAX_MSSQL_ASSET_URI_CHARS",
    "MAX_MSSQL_HOST_CHARS",
    "MAX_MSSQL_PATH_SEGMENT_CHARS",
    "ParsedMssqlAssetUri",
    "canonicalize_mssql_asset_uri",
    "canonicalize_mssql_asset_uri_parts",
    "is_canonical_mssql_asset_uri",
    "parse_mssql_asset_uri",
    "quote_mssql_path_segment",
    "require_canonical_mssql_asset_uri",
]
