"""Dependency-free redaction for text crossing process output boundaries."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any

REDACTION_TOKEN = "[REDACTED]"

_SENSITIVE_KEY = (
    r"passwords?|passwds?|pwds?|"
    r"(?:(?:access|auth|bearer|id|oauth|refresh|security|session|vault)[_-]?)?tokens?|"
    r"credentials?|authorizations?|"
    r"secret(?:s|[_-]?(?:names?|refs?|keys?|ids?|values?))?|"
    r"(?:api|access|private)[_-]?keys?|client[_-]?secrets?|"
    r"connection[_-]?strings?|lease[_-]?ids?"
)
_QUALIFIED_SENSITIVE_KEY = rf"(?<![a-z0-9_])(?:[a-z0-9]+(?:[._:/-]|\[))*(?:{_SENSITIVE_KEY})\]?"
_KEY_VALUE_RE = re.compile(
    rf"(?i)(?P<prefix>(?P<key>{_QUALIFIED_SENSITIVE_KEY})[\"']?\s*[:=]\s*)"
    r"(?:(?P<quote>[\"'])(?P<quoted>(?:\\[^\r\n]|(?!(?P=quote))[^\r\n])*)(?P=quote)|"
    r"(?P<bare>[^,;&#}\]\r\n]*?))"
    r"(?=(?:\s+(?:failed\s+at|after|at|because|before|during|from|via|while)\s+)|"
    rf"\s+(?=(?:--(?:{_SENSITIVE_KEY})\s|{_QUALIFIED_SENSITIVE_KEY}[\"']?\s*[:=]))|"
    r"[,;&#}\]\r\n]|$)"
)
_FLAG_RE = re.compile(
    rf"(?i)(?P<prefix>--(?:{_SENSITIVE_KEY})\s+)"
    r"(?:(?P<quote>[\"'])(?P<quoted>(?:\\[^\r\n]|(?!(?P=quote))[^\r\n])*)(?P=quote)|"
    r"(?P<bare>[^\s]+))"
)
_URI_USERINFO_RE = re.compile(r"(?i)(?P<prefix>\b[a-z][a-z0-9+.-]*://)(?P<userinfo>[^/\s?#]*@)")
_SENSITIVE_QUERY_KEY = (
    rf"(?:{_SENSITIVE_KEY}|(?:[a-z0-9]+[_-])+(?:password|passwd|pwd|token|signature|key|credential)|"
    r"key|signature|sig|credential|awsaccesskeyid|googleaccessid|"
    r"x-(?:amz|goog)-(?:credential|signature|security-token))"
)
_URI_QUERY_SECRET_RE = re.compile(rf"(?i)(?P<prefix>[?&;](?:{_SENSITIVE_QUERY_KEY})=)(?P<value>[^&#\s]*)")
_URI_FRAGMENT_RE = re.compile(r"(?i)(?P<prefix>\b[a-z][a-z0-9+.-]*://[^\s#]*)#[^\s]*")
_BEARER_RE = re.compile(r"(?i)(?P<prefix>\bauthorization\s*[:=]\s*bearer\s+|\bbearer\s+)(?P<token>[^\s,;]+)")
_PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN(?P<private_key_label>(?: [A-Z0-9]+)* PRIVATE KEY(?: BLOCK)?)-----"
    r".*?(?:-----END(?P=private_key_label)-----|$)",
    re.DOTALL,
)
_PATH_TERMINATOR = (
    r"(?=(?:\s+(?:after|at|because|before|during|from|via|while)\s+)|"
    r"\s+(?:and|or)\s+(?=(?:\$ABSOLUTE_PATH|(?:[A-Za-z]:)?[\\/]))|"
    r"[\r\n'\"<>,;&)\]}]|$)"
)
_LOCAL_URI_RE = re.compile(
    rf"(?i)\b(?:(?:file|unix|sqlite(?:\+[a-z0-9+.-]+)?)://|"
    rf"vscode(?:-insiders)?://file/|[a-z][a-z0-9+.-]*:///)"
    rf"[^\r\n'\"<>]*?{_PATH_TERMINATOR}"
)
_URI_PARAMETER_RE = re.compile(r"(?i)(?P<prefix>[?&;,|][a-z0-9_.-]+\s*=\s*)(?P<value>[^?&;,|\r\n'\"<>]*)")
_REMOTE_URI_RE = re.compile(
    r"(?i)\b(?P<scheme>[a-z][a-z0-9+.-]*)://[^\s'\"<>]*?"
    r"(?=(?:[,|][a-z0-9_.-]+\s*=)|[\s'\"<>]|$)"
)
_POSIX_ABSOLUTE_PATH_RE = re.compile(rf"(?<![\w/])/(?!/)[^\r\n'\"<>]*?{_PATH_TERMINATOR}")
_WINDOWS_DRIVE_ABSOLUTE_PATH_RE = re.compile(rf"(?<![\w])[A-Za-z]:[\\/][^\r\n'\"<>]*?{_PATH_TERMINATOR}")
_WINDOWS_UNC_ABSOLUTE_PATH_RE = re.compile(rf"(?<![\w])(?:\\\\|//)[^\r\n'\"<>]*?{_PATH_TERMINATOR}")
_WINDOWS_ROOT_ABSOLUTE_PATH_RE = re.compile(rf"(?<![\w\\])\\(?!\\)[^\r\n'\"<>]*?{_PATH_TERMINATOR}")
_ENCODED_ABSOLUTE_PATH_RE = re.compile(r"(?i)^(?:%2f|%5c|[a-z]%3a(?:%2f|%5c))")
_URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_FIELD_KEY_RE = re.compile(r"^[A-Za-z0-9_.:/\[\]-]+$")
_REDACTED_PATH = "$ABSOLUTE_PATH"
_DIRECT_SENSITIVE_KEY_TOKENS = frozenset(
    {
        "authorization",
        "credential",
        "credentials",
        "passwd",
        "password",
        "pwd",
        "token",
    }
)
_EXACT_SENSITIVE_KEYS = frozenset(
    {
        "access_key",
        "api_key",
        "client_secret",
        "connection_string",
        "lease_id",
        "private_key",
        "secret",
        "secret_id",
        "secret_key",
        "secret_name",
        "secret_ref",
        "secret_value",
        "secrets",
        "vault_path",
    }
)
_SENSITIVE_KEY_TOKEN_PAIRS = (
    frozenset({"access", "key"}),
    frozenset({"api", "key"}),
    frozenset({"client", "secret"}),
    frozenset({"connection", "string"}),
    frozenset({"private", "key"}),
    frozenset({"secret", "key"}),
)
_STRUCTURED_CREDENTIAL_KEYS = frozenset(
    {
        "credential",
        "credentials",
        "credential_resolution",
        "credential_runtime",
    }
)
_SAFE_RUNTIME_MOUNT_RE = re.compile(r"^/run/secrets/dpone/[^\s\x00\\]+$")
_SAFE_AIRFLOW_CONNECTION_KEY_RE = re.compile(r"^AIRFLOW_CONN_[A-Z0-9_]+$")
_SAFE_MANAGED_SECRET_NAME_RE = re.compile(r"^dpone-[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_SAFE_CREDENTIAL_FIELD_ALIASES = frozenset(
    {
        "api_key",
        "api_token",
        "database",
        "host",
        "key",
        "login",
        "pass",
        "passwd",
        "password",
        "port",
        "pwd",
        "token",
        "uri",
        "user",
        "username",
    }
)
_SINGULAR_KEY_TOKENS = {
    "authorizations": "authorization",
    "credentials": "credential",
    "keys": "key",
    "passwords": "password",
    "secrets": "secret",
    "strings": "string",
    "tokens": "token",
}


def redact_text(text: str, *, extra_secrets: Iterable[str] = ()) -> str:
    """Return text with common secret values removed.

    Logical identifiers such as ``connection_ref`` and resolver names remain
    visible. Values assigned to secret-bearing keys are hidden even when they
    are quoted JSON/YAML scalars.
    """

    redacted = str(text)
    for secret in tuple(dict.fromkeys(str(item) for item in extra_secrets if str(item))):
        redacted = redacted.replace(secret, REDACTION_TOKEN)
    redacted = _PRIVATE_KEY_RE.sub(REDACTION_TOKEN, redacted)
    redacted = _URI_USERINFO_RE.sub(
        lambda match: f"{match.group('prefix')}{REDACTION_TOKEN}@",
        redacted,
    )
    redacted = _URI_QUERY_SECRET_RE.sub(
        lambda match: f"{match.group('prefix')}{REDACTION_TOKEN}",
        redacted,
    )
    redacted = _URI_FRAGMENT_RE.sub(
        lambda match: f"{match.group('prefix')}#{REDACTION_TOKEN}",
        redacted,
    )
    redacted = _KEY_VALUE_RE.sub(_redacted_match, redacted)
    redacted = _BEARER_RE.sub(lambda match: f"{match.group('prefix')}{REDACTION_TOKEN}", redacted)
    return _FLAG_RE.sub(_redacted_match, redacted)


def redact_absolute_paths(text: str) -> str:
    """Replace physical POSIX and Windows paths at public output boundaries."""

    redacted = _LOCAL_URI_RE.sub(_REDACTED_PATH, str(text))
    redacted = _URI_PARAMETER_RE.sub(_redact_uri_parameter, redacted)
    public_parts: list[str] = []
    cursor = 0
    for match in _REMOTE_URI_RE.finditer(redacted):
        if len(match.group("scheme")) == 1:
            continue
        public_parts.append(_redact_path_fragment(redacted[cursor : match.start()]))
        public_parts.append(match.group(0))
        cursor = match.end()
    public_parts.append(_redact_path_fragment(redacted[cursor:]))
    return "".join(public_parts)


def public_path_label(value: object, *, fallback: str = "artifact") -> str:
    """Return a stable relative/URI locator without exposing a physical root."""

    text = str(value or "").strip()
    if not text:
        return ""
    if any(char in text for char in ("\x00", "\n", "\r")):
        return fallback
    normalized = text.replace("\\", "/")
    if _URI_RE.match(normalized):
        if normalized.lower().startswith("file://"):
            return fallback
        public_uri = _public_uri_label(normalized)
        parts = public_uri.split("/")
        return (
            public_uri
            if ".." not in parts and all(parts[index] or index in {1, 2} for index in range(len(parts)))
            else fallback
        )
    candidate = PurePosixPath(normalized)
    windows_absolute = bool(re.match(r"^[A-Za-z]:/", normalized))
    unsafe = candidate.is_absolute() or windows_absolute or ".." in candidate.parts
    if not unsafe:
        return candidate.as_posix()
    leaf = PureWindowsPath(text).name if windows_absolute else candidate.name
    return leaf or fallback


def redact_public_value(value: Any) -> Any:
    """Recursively redact secrets and physical paths without changing value shape."""

    if isinstance(value, Mapping):
        redacted: dict[Any, Any] = {}
        for key, item in value.items():
            public_key = _unique_mapping_key(_public_mapping_key(key), redacted)
            sensitive_value = isinstance(key, str) and _is_sensitive_key(key)
            structured_credentials = (
                isinstance(key, str)
                and isinstance(item, Mapping)
                and _normalized_key(key) in _STRUCTURED_CREDENTIAL_KEYS
            )
            safe_runtime_mount = (
                isinstance(key, str)
                and isinstance(item, str)
                and _normalized_key(key) == "mount_path"
                and _is_safe_runtime_mount_path(item)
            )
            safe_airflow_connection_key = (
                isinstance(key, str)
                and isinstance(item, str)
                and _normalized_key(key) == "secret_key"
                and bool(_SAFE_AIRFLOW_CONNECTION_KEY_RE.fullmatch(item))
            )
            safe_managed_secret_name = (
                isinstance(key, str)
                and isinstance(item, str)
                and _normalized_key(key) == "secret_name"
                and bool(_SAFE_MANAGED_SECRET_NAME_RE.fullmatch(item))
            )
            safe_credential_fields = (
                isinstance(key, str)
                and isinstance(item, Mapping)
                and _normalized_key(key) == "fields"
                and _is_safe_credential_field_mapping(item)
            )
            preserve_presence = item is None or isinstance(item, bool)
            redacted[public_key] = (
                REDACTION_TOKEN
                if sensitive_value
                and not preserve_presence
                and not structured_credentials
                and not safe_airflow_connection_key
                and not safe_managed_secret_name
                else item
                if (
                    safe_runtime_mount
                    or safe_airflow_connection_key
                    or safe_managed_secret_name
                    or safe_credential_fields
                )
                else redact_public_value(item)
            )
        return redacted
    if isinstance(value, list):
        return [redact_public_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_public_value(item) for item in value)
    if isinstance(value, str):
        return redact_absolute_paths(redact_text(value))
    return value


def redact_public_text(
    value: object,
    *,
    fallback: str,
    max_length: int | None = None,
    strip_traceback: bool = False,
) -> str:
    """Return bounded public text without allowing diagnostic formatting to fail."""

    try:
        text = str("" if value is None else value)
        if strip_traceback and "Traceback (most recent call last):" in text:
            text = next((line.strip() for line in reversed(text.splitlines()) if line.strip()), fallback)
        public_text = redact_absolute_paths(redact_text(text))
    except Exception:  # noqa: BLE001 - this function is itself a fail-closed boundary.
        public_text = fallback
    return public_text if max_length is None else public_text[:max_length]


def redact_public_context(values: Mapping[str, object], *, max_length: int = 128) -> dict[str, str]:
    """Bound and redact logical logging context without exposing object representations."""

    return {key: redact_public_text(value, fallback="", max_length=max_length) for key, value in values.items()}


def redact_value(value: Any) -> Any:
    """Backward-compatible alias for the canonical public-value redactor."""

    return redact_public_value(value)


def is_sensitive_field_name(key: str) -> bool:
    """Return whether a structured field name denotes secret material."""

    return _is_sensitive_key(key)


def _redacted_match(match: re.Match[str]) -> str:
    raw_value = match.group("quoted") or match.group("bare") or ""
    key = match.groupdict().get("key")
    if key is not None:
        if not _is_sensitive_key(key):
            return match.group(0)
        if _normalized_key(key) in {"planned_secrets", "secrets"} and raw_value.casefold() in {
            "executed",
            "false",
            "no",
            "none",
            "not",
            "planned",
            "true",
            "yes",
        }:
            return match.group(0)
    token_without_closing_bracket = REDACTION_TOKEN[:-1]
    already_redacted = raw_value == REDACTION_TOKEN or (
        raw_value == token_without_closing_bracket and match.string[match.end() :].startswith("]")
    )
    if already_redacted:
        return match.group(0)
    quote = match.group("quote") or ""
    return f"{match.group('prefix')}{quote}{REDACTION_TOKEN}{quote}"


def _is_sensitive_key(key: str) -> bool:
    stripped = key.strip()
    if (
        not _FIELD_KEY_RE.fullmatch(stripped)
        or "://" in stripped
        or stripped.startswith(("/", "\\"))
        or re.match(r"^[A-Za-z]:[\\/]", stripped)
    ):
        return False
    normalized = _normalized_key(stripped)
    if not normalized:
        return False
    if normalized in _EXACT_SENSITIVE_KEYS or normalized.endswith(("_lease_id", "_secret", "_secrets")):
        return True
    tokens = frozenset(_SINGULAR_KEY_TOKENS.get(token, token) for token in normalized.split("_") if token)
    return bool(tokens & _DIRECT_SENSITIVE_KEY_TOKENS) or any(
        pair.issubset(tokens) for pair in _SENSITIVE_KEY_TOKEN_PAIRS
    )


def _normalized_key(key: str) -> str:
    snake_case_key = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
    return re.sub(r"[^a-z0-9]+", "_", snake_case_key.lower()).strip("_")


def _is_safe_runtime_mount_path(value: str) -> bool:
    if not _SAFE_RUNTIME_MOUNT_RE.fullmatch(value):
        return False
    parts = PurePosixPath(value).parts
    return parts[:4] == ("/", "run", "secrets", "dpone") and ".." not in parts


def _is_safe_credential_field_mapping(value: Mapping[Any, Any]) -> bool:
    if not value:
        return False
    for key, item in value.items():
        if (
            not isinstance(key, str)
            or not isinstance(item, str)
            or _normalized_key(key) not in _SAFE_CREDENTIAL_FIELD_ALIASES
            or _normalized_key(item) not in _SAFE_CREDENTIAL_FIELD_ALIASES
        ):
            return False
    return True


def _public_mapping_key(key: Any) -> Any:
    if not isinstance(key, str):
        return key
    return redact_absolute_paths(redact_text(key))


def _unique_mapping_key(key: Any, mapping: Mapping[Any, Any]) -> Any:
    if key not in mapping:
        return key
    suffix = 2
    candidate = f"{key}~{suffix}"
    while candidate in mapping:
        suffix += 1
        candidate = f"{key}~{suffix}"
    return candidate


def _redact_path_fragment(text: str) -> str:
    redacted = _WINDOWS_UNC_ABSOLUTE_PATH_RE.sub(_REDACTED_PATH, text)
    redacted = _WINDOWS_DRIVE_ABSOLUTE_PATH_RE.sub(_REDACTED_PATH, redacted)
    redacted = _WINDOWS_ROOT_ABSOLUTE_PATH_RE.sub(_REDACTED_PATH, redacted)
    return _POSIX_ABSOLUTE_PATH_RE.sub(_REDACTED_PATH, redacted)


def _redact_uri_parameter(match: re.Match[str]) -> str:
    value = match.group("value")
    public_value = _REDACTED_PATH if _ENCODED_ABSOLUTE_PATH_RE.match(value) else _redact_path_fragment(value)
    return f"{match.group('prefix')}{public_value}"


def _public_uri_label(uri: str) -> str:
    """Remove non-diagnostic URI components from a public path label."""

    path_only = uri.partition("#")[0].partition("?")[0]
    scheme, authority_and_path = path_only.split("://", maxsplit=1)
    authority, separator, path = authority_and_path.partition("/")
    public_authority = authority.rpartition("@")[2]
    return f"{scheme}://{public_authority}{separator}{path}"


__all__ = [
    "REDACTION_TOKEN",
    "public_path_label",
    "redact_absolute_paths",
    "redact_public_context",
    "redact_public_text",
    "redact_public_value",
    "redact_text",
    "redact_value",
]
