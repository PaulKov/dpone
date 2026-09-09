"""Redact credential-bearing artifact URIs before diagnostic publication."""

from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit

_URI_IN_TEXT = re.compile(r"(?P<uri>[A-Za-z][A-Za-z0-9+.-]*://[^\s<>\"']+)")
_TRAILING_PUNCTUATION = ".,;)]}"


def redact_artifact_uri(uri: str) -> str:
    """Remove userinfo and every query/fragment value from diagnostic URIs."""

    uri_text = str(uri).replace("://[REDACTED]@", "://%5BREDACTED%5D@")
    try:
        parsed = urlsplit(uri_text)
    except ValueError:
        return _redact_malformed_uri(uri_text)
    netloc = parsed.netloc
    if "@" in netloc:
        netloc = f"%5BREDACTED%5D@{netloc.rsplit('@', 1)[1]}"
    fragment = "[REDACTED]" if parsed.fragment else ""
    return urlunsplit((parsed.scheme, netloc, parsed.path, _redact_query(parsed.query), fragment))


def redact_artifact_uris_in_text(message: object) -> str:
    """Redact every absolute URI embedded in a diagnostic string."""

    def replace(match: re.Match[str]) -> str:
        candidate = match.group("uri")
        uri = candidate.rstrip(_TRAILING_PUNCTUATION)
        suffix = candidate[len(uri) :]
        return f"{redact_artifact_uri(uri)}{suffix}"

    return _URI_IN_TEXT.sub(replace, str(message))


def _redact_query(query: str) -> str:
    if not query:
        return query
    redacted: list[str] = []
    for component in query.split("&"):
        raw_name, separator, _raw_value = component.partition("=")
        redacted.append(f"{raw_name}=[REDACTED]" if separator else "[REDACTED]")
    return "&".join(redacted)


def _redact_malformed_uri(uri: str) -> str:
    location_and_query, fragment_separator, fragment = uri.partition("#")
    location, query_separator, query = location_and_query.partition("?")
    scheme, authority_separator, remainder = location.partition("://")
    if authority_separator:
        authority, path_separator, path = remainder.partition("/")
        if "@" in authority:
            authority = f"%5BREDACTED%5D@{authority.rsplit('@', 1)[1]}"
        location = f"{scheme}{authority_separator}{authority}{path_separator}{path}"
    redacted_query = _redact_query(query) if query_separator else ""
    redacted_fragment = "[REDACTED]" if fragment_separator else fragment
    return f"{location}{query_separator}{redacted_query}{fragment_separator}{redacted_fragment}"


__all__ = ["redact_artifact_uri", "redact_artifact_uris_in_text"]
