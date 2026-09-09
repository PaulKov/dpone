"""Bounded read-only PyPI adapter for prepublication classification."""

from __future__ import annotations

import http.client
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from typing import Any, Final

if __package__:
    from .pypi_prepublication_codec import decode_json
    from .pypi_prepublication_contract import PYPI_INDEX_URL, fail
else:
    from pypi_prepublication_codec import decode_json
    from pypi_prepublication_contract import PYPI_INDEX_URL, fail

MAX_PYPI_RESPONSE_BYTES: Final = 4 * 1024 * 1024


def version_url(package: str, version: str) -> str:
    """Return the canonical exact-version JSON endpoint."""

    package_path = urllib.parse.quote(package, safe="")
    version_path = urllib.parse.quote(version, safe="")
    return f"{PYPI_INDEX_URL}/pypi/{package_path}/{version_path}/json"


def fetch_pypi_version(package: str, version: str) -> Mapping[str, Any] | None:
    """Fetch one exact PyPI version with bounded strict JSON decoding."""

    request = urllib.request.Request(
        version_url(package, version),
        headers={"Cache-Control": "no-cache", "Pragma": "no-cache", "User-Agent": "dpone-prepublication-gate/1"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            raw = response.read(MAX_PYPI_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise fail("PYPI_PREPUBLICATION_PUBLIC_HTTP_ERROR", package=package) from exc
    except (urllib.error.URLError, http.client.HTTPException, TimeoutError, OSError) as exc:
        raise fail("PYPI_PREPUBLICATION_PUBLIC_NETWORK_ERROR", package=package) from exc
    if len(raw) > MAX_PYPI_RESPONSE_BYTES:
        raise fail("PYPI_PREPUBLICATION_PUBLIC_RESPONSE_OVERSIZED", package=package)
    payload = decode_json(raw, invalid_code="PYPI_PREPUBLICATION_PUBLIC_JSON_INVALID")
    if not isinstance(payload, dict):
        raise fail("PYPI_PREPUBLICATION_PUBLIC_SHAPE_INVALID", package=package)
    return payload


def public_files(payload: Mapping[str, Any], *, package: str, version: str) -> tuple[Mapping[str, Any], ...]:
    """Validate and return the complete exact-version public file rows."""

    info, rows = payload.get("info"), payload.get("urls")
    if not isinstance(info, dict) or info.get("version") != version or not isinstance(rows, list) or not rows:
        raise fail("PYPI_PREPUBLICATION_PUBLIC_RELEASE_INVALID", package=package)
    if re.sub(r"[-_.]+", "-", str(info.get("name", "")).lower()) != package:
        raise fail("PYPI_PREPUBLICATION_PUBLIC_PROJECT_MISMATCH", package=package)
    if not all(isinstance(row, dict) for row in rows):
        raise fail("PYPI_PREPUBLICATION_PUBLIC_FILE_SHAPE_INVALID", package=package)
    filenames = [row.get("filename") for row in rows]
    if not all(isinstance(filename, str) and filename for filename in filenames) or len(filenames) != len(
        set(filenames)
    ):
        raise fail("PYPI_PREPUBLICATION_PUBLIC_FILENAME_INVALID", package=package)
    return tuple(rows)
