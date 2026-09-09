"""Small GitHub REST API client helpers for agent receipt checks."""

from __future__ import annotations

import importlib.util
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

GITHUB_API_ROOT = "https://api.github.com"
REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


resource_limits = _load_sibling(
    "dpone_agent_pr_receipt_artifact_resource_limits",
    "artifact_resource_limits.py",
)


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def github_json(path_or_url: str, *, token: str, fresh: bool = False) -> dict[str, Any]:
    """Return a GitHub API response that must be a JSON object."""

    payload = github_request(path_or_url, token=token, fresh=fresh)
    if not isinstance(payload, dict):
        raise RuntimeError("GitHub API returned a non-object response.")
    return payload


def post_github_json(path_or_url: str, *, token: str, payload: dict[str, Any]) -> dict[str, Any]:
    """POST one JSON object and require a JSON object response."""

    return _send_github_json(path_or_url, token=token, payload=payload, method="POST")


def patch_github_json(path_or_url: str, *, token: str, payload: dict[str, Any]) -> dict[str, Any]:
    """PATCH one JSON object and require a JSON object response."""

    return _send_github_json(path_or_url, token=token, payload=payload, method="PATCH")


def _send_github_json(
    path_or_url: str,
    *,
    token: str,
    payload: dict[str, Any],
    method: str,
) -> dict[str, Any]:
    url = github_url(path_or_url)
    request = github_api_request(
        url,
        token=token,
        method=method,
        data=json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"),
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            raw = resource_limits.read_bounded(
                response,
                max_bytes=resource_limits.MAX_GITHUB_JSON_BYTES,
                resource="GitHub API JSON response",
            )
            decoded = json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"GitHub API request failed for {url}: {exc.code} {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"GitHub API request failed for {url}: {exc.reason}") from exc
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"GitHub API returned invalid JSON for {url}") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError("GitHub API returned a non-object response.")
    return decoded


def github_paginated_items(path_or_url: str, *, token: str, array_key: str | None = None) -> list[dict[str, Any]]:
    """Return bounded item objects from a non-cyclic GitHub API pagination chain."""

    items: list[dict[str, Any]] = []
    next_url: str | None = github_url(path_or_url)
    visited_urls: set[str] = set()
    page_count = 0
    item_count = 0
    while next_url:
        current_url = github_url(next_url)
        if current_url in visited_urls:
            raise ValueError("GitHub API pagination cycle detected")
        if page_count >= resource_limits.MAX_GITHUB_API_PAGES:
            raise ValueError(f"GitHub API pagination page limit {resource_limits.MAX_GITHUB_API_PAGES} exceeded")
        visited_urls.add(current_url)
        page_count += 1
        payload, link = github_request_with_link(current_url, token=token)
        if array_key:
            page_items = payload.get(array_key, []) if isinstance(payload, dict) else []
        else:
            page_items = payload if isinstance(payload, list) else []
        if not isinstance(page_items, list):
            page_items = []
        item_count += len(page_items)
        if item_count > resource_limits.MAX_GITHUB_API_ITEMS:
            raise ValueError(f"GitHub API pagination item limit {resource_limits.MAX_GITHUB_API_ITEMS} exceeded")
        items.extend(item for item in page_items if isinstance(item, dict))
        next_url = _next_link(link)
    return items


def github_bytes(path_or_url: str, *, token: str) -> bytes:
    """Return raw bytes from a GitHub API endpoint."""

    url = github_url(path_or_url)
    request = github_api_request(url, token=token)
    opener = urllib.request.build_opener(_NoRedirectHandler)
    try:
        with opener.open(request, timeout=30) as response:  # noqa: S310
            return resource_limits.read_bounded(
                response,
                max_bytes=resource_limits.MAX_COMPRESSED_ARTIFACT_BYTES,
                resource="GitHub artifact response",
            )
    except urllib.error.HTTPError as exc:
        location = exc.headers.get("Location")
        if exc.code in REDIRECT_STATUS_CODES and location:
            return public_bytes(location)
        raise RuntimeError(f"GitHub API request failed for {url}: {exc.code} {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"GitHub API request failed for {url}: {exc.reason}") from exc


def github_request(path_or_url: str, *, token: str, fresh: bool = False) -> Any:
    """Return decoded JSON from a GitHub API request."""

    payload, _link = github_request_with_link(path_or_url, token=token, fresh=fresh)
    return payload


def github_request_with_link(
    path_or_url: str,
    *,
    token: str,
    fresh: bool = False,
) -> tuple[Any, str | None]:
    """Return decoded JSON and the optional Link header from GitHub."""

    url = github_url(path_or_url)
    request = github_api_request(url, token=token, fresh=fresh)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            raw = resource_limits.read_bounded(
                response,
                max_bytes=resource_limits.MAX_GITHUB_JSON_BYTES,
                resource="GitHub API JSON response",
            )
            return json.loads(raw.decode("utf-8")), response.headers.get("Link")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"GitHub API request failed for {url}: {exc.code} {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"GitHub API request failed for {url}: {exc.reason}") from exc


def github_url(path_or_url: str) -> str:
    """Convert a GitHub REST path to an absolute API URL."""

    if path_or_url.startswith("https://"):
        return path_or_url
    return f"{GITHUB_API_ROOT}/{path_or_url.lstrip('/')}"


def github_api_request(
    url: str,
    *,
    token: str,
    method: str = "GET",
    data: bytes | None = None,
    fresh: bool = False,
) -> urllib.request.Request:
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if fresh:
        headers.update({"Cache-Control": "no-cache", "Pragma": "no-cache"})
    return urllib.request.Request(
        url,
        data=data,
        method=method,
        headers=headers,
    )


def public_bytes(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"Accept": "application/octet-stream"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            return resource_limits.read_bounded(
                response,
                max_bytes=resource_limits.MAX_COMPRESSED_ARTIFACT_BYTES,
                resource="redirected GitHub artifact response",
            )
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"GitHub API redirect request failed for {url}: {exc.code} {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"GitHub API redirect request failed for {url}: {exc.reason}") from exc


def _next_link(link_header: str | None) -> str | None:
    if not link_header:
        return None
    for part in link_header.split(","):
        url_part, _, rel_part = part.strip().partition(";")
        if 'rel="next"' in rel_part:
            return url_part.strip()[1:-1]
    return None
