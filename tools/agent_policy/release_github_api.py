"""Minimal GitHub REST helpers for Shape B draft staging (stdlib only)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

HttpCaller = Callable[
    [str, str, Mapping[str, str] | None, bytes | None], tuple[int, dict[str, Any] | list[Any] | None, str]
]


@dataclass(frozen=True)
class GitHubApiError(RuntimeError):
    """Non-success GitHub API response."""

    status: int
    body: str

    def __str__(self) -> str:
        return f"GitHub API HTTP {self.status}: {self.body[:500]}"


@dataclass(frozen=True)
class GitHubApi:
    """Thin token-authenticated GitHub REST client."""

    token: str
    api_base: str = "https://api.github.com"
    http: HttpCaller | None = None

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: Mapping[str, Any] | None = None,
        accept: str = "application/vnd.github+json",
    ) -> Any:
        body = None if payload is None else json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        headers = {
            "Accept": accept,
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "dpone-release-controller",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        caller = self.http or _default_http
        status, parsed, text = caller(method, f"{self.api_base}{path}", headers, body)
        if status >= 400:
            raise GitHubApiError(status=status, body=text)
        return parsed


def ensure_lightweight_tag(
    api: GitHubApi,
    *,
    owner: str,
    repo: str,
    tag_ref: str,
    commit_sha: str,
) -> dict[str, Any]:
    """Create ``tag_ref`` at ``commit_sha`` when missing; return the ref payload."""

    ref = tag_ref if tag_ref.startswith("refs/") else f"refs/tags/{tag_ref}"
    # Keep '/' so GitHub sees git/ref/tags/<name>; encode only other unsafe chars.
    encoded = quote(ref.removeprefix("refs/"), safe="/")
    try:
        existing = api.request("GET", f"/repos/{owner}/{repo}/git/ref/{encoded}")
        assert isinstance(existing, dict)
        return {"created": False, "ref": existing}
    except GitHubApiError as exc:
        if exc.status != 404:
            raise
    created = api.request(
        "POST",
        f"/repos/{owner}/{repo}/git/refs",
        payload={"ref": ref, "sha": commit_sha},
    )
    assert isinstance(created, dict)
    return {"created": True, "ref": created}


def resolve_default_branch_sha(api: GitHubApi, *, owner: str, repo: str) -> tuple[str, str]:
    """Return ``(default_branch, tip_sha)`` for the target repository."""

    repo_info = api.request("GET", f"/repos/{owner}/{repo}")
    assert isinstance(repo_info, dict)
    branch = str(repo_info["default_branch"])
    ref = api.request("GET", f"/repos/{owner}/{repo}/git/ref/heads/{quote(branch, safe='')}")
    assert isinstance(ref, dict)
    return branch, str(ref["object"]["sha"])


def get_release(
    api: GitHubApi,
    *,
    owner: str,
    repo: str,
    release_id: str | int,
) -> dict[str, Any]:
    """Return one release by exact provider ID."""

    release = api.request("GET", f"/repos/{owner}/{repo}/releases/{int(release_id)}")
    assert isinstance(release, dict)
    return release


def create_or_get_draft_release(
    api: GitHubApi,
    *,
    owner: str,
    repo: str,
    tag_name: str,
    name: str,
    body: str,
) -> dict[str, Any]:
    """Create a draft release for ``tag_name``, or return an existing draft for that tag."""

    releases = api.request("GET", f"/repos/{owner}/{repo}/releases?per_page=100")
    assert isinstance(releases, list)
    for item in releases:
        if not isinstance(item, dict):
            continue
        if str(item.get("tag_name")) == tag_name and bool(item.get("draft")):
            return {"created": False, "release": item}
    created = api.request(
        "POST",
        f"/repos/{owner}/{repo}/releases",
        payload={
            "tag_name": tag_name,
            "name": name,
            "body": body,
            "draft": True,
            "prerelease": True,
            "generate_release_notes": False,
        },
    )
    assert isinstance(created, dict)
    return {"created": True, "release": created}


def upload_release_asset(
    api: GitHubApi,
    *,
    upload_url_template: str,
    filename: str,
    content: bytes,
    content_type: str = "application/octet-stream",
) -> dict[str, Any]:
    """Upload one asset to a draft release using the ``upload_url`` template."""

    base = upload_url_template.split("{", 1)[0]
    url = f"{base}?name={quote(filename)}"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {api.token}",
        "Content-Type": content_type,
        "Content-Length": str(len(content)),
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "dpone-release-controller",
    }
    caller = api.http or _default_http
    status, parsed, text = caller("POST", url, headers, content)
    if status >= 400:
        raise GitHubApiError(status=status, body=text)
    assert isinstance(parsed, dict)
    return parsed


def _default_http(
    method: str,
    url: str,
    headers: Mapping[str, str] | None,
    body: bytes | None,
) -> tuple[int, dict[str, Any] | list[Any] | None, str]:
    request = urllib.request.Request(url, data=body, method=method, headers=dict(headers or {}))
    try:
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
            raw = response.read()
            text = raw.decode("utf-8") if raw else ""
            parsed: dict[str, Any] | list[Any] | None
            if text:
                loaded = json.loads(text)
                parsed = loaded if isinstance(loaded, (dict, list)) else None
            else:
                parsed = None
            return int(response.status), parsed, text
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        text = raw.decode("utf-8") if raw else str(exc)
        return int(exc.code), None, text
