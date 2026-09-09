"""Bounded stdlib GitHub reads for CI-shadow reconciliation acquisition."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from dpone.ports.ci_shadow_reconciliation import (
    ArtifactHttpResponse,
    CiShadowGitObjectProvider,
    CiShadowReconciliationProvider,
)


class GitHubReconciliationApiError(RuntimeError):
    """A provider read is unavailable, malformed, oversized, or untrustworthy."""


class _HttpResponse(Protocol):
    status: int
    headers: object

    def __enter__(self) -> _HttpResponse: ...

    def __exit__(self, *_: object) -> None: ...

    def read(self, amount: int) -> bytes: ...


class _ArtifactReadable(Protocol):
    """Minimal non-followed artifact response needed outside a context manager."""

    @property
    def headers(self) -> object: ...

    def read(self, amount: int) -> bytes: ...


class _RejectRedirect(HTTPRedirectHandler):
    """Surface redirects to the caller instead of letting urllib hide a dispatch."""

    def redirect_request(self, *_: object, **__: object) -> None:
        return None


_NO_REDIRECT_OPENER = build_opener(_RejectRedirect())


def _open_without_redirect(request: Request, *, timeout: float) -> _HttpResponse:
    return cast(_HttpResponse, _NO_REDIRECT_OPENER.open(request, timeout=timeout))


@dataclass(frozen=True)
class GitHubReconciliationApi(CiShadowReconciliationProvider, CiShadowGitObjectProvider):
    """Read Actions records without checkout, subprocess, cache, or write transport."""

    repository: str
    token: str
    opener: Callable[..., _HttpResponse] = _open_without_redirect

    def list_workflow_runs(
        self,
        *,
        workflow_id: int,
        event: str,
        created_from: datetime,
        created_to: datetime,
        page: int,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> bytes:
        """Read one provider page bounded to an exact closed UTC-second range."""

        query = {
            "event": event,
            "created": f"{_utc_second(created_from)}..{_utc_second(created_to)}",
            "per_page": "100",
            "page": str(_positive(page, "page")),
        }
        return self._get(
            f"/repos/{self.repository}/actions/workflows/{_positive(workflow_id, 'workflow_id')}/runs?{urlencode(query)}",
            timeout_seconds=timeout_seconds,
            max_response_bytes=max_response_bytes,
        )

    def get_workflow_run_attempt(
        self, *, run_id: int, attempt: int, timeout_seconds: float, max_response_bytes: int
    ) -> bytes:
        """Read an exact immutable workflow-run attempt record."""

        return self._get(
            f"/repos/{self.repository}/actions/runs/{_positive(run_id, 'run_id')}/attempts/{_positive(attempt, 'attempt')}",
            timeout_seconds=timeout_seconds,
            max_response_bytes=max_response_bytes,
        )

    def list_attempt_jobs(
        self, *, run_id: int, attempt: int, page: int, timeout_seconds: float, max_response_bytes: int
    ) -> bytes:
        """Read one exact-attempt Jobs page."""

        return self._get(
            f"/repos/{self.repository}/actions/runs/{_positive(run_id, 'run_id')}/attempts/{_positive(attempt, 'attempt')}/jobs?per_page=100&page={_positive(page, 'page')}",
            timeout_seconds=timeout_seconds,
            max_response_bytes=max_response_bytes,
        )

    def list_run_artifacts(self, *, run_id: int, page: int, timeout_seconds: float, max_response_bytes: int) -> bytes:
        """Read one exact workflow-run artifact metadata page."""

        return self._get(
            f"/repos/{self.repository}/actions/runs/{_positive(run_id, 'run_id')}/artifacts?per_page=100&page={_positive(page, 'page')}",
            timeout_seconds=timeout_seconds,
            max_response_bytes=max_response_bytes,
        )

    def download_artifact_archive(self, *, artifact_id: int, timeout_seconds: float, max_response_bytes: int) -> bytes:
        """Read one archive endpoint only when the transport does not redirect.

        GitHub normally redirects archive downloads to a storage host. The caller
        must use the redirect-aware transport boundary rather than silently let
        urllib follow it, because every redirect is independently metered.
        """

        return self._get(
            f"/repos/{self.repository}/actions/artifacts/{_positive(artifact_id, 'artifact_id')}/zip",
            timeout_seconds=timeout_seconds,
            max_response_bytes=max_response_bytes,
        )

    def request_artifact_archive(
        self, url: str, timeout_seconds: float, max_response_bytes: int
    ) -> ArtifactHttpResponse:
        """Return one bounded non-followed archive response.

        The GitHub token is attached only to GitHub's own archive endpoint.
        Redirect targets receive no credentials and are surfaced to the
        transport service for independent accounting.
        """

        if timeout_seconds <= 0 or max_response_bytes < 1:
            raise GitHubReconciliationApiError("provider read budget is exhausted")
        is_github_endpoint = url.startswith(f"https://api.github.com/repos/{self.repository}/actions/artifacts/")
        headers = {"Accept": "application/vnd.github+json"}
        if is_github_endpoint:
            headers.update(
                {
                    "Authorization": f"Bearer {self.token}",
                    "X-GitHub-Api-Version": "2022-11-28",
                }
            )
        request = Request(url, headers=headers)
        try:
            with self.opener(request, timeout=timeout_seconds) as response:
                return _artifact_response(response, response.status, max_response_bytes)
        except HTTPError as exc:
            if exc.code not in {301, 302, 303, 307, 308}:
                raise GitHubReconciliationApiError("GitHub artifact read is unavailable") from exc
            return _artifact_response(exc, exc.code, max_response_bytes)
        except (URLError, OSError) as exc:
            raise GitHubReconciliationApiError("GitHub artifact read is unavailable") from exc

    def get_git_commit_tree(self, *, commit_sha: str, timeout_seconds: float, max_response_bytes: int) -> bytes:
        """Read a commit only to obtain the immutable root-tree identity."""

        return self._get(
            f"/repos/{self.repository}/git/commits/{_sha(commit_sha, 'commit_sha')}",
            timeout_seconds=timeout_seconds,
            max_response_bytes=max_response_bytes,
        )

    def get_git_tree(self, *, tree_sha: str, timeout_seconds: float, max_response_bytes: int) -> bytes:
        """Read the full immutable Git tree without following mutable refs."""

        return self._get(
            f"/repos/{self.repository}/git/trees/{_sha(tree_sha, 'tree_sha')}?recursive=1",
            timeout_seconds=timeout_seconds,
            max_response_bytes=max_response_bytes,
        )

    def get_git_blob(self, *, blob_sha: str, timeout_seconds: float, max_response_bytes: int) -> bytes:
        """Read one immutable Git blob object."""

        return self._get(
            f"/repos/{self.repository}/git/blobs/{_sha(blob_sha, 'blob_sha')}",
            timeout_seconds=timeout_seconds,
            max_response_bytes=max_response_bytes,
        )

    def _get(self, path: str, *, timeout_seconds: float, max_response_bytes: int) -> bytes:
        if not self.repository or "/" not in self.repository:
            raise GitHubReconciliationApiError("repository identity is invalid")
        if timeout_seconds <= 0 or max_response_bytes < 1:
            raise GitHubReconciliationApiError("provider read budget is exhausted")
        request = Request(
            f"https://api.github.com{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with self.opener(request, timeout=timeout_seconds) as response:
                payload = response.read(max_response_bytes + 1)
        except (HTTPError, URLError, OSError) as exc:
            raise GitHubReconciliationApiError("GitHub provider read is unavailable") from exc
        if len(payload) > max_response_bytes:
            raise GitHubReconciliationApiError("GitHub provider response exceeds the bounded read")
        return payload


def _artifact_response(response: _ArtifactReadable, status: int, maximum: int) -> ArtifactHttpResponse:
    payload = response.read(maximum + 1)
    if len(payload) > maximum:
        raise GitHubReconciliationApiError("GitHub artifact response exceeds the bounded read")
    raw_headers = response.headers
    header_items = getattr(raw_headers, "items", None)
    if not callable(header_items):
        raise GitHubReconciliationApiError("GitHub artifact response headers are unavailable")
    return ArtifactHttpResponse(status=status, headers=dict(header_items()), body=payload)


def _positive(value: int, name: str) -> int:
    if isinstance(value, bool) or value < 1:
        raise GitHubReconciliationApiError(f"{name} must be positive")
    return value


def _sha(value: str, name: str) -> str:
    if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
        raise GitHubReconciliationApiError(f"{name} must be a lowercase full SHA")
    return value


def _utc_second(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None or value.microsecond:
        raise GitHubReconciliationApiError("provider interval must use whole UTC seconds")
    rendered = value.isoformat().replace("+00:00", "Z")
    if not rendered.endswith("Z"):
        raise GitHubReconciliationApiError("provider interval must use UTC")
    return rendered


__all__ = ["GitHubReconciliationApi", "GitHubReconciliationApiError"]
