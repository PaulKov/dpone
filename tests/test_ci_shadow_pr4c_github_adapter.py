from __future__ import annotations

from datetime import UTC, datetime
from urllib.error import HTTPError, URLError

import pytest

from dpone.adapters.ci_shadow_reconciliation_github import GitHubReconciliationApi, GitHubReconciliationApiError


class _Response:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, amount: int) -> bytes:
        return self.payload[:amount]

    def close(self) -> None:
        return None


class _ArtifactResponse(_Response):
    def __init__(self, status: int, headers: dict[str, str], payload: bytes) -> None:
        super().__init__(payload)
        self.status = status
        self.headers = headers


def test_adapter_uses_closed_created_query_and_bounded_raw_response() -> None:
    captured: list[object] = []

    def opener(request: object, *, timeout: float) -> _Response:
        captured.extend([request, timeout])
        return _Response(b'{"total_count":0,"workflow_runs":[]}')

    api = GitHubReconciliationApi("PaulKov/dpone", "token", opener=opener)
    payload = api.list_workflow_runs(
        workflow_id=12,
        event="pull_request",
        created_from=datetime(2026, 8, 1, 0, 0, tzinfo=UTC),
        created_to=datetime(2026, 8, 2, 0, 0, tzinfo=UTC),
        page=2,
        timeout_seconds=7.5,
        max_response_bytes=100,
    )

    request = captured[0]
    assert payload.startswith(b"{")
    assert "created=2026-08-01T00%3A00%3A00Z..2026-08-02T00%3A00%3A00Z" in request.full_url  # type: ignore[attr-defined]
    assert "per_page=100&page=2" in request.full_url  # type: ignore[attr-defined]
    assert captured[1] == 7.5


def test_adapter_rejects_oversized_or_unavailable_responses() -> None:
    oversized = GitHubReconciliationApi("PaulKov/dpone", "token", opener=lambda *_args, **_kwargs: _Response(b"123"))
    with pytest.raises(GitHubReconciliationApiError, match="exceeds"):
        oversized.list_run_artifacts(run_id=1, page=1, timeout_seconds=1, max_response_bytes=2)

    unavailable = GitHubReconciliationApi(
        "PaulKov/dpone", "token", opener=lambda *_args, **_kwargs: (_ for _ in ()).throw(URLError("down"))
    )
    with pytest.raises(GitHubReconciliationApiError, match="unavailable"):
        unavailable.get_workflow_run_attempt(run_id=1, attempt=1, timeout_seconds=1, max_response_bytes=2)


def test_adapter_rejects_non_utc_or_exhausted_budgets() -> None:
    api = GitHubReconciliationApi("PaulKov/dpone", "token", opener=lambda *_args, **_kwargs: _Response(b"{}"))
    with pytest.raises(GitHubReconciliationApiError):
        api.list_workflow_runs(
            workflow_id=1,
            event="pull_request",
            created_from=datetime(2026, 8, 1, 0, 0),
            created_to=datetime(2026, 8, 2, 0, 0, tzinfo=UTC),
            page=1,
            timeout_seconds=1,
            max_response_bytes=1,
        )
    with pytest.raises(GitHubReconciliationApiError, match="budget"):
        api.list_attempt_jobs(run_id=1, attempt=1, page=1, timeout_seconds=1, max_response_bytes=0)


def test_artifact_request_surfaces_redirect_and_never_sends_token_to_storage() -> None:
    requests: list[object] = []

    def opener(request: object, *, timeout: float) -> _ArtifactResponse:
        requests.append(request)
        if len(requests) == 1:
            raise HTTPError(
                request.full_url,  # type: ignore[attr-defined]
                302,
                "redirect",
                {"Location": "https://storage.example/archive"},
                _ArtifactResponse(302, {}, b"redirect"),
            )
        return _ArtifactResponse(200, {}, b"archive")

    api = GitHubReconciliationApi("PaulKov/dpone", "token", opener=opener)
    initial = api.request_artifact_archive("https://api.github.com/repos/PaulKov/dpone/actions/artifacts/1/zip", 1, 100)
    redirected = api.request_artifact_archive("https://storage.example/archive", 1, 100)

    assert initial.status == 302
    assert initial.headers["Location"] == "https://storage.example/archive"
    assert redirected.body == b"archive"
    assert requests[0].get_header("Authorization") == "Bearer token"  # type: ignore[attr-defined]
    assert requests[1].get_header("Authorization") is None  # type: ignore[attr-defined]


def test_adapter_reads_only_full_sha_git_objects() -> None:
    urls: list[str] = []

    def opener(request: object, *, timeout: float) -> _Response:
        urls.append(request.full_url)  # type: ignore[attr-defined]
        return _Response(b"{}")

    api = GitHubReconciliationApi("PaulKov/dpone", "token", opener=opener)
    sha = "a" * 40
    api.get_git_commit_tree(commit_sha=sha, timeout_seconds=1, max_response_bytes=100)
    api.get_git_tree(tree_sha=sha, timeout_seconds=1, max_response_bytes=100)
    api.get_git_blob(blob_sha=sha, timeout_seconds=1, max_response_bytes=100)

    assert urls == [
        f"https://api.github.com/repos/PaulKov/dpone/git/commits/{sha}",
        f"https://api.github.com/repos/PaulKov/dpone/git/trees/{sha}?recursive=1",
        f"https://api.github.com/repos/PaulKov/dpone/git/blobs/{sha}",
    ]
    with pytest.raises(GitHubReconciliationApiError, match="full SHA"):
        api.get_git_blob(blob_sha="not-a-sha", timeout_seconds=1, max_response_bytes=100)
