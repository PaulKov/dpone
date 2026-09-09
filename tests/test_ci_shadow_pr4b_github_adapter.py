from __future__ import annotations

import base64
import json
from typing import Any

from dpone.adapters.ci_shadow_audit_github import GitHubAuditApi
from dpone.ports.ci_shadow_audit import CiShadowAuditDeadline


class Response:
    def __init__(self, payload: object) -> None:
        self._raw = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> Response:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, _: int) -> bytes:
        return self._raw


class Opener:
    def __init__(self, payloads: dict[str, object]) -> None:
        self.payloads = payloads
        self.urls: list[str] = []
        self.timeouts: list[float] = []

    def __call__(self, request: Any, *, timeout: float) -> Response:
        assert 0 < timeout <= 10
        self.urls.append(request.full_url)
        self.timeouts.append(timeout)
        for suffix, payload in self.payloads.items():
            if request.full_url.endswith(suffix):
                return Response(payload)
        raise AssertionError(request.full_url)


def _pull() -> dict[str, object]:
    return {
        "number": 7,
        "state": "open",
        "mergeable": True,
        "merge_commit_sha": "c" * 40,
        "base": {"ref": "master", "sha": "a" * 40, "repo": {"id": 1}},
        "head": {"ref": "feature/audit", "sha": "b" * 40, "repo": {"id": 2}},
    }


def test_adapter_pages_and_normalizes_only_read_api_records() -> None:
    list_pull = _pull()
    list_pull.pop("mergeable")
    opener = Opener(
        {
            "pulls?state=open&per_page=100&page=1": [list_pull],
            "pulls/7": _pull(),
            "git/ref/pull/7/merge": {"object": {"sha": "c" * 40}},
            f"git/commits/{'c' * 40}": {
                "parents": [{"sha": "a" * 40}, {"sha": "b" * 40}],
                "tree": {"sha": "d" * 40},
            },
            f"git/trees/{'d' * 40}?recursive=1": {
                "truncated": False,
                "tree": [{"path": "workflow.yml", "sha": "e" * 40, "mode": "100644", "type": "blob"}],
            },
            f"git/blobs/{'e' * 40}": {"encoding": "base64", "content": base64.b64encode(b"workflow").decode()},
        }
    )
    api = GitHubAuditApi(repository="owner/repo", token="token", opener=opener)
    deadline = CiShadowAuditDeadline(clock=lambda: 0.0, expires_at=30.0)

    listed_pull = api.list_open_pull_requests_bounded(deadline=deadline)[0]
    assert listed_pull["head_sha"] == "b" * 40
    assert listed_pull["mergeable"] is None
    assert listed_pull["merge_commit_sha"] == "c" * 40
    assert api.get_pull_request_bounded(7, deadline=deadline)["base_sha"] == "a" * 40
    assert api.resolve_pull_merge_ref_bounded(7, deadline=deadline) == "c" * 40
    assert api.get_commit_parents_bounded("c" * 40, deadline=deadline) == ["a" * 40, "b" * 40]
    assert api.get_git_tree("c" * 40)["workflow.yml"]["sha"] == "e" * 40
    assert api.get_git_blob("e" * 40) == b"workflow"
    assert all("api.github.com/repos/owner/repo/" in url for url in opener.urls)
    assert "https://api.github.com/repos/owner/repo/git/ref/pull/7/merge" in opener.urls
    assert api.list_open_pull_requests()[0]["head_sha"] == "b" * 40
    assert api.get_pull_request(7)["base_sha"] == "a" * 40
    assert api.resolve_pull_merge_ref(7) == "c" * 40
    assert api.get_commit_parents("c" * 40) == ["a" * 40, "b" * 40]


def test_adapter_caps_request_timeout_to_observation_deadline() -> None:
    opener = Opener({"pulls?state=open&per_page=100&page=1": []})
    api = GitHubAuditApi(repository="owner/repo", token="token", opener=opener)
    deadline = CiShadowAuditDeadline(clock=lambda: 29.25, expires_at=30.0)

    assert api.list_open_pull_requests_bounded(deadline=deadline) == []
    assert opener.timeouts == [0.75]


def test_workflow_run_normalization_preserves_source_repository_and_branch() -> None:
    opener = Opener(
        {
            "actions/runs/10": {
                "repository": {"id": 1},
                "head_repository": {"id": 2},
                "id": 10,
                "run_attempt": 1,
                "head_branch": "feature/audit",
                "head_sha": "b" * 40,
                "status": "completed",
                "conclusion": "success",
                "event": "pull_request",
                "path": ".github/workflows/pr-gate-shadow.yml",
                "workflow_id": 10,
            }
        }
    )
    api = GitHubAuditApi(repository="owner/repo", token="token", opener=opener)

    run = api.get_workflow_run(10)

    assert run["head_repository_id"] == 2
    assert run["head_branch"] == "feature/audit"
