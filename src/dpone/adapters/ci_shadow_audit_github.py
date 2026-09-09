"""Bounded stdlib GitHub REST adapter for the trusted shadow auditor."""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from http.client import HTTPException
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dpone.ports.ci_shadow_audit import CiShadowAuditDeadline, CiShadowAuditProvider, CiShadowAuditProviderError


class GitHubAuditApiError(CiShadowAuditProviderError):
    """GitHub Actions-read API input could not be safely acquired."""


class _HttpResponse(Protocol):
    def __enter__(self) -> _HttpResponse: ...

    def __exit__(self, *_: object) -> None: ...

    def read(self, amount: int) -> bytes: ...


@dataclass(frozen=True)
class GitHubAuditApi(CiShadowAuditProvider):
    """Read exact GitHub REST records without repository checkout or shell use."""

    repository: str
    token: str
    opener: Callable[..., _HttpResponse] = cast(Callable[..., _HttpResponse], urlopen)

    def get_workflow_run(self, run_id: int) -> Mapping[str, object]:
        """Read the producer's exact run metadata from GitHub."""

        payload = _object(self._get(f"/repos/{self.repository}/actions/runs/{run_id}"), "workflow run")
        return {
            "repository_id": _object(payload.get("repository"), "run repository").get("id"),
            "head_repository_id": _object(payload.get("head_repository"), "run head repository").get("id"),
            "run_id": payload.get("id"),
            "run_attempt": payload.get("run_attempt"),
            "head_branch": payload.get("head_branch"),
            "head_sha": payload.get("head_sha"),
            "status": payload.get("status"),
            "conclusion": payload.get("conclusion"),
            "event": payload.get("event"),
            "path": payload.get("path"),
            "workflow_id": payload.get("workflow_id"),
        }

    def list_attempt_jobs(self, run_id: int, attempt: int) -> Sequence[Mapping[str, object]]:
        """Read all job rows bound to one immutable run attempt."""

        records: list[Mapping[str, object]] = []
        page = 1
        while True:
            payload = _object(
                self._get(
                    f"/repos/{self.repository}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100&page={page}"
                ),
                "attempt jobs page",
            )
            jobs = payload.get("jobs")
            if not isinstance(jobs, list) or not all(isinstance(job, Mapping) for job in jobs):
                raise GitHubAuditApiError("attempt jobs page is malformed")
            records.extend(cast(Mapping[str, object], job) for job in jobs)
            total_count = payload.get("total_count")
            if not isinstance(total_count, int) or total_count < len(records):
                raise GitHubAuditApiError("attempt jobs total count is malformed")
            if len(records) == total_count:
                return records
            page += 1
            if page > 100:
                raise GitHubAuditApiError("attempt jobs pagination exceeded bound")

    def list_open_pull_requests(self) -> Sequence[Mapping[str, object]]:
        return self._list_open_pull_requests(deadline=None)

    def list_open_pull_requests_bounded(self, *, deadline: CiShadowAuditDeadline) -> Sequence[Mapping[str, object]]:
        return self._list_open_pull_requests(deadline=deadline)

    def _list_open_pull_requests(self, *, deadline: CiShadowAuditDeadline | None) -> Sequence[Mapping[str, object]]:
        records: list[Mapping[str, object]] = []
        page = 1
        while True:
            payload = self._get(
                f"/repos/{self.repository}/pulls?state=open&per_page=100&page={page}",
                deadline=deadline,
            )
            if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
                raise GitHubAuditApiError("open pull-request page is malformed")
            records.extend(_normalize_pull(item) for item in payload)
            if len(payload) < 100:
                return records
            page += 1
            if page > 100:
                raise GitHubAuditApiError("open pull-request pagination exceeded bound")

    def get_pull_request(self, number: int) -> Mapping[str, object]:
        return self._get_pull_request(number, deadline=None)

    def get_pull_request_bounded(self, number: int, *, deadline: CiShadowAuditDeadline) -> Mapping[str, object]:
        return self._get_pull_request(number, deadline=deadline)

    def _get_pull_request(self, number: int, *, deadline: CiShadowAuditDeadline | None) -> Mapping[str, object]:
        payload = self._get(f"/repos/{self.repository}/pulls/{number}", deadline=deadline)
        return _normalize_pull(_object(payload, "pull request"))

    def resolve_pull_merge_ref(self, number: int) -> str:
        return self._resolve_pull_merge_ref(number, deadline=None)

    def resolve_pull_merge_ref_bounded(self, number: int, *, deadline: CiShadowAuditDeadline) -> str:
        return self._resolve_pull_merge_ref(number, deadline=deadline)

    def _resolve_pull_merge_ref(self, number: int, *, deadline: CiShadowAuditDeadline | None) -> str:
        payload = _object(
            self._get(f"/repos/{self.repository}/git/ref/pull/{number}/merge", deadline=deadline),
            "merge ref",
        )
        return _string(_object(payload.get("object"), "merge ref object").get("sha"), "merge ref SHA")

    def get_commit_parents(self, sha: str) -> Sequence[str]:
        return self._get_commit_parents(sha, deadline=None)

    def get_commit_parents_bounded(self, sha: str, *, deadline: CiShadowAuditDeadline) -> Sequence[str]:
        return self._get_commit_parents(sha, deadline=deadline)

    def _get_commit_parents(self, sha: str, *, deadline: CiShadowAuditDeadline | None) -> Sequence[str]:
        payload = _object(self._get(f"/repos/{self.repository}/git/commits/{sha}", deadline=deadline), "commit")
        parents = payload.get("parents")
        if not isinstance(parents, list):
            raise GitHubAuditApiError("commit parents are malformed")
        return [_string(_object(parent, "commit parent").get("sha"), "parent SHA") for parent in parents]

    def get_git_tree(self, commit_sha: str) -> Mapping[str, Mapping[str, object]]:
        """Read a complete immutable Git tree without a working-tree checkout."""

        commit = _object(self._get(f"/repos/{self.repository}/git/commits/{commit_sha}"), "commit")
        tree_sha = _string(_object(commit.get("tree"), "commit tree").get("sha"), "tree SHA")
        payload = _object(
            self._get(f"/repos/{self.repository}/git/trees/{tree_sha}?recursive=1", max_bytes=16 * 1024 * 1024),
            "tree",
        )
        if payload.get("truncated") is not False:
            raise GitHubAuditApiError("Git tree is incomplete")
        entries = payload.get("tree")
        if not isinstance(entries, list) or not all(isinstance(entry, Mapping) for entry in entries):
            raise GitHubAuditApiError("Git tree is malformed")
        result: dict[str, Mapping[str, object]] = {}
        for raw in entries:
            path = _string(raw.get("path"), "tree path")
            if path.startswith("/") or ".." in path.split("/") or path in result:
                raise GitHubAuditApiError("Git tree path is unsafe")
            result[path] = {
                "sha": _string(raw.get("sha"), "tree blob SHA"),
                "mode": _string(raw.get("mode"), "tree mode"),
                "type": _string(raw.get("type"), "tree type"),
            }
        return result

    def get_git_blob(self, blob_sha: str) -> bytes:
        """Decode one bounded GitHub Git-data blob with strict base64 handling."""

        payload = _object(self._get(f"/repos/{self.repository}/git/blobs/{blob_sha}"), "blob")
        if payload.get("encoding") != "base64":
            raise GitHubAuditApiError("Git blob encoding is unsupported")
        content = _string(payload.get("content"), "blob content").replace("\n", "")
        try:
            return base64.b64decode(content, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise GitHubAuditApiError("Git blob base64 is malformed") from exc

    def _get(
        self,
        path: str,
        *,
        max_bytes: int = 1_048_576,
        deadline: CiShadowAuditDeadline | None = None,
    ) -> object:
        request = Request(
            f"https://api.github.com{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        timeout_seconds = 10.0 if deadline is None else min(10.0, deadline.remaining_seconds())
        try:
            with self.opener(request, timeout=timeout_seconds) as response:
                raw = response.read(max_bytes + 1)
        except (HTTPError, URLError, HTTPException, OSError) as exc:
            raise GitHubAuditApiError("GitHub API is unavailable") from exc
        if deadline is not None:
            deadline.remaining_seconds()
        if len(raw) > max_bytes:
            raise GitHubAuditApiError("GitHub API response exceeds byte bound")
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise GitHubAuditApiError("GitHub API response is not JSON") from exc


def _normalize_pull(raw: Mapping[str, object]) -> Mapping[str, object]:
    base = _object(raw.get("base"), "base")
    head = _object(raw.get("head"), "head")
    base_repository = _object(base.get("repo"), "base repository")
    head_repository = _object(head.get("repo"), "head repository")
    return {
        "number": raw.get("number"),
        "repository_id": base_repository.get("id"),
        "base_repository_id": base_repository.get("id"),
        "head_repository_id": head_repository.get("id"),
        "base_ref": base.get("ref"),
        "head_ref": head.get("ref"),
        "base_sha": base.get("sha"),
        "head_sha": head.get("sha"),
        "mergeable": raw.get("mergeable"),
        "merge_commit_sha": raw.get("merge_commit_sha"),
        "state": raw.get("state"),
    }


def _object(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise GitHubAuditApiError(f"{name} is malformed")
    return value


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise GitHubAuditApiError(f"{name} is malformed")
    return value
