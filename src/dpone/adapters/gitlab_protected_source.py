"""GitLab adapter authorizing one protected repository head."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen

from dpone.ports.airflow_desired_state import (
    DesiredStateSourceUnauthorized,
    DesiredStateSourceUnavailable,
)

MAX_GITLAB_BRANCH_RESPONSE_BYTES = 64 * 1024


class GitLabProtectedSourceAuthorizer:
    """Authorize the exact current head of one configured protected ref."""

    def __init__(
        self,
        *,
        api_url: str,
        project: str,
        protected_ref: str,
        job_token: str,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        if not api_url.startswith("https://") or not project or not protected_ref:
            raise ValueError("GitLab protected-source authority is invalid")
        if not job_token:
            raise ValueError("GitLab protected-source job token is required")
        self._api_url = api_url.rstrip("/")
        self._project = project
        self._protected_ref = protected_ref
        self._job_token = job_token
        self._opener = opener

    def authorize(self, *, project: str, git_sha: str) -> None:
        if project != self._project:
            raise DesiredStateSourceUnauthorized("source project is outside the trusted authority")
        endpoint = (
            f"{self._api_url}/projects/{quote(self._project, safe='')}"
            f"/repository/branches/{quote(self._protected_ref, safe='')}"
        )
        request = Request(endpoint, headers={"JOB-TOKEN": self._job_token})
        try:
            with self._opener(request, timeout=10) as response:
                body = response.read(MAX_GITLAB_BRANCH_RESPONSE_BYTES + 1)
        except Exception as exc:
            raise DesiredStateSourceUnavailable("protected source head could not be read") from exc
        if len(body) > MAX_GITLAB_BRANCH_RESPONSE_BYTES:
            raise DesiredStateSourceUnavailable("protected source response exceeds its size limit")
        try:
            payload = json.loads(body.decode("utf-8", errors="strict"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DesiredStateSourceUnavailable("protected source response is invalid") from exc
        commit = payload.get("commit") if isinstance(payload, dict) else None
        head = commit.get("id") if isinstance(commit, dict) else None
        if (
            not isinstance(payload, dict)
            or payload.get("name") != self._protected_ref
            or payload.get("protected") is not True
            or head != git_sha
        ):
            raise DesiredStateSourceUnauthorized("source SHA is not the trusted protected-ref head")


__all__ = [
    "GitLabProtectedSourceAuthorizer",
    "MAX_GITLAB_BRANCH_RESPONSE_BYTES",
]
