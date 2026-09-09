from __future__ import annotations

import json
from io import BytesIO

import pytest

from dpone.adapters.gitlab_protected_source import GitLabProtectedSourceAuthorizer
from dpone.ports.airflow_desired_state import (
    DesiredStateSourceUnauthorized,
    DesiredStateSourceUnavailable,
)


class _Response(BytesIO):
    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _authorizer(payload: object) -> GitLabProtectedSourceAuthorizer:
    return GitLabProtectedSourceAuthorizer(
        api_url="https://git.example.test/api/v4",
        project="platform/example-workloads",
        protected_ref="master",
        job_token="masked",
        opener=lambda *_args, **_kwargs: _Response(json.dumps(payload).encode()),
    )


def test_gitlab_authorizer_requires_exact_protected_head() -> None:
    sha = "a" * 40
    authorizer = _authorizer(
        {
            "name": "master",
            "protected": True,
            "commit": {"id": sha},
        }
    )

    authorizer.authorize(project="platform/example-workloads", git_sha=sha)

    with pytest.raises(DesiredStateSourceUnauthorized):
        authorizer.authorize(project="platform/example-workloads", git_sha="b" * 40)
    with pytest.raises(DesiredStateSourceUnauthorized):
        authorizer.authorize(project="other/project", git_sha=sha)


def test_gitlab_authorizer_fails_closed_when_head_is_unavailable() -> None:
    authorizer = GitLabProtectedSourceAuthorizer(
        api_url="https://git.example.test/api/v4",
        project="platform/example-workloads",
        protected_ref="master",
        job_token="masked",
        opener=lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("token payload")),
    )

    with pytest.raises(DesiredStateSourceUnavailable):
        authorizer.authorize(
            project="platform/example-workloads",
            git_sha="a" * 40,
        )
