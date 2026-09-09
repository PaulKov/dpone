from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus

import requests


@dataclass(frozen=True)
class GitLabMergeRequestResult:
    web_url: str | None
    iid: int | None
    source_branch: str
    target_branch: str
    existed: bool
    payload: dict[str, Any]


class GitLabMrClient:
    def __init__(self, *, api_url: str, private_token: str, session: requests.Session | None = None) -> None:
        if not api_url:
            raise ValueError("api_url must be non-empty")
        if not private_token:
            raise ValueError("private_token must be non-empty")
        self.api_url = api_url.rstrip("/")
        self.session = session or requests.Session()
        self.session.headers.update({"PRIVATE-TOKEN": private_token})

    def _project_url(self, project_id: str | int) -> str:
        return f"{self.api_url}/projects/{quote_plus(str(project_id))}/merge_requests"

    def find_open_merge_request(
        self,
        *,
        project_id: str | int,
        source_branch: str,
        target_branch: str,
    ) -> GitLabMergeRequestResult | None:
        response = self.session.get(
            self._project_url(project_id),
            params={
                "state": "opened",
                "source_branch": source_branch,
                "target_branch": target_branch,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload:
            return None
        first = payload[0]
        return GitLabMergeRequestResult(
            web_url=first.get("web_url"),
            iid=first.get("iid"),
            source_branch=source_branch,
            target_branch=target_branch,
            existed=True,
            payload=first,
        )

    def create_merge_request(
        self,
        *,
        project_id: str | int,
        source_branch: str,
        target_branch: str,
        title: str,
        description: str,
        labels: Iterable[str] = (),
        remove_source_branch: bool = False,
        squash: bool = False,
        draft: bool = False,
    ) -> GitLabMergeRequestResult:
        actual_title = f"Draft: {title}" if draft and not title.startswith("Draft:") else title
        response = self.session.post(
            self._project_url(project_id),
            json={
                "source_branch": source_branch,
                "target_branch": target_branch,
                "title": actual_title,
                "description": description,
                "labels": ",".join(label for label in labels if label),
                "remove_source_branch": remove_source_branch,
                "squash": squash,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return GitLabMergeRequestResult(
            web_url=payload.get("web_url"),
            iid=payload.get("iid"),
            source_branch=source_branch,
            target_branch=target_branch,
            existed=False,
            payload=payload,
        )

    def ensure_merge_request(
        self,
        *,
        project_id: str | int,
        source_branch: str,
        target_branch: str,
        title: str,
        description: str,
        labels: Iterable[str] = (),
        remove_source_branch: bool = False,
        squash: bool = False,
        draft: bool = False,
    ) -> GitLabMergeRequestResult:
        existing = self.find_open_merge_request(
            project_id=project_id,
            source_branch=source_branch,
            target_branch=target_branch,
        )
        if existing is not None:
            return existing
        return self.create_merge_request(
            project_id=project_id,
            source_branch=source_branch,
            target_branch=target_branch,
            title=title,
            description=description,
            labels=labels,
            remove_source_branch=remove_source_branch,
            squash=squash,
            draft=draft,
        )
