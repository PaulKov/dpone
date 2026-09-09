from __future__ import annotations

import importlib.util
import json
import sys
import urllib.error
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


github_api = _load("dpone_agent_pr_receipt_github_api_test", "tools/agent_policy/pr_receipt_github_api.py")


class _BytesResponse:
    def __init__(self, content: bytes) -> None:
        self._content = content
        self._offset = 0
        self.headers: dict[str, str] = {}

    def __enter__(self) -> _BytesResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._content) - self._offset
        start = self._offset
        self._offset = min(len(self._content), start + size)
        return self._content[start : self._offset]


def test_github_bytes_drops_github_authorization_on_artifact_redirect(monkeypatch: Any) -> None:
    requests = []

    class RedirectingOpener:
        def open(self, request: object, *, timeout: int) -> object:
            assert timeout == 30
            requests.append(request)
            raise urllib.error.HTTPError(
                url="https://api.github.com/repos/PaulKov/dpone/actions/artifacts/42/zip",
                code=302,
                msg="Found",
                hdrs={"Location": "https://blob.example/artifact.zip?sig=abc"},
                fp=None,
            )

    def fake_build_opener(*_handlers: object) -> RedirectingOpener:
        return RedirectingOpener()

    def fake_urlopen(request: object, *, timeout: int) -> _BytesResponse:
        assert timeout == 30
        requests.append(request)
        return _BytesResponse(b"zip-bytes")

    monkeypatch.setattr(github_api.urllib.request, "build_opener", fake_build_opener)
    monkeypatch.setattr(github_api.urllib.request, "urlopen", fake_urlopen)

    content = github_api.github_bytes(
        "repos/PaulKov/dpone/actions/artifacts/42/zip",
        token="token",
    )

    assert content == b"zip-bytes"
    assert requests[0].get_header("Authorization") == "Bearer token"
    assert requests[1].full_url == "https://blob.example/artifact.zip?sig=abc"
    assert requests[1].get_header("Authorization") is None


def test_post_github_json_uses_bounded_authenticated_json_request(monkeypatch: Any) -> None:
    observed: list[Any] = []

    def fake_urlopen(request: object, *, timeout: int) -> _BytesResponse:
        assert timeout == 30
        observed.append(request)
        return _BytesResponse(b'{"id":42,"status":"completed"}')

    monkeypatch.setattr(github_api.urllib.request, "urlopen", fake_urlopen)

    response = github_api.post_github_json(
        "repos/PaulKov/dpone/check-runs",
        token="approved-token",
        payload={"name": "Agent PR receipt", "head_sha": "a" * 40},
    )

    request = observed[0]
    assert response == {"id": 42, "status": "completed"}
    assert request.method == "POST"
    assert request.full_url == "https://api.github.com/repos/PaulKov/dpone/check-runs"
    assert request.get_header("Authorization") == "Bearer approved-token"
    assert request.get_header("Content-type") == "application/json"
    assert json.loads(request.data) == {"head_sha": "a" * 40, "name": "Agent PR receipt"}


def test_patch_github_json_uses_exact_check_run_endpoint(monkeypatch: Any) -> None:
    observed: list[Any] = []

    def fake_urlopen(request: object, *, timeout: int) -> _BytesResponse:
        assert timeout == 30
        observed.append(request)
        return _BytesResponse(b'{"id":42,"conclusion":"success"}')

    monkeypatch.setattr(github_api.urllib.request, "urlopen", fake_urlopen)

    response = github_api.patch_github_json(
        "repos/PaulKov/dpone/check-runs/42",
        token="approved-token",
        payload={"status": "completed", "conclusion": "success"},
    )

    request = observed[0]
    assert response == {"id": 42, "conclusion": "success"}
    assert request.method == "PATCH"
    assert request.full_url == "https://api.github.com/repos/PaulKov/dpone/check-runs/42"
    assert json.loads(request.data) == {"conclusion": "success", "status": "completed"}


def test_github_json_rejects_limit_plus_one(monkeypatch: Any) -> None:
    monkeypatch.setattr(github_api.resource_limits, "MAX_GITHUB_JSON_BYTES", 5)
    monkeypatch.setattr(
        github_api.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _BytesResponse(b"123456"),
    )

    with pytest.raises(ValueError, match="JSON response.*limit"):
        github_api.github_json("repos/PaulKov/dpone", token="token")


def test_fresh_github_json_bypasses_cached_governance_state(monkeypatch: Any) -> None:
    observed: list[Any] = []

    def fake_urlopen(request: object, *, timeout: int) -> _BytesResponse:
        assert timeout == 30
        observed.append(request)
        return _BytesResponse(b'{"id":18806829}')

    monkeypatch.setattr(github_api.urllib.request, "urlopen", fake_urlopen)

    assert github_api.github_json(
        "repos/PaulKov/dpone/rulesets/18806829",
        token="approved-token",
        fresh=True,
    ) == {"id": 18806829}

    request = observed[0]
    assert request.get_header("Cache-control") == "no-cache"
    assert request.get_header("Pragma") == "no-cache"


def test_redirected_artifact_rejects_limit_plus_one(monkeypatch: Any) -> None:
    class RedirectingOpener:
        def open(self, *_args: object, **_kwargs: object) -> object:
            raise urllib.error.HTTPError(
                url="https://api.github.com/artifact",
                code=302,
                msg="Found",
                hdrs={"Location": "https://blob.example/artifact.zip"},
                fp=None,
            )

    monkeypatch.setattr(github_api.resource_limits, "MAX_COMPRESSED_ARTIFACT_BYTES", 5)
    monkeypatch.setattr(github_api.urllib.request, "build_opener", lambda *_args: RedirectingOpener())
    monkeypatch.setattr(
        github_api.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _BytesResponse(b"123456"),
    )

    with pytest.raises(ValueError, match="artifact response.*limit"):
        github_api.github_bytes("repos/PaulKov/dpone/actions/artifacts/42/zip", token="token")
