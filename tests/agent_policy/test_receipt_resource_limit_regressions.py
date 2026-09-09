from __future__ import annotations

import importlib.util
import io
import json
import sys
import zipfile
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


limits = _load(
    "dpone_agent_artifact_resource_limit_regression_test",
    "tools/agent_policy/artifact_resource_limits.py",
)
github_api = _load(
    "dpone_agent_pr_receipt_github_api_regression_test",
    "tools/agent_policy/pr_receipt_github_api.py",
)
merge_receipt = _load(
    "dpone_agent_pr_merge_receipt_resource_regression_test",
    "tools/agent_policy/pr_merge_receipt.py",
)
merge_gate = _load(
    "dpone_agent_release_merge_receipt_resource_regression_test",
    "tools/agent_policy/release_merge_receipt_gate.py",
)
github_receipt = _load(
    "dpone_agent_pr_receipt_github",
    "tools/agent_policy/pr_receipt_github.py",
)
pr_receipt = _load(
    "dpone_agent_pr_receipt_resource_regression_test",
    "tools/agent_policy/pr_receipt.py",
)


def _malformed_deflate_archive() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("receipt.json", b'{"status":"PASS"}')
    raw = bytearray(buffer.getvalue())
    filename_length = int.from_bytes(raw[26:28], "little")
    extra_length = int.from_bytes(raw[28:30], "little")
    compressed_size = int.from_bytes(raw[18:22], "little")
    content_start = 30 + filename_length + extra_length
    raw[content_start : content_start + compressed_size] = b"\x07" + b"\x00" * (compressed_size - 1)
    return bytes(raw)


def _pagination_response(
    page: int,
    *,
    item_count: int = 1,
    next_page: int | None,
) -> tuple[list[dict[str, int]], str | None]:
    items = [{"id": page * 100 + item} for item in range(item_count)]
    link = f'<https://api.github.test/items?page={next_page}>; rel="next"' if next_page is not None else None
    return items, link


def test_malformed_deflate_is_normalized_and_writes_closed_failure_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _malformed_deflate_archive()
    with pytest.raises(ValueError, match="valid ZIP archive"):
        limits.read_bounded_zip(raw, resource="source Agent PR receipt artifact")

    event = tmp_path / "event.json"
    event.write_text("{}\n", encoding="utf-8")
    policy = tmp_path / "policy.yml"
    policy.write_text(
        "schema_version: 2\nbranch_governance:\n  ruleset:\n    branches:\n      - master\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "receipt"
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("TEST_GITHUB_TOKEN", "approved-token")

    def reject_malformed_archive(**_kwargs: Any) -> Any:
        return limits.read_bounded_zip(raw, resource="source Agent PR receipt artifact")

    monkeypatch.setattr(merge_receipt, "derive_merge_receipt", reject_malformed_archive)

    exit_code = merge_receipt.main(
        [
            "--event",
            str(event),
            "--repository",
            "PaulKov/dpone",
            "--policy",
            str(policy),
            "--workflow",
            "Agent PR receipt",
            "--run-id",
            "101",
            "--run-attempt",
            "1",
            "--github-token-env",
            "TEST_GITHUB_TOKEN",
            "--output-dir",
            str(output_dir),
        ]
    )

    payload = json.loads((output_dir / merge_receipt.RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert exit_code == 1
    assert payload["status"] == "FAIL"
    assert payload["errors"] == ["source Agent PR receipt artifact is not a valid ZIP archive"]
    assert not (output_dir / merge_receipt.SOURCE_ARCHIVE_FILENAME).exists()


def test_pagination_accepts_exact_page_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(github_api.resource_limits, "MAX_GITHUB_API_PAGES", 2)
    monkeypatch.setattr(github_api.resource_limits, "MAX_GITHUB_API_ITEMS", 10)

    def request(
        url: str,
        *,
        token: str,
    ) -> tuple[dict[str, list[dict[str, int]]], str | None]:
        assert token == "token"
        page = 2 if "page=2" in url else 1
        items, link = _pagination_response(page, next_page=2 if page == 1 else None)
        return {"artifacts": items}, link

    monkeypatch.setattr(github_api, "github_request_with_link", request)

    assert github_api.github_paginated_items("items", token="token", array_key="artifacts") == [
        {"id": 100},
        {"id": 200},
    ]


def test_pagination_rejects_page_limit_plus_one_before_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(github_api.resource_limits, "MAX_GITHUB_API_PAGES", 2)
    monkeypatch.setattr(github_api.resource_limits, "MAX_GITHUB_API_ITEMS", 10)
    requested: list[str] = []

    def request(url: str, *, token: str) -> tuple[list[dict[str, int]], str | None]:
        assert token == "token"
        requested.append(url)
        page = len(requested)
        return _pagination_response(page, next_page=page + 1)

    monkeypatch.setattr(github_api, "github_request_with_link", request)

    with pytest.raises(ValueError, match="page limit 2"):
        github_api.github_paginated_items("items", token="token")

    assert len(requested) == 2


def test_pagination_accepts_exact_item_limit_and_rejects_limit_plus_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(github_api.resource_limits, "MAX_GITHUB_API_PAGES", 2)
    monkeypatch.setattr(github_api.resource_limits, "MAX_GITHUB_API_ITEMS", 2)
    responses = iter(
        [
            _pagination_response(1, item_count=2, next_page=None),
            _pagination_response(1, item_count=3, next_page=None),
        ]
    )
    monkeypatch.setattr(
        github_api,
        "github_request_with_link",
        lambda *_args, **_kwargs: next(responses),
    )

    assert github_api.github_paginated_items("items", token="token") == [
        {"id": 100},
        {"id": 101},
    ]
    with pytest.raises(ValueError, match="item limit 2"):
        github_api.github_paginated_items("items", token="token")


def test_pagination_rejects_repeated_next_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(github_api.resource_limits, "MAX_GITHUB_API_PAGES", 10)
    monkeypatch.setattr(github_api.resource_limits, "MAX_GITHUB_API_ITEMS", 10)
    requested: list[str] = []

    def request(url: str, *, token: str) -> tuple[list[dict[str, int]], str | None]:
        assert token == "token"
        requested.append(url)
        return _pagination_response(1, next_page=1)

    monkeypatch.setattr(github_api, "github_request_with_link", request)

    with pytest.raises(ValueError, match="pagination cycle"):
        github_api.github_paginated_items("https://api.github.test/items?page=1", token="token")

    assert requested == ["https://api.github.test/items?page=1"]


def test_pagination_limit_is_terminal_for_merge_receipt_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def reject_limit(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        raise ValueError("GitHub API pagination item limit 10000 exceeded")

    sleeps: list[float] = []
    monkeypatch.setattr(merge_gate.github_api, "github_paginated_items", reject_limit)
    adapter = merge_gate.GitHubMergeReceiptAdapter(
        "token",
        attempts=3,
        sleeper=sleeps.append,
    )

    with pytest.raises(ValueError, match="pagination item limit"):
        adapter.list_check_runs("PaulKov/dpone", "a" * 40)

    assert calls == 1
    assert sleeps == []


def test_pre_merge_receipt_writes_structured_failure_for_pagination_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = tmp_path / "body.md"
    body.write_text("", encoding="utf-8")
    output = tmp_path / "agent_pr_receipt.json"
    calls = 0

    def reject_limit(**_kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        raise ValueError("GitHub API pagination item limit 10000 exceeded")

    monkeypatch.setenv("TEST_GITHUB_TOKEN", "approved-token")
    monkeypatch.setattr(github_receipt, "fetch_github_evidence", reject_limit)

    exit_code = pr_receipt.main(
        [
            "--body-file",
            str(body),
            "--changed-paths",
            ".agents/policy/required.yml",
            "--output",
            str(output),
            "--format",
            "json",
            "--repo",
            "PaulKov/dpone",
            "--head-sha",
            "a" * 40,
            "--ruleset-id",
            "1",
            "--github-token-env",
            "TEST_GITHUB_TOKEN",
            "--require-github-evidence",
        ]
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert payload["status"] == "FAIL"
    assert payload["github_evidence"]["errors"] == ["GitHub API pagination item limit 10000 exceeded"]
    assert "GitHub API pagination item limit 10000 exceeded" in payload["errors"]
    assert calls == 1
