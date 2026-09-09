from __future__ import annotations

import hashlib
import importlib.util
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "PaulKov/dpone"
HEAD_SHA = "a" * 40
MERGED_AT = "2026-07-29T00:03:00Z"
RAW_ARCHIVE = b"downloaded source archive"


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


receipt_source = _load(
    "dpone_agent_pr_receipt_source_resilience_test",
    "tools/agent_policy/pr_receipt_source.py",
)


def _digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _check(**overrides: Any) -> dict[str, Any]:
    payload = {
        "id": 11,
        "name": "Agent PR receipt",
        "status": "completed",
        "conclusion": "success",
        "head_sha": HEAD_SHA,
        "app": {"slug": "github-actions"},
        "completed_at": "2026-07-29T00:02:00Z",
        "details_url": f"https://github.com/{REPOSITORY}/actions/runs/101/job/1",
    }
    return {**payload, **overrides}


def _run(**overrides: Any) -> dict[str, Any]:
    payload = {
        "id": 101,
        "name": "Agent PR receipt",
        "event": "pull_request",
        "status": "completed",
        "conclusion": "success",
        "head_sha": HEAD_SHA,
        "repository": {"full_name": REPOSITORY},
        "path": ".github/workflows/agent-pr-receipt.yml@refs/pull/455/merge",
        "updated_at": "2026-07-29T00:02:30Z",
        "run_attempt": 1,
    }
    return {**payload, **overrides}


def _artifact(*, workflow_run: Any = "valid") -> dict[str, Any]:
    return {
        "id": 201,
        "name": "agent-pr-receipt",
        "expired": False,
        "created_at": "2026-07-29T00:02:30Z",
        "digest": _digest(RAW_ARCHIVE),
        "size_in_bytes": len(RAW_ARCHIVE),
        "archive_download_url": "https://api.github.test/artifacts/201/zip",
        "workflow_run": ({"id": 101, "head_sha": HEAD_SHA} if workflow_run == "valid" else workflow_run),
    }


@dataclass
class Adapter:
    check: dict[str, Any]
    run: dict[str, Any]
    artifact: dict[str, Any]
    downloads: Iterator[bytes] | None = None

    def list_check_runs(self, _repository: str, _head_sha: str) -> list[dict[str, Any]]:
        return [self.check]

    def get_workflow_run(self, _repository: str, _run_id: int) -> dict[str, Any]:
        return self.run

    def list_run_artifacts(self, _repository: str, _run_id: int) -> list[dict[str, Any]]:
        return [self.artifact]

    def download_artifact(self, _url: str) -> bytes:
        return next(self.downloads) if self.downloads is not None else RAW_ARCHIVE


def _select(adapter: Adapter) -> Any:
    return receipt_source.select_source_receipt(
        repository=REPOSITORY,
        reviewed_head_sha=HEAD_SHA,
        merged_at=MERGED_AT,
        adapter=adapter,
    )


@pytest.mark.parametrize(
    ("check_override", "run_override"),
    [
        ({"app": None}, {}),
        ({"app": {"slug": "untrusted"}}, {}),
        ({"head_sha": "f" * 40}, {}),
        ({}, {"id": None}),
        ({}, {"id": 999}),
        ({}, {"name": "Different workflow"}),
        ({}, {"status": None}),
        ({}, {"repository": None}),
        ({}, {"repository": {"full_name": "attacker/fork"}}),
        ({}, {"path": ".github/workflows/other.yml"}),
    ],
)
def test_source_workflow_provider_identity_is_required(
    check_override: dict[str, Any],
    run_override: dict[str, Any],
) -> None:
    adapter = Adapter(_check(**check_override), _run(**run_override), _artifact())

    with pytest.raises(ValueError, match="pre-merge|workflow"):
        _select(adapter)


@pytest.mark.parametrize(
    ("workflow_run", "message"),
    [
        (None, "identity"),
        ({"id": 999, "head_sha": HEAD_SHA}, "workflow run"),
        ({"id": 101, "head_sha": "f" * 40}, "head SHA"),
    ],
)
def test_source_artifact_provider_identity_is_required(
    workflow_run: Any,
    message: str,
) -> None:
    adapter = Adapter(_check(), _run(), _artifact(workflow_run=workflow_run))

    with pytest.raises(ValueError, match=message):
        _select(adapter)


def test_truncated_archive_is_terminal_and_not_retried_into_success() -> None:
    downloads = iter((b"truncated", RAW_ARCHIVE))
    adapter = Adapter(
        _check(),
        _run(),
        _artifact(),
        downloads=downloads,
    )

    with pytest.raises(ValueError, match="digest mismatch"):
        _select(adapter)

    assert next(downloads) == RAW_ARCHIVE


def test_github_adapter_retries_bounded_transient_failures(monkeypatch: Any) -> None:
    calls = 0

    def flaky(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")
        return {"id": 101}

    sleeps: list[float] = []
    monkeypatch.setattr(receipt_source.github_api, "github_json", flaky)
    adapter = receipt_source.GitHubSourceReceiptAdapter("token", attempts=2, sleeper=sleeps.append)

    assert adapter.get_workflow_run(REPOSITORY, 101) == {"id": 101}
    assert sleeps == [0.25]


def test_github_adapter_stops_after_bounded_retry_exhaustion(monkeypatch: Any) -> None:
    calls = 0

    def fail(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        raise RuntimeError("temporary")

    monkeypatch.setattr(receipt_source.github_api, "github_json", fail)
    adapter = receipt_source.GitHubSourceReceiptAdapter("token", attempts=3, sleeper=lambda _delay: None)

    with pytest.raises(RuntimeError, match="after 3 attempts"):
        adapter.get_workflow_run(REPOSITORY, 101)
    assert calls == 3


@pytest.mark.parametrize("error_location", ["receipt", "github_evidence"])
def test_source_pass_cannot_contain_contradictory_errors(error_location: str) -> None:
    selected = {
        "artifact_id": 301,
        "digest": "sha256:" + "b" * 64,
        "workflow_run_head_sha": HEAD_SHA,
    }
    receipt = {
        "errors": ["contradictory PASS error"] if error_location == "receipt" else [],
        "github_evidence": {
            "head_sha": HEAD_SHA,
            "errors": ["unresolved provider error"] if error_location == "github_evidence" else [],
            "artifacts": [selected],
        },
        "evidence_chain": {"head_sha": HEAD_SHA, "governance_artifact": selected},
    }

    if error_location == "receipt":
        with pytest.raises(ValueError, match="must not contain errors"):
            receipt["control_surface_changed"] = True
            receipt_source.source_archive._validate_receipt_semantics(receipt, status="PASS")
    else:
        with pytest.raises(ValueError, match="unresolved errors"):
            receipt_source.source_archive._validate_source_evidence(
                receipt,
                reviewed_head_sha=HEAD_SHA,
                status="PASS",
            )


@pytest.mark.parametrize(
    ("status", "control_surface_changed"),
    [("PASS", False), ("N/A", True)],
)
def test_source_status_and_control_surface_flag_cannot_contradict(
    status: str,
    control_surface_changed: bool,
) -> None:
    receipt = {"status": status, "control_surface_changed": control_surface_changed, "errors": []}

    with pytest.raises(ValueError, match="control_surface_changed"):
        receipt_source.source_archive._validate_receipt_semantics(receipt, status=status)
