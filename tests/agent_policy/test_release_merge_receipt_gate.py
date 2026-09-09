from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import stat
import sys
import zipfile
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "PaulKov/dpone"
INTEGRATION_SHA = "d" * 40
HEAD_SHA = "a" * 40
RUN_ID = 101
RUN_ATTEMPT = 2


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


gate = _load(
    "dpone_agent_release_merge_receipt_gate_test",
    "tools/agent_policy/release_merge_receipt_gate.py",
)


def _digest(raw: bytes) -> str:
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _receipt(source_archive: bytes) -> dict[str, Any]:
    payload = {
        "schema_version": 1,
        "status": "PASS",
        "binding_id": f"sha256:{'0' * 64}",
        "repository": REPOSITORY,
        "protected_base_ref": "master",
        "pr_number": 455,
        "merged_at": "2026-07-29T00:00:00Z",
        "integration_method": "merge",
        "reviewed_head_sha": HEAD_SHA,
        "reviewed_head_tree": "b" * 40,
        "base_parent_sha": "c" * 40,
        "integration_commit_sha": INTEGRATION_SHA,
        "integration_tree": "b" * 40,
        "changed_paths": [],
        "pr_body_sha256": f"sha256:{'1' * 64}",
        "source_receipt": {
            "check_run_id": 1,
            "workflow_run_id": 2,
            "workflow_run_attempt": 1,
            "artifact_id": 3,
            "artifact_digest": f"sha256:{'2' * 64}",
            "artifact_size_bytes": 1,
            "archive_sha256": _digest(source_archive),
            "archive_size_bytes": len(source_archive),
            "created_at": "2026-07-29T00:00:00Z",
            "completed_at": "2026-07-29T00:00:00Z",
            "receipt_sha256": f"sha256:{'4' * 64}",
            "audit_manifest_sha256": f"sha256:{'5' * 64}",
            "body_sha256": f"sha256:{'1' * 64}",
            "status": "PASS",
        },
        "producer": {
            "workflow": "Agent PR receipt",
            "run_id": str(RUN_ID),
            "run_attempt": str(RUN_ATTEMPT),
        },
        "errors": [],
        "warnings": [],
    }
    binding_fields = (
        "repository",
        "protected_base_ref",
        "pr_number",
        "merged_at",
        "integration_method",
        "reviewed_head_sha",
        "reviewed_head_tree",
        "base_parent_sha",
        "integration_commit_sha",
        "integration_tree",
        "changed_paths",
        "pr_body_sha256",
        "source_receipt",
    )
    binding = {field: payload[field] for field in binding_fields}
    payload["binding_id"] = _digest(json.dumps(binding, sort_keys=True, separators=(",", ":")).encode())
    return payload


def _archive(*, receipt: dict[str, Any] | None = None, exit_code: str = "0", source: bytes = b"source") -> bytes:
    payload = receipt or _receipt(source)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("agent_pr_merge_receipt.json", json.dumps(payload))
        archive.writestr("pr-receipt-exit-code.txt", f"{exit_code}\n")
        archive.writestr("source-agent-pr-receipt.zip", source)
    return buffer.getvalue()


def _check(**overrides: Any) -> dict[str, Any]:
    return {
        "id": 42,
        "name": "Agent PR receipt",
        "head_sha": INTEGRATION_SHA,
        "status": "completed",
        "conclusion": "success",
        "details_url": f"https://github.com/{REPOSITORY}/runs/42",
        "external_id": f"agent-pr-merge-closure:{RUN_ID}:{RUN_ATTEMPT}",
        "app": {"id": 15368, "slug": "github-actions"},
        **overrides,
    }


def _run(**overrides: Any) -> dict[str, Any]:
    return {
        "id": RUN_ID,
        "name": "Agent PR receipt",
        "event": "pull_request",
        "status": "completed",
        "conclusion": "success",
        "run_attempt": RUN_ATTEMPT,
        "head_sha": HEAD_SHA,
        "repository": {"full_name": REPOSITORY},
        "path": ".github/workflows/agent-pr-receipt.yml@refs/pull/455/merge",
        **overrides,
    }


def _artifact(raw: bytes, **overrides: Any) -> dict[str, Any]:
    return {
        "id": 201,
        "name": "agent-pr-receipt",
        "expired": False,
        "digest": _digest(raw),
        "size_in_bytes": len(raw),
        "archive_download_url": "https://api.github.test/artifacts/201/zip",
        "workflow_run": {"id": RUN_ID, "head_sha": HEAD_SHA},
        **overrides,
    }


@dataclass
class Adapter:
    checks: list[dict[str, Any]]
    run: dict[str, Any]
    artifacts: list[dict[str, Any]]
    raw: bytes

    def list_check_runs(self, repository: str, commit_sha: str) -> list[dict[str, Any]]:
        assert (repository, commit_sha) == (REPOSITORY, INTEGRATION_SHA)
        return self.checks

    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]:
        assert repository == REPOSITORY
        return self.run

    def list_run_artifacts(self, repository: str, run_id: int) -> list[dict[str, Any]]:
        assert (repository, run_id) == (REPOSITORY, RUN_ID)
        return self.artifacts

    def download_artifact(self, url: str) -> bytes:
        assert url.endswith("/201/zip")
        return self.raw


def _adapter() -> Adapter:
    raw = _archive()
    return Adapter([_check()], _run(), [_artifact(raw)], raw)


def _verify(adapter: Adapter) -> Any:
    return gate.verify_merge_receipt(
        repository=REPOSITORY,
        integration_commit_sha=INTEGRATION_SHA,
        adapter=adapter,
    )


def _required_check_report(evidence: Any) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "PASS",
        "repository": REPOSITORY,
        "commit_sha": INTEGRATION_SHA,
        "contexts": [
            {
                "context": "Agent PR receipt",
                "status": "PASS",
                "integration_id": evidence.github_app_id,
                "current_observations": [
                    {
                        "source": "check_run",
                        "evidence_id": evidence.check_run_id,
                        "state": "success",
                        "integration_id": evidence.github_app_id,
                        "commit_sha": INTEGRATION_SHA,
                    }
                ],
            }
        ],
    }


def test_exact_check_is_bound_to_successful_run_and_durable_receipt_bytes() -> None:
    evidence = _verify(_adapter())

    assert evidence.status == "PASS"
    assert (evidence.check_run_id, evidence.github_app_id) == (42, 15368)
    assert (evidence.workflow_run_id, evidence.workflow_run_attempt) == (RUN_ID, RUN_ATTEMPT)
    assert evidence.integration_commit_sha == INTEGRATION_SHA
    assert evidence.artifact_id == 201


def test_required_check_report_reconciles_exact_check_and_app(tmp_path: Path) -> None:
    evidence = _verify(_adapter())
    path = tmp_path / "checks.json"
    path.write_text(json.dumps(_required_check_report(evidence)), encoding="utf-8")

    gate.reconcile.reconcile_required_check_report(evidence, path)


@pytest.mark.parametrize("mismatch", ["status", "check", "app", "commit"])
def test_required_check_report_mismatch_fails_closed(tmp_path: Path, mismatch: str) -> None:
    evidence = _verify(_adapter())
    report = _required_check_report(evidence)
    context = report["contexts"][0]
    observation = context["current_observations"][0]
    if mismatch == "status":
        report["status"] = "FAIL"
    elif mismatch == "check":
        observation["evidence_id"] = 999
    elif mismatch == "app":
        context["integration_id"] = 999
    else:
        report["commit_sha"] = "e" * 40
    path = tmp_path / "checks.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(ValueError, match="report|observation"):
        gate.reconcile.reconcile_required_check_report(evidence, path)


def test_newer_failed_check_prevents_reuse_of_stale_success() -> None:
    adapter = _adapter()
    adapter.checks = [_check(id=41), _check(id=42, conclusion="failure")]

    with pytest.raises(ValueError, match="latest"):
        _verify(adapter)


@pytest.mark.parametrize(
    "check",
    [
        _check(details_url=f"https://github.com/{REPOSITORY}/actions/runs/999"),
        _check(details_url=f"https://github.com/{REPOSITORY}/runs/999"),
        _check(external_id="agent-pr-merge-closure:999:2"),
        _check(app={"id": 15368, "slug": "untrusted"}),
    ],
)
def test_check_provider_and_producer_identity_is_closed(check: dict[str, Any]) -> None:
    adapter = _adapter()
    adapter.checks = [check]

    with pytest.raises(ValueError):
        _verify(adapter)


def test_unsuccessful_workflow_run_cannot_back_a_green_check() -> None:
    adapter = _adapter()
    adapter.run = _run(conclusion="failure")

    with pytest.raises(ValueError, match="run identity"):
        _verify(adapter)


@pytest.mark.parametrize(
    "artifact_override",
    [
        {"expired": True},
        {"workflow_run": {"id": 999, "head_sha": HEAD_SHA}},
        {"digest": f"sha256:{'f' * 64}"},
    ],
)
def test_artifact_provider_identity_and_bytes_are_verified(artifact_override: dict[str, Any]) -> None:
    adapter = _adapter()
    adapter.artifacts = [{**adapter.artifacts[0], **artifact_override}]

    with pytest.raises(ValueError, match="artifact|metadata"):
        _verify(adapter)


@pytest.mark.parametrize("failure", ["exit", "commit", "head", "producer", "binding", "source"])
def test_receipt_semantics_fail_closed(failure: str) -> None:
    source = b"source"
    receipt = _receipt(source)
    exit_code = "0"
    archived_source = source
    if failure == "exit":
        exit_code = "1"
    elif failure == "commit":
        receipt["integration_commit_sha"] = "e" * 40
    elif failure == "head":
        receipt["reviewed_head_sha"] = "e" * 40
    elif failure == "producer":
        receipt["producer"] = {**receipt["producer"], "run_attempt": "9"}
    elif failure == "binding":
        receipt["binding_id"] = f"sha256:{'f' * 64}"
    else:
        archived_source = b"tampered"
    raw = _archive(receipt=deepcopy(receipt), exit_code=exit_code, source=archived_source)
    adapter = Adapter([_check()], _run(), [_artifact(raw)], raw)

    with pytest.raises(ValueError):
        _verify(adapter)


def test_github_adapter_retries_bounded_read_failure(monkeypatch: Any) -> None:
    calls = 0

    def flaky(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")
        return {"id": RUN_ID}

    sleeps: list[float] = []
    monkeypatch.setattr(gate.github_api, "github_json", flaky)
    adapter = gate.GitHubMergeReceiptAdapter("token", attempts=2, sleeper=sleeps.append)

    assert adapter.get_workflow_run(REPOSITORY, RUN_ID) == {"id": RUN_ID}
    assert calls == 2
    assert sleeps == [0.25]


@pytest.mark.parametrize("unsafe", ["extra", "symlink"])
def test_outer_archive_rejects_extra_files_and_symlinks(unsafe: str) -> None:
    raw = _archive()
    source = io.BytesIO(raw)
    rewritten = io.BytesIO()
    with zipfile.ZipFile(source) as original, zipfile.ZipFile(rewritten, "w") as target:
        for info in original.infolist():
            if unsafe == "symlink" and info.filename == "source-agent-pr-receipt.zip":
                continue
            target.writestr(info.filename, original.read(info.filename))
        if unsafe == "extra":
            target.writestr("unexpected.txt", b"unsafe")
        else:
            link = zipfile.ZipInfo("source-agent-pr-receipt.zip")
            link.create_system = 3
            link.external_attr = (stat.S_IFLNK | 0o777) << 16
            target.writestr(link, b"agent_pr_merge_receipt.json")
    unsafe_raw = rewritten.getvalue()
    adapter = Adapter([_check()], _run(), [_artifact(unsafe_raw)], unsafe_raw)

    with pytest.raises(ValueError, match="file set|symlink"):
        _verify(adapter)


def test_cli_without_approved_token_emits_failure_report(
    tmp_path: Path,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    checks = tmp_path / "checks.json"
    checks.write_text("{}\n", encoding="utf-8")
    monkeypatch.delenv("MISSING_TOKEN", raising=False)

    exit_code = gate.main(
        [
            "--repo",
            REPOSITORY,
            "--commit-sha",
            INTEGRATION_SHA,
            "--github-token-env",
            "MISSING_TOKEN",
            "--required-check-report",
            str(checks),
        ]
    )

    assert exit_code == 1
    assert json.loads(capsys.readouterr().out)["status"] == "FAIL"
