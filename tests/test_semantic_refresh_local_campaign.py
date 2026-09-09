from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any, cast

import pytest

from dpone.adapters.semantic_refresh_local_campaign_evidence import (
    CreateOnlyLocalCampaignReportWriter,
    LocalCampaignEvidenceWriteError,
)
from dpone.ops.semantic_refresh_local_campaign import (
    LocalCampaignCheck,
    LocalCommandObservation,
    SemanticRefreshLocalCampaign,
)


class _Runner:
    def __init__(self, observations: tuple[LocalCommandObservation, ...]) -> None:
        self._observations = iter(observations)

    def __call__(self, **_: object) -> LocalCommandObservation:
        return next(self._observations)


def _check(check_id: str) -> LocalCampaignCheck:
    return LocalCampaignCheck(check_id, ("pytest", check_id), {"PASSWORD": "must-not-persist"})


def test_kubernetes_campaign_check_installs_its_declared_extra() -> None:
    module = _load_tool()
    checks = {item.check_id: item for item in cast(Any, module)._checks()}

    assert checks["kubernetes_termination"].argv[:4] == ("uv", "run", "--extra", "kubernetes")
    assert checks["vault_kubernetes_auth"].argv[:4] == ("uv", "run", "--extra", "vault")


def _load_tool() -> object:
    tool_path = Path("tools/semantic_refresh_local_campaign.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_semantic_refresh_local_campaign", tool_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_list_does_not_require_output(capsys: pytest.CaptureFixture[str]) -> None:
    module = cast(Any, _load_tool())

    assert module.main(["--list"]) == 0

    assert "contracts" in capsys.readouterr().out


def test_campaign_execution_requires_explicit_output() -> None:
    module = cast(Any, _load_tool())

    with pytest.raises(SystemExit) as exc_info:
        module.main([])

    assert exc_info.value.code == 2


def test_complete_dirty_local_campaign_is_unverified(tmp_path: Path) -> None:
    report = SemanticRefreshLocalCampaign(
        command_runner=_Runner((LocalCommandObservation(0, "1 passed", "", 10),)),
        now=lambda: "2026-08-08T10:00:00Z",
    ).run(
        repo_root=tmp_path,
        output_path=tmp_path / "evidence.json",
        campaign_id="local-campaign",
        git_head_sha="a" * 40,
        source_snapshot_sha256="sha256:" + "b" * 64,
        worktree_dirty=True,
        checks=(_check("mssql"),),
        expected_check_ids=frozenset({"mssql"}),
    )

    payload = json.loads((tmp_path / "evidence.json").read_text())
    assert report.status == "UNVERIFIED"
    assert payload["status"] == "UNVERIFIED"
    assert payload["production_certification"] == "UNVERIFIED"
    assert "PASSWORD" not in json.dumps(payload)
    assert "must-not-persist" not in json.dumps(payload)


def test_skip_or_partial_campaign_is_unverified(tmp_path: Path) -> None:
    report = SemanticRefreshLocalCampaign(
        command_runner=_Runner((LocalCommandObservation(0, "1 skipped", "", 4, skipped=True),)),
        now=lambda: "2026-08-08T10:00:00Z",
    ).run(
        repo_root=tmp_path,
        output_path=tmp_path / "evidence.json",
        campaign_id="local-campaign",
        git_head_sha="a" * 40,
        source_snapshot_sha256="sha256:" + "b" * 64,
        worktree_dirty=False,
        checks=(_check("mssql"),),
        expected_check_ids=frozenset({"mssql", "clickhouse"}),
    )

    assert report.status == "UNVERIFIED"
    assert report.results[0].status == "UNVERIFIED"


def test_command_failure_fails_campaign_and_persists_only_redacted_digest(tmp_path: Path) -> None:
    report = SemanticRefreshLocalCampaign(
        command_runner=_Runner((LocalCommandObservation(1, "password=secret", "/private/path", 9),)),
        now=lambda: "2026-08-08T10:00:00Z",
    ).run(
        repo_root=tmp_path,
        output_path=tmp_path / "evidence.json",
        campaign_id="local-campaign",
        git_head_sha="a" * 40,
        source_snapshot_sha256="sha256:" + "b" * 64,
        worktree_dirty=False,
        checks=(_check("mssql"),),
        expected_check_ids=frozenset({"mssql"}),
    )

    payload = (tmp_path / "evidence.json").read_text()
    assert report.status == "FAIL"
    assert "secret" not in payload
    assert "/private/path" not in payload


def test_create_only_report_writer_rejects_conflicting_existing_evidence(tmp_path: Path) -> None:
    output = tmp_path / "evidence.json"
    output.write_text("old\n", encoding="utf-8")

    with pytest.raises(LocalCampaignEvidenceWriteError, match="already exists"):
        CreateOnlyLocalCampaignReportWriter()(output, "new\n")

    assert output.read_text(encoding="utf-8") == "old\n"


def test_create_only_report_writer_publishes_complete_bytes_once(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "evidence.json"

    CreateOnlyLocalCampaignReportWriter()(output, '{"status":"PASS"}\n')

    assert output.read_bytes() == b'{"status":"PASS"}\n'
