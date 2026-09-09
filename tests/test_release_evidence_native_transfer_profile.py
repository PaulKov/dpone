from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.ops.live_state_reconciliation import LiveStateReconciliationCertificationService
from dpone.ops.performance_certification import PerformanceCertificationService
from dpone.ops.release_evidence_pack import ReleaseEvidencePackService
from dpone.strategy_intelligence.certification_bundle import (
    CertificationEvidenceInput,
    StrategyCertificationEvidenceBundleWriter,
)


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_native_transfer_release_profile_requires_strategy_certification_bundle(tmp_path: Path) -> None:
    artifacts = _release_artifacts(tmp_path)
    artifacts.pop("strategy_certification_bundle")

    report = ReleaseEvidencePackService().build(
        output_dir=tmp_path / "release-pack",
        release="v0.8.0",
        profile="native_transfer",
        artifacts=artifacts,
    )

    assert report.passed is False
    assert "strategy_certification_bundle.missing" in report.blockers


def test_native_transfer_release_profile_passes_with_strategy_certification_bundle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    artifact_args = []
    for name, path in _release_artifacts(tmp_path).items():
        artifact_args.extend(["--artifact", f"{name}={path}"])

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-evidence-pack",
                "--release",
                "v0.8.0",
                "--profile",
                "native_transfer",
                "--output-dir",
                str(tmp_path / "pack"),
                "--format",
                "json",
                *artifact_args,
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert [item["name"] for item in payload["artifacts"]][-1] == "strategy_certification_bundle"


def test_release_pack_explicit_required_overrides_native_transfer_profile(tmp_path: Path) -> None:
    artifacts = {"performance_certification": _release_artifacts(tmp_path)["performance_certification"]}

    report = ReleaseEvidencePackService().build(
        output_dir=tmp_path / "release-pack",
        release="v0.8.0",
        profile="native_transfer",
        artifacts=artifacts,
        required=("performance_certification",),
    )

    assert report.passed is True
    assert [item.name for item in report.artifacts] == ["performance_certification"]


def test_documented_behavioral_producers_feed_release_evidence_pack(tmp_path: Path) -> None:
    state = _write_json(
        tmp_path / "state.json",
        {"passed": True, "state_backends": ["mssql"], "checks": ["checkpoint"]},
    )
    reconciliation = _write_json(
        tmp_path / "reconciliation.json",
        {"passed": True, "delete_reconciliation": {"passed": True, "physical_deletes_checked": 1}},
    )
    performance = PerformanceCertificationService().certify(
        output_dir=tmp_path / "performance",
        profile="real_local",
        row_count=10_000,
        metrics={"throughput_rows_per_second": 1_000, "failure_rate": 0},
        minimums={"throughput_rows_per_second": 500},
        maximums={"failure_rate": 0},
    )
    live_state = LiveStateReconciliationCertificationService().certify(
        output_dir=tmp_path / "live-state",
        profile="real_local",
        artifacts={"state": state, "reconciliation": reconciliation},
    )
    strategy = StrategyCertificationEvidenceBundleWriter(tmp_path / "strategy").write(
        CertificationEvidenceInput(bundle_id="v0.74.0", connector_artifacts=(state,))
    )

    report = ReleaseEvidencePackService().build(
        output_dir=tmp_path / "pack",
        release="v0.74.0",
        profile="native_transfer",
        artifacts={
            "performance_certification": Path(performance.json_path),
            "live_state_reconciliation": Path(live_state.json_path),
            "strategy_certification_bundle": strategy.json_path,
        },
        required=(
            "performance_certification",
            "live_state_reconciliation",
            "strategy_certification_bundle",
        ),
    )

    assert report.passed is True
    assert report.blockers == ()


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _release_artifacts(tmp_path: Path) -> dict[str, Path]:
    return {
        "service_markers": _write_json(tmp_path / "service_markers.json", {"passed": True}),
        "certification_pack": _write_json(tmp_path / "certification_pack.json", _trusted_pass()),
        "performance_certification": _write_json(
            tmp_path / "performance_certification.json",
            _trusted_pass(),
        ),
        "live_state_reconciliation": _write_json(
            tmp_path / "live_state_reconciliation.json",
            _trusted_pass(),
        ),
        "pre_release_checklist": _write_json(tmp_path / "pre_release_checklist.json", {"passed": True}),
        "evidence_chain": _write_json(tmp_path / "evidence_chain.json", {"passed": True}),
        "strategy_certification_bundle": _write_json(
            tmp_path / "strategy_certification_bundle.json",
            {
                "schema_version": "dpone.strategy.certification_bundle.v1",
                "passed": True,
                "evidence_status": "PASS",
                "blockers": [],
                "evidence_items": [{"kind": "native_transfer", "passed": True}],
            },
        ),
    }


def _trusted_pass() -> dict[str, object]:
    return {"passed": True, "evidence_status": "PASS", "blockers": []}


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path
