from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_data_product_rollout_cli_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    bundle = _write_json(tmp_path / "bundle.json", _bundle())
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    for kind, payload in _green_evidence().items():
        _write_json(evidence_dir / f"{kind}.json", payload)

    for args in (
        ["data", "product", "rollout", "--help"],
        ["data", "product", "rollout", "plan", "--help"],
        ["data", "product", "rollout", "shadow-validate", "--help"],
        ["data", "product", "rollout", "ring", "gate", "--help"],
        ["data", "product", "rollout", "promote", "--help"],
        ["data", "product", "rollout", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()
    for args in (
        ["schema", "migration", "bundle", "build", "--help"],
        ["schema", "migration", "registry", "record", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
        help_text = capsys.readouterr().out
        assert "--data-product-ring-gate" in help_text
        assert "--data-product-shadow-validation" in help_text
        assert "--data-product-rollout-promotion" in help_text
        assert "--data-product-rollout-report" in help_text

    plan_path = tmp_path / "rollout-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "rollout",
                "plan",
                "--manifest",
                str(manifest),
                "--bundle",
                str(bundle),
                "--evidence-dir",
                str(evidence_dir),
                "--format",
                "json",
                "--output",
                str(plan_path),
            ]
        )
    assert plan_exit.value.code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["schema_version"] == "dpone.data_product_rollout_plan.v1"
    assert _read_json(plan_path)["rollout_plan_id"] == plan["rollout_plan_id"]

    candidate = _write_json(tmp_path / "candidate.json", _runtime())
    baseline = _write_json(tmp_path / "baseline.json", _runtime())
    shadow_path = tmp_path / "shadow.json"
    with pytest.raises(SystemExit) as shadow_exit:
        cli_main.main(
            [
                "data",
                "product",
                "rollout",
                "shadow-validate",
                "--plan",
                str(plan_path),
                "--runtime-artifact",
                str(candidate),
                "--baseline-runtime-artifact",
                str(baseline),
                "--format",
                "json",
                "--output",
                str(shadow_path),
            ]
        )
    assert shadow_exit.value.code == 0
    shadow = json.loads(capsys.readouterr().out)
    assert shadow["status"] == "passed"

    staging_promotion = _write_json(
        tmp_path / "staging-promotion.json",
        {"status": "promoted", "from_ring": "staging", "to_ring": "canary"},
    )
    gate_path = tmp_path / "canary-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "rollout",
                "ring",
                "gate",
                "--plan",
                str(plan_path),
                "--ring",
                "canary",
                "--shadow-validation",
                str(shadow_path),
                "--promotion",
                str(staging_promotion),
                "--evidence-dir",
                str(evidence_dir),
                "--format",
                "json",
                "--output",
                str(gate_path),
            ]
        )
    assert gate_exit.value.code == 0
    gate = json.loads(capsys.readouterr().out)
    assert gate["status"] == "allowed"

    authority = _write_json(tmp_path / "authority.json", {"status": "allowed", "authority_gate_id": "sha256:auth"})
    promotion_path = tmp_path / "promotion.json"
    with pytest.raises(SystemExit) as promote_exit:
        cli_main.main(
            [
                "data",
                "product",
                "rollout",
                "promote",
                "--ring-gate",
                str(gate_path),
                "--target-ring",
                "prod_full",
                "--authority-gate",
                str(authority),
                "--format",
                "json",
                "--output",
                str(promotion_path),
            ]
        )
    assert promote_exit.value.code == 0
    promotion = json.loads(capsys.readouterr().out)
    assert promotion["status"] == "promoted"

    report_path = tmp_path / "rollout.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "rollout",
                "report",
                "--promotion",
                str(promotion_path),
                "--format",
                "md",
                "--output",
                str(report_path),
            ]
        )
    assert report_exit.value.code == 0
    report = capsys.readouterr().out
    assert "# Data Product Rollout Report" in report
    assert report_path.read_text(encoding="utf-8") == report


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _write_yaml(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _manifest() -> dict:
    from tests.test_data_product_rollout_contracts import _manifest

    return _manifest()


def _bundle() -> dict:
    from tests.test_data_product_rollout_contracts import _bundle

    return _bundle()


def _green_evidence() -> dict[str, dict]:
    from tests.test_data_product_rollout_contracts import _green_canary_evidence

    return _green_canary_evidence()


def _runtime() -> dict:
    from tests.test_data_product_rollout_contracts import _runtime

    return _runtime(row_count=1000, typed_hash="sha256:ok")
