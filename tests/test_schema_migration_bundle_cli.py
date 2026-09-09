from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.readiness.migration_control import MigrationPack, MigrationTarget


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_schema_migration_bundle_and_review_help_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_cli(monkeypatch)

    commands = (
        ["schema", "migration", "bundle", "--help"],
        ["schema", "migration", "bundle", "build", "--help"],
        ["schema", "migration", "bundle", "verify", "--help"],
        ["schema", "migration", "bundle", "gate", "--help"],
        ["schema", "migration", "bundle", "diff", "--help"],
        ["schema", "migration", "bundle", "attest", "--help"],
        ["schema", "migration", "bundle", "trust", "--help"],
        ["schema", "migration", "bundle", "trust", "verify", "--help"],
        ["schema", "migration", "review", "--help"],
        ["schema", "migration", "review", "render", "--help"],
    )

    for command in commands:
        with pytest.raises(SystemExit) as exc:
            cli_main.main(command)
        assert exc.value.code == 0


def test_bundle_build_verify_and_review_render_json_md_text(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_json(tmp_path / "orders.pack.json", pack.to_dict(command="plan"))
    impact_path = _write_json(
        tmp_path / "orders.impact.json",
        {
            "schema_version": "dpone.schema_impact_plan.v1",
            "pack_id": pack.pack_id,
            "impact_plan_id": "sha256:" + "1" * 64,
            "required_approvals": ["compatibility_breaking"],
            "blockers": [],
            "warnings": ["finance.daily_margin reads amount"],
        },
    )
    approval_path = _write_json(
        tmp_path / "orders.approval.json",
        {
            "pack_id": pack.pack_id,
            "impact_plan_id": "sha256:" + "1" * 64,
            "approved_by": "finance-data-owner",
            "approved_risks": ["compatibility_breaking"],
        },
    )
    output_dir = tmp_path / "review"
    build_output = tmp_path / "bundle-build-output.json"

    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--impact",
                str(impact_path),
                "--approval",
                str(approval_path),
                "--attest",
                "--output-dir",
                str(output_dir),
                "--output",
                str(build_output),
                "--format",
                "json",
            ]
        )

    assert build_exit.value.code == 0
    bundle = json.loads(capsys.readouterr().out)
    bundle_path = output_dir / "bundle.json"
    assert bundle["schema_version"] == "dpone.schema_migration_bundle.v1"
    assert bundle["status"] == "ready"
    assert json.loads(bundle_path.read_text(encoding="utf-8"))["bundle_id"] == bundle["bundle_id"]
    assert json.loads(build_output.read_text(encoding="utf-8"))["bundle_id"] == bundle["bundle_id"]

    with pytest.raises(SystemExit) as verify_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "verify",
                "--bundle",
                str(bundle_path),
                "--require-attestation",
                "--format",
                "json",
            ]
        )

    assert verify_exit.value.code == 0
    verification = json.loads(capsys.readouterr().out)
    assert verification["schema_version"] == "dpone.schema_migration_bundle_verification.v1"
    assert verification["status"] == "passed"

    review_path = output_dir / "review.md"
    with pytest.raises(SystemExit) as review_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "review",
                "render",
                "--bundle",
                str(bundle_path),
                "--format",
                "md",
                "--output",
                str(review_path),
            ]
        )

    assert review_exit.value.code == 0
    markdown = capsys.readouterr().out
    assert "# Schema Migration Review" in markdown
    assert "finance-data-owner" in markdown
    assert review_path.read_text(encoding="utf-8") == markdown

    with pytest.raises(SystemExit) as review_json_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "review",
                "render",
                "--bundle",
                str(bundle_path),
                "--format",
                "json",
            ]
        )

    assert review_json_exit.value.code == 0
    review_payload = json.loads(capsys.readouterr().out)
    assert review_payload["schema_version"] == "dpone.schema_migration_review.v1"
    assert "# Schema Migration Review" in review_payload["markdown"]


def test_bundle_gate_cli_evaluates_profile_and_writes_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_json(tmp_path / "orders.pack.json", pack.to_dict(command="plan"))
    impact_path = _write_json(
        tmp_path / "orders.impact.json",
        {
            "schema_version": "dpone.schema_impact_plan.v1",
            "pack_id": pack.pack_id,
            "impact_plan_id": "sha256:" + "2" * 64,
            "required_approvals": ["compatibility_breaking"],
            "blockers": [],
            "warnings": [],
        },
    )
    approval_path = _write_json(
        tmp_path / "orders.approval.json",
        {
            "pack_id": pack.pack_id,
            "impact_plan_id": "sha256:" + "2" * 64,
            "approved_by": "finance-data-owner",
            "approved_risks": ["compatibility_breaking"],
        },
    )
    output_dir = tmp_path / "review"

    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--impact",
                str(impact_path),
                "--approval",
                str(approval_path),
                "--attest",
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    capsys.readouterr()

    gate_output = output_dir / "bundle-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "gate",
                "--bundle",
                str(output_dir / "bundle.json"),
                "--profile",
                "pr_review",
                "--output",
                str(gate_output),
                "--format",
                "json",
            ]
        )

    assert gate_exit.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.schema_migration_bundle_gate.v1"
    assert payload["status"] == "allowed"
    assert payload["profile"] == "pr_review"
    assert payload["pack_id"] == pack.pack_id
    assert json.loads(gate_output.read_text(encoding="utf-8"))["gate_id"] == payload["gate_id"]


def test_bundle_attest_trust_verify_and_gate_cli(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    monkeypatch.setenv("DPONE_BUNDLE_SIGNING_KEY", "secret")
    pack_path = _write_json(tmp_path / "orders.pack.json", _pack().to_dict(command="plan"))
    output_dir = tmp_path / "review"

    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--attest",
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    capsys.readouterr()

    provenance_path = output_dir / "provenance.json"
    with pytest.raises(SystemExit) as attest_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "attest",
                "--bundle",
                str(output_dir / "bundle.json"),
                "--output",
                str(provenance_path),
                "--signing-key-env",
                "DPONE_BUNDLE_SIGNING_KEY",
                "--signing-key-id",
                "ci-hmac",
                "--provenance-source",
                "github_actions",
                "--repository",
                "https://github.com/acme/data-platform",
                "--commit-sha",
                "a" * 40,
                "--ref",
                "refs/heads/main",
                "--run-id",
                "12345",
                "--workflow",
                ".github/workflows/schema-migration.yml",
                "--protected-ref",
                "--format",
                "json",
            ]
        )

    assert attest_exit.value.code == 0
    provenance = json.loads(capsys.readouterr().out)
    assert provenance["schema_version"] == "dpone.schema_migration_provenance.v1"
    assert provenance["status"] == "attested"
    assert provenance_path.exists()
    assert json.loads(provenance_path.read_text(encoding="utf-8"))["provenance_id"] == provenance["provenance_id"]

    policy_path = _write_json(
        tmp_path / "trust-policy.json",
        {
            "schema_version": "dpone.schema_migration_trust_policy.v1",
            "profile": "prod_trusted",
            "require_signed_provenance": True,
            "allowed_repositories": ["https://github.com/acme/data-platform"],
            "allowed_refs": ["refs/heads/main"],
            "require_protected_ref": True,
            "require_commit_sha": True,
            "require_ci_run": True,
            "signature": {"allowed_algorithms": ["HMAC-SHA256"], "allowed_key_ids": ["ci-hmac"]},
        },
    )
    trust_output = output_dir / "trust-verification.json"
    with pytest.raises(SystemExit) as trust_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "trust",
                "verify",
                "--bundle",
                str(output_dir / "bundle.json"),
                "--provenance",
                str(provenance_path),
                "--policy",
                str(policy_path),
                "--signing-key-env",
                "DPONE_BUNDLE_SIGNING_KEY",
                "--output",
                str(trust_output),
                "--format",
                "json",
            ]
        )

    assert trust_exit.value.code == 0
    trust = json.loads(capsys.readouterr().out)
    assert trust["schema_version"] == "dpone.schema_migration_trust_verification.v1"
    assert trust["status"] == "trusted"
    assert (
        json.loads(trust_output.read_text(encoding="utf-8"))["trust_verification_id"] == trust["trust_verification_id"]
    )

    gate_policy = _write_json(
        tmp_path / "bundle-policy.json",
        {
            "schema_version": "dpone.schema_migration_bundle_policy.v1",
            "profile": "prod_strict",
            "required_artifacts": ["migration_pack"],
            "require_trusted_provenance": True,
            "fail_on_warnings": False,
        },
    )
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "gate",
                "--bundle",
                str(output_dir / "bundle.json"),
                "--policy",
                str(gate_policy),
                "--trust-verification",
                str(trust_output),
                "--format",
                "json",
            ]
        )

    assert gate_exit.value.code == 0
    gate = json.loads(capsys.readouterr().out)
    assert gate["status"] == "allowed"
    assert gate["checks"][-1]["name"] == "trusted_provenance"


@pytest.mark.parametrize("output_format", ["text", "md", "table"])
def test_bundle_gate_cli_supports_human_formats(
    output_format: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack_path = _write_json(tmp_path / "orders.pack.json", _pack().to_dict(command="plan"))
    output_dir = tmp_path / "review"

    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--attest",
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "gate",
                "--bundle",
                str(output_dir / "bundle.json"),
                "--profile",
                "advisory",
                "--format",
                output_format,
            ]
        )

    assert gate_exit.value.code == 0
    output = capsys.readouterr().out
    assert "dpone.schema_migration_bundle_gate.v1" in output or "Schema Migration Bundle Gate" in output
    assert "allowed" in output


def test_bundle_gate_cli_blocks_invalid_policy(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack_path = _write_json(tmp_path / "orders.pack.json", _pack().to_dict(command="plan"))
    output_dir = tmp_path / "review"
    policy_path = _write_json(
        tmp_path / "policy.json",
        {"schema_version": "dpone.schema_migration_bundle_policy.v1", "profile": "unknown"},
    )

    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--attest",
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "gate",
                "--bundle",
                str(output_dir / "bundle.json"),
                "--policy",
                str(policy_path),
                "--format",
                "json",
            ]
        )

    assert gate_exit.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "migration_bundle_gate.policy_invalid:unknown policy profile: unknown" in payload["blockers"]


def test_bundle_gate_cli_accepts_yaml_policy_override(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack_path = _write_json(tmp_path / "orders.pack.json", _pack().to_dict(command="plan"))
    output_dir = tmp_path / "review"
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        "\n".join(
            (
                "schema_version: dpone.schema_migration_bundle_policy.v1",
                "profile: prod_strict",
                "required_artifacts:",
                "  - migration_pack",
                "fail_on_warnings: false",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--attest",
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "gate",
                "--bundle",
                str(output_dir / "bundle.json"),
                "--policy",
                str(policy_path),
                "--format",
                "json",
            ]
        )

    assert gate_exit.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "allowed"
    assert payload["profile"] == "prod_strict"


def test_bundle_diff_cli_renders_pack_delta_json_and_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    base_pack = _pack()
    head_pack = _pack(ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 16384",))
    base_pack_path = _write_json(tmp_path / "base.pack.json", base_pack.to_dict(command="plan"))
    head_pack_path = _write_json(tmp_path / "head.pack.json", head_pack.to_dict(command="plan"))
    base_dir = tmp_path / "base-review"
    head_dir = tmp_path / "head-review"

    for pack_path, output_dir in ((base_pack_path, base_dir), (head_pack_path, head_dir)):
        with pytest.raises(SystemExit) as build_exit:
            cli_main.main(
                [
                    "schema",
                    "migration",
                    "bundle",
                    "build",
                    "--pack",
                    str(pack_path),
                    "--attest",
                    "--output-dir",
                    str(output_dir),
                    "--format",
                    "json",
                ]
            )
        assert build_exit.value.code == 0
        capsys.readouterr()

    diff_output = tmp_path / "bundle-diff.json"
    with pytest.raises(SystemExit) as diff_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "diff",
                "--base",
                str(base_dir / "bundle.json"),
                "--head",
                str(head_dir / "bundle.json"),
                "--require-attestation",
                "--format",
                "json",
                "--output",
                str(diff_output),
            ]
        )

    assert diff_exit.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.schema_migration_bundle_diff.v1"
    assert payload["status"] == "breaking"
    assert payload["summary"]["changes_count"] >= 1
    assert payload["changes"][0]["kind"] == "migration_pack"
    assert json.loads(diff_output.read_text(encoding="utf-8"))["diff_id"] == payload["diff_id"]


@pytest.mark.parametrize("output_format", ["text", "md", "table"])
def test_bundle_diff_cli_supports_human_formats(
    output_format: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack_path = _write_json(tmp_path / "orders.pack.json", _pack().to_dict(command="plan"))
    base_dir = tmp_path / "base-review"
    head_dir = tmp_path / "head-review"

    for output_dir in (base_dir, head_dir):
        with pytest.raises(SystemExit) as build_exit:
            cli_main.main(
                [
                    "schema",
                    "migration",
                    "bundle",
                    "build",
                    "--pack",
                    str(pack_path),
                    "--attest",
                    "--output-dir",
                    str(output_dir),
                    "--format",
                    "json",
                ]
            )
        assert build_exit.value.code == 0
        capsys.readouterr()

    with pytest.raises(SystemExit) as diff_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "diff",
                "--base",
                str(base_dir / "bundle.json"),
                "--head",
                str(head_dir / "bundle.json"),
                "--format",
                output_format,
            ]
        )

    assert diff_exit.value.code == 0
    output = capsys.readouterr().out
    assert "Schema Migration Bundle Diff" in output or "dpone.schema_migration_bundle_diff.v1" in output
    assert "same" in output


def test_bundle_verify_blocks_when_artifact_bytes_change(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack_path = _write_json(tmp_path / "orders.pack.json", _pack().to_dict(command="plan"))
    output_dir = tmp_path / "review"

    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    capsys.readouterr()
    pack_path.write_text('{"changed": true}', encoding="utf-8")

    with pytest.raises(SystemExit) as verify_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "verify",
                "--bundle",
                str(output_dir / "bundle.json"),
                "--format",
                "json",
            ]
        )

    assert verify_exit.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "migration_bundle.artifact_digest_mismatch:migration_pack" in payload["blockers"]


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _pack(
    *, ddl: tuple[str, ...] = ("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",)
) -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=ddl,
        strategy="online_safe",
    )


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return path
