from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.contracts.route_attestation import SignatureVerificationResult, sha256_bytes

_RELEASE_ID = "sha256:" + "a" * 64
_DEPLOYMENT_ID = "sha256:" + "b" * 64
_IMAGE_DIGEST = "sha256:" + "c" * 64


class _Logger:
    def info(self, message: str, *args: object) -> None:
        del message, args

    def error(self, message: str, *args: object) -> None:
        del message, args


class _Verifier:
    def __init__(self, result: SignatureVerificationResult | None = None) -> None:
        self.result = result or SignatureVerificationResult.verified("3.0.4")

    def verify_blob(self, **kwargs: object) -> SignatureVerificationResult:
        del kwargs
        return self.result


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _Logger())
    monkeypatch.setattr(
        cli_main.AppContext,
        "from_env",
        staticmethod(lambda logger: SimpleNamespace(logger=logger)),
    )


def _write(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _certification(path: Path) -> Path:
    return _write(
        path,
        {
            "schema_version": "dpone.route_certification_bundle.v1",
            "release": "0.72.6",
            "profile": "vendor_live",
            "route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"},
            "passed": True,
            "evidence_status": "PASS",
            "level": "certified",
            "score": 100.0,
            "blockers": [],
            "artifact_index": {},
        },
    )


def _deployment(path: Path) -> Path:
    return _write(
        path,
        {
            "schema": "dpone.deployment-set.v1",
            "deployment_id": _DEPLOYMENT_ID,
            "deployment_type": "environment",
            "runnable": True,
            "environment": "production",
            "release_ref": _RELEASE_ID,
            "runtime_image_digest": _IMAGE_DIGEST,
            "runtime_artifact_delivery": {"mode": "init_fetch"},
        },
    )


def _policy(path: Path, root: Path, *, identity: str | None = None) -> Path:
    return _write(
        path,
        {
            "schema": "dpone.route-attestation-policy.v1",
            "backend": "cosign_keyless_v1",
            "certificate_identity": identity
            or "https://github.com/PaulKov/dpone/.github/workflows/route-attestation.yml@refs/heads/master",
            "certificate_oidc_issuer": "https://token.actions.githubusercontent.com",
            "trusted_root": {"path": root.name, "sha256": sha256_bytes(root.read_bytes())},
            "cosign": {"minimum_version": "3.0.4", "maximum_version_exclusive": "4.0.0", "timeout_seconds": 30},
            "allowed_environments": ["production"],
            "allowed_authorization_profiles": ["safe_sample_production"],
            "allowed_certification_profiles": ["vendor_live"],
            "minimum_certification_level": "certified",
            "max_validity_seconds": 86400,
            "clock_skew_seconds": 3600,
            "revoked_attestation_ids": [],
            "revoked_certificate_identities": [],
        },
    )


def _build(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> tuple[Path, Path, Path]:
    _patch_cli(monkeypatch)
    certification = _certification(tmp_path / "route-certification.json")
    deployment = _deployment(tmp_path / "deployment.json")
    attestation = tmp_path / "route-attestation.json"
    now = datetime.now(UTC).replace(microsecond=0)
    not_before = _utc_text(now - timedelta(minutes=5))
    expires_at = _utc_text(now + timedelta(hours=1))
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-attestation-build",
                "--route-id",
                "mssql_clickhouse_incremental_merge_airflow_kpo",
                "--route-certification-bundle",
                str(certification),
                "--deployment-set",
                str(deployment),
                "--issued-at",
                not_before,
                "--not-before",
                not_before,
                "--expires-at",
                expires_at,
                "--output",
                str(attestation),
            ]
        )
    assert exc.value.code == 0
    assert json.loads(capsys.readouterr().out)["attestation_id"].startswith("sha256:")
    return attestation, certification, deployment


def _utc_text(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def test_route_attestation_build_and_verify_cli_are_create_only_and_security_gated(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    attestation, certification, deployment = _build(monkeypatch, capsys, tmp_path)
    root = tmp_path / "trusted-root.json"
    root.write_bytes(b"trusted-root")
    policy = _policy(tmp_path / "policy.json", root)
    sigstore = tmp_path / "route-attestation.sigstore.json"
    sigstore.write_text("{}\n", encoding="utf-8")
    from dpone.readiness import route_attestation as readiness

    monkeypatch.setattr(readiness, "_default_signature_verifier", lambda: _Verifier())
    output = tmp_path / "verification"
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-attestation-verify",
                "--attestation",
                str(attestation),
                "--sigstore-bundle",
                str(sigstore),
                "--route-certification-bundle",
                str(certification),
                "--policy",
                str(policy),
                "--deployment-set",
                str(deployment),
                "--output-dir",
                str(output),
            ]
        )
    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["decision"] == "verified"
    assert (output / "route-attestation-verification.json").exists()


def test_route_attestation_verify_cli_returns_security_exit_for_invalid_signature(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    attestation, certification, deployment = _build(monkeypatch, capsys, tmp_path)
    root = tmp_path / "trusted-root.json"
    root.write_bytes(b"trusted-root")
    policy = _policy(
        tmp_path / "policy.json",
        root,
        identity="ci://dpone/route-attestation",
    )
    sigstore = tmp_path / "route-attestation.sigstore.json"
    sigstore.write_text("{}\n", encoding="utf-8")
    from dpone.readiness import route_attestation as readiness

    monkeypatch.setattr(
        readiness,
        "_default_signature_verifier",
        lambda: _Verifier(
            SignatureVerificationResult.invalid(
                "DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID",
                "The route-attestation signature or signer identity is invalid.",
            )
        ),
    )
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-attestation-verify",
                "--attestation",
                str(attestation),
                "--sigstore-bundle",
                str(sigstore),
                "--route-certification-bundle",
                str(certification),
                "--policy",
                str(policy),
                "--deployment-set",
                str(deployment),
                "--output-dir",
                str(tmp_path / "invalid"),
            ]
        )
    assert exc.value.code == 4
    assert json.loads(capsys.readouterr().out)["decision"] == "invalid"


def test_route_attestation_verify_cli_classifies_malformed_policy_as_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    attestation, certification, deployment = _build(monkeypatch, capsys, tmp_path)
    root = tmp_path / "trusted-root.json"
    root.write_bytes(b"trusted-root")
    policy = _policy(tmp_path / "policy.json", root)
    payload = json.loads(policy.read_text(encoding="utf-8"))
    payload.pop("allowed_environments")
    _write(policy, payload)
    sigstore = tmp_path / "route-attestation.sigstore.json"
    sigstore.write_text("{}\n", encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-attestation-verify",
                "--attestation",
                str(attestation),
                "--sigstore-bundle",
                str(sigstore),
                "--route-certification-bundle",
                str(certification),
                "--policy",
                str(policy),
                "--deployment-set",
                str(deployment),
                "--output-dir",
                str(tmp_path / "invalid-policy"),
            ]
        )

    assert exc.value.code == 2
    result = json.loads(capsys.readouterr().out)
    assert result["decision"] == "invalid"
    assert result["code"] == "DPONE_ROUTE_ATTESTATION_POLICY_INVALID"
