from __future__ import annotations

import json
from pathlib import Path

from dpone.supply_chain.attestation import SupplyChainAttestationService


def test_supply_chain_attestation_builds_sbom_provenance_and_signature(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "pyproject.toml").write_text(
        """
[project]
name = "demo"
version = "1.2.3"
dependencies = ["pyyaml>=6", "requests>=2"]

[project.optional-dependencies]
s3 = ["boto3>=1.34,<2"]
""".strip(),
        encoding="utf-8",
    )
    dist = project / "dist"
    dist.mkdir()
    wheel = dist / "demo-1.2.3-py3-none-any.whl"
    wheel.write_bytes(b"fake wheel bytes")

    report = SupplyChainAttestationService().build(
        project_root=project,
        output_dir=tmp_path / "out",
        release="v1.2.3",
        subjects=[wheel],
        repository="https://github.com/example/demo",
        commit_sha="abc123",
        builder_id="local-test",
        signing_key="local-secret",
        signing_key_id="local-ci-key",
    )

    assert report.passed is True
    assert report.release == "v1.2.3"
    assert Path(report.sbom_spdx_path).is_file()
    assert Path(report.sbom_cyclonedx_path).is_file()
    assert Path(report.provenance_path).is_file()
    assert Path(report.signature_path).is_file()
    assert Path(report.bundle_path).is_file()

    spdx = json.loads(Path(report.sbom_spdx_path).read_text(encoding="utf-8"))
    cyclonedx = json.loads(Path(report.sbom_cyclonedx_path).read_text(encoding="utf-8"))
    provenance = json.loads(Path(report.provenance_path).read_text(encoding="utf-8"))
    signature = json.loads(Path(report.signature_path).read_text(encoding="utf-8"))

    assert spdx["SPDXID"] == "SPDXRef-DOCUMENT"
    assert {package["name"] for package in spdx["packages"]} >= {"pyyaml", "requests", "boto3"}
    assert cyclonedx["bomFormat"] == "CycloneDX"
    assert {component["name"] for component in cyclonedx["components"]} >= {"pyyaml", "requests", "boto3"}
    assert provenance["subject"][0]["name"].endswith("demo-1.2.3-py3-none-any.whl")
    assert len(provenance["subject"][0]["digest"]["sha256"]) == 64
    assert signature["key_id"] == "local-ci-key"
    assert len(signature["signature"]) == 64
    assert "local-secret" not in json.dumps(signature)
