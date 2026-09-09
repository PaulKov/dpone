"""Supply-chain attestation bundle service."""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any


def _symbol(path: str) -> Any:
    module_name, attr = path.split(":", 1)
    return getattr(import_module(module_name), attr)


@dataclass(frozen=True, slots=True)
class SupplyChainAttestationReport:
    passed: bool
    release: str
    output_dir: str
    sbom_spdx_path: str
    sbom_cyclonedx_path: str
    provenance_path: str
    signature_path: str
    bundle_path: str
    markdown_path: str
    blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "release": self.release,
            "output_dir": self.output_dir,
            "sbom_spdx_path": self.sbom_spdx_path,
            "sbom_cyclonedx_path": self.sbom_cyclonedx_path,
            "provenance_path": self.provenance_path,
            "signature_path": self.signature_path,
            "bundle_path": self.bundle_path,
            "markdown_path": self.markdown_path,
            "blockers": list(self.blockers),
        }

    def to_markdown(self) -> str:
        blockers = "\n".join(f"- `{item}`" for item in self.blockers) if self.blockers else "- none"
        return "\n".join(
            [
                "# dpone supply-chain attestation",
                "",
                f"- Release: `{self.release}`",
                f"- Passed: `{self.passed}`",
                f"- Output dir: `{self.output_dir}`",
                "",
                "| artifact | path |",
                "|---|---|",
                f"| SPDX SBOM | `{self.sbom_spdx_path}` |",
                f"| CycloneDX SBOM | `{self.sbom_cyclonedx_path}` |",
                f"| Provenance | `{self.provenance_path}` |",
                f"| Signature envelope | `{self.signature_path}` |",
                f"| Bundle | `{self.bundle_path}` |",
                "",
                "## Blockers",
                "",
                blockers,
                "",
            ]
        )


class SupplyChainAttestationService:
    """Create SBOM, provenance, signature and bundle evidence."""

    def __init__(
        self,
        *,
        sbom: Any | None = None,
        provenance: Any | None = None,
        signing: Any | None = None,
    ) -> None:
        self._sbom = sbom or _symbol("dpone.supply_chain.sbom:SBOMService")()
        self._provenance = provenance or _symbol("dpone.supply_chain.provenance:ProvenanceService")()
        self._signing = signing or _symbol("dpone.supply_chain.signing:LocalHMACSignatureService")()

    def build(
        self,
        *,
        project_root: str | Path,
        output_dir: str | Path,
        release: str,
        subjects: list[str | Path] | tuple[str | Path, ...],
        repository: str,
        commit_sha: str,
        builder_id: str,
        signing_key: str | None = None,
        signing_key_id: str = "local-hmac",
    ) -> SupplyChainAttestationReport:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        blockers: list[str] = []
        subject_paths = [Path(item) for item in subjects]
        if not subject_paths:
            blockers.append("subjects.empty")
        for path in subject_paths:
            if not path.is_file():
                blockers.append(f"subject.missing.{path}")

        sbom = self._sbom.build(project_root=project_root, output_dir=out)
        provenance = self._provenance.build(
            output_dir=out,
            release=release,
            subjects=subject_paths,
            repository=repository,
            commit_sha=commit_sha,
            builder_id=builder_id,
        )
        signature_path = ""
        if signing_key:
            signature = self._signing.sign_file(
                path=provenance.path,
                output_dir=out,
                key=signing_key,
                key_id=signing_key_id,
            )
            signature_path = signature.path
        else:
            blockers.append("signature.missing_key")

        bundle_path = out / "supply_chain_attestation.json"
        markdown_path = out / "supply_chain_attestation.md"
        report = SupplyChainAttestationReport(
            passed=not blockers,
            release=release,
            output_dir=str(out),
            sbom_spdx_path=sbom.spdx_path,
            sbom_cyclonedx_path=sbom.cyclonedx_path,
            provenance_path=provenance.path,
            signature_path=signature_path,
            bundle_path=str(bundle_path),
            markdown_path=str(markdown_path),
            blockers=tuple(blockers),
        )
        bundle_path.write_text(
            json.dumps(self._bundle_payload(report), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report

    @staticmethod
    def _bundle_payload(report: SupplyChainAttestationReport) -> dict[str, object]:
        artifacts = [
            ("sbom_spdx", report.sbom_spdx_path),
            ("sbom_cyclonedx", report.sbom_cyclonedx_path),
            ("provenance", report.provenance_path),
            ("signature", report.signature_path),
        ]
        return {
            **report.to_dict(),
            "artifacts": [
                {"name": name, "path": path, "sha256": _symbol("dpone.supply_chain.checksums:sha256_file")(path)}
                for name, path in artifacts
                if path
            ],
        }
