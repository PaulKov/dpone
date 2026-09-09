"""SBOM builders for SPDX-like and CycloneDX-like JSON documents."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from dpone.supply_chain.pyproject_reader import ProjectMetadata, PyprojectMetadataReader


@dataclass(frozen=True, slots=True)
class SBOMArtifacts:
    spdx_path: str
    cyclonedx_path: str
    package_count: int


class SBOMService:
    """Build dependency-light SBOM documents from project metadata."""

    def __init__(self, *, reader: PyprojectMetadataReader | None = None) -> None:
        self._reader = reader or PyprojectMetadataReader()

    def build(self, *, project_root: str | Path, output_dir: str | Path) -> SBOMArtifacts:
        metadata = self._reader.read(project_root)
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        spdx = self._spdx(metadata)
        cyclonedx = self._cyclonedx(metadata)
        spdx_path = out / "sbom.spdx.json"
        cyclonedx_path = out / "sbom.cyclonedx.json"
        spdx_path.write_text(json.dumps(spdx, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        cyclonedx_path.write_text(
            json.dumps(cyclonedx, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return SBOMArtifacts(
            spdx_path=str(spdx_path),
            cyclonedx_path=str(cyclonedx_path),
            package_count=len(metadata.dependencies),
        )

    def _spdx(self, metadata: ProjectMetadata) -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        packages = [
            {
                "SPDXID": f"SPDXRef-Package-{self._spdx_id(dep.name)}-{index}",
                "name": dep.name,
                "versionInfo": dep.requirement,
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
                "supplier": "NOASSERTION",
                "comment": f"scope={dep.scope}",
            }
            for index, dep in enumerate(metadata.dependencies, 1)
        ]
        return {
            "spdxVersion": "SPDX-2.3",
            "dataLicense": "CC0-1.0",
            "SPDXID": "SPDXRef-DOCUMENT",
            "name": f"{metadata.name}-{metadata.version}",
            "documentNamespace": f"https://github.com/PaulKov/dpone/sbom/{metadata.name}/{metadata.version}",
            "creationInfo": {"created": now, "creators": ["Tool: dpone"]},
            "packages": packages,
        }

    def _cyclonedx(self, metadata: ProjectMetadata) -> dict[str, object]:
        return {
            "bomFormat": "CycloneDX",
            "specVersion": "1.5",
            "version": 1,
            "metadata": {
                "component": {
                    "type": "application",
                    "name": metadata.name,
                    "version": metadata.version,
                }
            },
            "components": [
                {
                    "type": "library",
                    "name": dep.name,
                    "version": dep.requirement,
                    "scope": dep.scope,
                    "purl": f"pkg:pypi/{dep.name}",
                }
                for dep in metadata.dependencies
            ],
        }

    @staticmethod
    def _spdx_id(value: str) -> str:
        return "".join(ch if ch.isalnum() else "-" for ch in value)
