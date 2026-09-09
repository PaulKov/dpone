from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ConnectorSdkScaffoldResult:
    """Files generated for a connector SDK package."""

    connector: str
    connector_type: str
    capabilities: tuple[str, ...]
    package_name: str
    import_package: str
    sdk_root: Path
    files: tuple[Path, ...]
    certification_manifest: Path | None
    native_capabilities: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "connector": self.connector,
            "connector_type": self.connector_type,
            "capabilities": list(self.capabilities),
            "native_capabilities": list(self.native_capabilities),
            "package_name": self.package_name,
            "import_package": self.import_package,
            "sdk_root": str(self.sdk_root),
            "files": [str(path) for path in self.files],
            "certification_manifest": str(self.certification_manifest) if self.certification_manifest else None,
        }
