"""Source-native artifact wrappers used by typed native transfer."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.native_wire_models import SourceNativeWireContract


class SourceNativeArtifact(FileExportArtifact):
    """File artifact that carries an explicit source-native decode contract."""

    def __init__(
        self,
        file_path: str | Path,
        *,
        columns: list[str],
        native_wire_contract: SourceNativeWireContract,
        estimated_rows: int | None = None,
        bulk_wire_contract: Any | None = None,
    ) -> None:
        super().__init__(
            str(file_path),
            columns,
            compressed=False,
            format=native_wire_contract.source_format,
            estimated_rows=estimated_rows,
        )
        self.native_wire_contract = native_wire_contract
        self.bulk_wire_contract = bulk_wire_contract


__all__ = ["SourceNativeArtifact"]
