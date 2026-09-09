from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class ArtifactLease:
    """Owns one temporary transfer file and its cleanup outcome."""

    path: Path
    debug_dir: Path | None = None
    failed_files: str = "delete"

    def close(self, *, success: bool) -> None:
        path = Path(self.path)
        if not path.exists():
            return
        if not success and self.failed_files == "keep" and self.debug_dir is not None:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, self.debug_dir / path.name)
        path.unlink(missing_ok=True)


__all__ = ["ArtifactLease"]
