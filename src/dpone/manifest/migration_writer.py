from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml


def _write_batch_yaml(path: Path, manifest: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Add yaml-language-server schema hint for IDE (best-effort, matches existing repo layout)
    header = "# yaml-language-server: $schema=../schema/etl-batch-manifest.schema.json\n"
    dumped = yaml.safe_dump(
        dict(manifest),
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    )
    path.write_text(header + dumped, encoding="utf-8")
