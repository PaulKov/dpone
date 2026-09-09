"""Application service for nested normalization CLI workflows."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from dpone.runtime.normalization import NestedNormalizationOptions, NormalizationPreviewService


class NormalizationCliService:
    """Prepare normalization preview/infer payloads for CLI adapters."""

    def __init__(self, preview_service: NormalizationPreviewService | None = None) -> None:
        self._preview_service = preview_service or NormalizationPreviewService()

    def preview(
        self,
        *,
        sample: str,
        root_table: str,
        config_path: str | None = None,
        nested_level: int | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        config = self.load_config(config_path)
        if nested_level is not None:
            config["nested_level"] = nested_level
        config.setdefault("enabled", True)
        payload = self._preview_service.preview_rows(
            _read_rows(Path(sample), limit=max(1, int(limit))),
            root_table=root_table,
            options=NestedNormalizationOptions.from_config(config),
        )
        payload["mode"] = "preview"
        payload["sample"] = str(sample)
        return payload

    def infer(self, **kwargs: Any) -> dict[str, Any]:
        payload = self.preview(**kwargs)
        payload["mode"] = "infer"
        return payload

    def load_config(self, path: str | None) -> dict[str, Any]:
        if not path:
            return {"enabled": True}
        raw = Path(path).read_text(encoding="utf-8")
        if path.endswith(".json"):
            loaded = json.loads(raw)
        else:
            try:
                import yaml
            except Exception as exc:  # pragma: no cover - dependency is present in normal dev envs
                raise RuntimeError("YAML normalization config requires PyYAML") from exc
            loaded = yaml.safe_load(raw)
        if loaded is None:
            return {"enabled": True}
        if not isinstance(loaded, dict):
            raise ValueError("normalization config must be a mapping")
        if "normalization" in loaded and isinstance(loaded["normalization"], dict):
            return dict(loaded["normalization"])
        return dict(loaded)


def _read_rows(path: Path, *, limit: int) -> list[dict[str, object]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        json_rows = payload if isinstance(payload, list) else [payload]
        return [dict(row) for row in json_rows[:limit] if isinstance(row, dict)]
    if suffix in {".jsonl", ".ndjson"}:
        jsonl_rows: list[dict[str, object]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            item = json.loads(line)
            if isinstance(item, dict):
                jsonl_rows.append(dict(item))
            if len(jsonl_rows) >= limit:
                break
        return jsonl_rows
    delimiter = "\t" if suffix == ".tsv" else ","
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        return [dict(row) for _, row in zip(range(limit), reader)]
