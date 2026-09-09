"""Layout-specific paths for one self-service scaffold unit."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol


class ScaffoldLayout(Protocol):
    @property
    def root(self) -> str: ...

    @property
    def is_domain_first(self) -> bool: ...


def pipeline_path(
    layout: ScaffoldLayout,
    domain: str,
    pipeline_id: str,
) -> Path:
    if layout.is_domain_first:
        return Path(layout.root) / domain / "pipelines" / pipeline_id / "pipeline.yaml"
    return Path("pipelines") / pipeline_id / "pipeline.yaml"


def test_paths(
    layout: ScaffoldLayout,
    source_path: Path,
    pipeline_id: str,
) -> tuple[Path, Path, str | None, str | None]:
    """Return test locations and references relative to the test contract."""

    if not layout.is_domain_first:
        return (
            Path("tests") / f"{pipeline_id}.test.yaml",
            Path("tests/fixtures") / f"{pipeline_id}.input.jsonl",
            None,
            None,
        )
    test_root = source_path.parent / "tests"
    return (
        test_root / "pipeline.test.yaml",
        test_root / "fixtures/input.jsonl",
        "../pipeline.yaml",
        "fixtures/input.jsonl",
    )


__all__ = ["ScaffoldLayout", "pipeline_path", "test_paths"]
