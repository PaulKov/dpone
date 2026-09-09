from __future__ import annotations

from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "dpone"
WARN_LIMIT = 450


def _loc(path: Path) -> int:
    return len(path.read_text(encoding="utf-8").splitlines())


def test_no_production_module_exceeds_warning_loc_threshold() -> None:
    oversized = sorted(
        (path.relative_to(SRC).as_posix(), _loc(path)) for path in SRC.rglob("*.py") if _loc(path) > WARN_LIMIT
    )
    assert oversized == []
