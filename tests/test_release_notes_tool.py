from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    path = ROOT / "tools" / "release_notes.py"
    spec = importlib.util.spec_from_file_location("release_notes", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_extract_release_notes_accepts_tagged_version() -> None:
    module = _load_module()
    changelog = """# Changelog

## 0.7.2 - 2026-06-11

### Fixed

- Current release.

## 0.7.1 - 2026-06-09

### Changed

- Previous release.
"""

    notes = module.extract_release_notes(changelog, "v0.7.2")

    assert "## 0.7.2 - 2026-06-11" in notes
    assert "Current release" in notes
    assert "0.7.1" not in notes


def test_extract_release_notes_fails_for_missing_version() -> None:
    module = _load_module()

    try:
        module.extract_release_notes("# Changelog\n", "v9.9.9")
    except ValueError as exc:
        assert "9.9.9" in str(exc)
    else:
        raise AssertionError("missing release section must fail")
