"""Bounded parent snapshots shared by both Python import proof attempts."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Literal

from dpone.readiness.python_import_bounded import bounded_list
from dpone.readiness.python_import_probe_child import MAX_PATH_ENTRIES

_MAX_TEXT_CHARACTERS = 64 * 1024


class ProbePayloadError(ValueError):
    """A redacted path or policy serialization failure."""

    def __init__(self, kind: Literal["path", "policy"]) -> None:
        super().__init__(kind)
        self.kind = kind


@dataclass(frozen=True, slots=True)
class ProbePathSnapshot:
    """One bounded target working directory and import-path snapshot."""

    cwd: str
    entries: tuple[str, ...]


def capture_probe_path() -> ProbePathSnapshot:
    """Capture one immutable bounded path snapshot shared by both probes."""

    try:
        cwd = os.getcwd()
    except OSError as exc:
        raise ProbePayloadError("path") from exc
    entries = bounded_list(sys.path, MAX_PATH_ENTRIES)
    if entries is None or not _safe_text(cwd):
        raise ProbePayloadError("path")
    typed_entries: list[str] = []
    for entry in entries:
        if type(entry) is not str or not _safe_text(entry):
            raise ProbePayloadError("path")
        typed_entries.append(entry)
    return ProbePathSnapshot(cwd=cwd, entries=tuple(typed_entries))


def capture_warning_options(maximum: int) -> tuple[object, ...] | Literal[False] | None:
    """Copy no more than the allowed number of effective warning options."""

    options = bounded_list(sys.warnoptions, maximum)
    return False if options is None else options


def _safe_text(value: str) -> bool:
    return type(value) is str and len(value) <= _MAX_TEXT_CHARACTERS and "\x00" not in value
