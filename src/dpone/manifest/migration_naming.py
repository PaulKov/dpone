from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def _infer_dataset_vars_and_naming(dataset: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Infers vars + naming.sink_dataset when dataset matches `<layer>__<src>__<db>`."""
    parts = [p for p in dataset.split("__") if p != ""]
    if len(parts) == 3:
        layer, src, db = parts
        vars_block = {"layer": layer, "src_system": src, "src_database": db}
        naming_block = {"sink_dataset": "{{ layer }}__{{ src_system }}__{{ src_database }}"}
        return vars_block, naming_block
    return {}, {}


def _ratio(flags: Sequence[bool]) -> float:
    if not flags:
        return 0.0
    return sum(1 for f in flags if f) / float(len(flags))


def _to_identifier(value: str) -> str:
    # Similar to dpone_ident filter; keep it local to avoid importing jinja env here.
    import re

    s = value.strip().lower()
    s = s.replace(" ", "_").replace("-", "_")
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "batch"
