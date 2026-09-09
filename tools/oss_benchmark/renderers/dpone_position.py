"""dpone-specific position copy for the OSS benchmark."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_int


def render_contract_position(taxonomy: dict[str, Any]) -> str:
    """Render evidence-driven dpone architecture-position copy."""
    contract_score_value = (taxonomy.get("contract_conformance") or {}).get("score")
    contract_score = format_int(contract_score_value)
    taxonomy_band = taxonomy.get("band", "n/a")
    if isinstance(contract_score_value, int | float) and contract_score_value >= 100:
        return (
            f"The new architecture taxonomy shows dpone at `{taxonomy_band}` on taxonomy and `{contract_score}` "
            "on contract conformance after documented compatibility facades are separated from active-risk scoring. "
            "The source/sink runtime now depends on connector ports and shared support contracts instead of concrete "
            "connector implementations, so the next target is governance: keep that DIP boundary green as connector "
            "coverage expands."
        )
    return (
        f"The new architecture taxonomy makes the next quality target explicit: dpone is currently `{taxonomy_band}` "
        f"on taxonomy and `{contract_score}` on contract conformance after documented compatibility facades are "
        "separated from active-risk scoring. The remaining target is narrower: source/sink runtime code should "
        "depend on connector ports/protocols instead of concrete connector implementations. This is a focused "
        "refactor target, not a product-readiness blocker: the benchmark now names the exact seams to clean before "
        "connector breadth scales further."
    )


__all__ = ["render_contract_position"]
