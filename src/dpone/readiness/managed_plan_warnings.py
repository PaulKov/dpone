"""Warning policy for dry-run execution plans."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def plan_warnings(plan: Mapping[str, Any]) -> list[str]:
    """Return deterministic operator warnings for one resolved plan."""

    warnings: list[str] = []
    if plan["sink"]["type"] == "mssql" and "bcp" not in str(plan["bulk_path"]):
        warnings.append("MSSQL sink should use native bcp for large loads")
    if plan["estimated_rows"] is None:
        warnings.append("estimated_rows is unknown; performance advisor will use conservative defaults")
    warnings.extend(str(item) for item in (plan.get("runtime_storage") or {}).get("warnings", ()))
    warnings.extend(f"columnar_fast_path:{item}" for item in (plan.get("columnar_fast_path") or {}).get("blockers", ()))
    warnings.extend(str(item.get("code")) for item in plan.get("source_impact", ()) if item.get("severity") == "high")
    return warnings


__all__ = ["plan_warnings"]
