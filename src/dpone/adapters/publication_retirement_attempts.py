"""Owner-private, non-reclaimable local attempts for the operator workflow.

Every invocation for one operation must use this same admitted persistent directory.
It is not a distributed lock and is never a substitute for SQL slot CAS or the
deployment freeze. A missing/unavailable directory fails before SQL. Preserve
attempt markers after unknown outcomes; there is deliberately no reset API.
"""

from __future__ import annotations

import re
from pathlib import Path

from dpone.adapters.publication_plan_file import write_private_plan


class PrivateRetirementAttempts:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    def claim(self, operation_key: str) -> bool:
        """Durably reserve exactly one attempt; collisions only allow readback."""
        if not isinstance(operation_key, str) or re.fullmatch(r"[0-9a-f]{64}", operation_key) is None:
            raise ValueError("retirement attempt requires a canonical operation key")
        payload = f"dpone.publication-retirement-attempt.v1\n{operation_key}\n".encode()
        try:
            write_private_plan(self._directory / f"{operation_key}.attempt", payload)
        except FileExistsError:
            return False
        return True
