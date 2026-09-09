"""Execution selection carried by the immutable Airflow init-fetch plan."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_EXECUTION_TOKEN_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,256}$")
_HOOK_EXECUTION_MODES = frozenset({"inline", "externalized"})
_EXECUTION_SCOPES = frozenset({"workload", "process"})


@dataclass(frozen=True, slots=True)
class RuntimeExecutionSelection:
    """Select one verified workload command and its hook ownership boundary."""

    kind: str
    selector: str
    scope: str | None = None
    process_selector: str | None = None
    hook_name: str | None = None
    hook_execution: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in {"runtime", "pre_hook"}:
            raise ValueError("execution.kind must be runtime or pre_hook")
        require_execution_token("execution.selector", self.selector)
        if self.process_selector is not None:
            require_execution_token("execution.process_selector", self.process_selector)
        if self.kind == "pre_hook":
            require_execution_token("execution.hook_name", self.hook_name)
        elif self.hook_name is not None:
            raise ValueError("execution.hook_name is only valid for pre_hook")
        if self.hook_execution is not None and self.hook_execution not in _HOOK_EXECUTION_MODES:
            raise ValueError("execution.hook_execution must be inline or externalized")
        if self.hook_execution is not None and self.scope not in _EXECUTION_SCOPES:
            raise ValueError("execution.scope must be workload or process")
        if self.hook_execution is None and self.scope is not None:
            raise ValueError("execution.scope requires explicit hook execution")
        if self.kind == "pre_hook" and self.hook_execution not in {None, "externalized"}:
            raise ValueError("pre_hook execution must be externalized")
        if self.scope == "workload":
            if self.process_selector is not None:
                raise ValueError("workload execution cannot select one process")
            if self.hook_execution != "externalized":
                raise ValueError("workload execution must externalize hooks")
        if self.process_selector is not None and self.scope != "process":
            raise ValueError("execution.process_selector requires process scope")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "selector": self.selector,
            "hook_name": self.hook_name,
        }
        if self.hook_execution is not None:
            payload["scope"] = self.scope
            payload["process_selector"] = self.process_selector
            payload["hook_execution"] = self.hook_execution
        return payload


def require_execution_token(field: str, value: object) -> None:
    """Require one bounded logical selector safe for the JSON wire."""

    if not isinstance(value, str) or not _EXECUTION_TOKEN_RE.fullmatch(value):
        raise ValueError(f"{field} must be a bounded logical token")


__all__ = ["RuntimeExecutionSelection", "require_execution_token"]
