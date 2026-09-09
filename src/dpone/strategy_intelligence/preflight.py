from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any

from dpone.strategy_intelligence.native_paths import NativeFastPathCatalog

ToolResolver = Callable[[str], str | None]


@dataclass(frozen=True, slots=True)
class ToolCheck:
    name: str
    available: bool
    path: str | None
    install_hint: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NativeFastPathPreflightResult:
    path_id: str
    ready: bool
    tools: dict[str, ToolCheck]
    fallback_path: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "path_id": self.path_id,
            "ready": self.ready,
            "tools": {name: check.to_dict() for name, check in self.tools.items()},
            "fallback_path": self.fallback_path,
        }


class NativeFastPathPreflightService:
    """Check whether local native tools required by a fast path are available."""

    def __init__(
        self,
        *,
        native_paths: NativeFastPathCatalog | None = None,
        tool_resolver: ToolResolver = shutil.which,
    ) -> None:
        self._native_paths = native_paths or NativeFastPathCatalog()
        self._tool_resolver = tool_resolver

    def check_path(self, source_type: str, sink_type: str) -> NativeFastPathPreflightResult:
        path = self._native_paths.resolve(source_type, sink_type)
        tools = {tool: self._check_tool(tool) for tool in path.required_tools}
        return NativeFastPathPreflightResult(
            path_id=path.path_id,
            ready=all(check.available for check in tools.values()),
            tools=tools,
            fallback_path=path.fallback_path,
        )

    def _check_tool(self, name: str) -> ToolCheck:
        executable = _executable_name(name)
        resolved = self._tool_resolver(executable)
        return ToolCheck(
            name=name,
            available=resolved is not None,
            path=resolved,
            install_hint=_install_hint(name),
        )


def _executable_name(name: str) -> str:
    aliases = {
        "psycopg": "python",
        "clickhouse-client or curl": "clickhouse-client",
    }
    return aliases.get(name, name)


def _install_hint(name: str) -> str:
    hints = {
        "bcp": "Install Microsoft mssql-tools18 and ensure bcp is on PATH.",
        "psycopg": "Install dpone[postgres] or psycopg in the current environment.",
        "clickhouse-client": "Install ClickHouse client or use HTTP streaming fallback.",
        "clickhouse-client or curl": "Install clickhouse-client or curl for HTTP streaming.",
    }
    return hints.get(name, f"Install {name} and ensure it is on PATH.")
