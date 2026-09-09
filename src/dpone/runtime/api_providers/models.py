"""Runtime API provider spec models."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.api_sources import APISourceDefaults


from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

ConnectorKwargsFactory = Callable[[Mapping[str, Any]], dict[str, Any]]
ResolvedConnectorMode = Literal[
    "standard",
    "concurrent",
    "appsflyer",
    "direct",
]


@dataclass(frozen=True)
class APIProviderRuntimeSpec:
    """Runtime wiring for a logical ``api_type``."""

    defaults: APISourceDefaults
    connector_target: str | None = None
    credentials_target: str | None = None
    source_target: str | None = None
    connector_kwargs_factory: ConnectorKwargsFactory | None = None
    resolved_connector_mode: ResolvedConnectorMode = "standard"

    @property
    def api_type(self) -> str:
        return self.defaults.api_type

    @property
    def is_implemented(self) -> bool:
        return bool(self.connector_target and self.source_target)


__all__ = [
    "APIProviderRuntimeSpec",
    "ConnectorKwargsFactory",
    "ResolvedConnectorMode",
]
