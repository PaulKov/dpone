"""API provider runtime adapter package."""

from __future__ import annotations

from typing import Any

from dpone.lazy_exports import exported_dir, resolve_export

_EXPORTS: dict[str, str] = {
    "APIProviderRuntimeSpec": "dpone.runtime.api_providers.models:APIProviderRuntimeSpec",
    "ConnectorKwargsFactory": "dpone.runtime.api_providers.models:ConnectorKwargsFactory",
    "load_runtime_specs": "dpone.runtime.api_providers.specs:load_runtime_specs",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_export(name, exports=_EXPORTS, namespace=globals(), module_name=__name__)


def __dir__() -> list[str]:
    return exported_dir(globals(), _EXPORTS)
