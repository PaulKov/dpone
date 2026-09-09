"""Compatibility facade for API connector infrastructure.

New code should import focused components from ``dpone.runtime.connectors.api``
modules such as ``connector``, ``credentials``, ``rate_limit`` and ``parallel``.
This module keeps the historical ``dpone.runtime.connectors.api.base`` imports
stable for provider connectors.
"""

from __future__ import annotations

from typing import Any

from dpone.lazy_exports import exported_dir, resolve_export

_EXPORTS: dict[str, str] = {
    "AbstractAPIConnector": "dpone.runtime.connectors.api.connector:AbstractAPIConnector",
    "APICredentials": "dpone.runtime.connectors.api.credentials:APICredentials",
    "APIRateLimitConfig": "dpone.runtime.connectors.api.config:APIRateLimitConfig",
    "APIRetryConfig": "dpone.runtime.connectors.api.config:APIRetryConfig",
    "ConcurrencyConfig": "dpone.runtime.connectors.api.config:ConcurrencyConfig",
    "PaginationConfig": "dpone.runtime.connectors.api.config:PaginationConfig",
    "ParallelTaskExecutor": "dpone.runtime.connectors.api.parallel:ParallelTaskExecutor",
    "TaskResult": "dpone.runtime.connectors.api.parallel:TaskResult",
    "ThreadSafeRateLimiter": "dpone.runtime.connectors.api.rate_limit:ThreadSafeRateLimiter",
    "resolve_generic_api_token": "dpone.runtime.connectors.api.credentials:resolve_generic_api_token",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_export(name, exports=_EXPORTS, namespace=globals(), module_name=__name__)


def __dir__() -> list[str]:
    return exported_dir(globals(), _EXPORTS)
