"""Adapter from runtime cache operations to the lightweight Airflow contract."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from dpone_airflow_pack.cache_layout import (
    EXACT_DEPLOYMENT_LAYOUT,
    CacheLayoutError,
    assert_cache_layout_compatible,
    ensure_cache_layout,
)
from dpone_airflow_pack.cache_permissions import SHARED_CONTROL_MODE, ensure_control_file_mode


@dataclass(frozen=True, slots=True)
class AirflowCacheLayoutFailure(RuntimeError):
    """Dependency-neutral projection of a lightweight cache-layout failure."""

    code: str
    message: str
    path: str


def assert_exact_cache_layout(cache_root: Path) -> None:
    """Reject a non-exact cache root before runtime mutation."""

    _invoke_layout(assert_cache_layout_compatible, cache_root)


def ensure_exact_cache_layout(cache_root: Path) -> None:
    """Create or validate the exact-deployment cache marker."""

    _invoke_layout(ensure_cache_layout, cache_root)


def normalize_shared_control_file(descriptor: int) -> None:
    """Apply the shared cache control mode only when the inode needs it."""

    ensure_control_file_mode(descriptor, SHARED_CONTROL_MODE)


def shared_control_mode() -> int:
    return SHARED_CONTROL_MODE


def _invoke_layout(operation: Callable[..., object], cache_root: Path) -> None:
    try:
        operation(cache_root, expected_layout=EXACT_DEPLOYMENT_LAYOUT)
    except CacheLayoutError as exc:
        raise AirflowCacheLayoutFailure(exc.code, exc.message, exc.path) from exc


__all__ = [
    "AirflowCacheLayoutFailure",
    "assert_exact_cache_layout",
    "ensure_exact_cache_layout",
    "normalize_shared_control_file",
    "shared_control_mode",
]
