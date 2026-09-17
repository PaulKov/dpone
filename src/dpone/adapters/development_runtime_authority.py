"""Discover the one development authority adapter pinned in a runtime image."""

from __future__ import annotations

import importlib.metadata
from collections.abc import Callable, Iterable
from typing import Any, cast

from dpone.ports.development_runtime_authority import DevelopmentRuntimeAuthority

ENTRY_POINT_GROUP = "dpone.development_runtime_authority"


class DevelopmentRuntimeAuthorityAdapterError(RuntimeError):
    """Redactable adapter discovery or construction failure."""


def load_development_runtime_authority(
    *,
    discover: Callable[[], Iterable[importlib.metadata.EntryPoint]] | None = None,
) -> DevelopmentRuntimeAuthority:
    """Load exactly one image-installed zero-argument adapter factory."""

    try:
        candidates = tuple(
            discover() if discover is not None else importlib.metadata.entry_points(group=ENTRY_POINT_GROUP)
        )
        if len(candidates) != 1:
            raise DevelopmentRuntimeAuthorityAdapterError("adapter cardinality is invalid")
        factory: Any = candidates[0].load()
        if not callable(factory):
            raise DevelopmentRuntimeAuthorityAdapterError("adapter entry point is not a factory")
        adapter = factory()
        if not callable(getattr(adapter, "authorize", None)):
            raise DevelopmentRuntimeAuthorityAdapterError("adapter does not implement authorize")
        return cast(DevelopmentRuntimeAuthority, adapter)
    except DevelopmentRuntimeAuthorityAdapterError:
        raise
    except Exception as exc:
        raise DevelopmentRuntimeAuthorityAdapterError("adapter could not be loaded") from exc


__all__ = [
    "DevelopmentRuntimeAuthorityAdapterError",
    "ENTRY_POINT_GROUP",
    "load_development_runtime_authority",
]
