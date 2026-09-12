"""Canonical dependency-injection root for lazy ETL runtime execution."""

from __future__ import annotations

import logging
from typing import Any

from dpone.app.context import AppContext
from dpone.ports.process_runner import register_process_runner
from dpone.ports.runtime_hydrator import register_runtime_hydrator
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.bootstrap_runner import DefaultProcessRunner


def build_postgres_mssql_runtime_route_resolver() -> Any | None:
    """Read platform selection only when real execution requests hydration."""

    context = AppContext.from_env(logger=logging.getLogger("dpone.runtime"))
    return context.build_postgres_mssql_correctness_route_resolver()


def build_default_runtime_hydrator() -> DefaultRuntimeHydrator:
    """Compose lazy platform selection without import-time catalog I/O."""

    return DefaultRuntimeHydrator(
        postgres_mssql_correctness_route_resolver_provider=(build_postgres_mssql_runtime_route_resolver),
    )


register_runtime_hydrator(build_default_runtime_hydrator())
register_process_runner(DefaultProcessRunner())

__all__ = ["DefaultProcessRunner", "DefaultRuntimeHydrator", "build_default_runtime_hydrator"]
