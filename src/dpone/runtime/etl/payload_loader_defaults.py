"""Default runtime service factories for ``PayloadLoadService``."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def default_schema_evolution_service() -> Any:
    module = import_module("dpone.runtime.schema_evolution")
    return module.SchemaEvolutionService()


def default_schema_identity_service() -> Any:
    module = import_module("dpone.runtime.schema_identity")
    return module.SchemaIdentityProjectionService()


def default_runtime_lifecycle_service() -> Any:
    module = import_module("dpone.runtime.etl.lifecycle")
    return module.RuntimeLifecycleService()


def default_native_transfer_runtime_service() -> Any:
    module = import_module("dpone.runtime.native_transfer")
    return module.NativeTransferRuntimeService()
