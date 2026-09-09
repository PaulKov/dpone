"""Public facade for ClickHouse CDC typed materialization."""

from __future__ import annotations

from typing import Any

from dpone.lazy_exports import exported_dir, resolve_export

_EXPORTS: dict[str, str] = {
    "ClickHouseCdcPayloadProjector": "dpone.runtime.cdc.typed_materialization_projection:ClickHouseCdcPayloadProjector",
    "ClickHouseCdcTypedColumn": "dpone.runtime.cdc.typed_materialization_models:ClickHouseCdcTypedColumn",
    "ClickHouseCdcTypedMaterializationPlan": (
        "dpone.runtime.cdc.typed_materialization_models:ClickHouseCdcTypedMaterializationPlan"
    ),
    "ClickHouseCdcTypedMaterializationPolicy": (
        "dpone.runtime.cdc.typed_materialization_models:ClickHouseCdcTypedMaterializationPolicy"
    ),
    "ClickHouseCdcTypedMaterializationReport": (
        "dpone.runtime.cdc.typed_materialization_models:ClickHouseCdcTypedMaterializationReport"
    ),
    "ClickHouseCdcTypedMaterializationService": (
        "dpone.runtime.cdc.typed_materialization_service:ClickHouseCdcTypedMaterializationService"
    ),
    "ClickHouseCdcTypedParseFailure": "dpone.runtime.cdc.typed_materialization_quality:ClickHouseCdcTypedParseFailure",
    "ClickHouseCdcTypedParseQuarantine": (
        "dpone.runtime.cdc.typed_materialization_quality:ClickHouseCdcTypedParseQuarantine"
    ),
    "ClickHouseCdcTypedQualityEvidence": "dpone.runtime.cdc.typed_materialization_quality:ClickHouseCdcTypedQualityEvidence",
    "ClickHouseCdcTypedQualityPolicy": "dpone.runtime.cdc.typed_materialization_quality:ClickHouseCdcTypedQualityPolicy",
    "ClickHouseCdcTypedSchemaDrift": "dpone.runtime.cdc.typed_materialization_quality:ClickHouseCdcTypedSchemaDrift",
    "DeleteMode": "dpone.runtime.cdc.typed_materialization_common:DeleteMode",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_export(name, exports=_EXPORTS, namespace=globals(), module_name=__name__)


def __dir__() -> list[str]:
    return exported_dir(globals(), _EXPORTS)
