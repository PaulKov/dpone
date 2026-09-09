"""Canonical manual integration matrix for source -> sink strategy certification.

The public docs promise one guide per source/sink pair and one strategy contract
per supported load mode. This compatibility facade keeps the old
``dpone.integration_matrix`` import stable while implementation details live in
focused helper modules.
"""

from __future__ import annotations

from dpone.integration_matrix_constants import (
    BASE_LOAD_STRATEGIES,
    COMMON_PRODUCTION_STRATEGIES,
    DB_TARGET_PRODUCTION_STRATEGIES,
    DEFAULT_MOCK_ROW_COUNT,
    INCREMENTAL_CHANGE_RATIO,
    LOAD_STRATEGIES,
    MAX_MOCK_ROW_COUNT,
    PARTITION_REPLACE_SINKS,
    PHYSICAL_DELETE_RATIO,
    SINK_FAMILIES,
    SOURCE_FAMILIES,
    SOURCE_SPECIFIC_STRATEGIES,
    MatrixRow,
)
from dpone.integration_matrix_models import (
    IntegrationMatrix,
    IntegrationMatrixCase,
    MatrixStrategyBehaviorResult,
    matrix_case_selected,
)

DEFAULT_INTEGRATION_MATRIX = IntegrationMatrix.build_default()

__all__ = [
    "BASE_LOAD_STRATEGIES",
    "COMMON_PRODUCTION_STRATEGIES",
    "DB_TARGET_PRODUCTION_STRATEGIES",
    "DEFAULT_INTEGRATION_MATRIX",
    "DEFAULT_MOCK_ROW_COUNT",
    "INCREMENTAL_CHANGE_RATIO",
    "IntegrationMatrix",
    "IntegrationMatrixCase",
    "LOAD_STRATEGIES",
    "MAX_MOCK_ROW_COUNT",
    "MatrixRow",
    "MatrixStrategyBehaviorResult",
    "PARTITION_REPLACE_SINKS",
    "PHYSICAL_DELETE_RATIO",
    "SINK_FAMILIES",
    "SOURCE_FAMILIES",
    "SOURCE_SPECIFIC_STRATEGIES",
    "matrix_case_selected",
]
