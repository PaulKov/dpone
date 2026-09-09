"""Canonical shared contracts and value objects for dpone."""

from .api_sources import APICredentialsMode, APISourceDefaults, get_api_source_defaults, list_api_source_types
from .configuration_errors import ETLConfigurationError, RuntimeConfigurationError
from .credential_security import FORBIDDEN_SECRET_KEYS
from .incremental_snapshot import (
    DELTA_HASH_COLUMN,
    DeltaSnapshotReceipt,
    IncrementalSnapshotEnvelope,
    KeySnapshotReceipt,
    KeySnapshotReconciliationPolicy,
)
from .live_certification import LIVE_CERTIFICATION_PROFILE_CHOICES, LOCAL_LIVE_CERTIFICATION_PROFILES
from .process_errors import ETLProcessError
from .process_types import DependencyConfig, ProcessResult, TransformConfig
from .repair_authority import (
    ExpectedCheckpoint,
    RepairAllowance,
    RepairAuthority,
    RepairAuthorityError,
    RepairAuthorityUse,
    TargetAuthorityTransfer,
    repair_authority_digest,
)
from .run_context import RunContext
from .soft_delete import SoftDeleteMode, SoftDeletePolicy
from .technical_columns import (
    TechnicalColumnsMode,
    TechnicalColumnsResolution,
    include_technical_columns,
    parse_mode,
    resolve_technical_columns,
)

__all__ = [
    "ETLConfigurationError",
    "ETLProcessError",
    "RuntimeConfigurationError",
    "FORBIDDEN_SECRET_KEYS",
    "LIVE_CERTIFICATION_PROFILE_CHOICES",
    "LOCAL_LIVE_CERTIFICATION_PROFILES",
    "DELTA_HASH_COLUMN",
    "DeltaSnapshotReceipt",
    "IncrementalSnapshotEnvelope",
    "KeySnapshotReceipt",
    "KeySnapshotReconciliationPolicy",
    "TransformConfig",
    "DependencyConfig",
    "ProcessResult",
    "ExpectedCheckpoint",
    "RepairAllowance",
    "RepairAuthority",
    "RepairAuthorityError",
    "RepairAuthorityUse",
    "TargetAuthorityTransfer",
    "repair_authority_digest",
    "RunContext",
    "TechnicalColumnsMode",
    "TechnicalColumnsResolution",
    "SoftDeleteMode",
    "SoftDeletePolicy",
    "include_technical_columns",
    "parse_mode",
    "resolve_technical_columns",
    "APICredentialsMode",
    "APISourceDefaults",
    "get_api_source_defaults",
    "list_api_source_types",
]
