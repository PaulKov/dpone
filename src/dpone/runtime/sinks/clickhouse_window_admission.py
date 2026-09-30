"""Compatibility imports for target-owned window admission.

New callers use clickhouse_window_target; this module contains no policy.
"""

from dpone.runtime.sinks.clickhouse_window_target import (
    validate_configuration as validate_configuration,
)
from dpone.runtime.sinks.clickhouse_window_target import (
    validate_target as validate_target,
)
from dpone.runtime.sinks.clickhouse_window_target import (
    window_schema_fingerprint as window_schema_fingerprint,
)
