"""Compatibility-window metadata for the legacy dbt publishing facade."""

from __future__ import annotations

import warnings

from packaging.version import InvalidVersion, Version

from dpone.version import installed_version

DBT_PUBLISH_DEPRECATION_START_VERSION = "0.73.21"
DBT_PUBLISH_MINIMUM_MINOR_RELEASES = 2
DBT_PUBLISH_MINIMUM_SUPPORT_MONTHS = 12
DBT_PUBLISH_EARLIEST_REMOVAL_VERSION = "0.75.0"
DBT_RAW_PHYSICAL_OVERRIDE_DEPRECATION_START_VERSION = "0.73.21"
DBT_RAW_PHYSICAL_OVERRIDE_MINIMUM_DEPRECATION_RELEASES = 1
DBT_RAW_PHYSICAL_OVERRIDE_EARLIEST_REMOVAL_VERSION = "0.74.0"

_DBT_PUBLISH_DEPRECATION_MESSAGE = (
    "dpone.dbt_publish is deprecated starting in dpone 0.73.21; import from the "
    "canonical dpone.contracts, dpone.manifest, dpone.services, dpone.readiness, "
    "or dpone.adapters module instead. Removal is forbidden before dpone 0.75.0 "
    "and before 12 months after the 0.73.21 release date; the later condition wins."
)
_warning_emitted = False


def warn_legacy_dbt_publish_once() -> None:
    """Emit the facade deprecation once for the lifetime of this process."""

    global _warning_emitted
    if _warning_emitted:
        return
    try:
        if Version(installed_version()) < Version(DBT_PUBLISH_DEPRECATION_START_VERSION):
            return
    except InvalidVersion:
        return
    _warning_emitted = True
    warnings.warn(_DBT_PUBLISH_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=3)


__all__ = [
    "DBT_PUBLISH_DEPRECATION_START_VERSION",
    "DBT_PUBLISH_EARLIEST_REMOVAL_VERSION",
    "DBT_PUBLISH_MINIMUM_MINOR_RELEASES",
    "DBT_PUBLISH_MINIMUM_SUPPORT_MONTHS",
    "DBT_RAW_PHYSICAL_OVERRIDE_DEPRECATION_START_VERSION",
    "DBT_RAW_PHYSICAL_OVERRIDE_EARLIEST_REMOVAL_VERSION",
    "DBT_RAW_PHYSICAL_OVERRIDE_MINIMUM_DEPRECATION_RELEASES",
]
