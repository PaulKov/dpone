"""Private startup-policy vocabulary shared by Python import probe components."""

from __future__ import annotations

MAX_PATH_ENTRIES = 4_096
MAX_WARN_OPTION_COUNT = 64
MAX_XOPTION_COUNT = 32

CONFINED_STARTUP_ENVIRONMENT = frozenset(
    {
        "APPDATA",
        "HOME",
        "PYTHONUSERBASE",
        "USERPROFILE",
    }
)
SANITIZED_STARTUP_ENVIRONMENT = frozenset(
    {
        *CONFINED_STARTUP_ENVIRONMENT,
        "__PYVENV_LAUNCHER__",
        "PYTHONBREAKPOINT",
        "PYTHONCASEOK",
        "PYTHONCOERCECLOCALE",
        "PYTHONDEBUG",
        "PYTHONDEVMODE",
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONEXECUTABLE",
        "PYTHONFAULTHANDLER",
        "PYTHONHASHSEED",
        "PYTHONHOME",
        "PYTHONIOENCODING",
        "PYTHONINSPECT",
        "PYTHONINTMAXSTRDIGITS",
        "PYTHONMALLOC",
        "PYTHONMALLOCSTATS",
        "PYTHONLEGACYWINDOWSFSENCODING",
        "PYTHONLEGACYWINDOWSSTDIO",
        "PYTHONNODEBUGRANGES",
        "PYTHONNOUSERSITE",
        "PYTHONOPTIMIZE",
        "PYTHONPATH",
        "PYTHONPYCACHEPREFIX",
        "PYTHONPLATLIBDIR",
        "PYTHONPERFSUPPORT",
        "PYTHONPROFILEIMPORTTIME",
        "PYTHONSAFEPATH",
        "PYTHONSTARTUP",
        "PYTHONTRACEMALLOC",
        "PYTHONUNBUFFERED",
        "PYTHONUTF8",
        "PYTHONWARNDEFAULTENCODING",
        "PYTHONWARNINGS",
        "PYTHONVERBOSE",
    }
)
EFFECTIVE_STARTUP_ENVIRONMENT = SANITIZED_STARTUP_ENVIRONMENT - {
    *CONFINED_STARTUP_ENVIRONMENT,
    "PYTHONPATH",
    "PYTHONWARNINGS",
}

BOOLEAN_XOPTIONS = frozenset(
    {
        "dev",
        "faulthandler",
        "importtime",
        "no_debug_ranges",
        "perf",
        "showrefcount",
        "warn_default_encoding",
    }
)
VALUE_XOPTIONS = frozenset(
    {
        "frozen_modules",
        "int_max_str_digits",
        "pycache_prefix",
        "tracemalloc",
        "utf8",
    }
)
ALLOWED_XOPTIONS = BOOLEAN_XOPTIONS | VALUE_XOPTIONS

__all__ = [
    "ALLOWED_XOPTIONS",
    "BOOLEAN_XOPTIONS",
    "CONFINED_STARTUP_ENVIRONMENT",
    "EFFECTIVE_STARTUP_ENVIRONMENT",
    "MAX_PATH_ENTRIES",
    "MAX_WARN_OPTION_COUNT",
    "MAX_XOPTION_COUNT",
    "SANITIZED_STARTUP_ENVIRONMENT",
    "VALUE_XOPTIONS",
]
