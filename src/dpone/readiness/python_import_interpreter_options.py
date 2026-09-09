"""Validated command-line and warning-policy replay for import probes."""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping, Sequence

from dpone.readiness import python_import_startup_state as _startup_state
from dpone.readiness.python_import_probe_protocol import (
    interpreter_xoption_arguments,
    normalize_xoptions,
)
from dpone.readiness.python_import_probe_startup_policy import MAX_WARN_OPTION_COUNT

_MIN_INT_MAX_STR_DIGITS = 640
_MAX_INT_MAX_STR_DIGITS = 2_147_483_647
_MAX_STARTUP_WARNINGS_BYTES = 64 * 1024


def _bounded_flag(flags: object, name: str, maximum: int) -> int | None:
    value = getattr(flags, name, 0)
    return value if isinstance(value, int) and 0 <= value <= maximum else None


def _startup_xoptions_replayable(
    xoptions: Mapping[str, bool | str],
    startup_state: _startup_state.StartupRuntimeState,
) -> bool:
    """Keep startup-option validation behind the interpreter-options boundary."""

    return _startup_state.startup_xoptions_replayable(xoptions, startup_state)


def _interpreter_probe_options(
    flags: object | None = None,
    warnoptions: Sequence[object] | None = None,
    xoptions: Mapping[str, object] | None = None,
    startup_state: _startup_state.StartupRuntimeState | None = None,
) -> tuple[str, ...] | None:
    """Mirror bounded startup policy without putting path values in argv."""

    effective_flags = sys.flags if flags is None else flags
    normalized_xoptions = normalize_xoptions(xoptions)
    effective_warnoptions = sys.warnoptions if warnoptions is None else warnoptions
    effective_startup_state = startup_state or _startup_state.StartupRuntimeState(False, None)
    if normalized_xoptions is None or any(
        type(option) is not str or "\x00" in option for option in effective_warnoptions
    ):
        return None

    binary_names = (
        "isolated",
        "ignore_environment",
        "no_user_site",
        "no_site",
        "dont_write_bytecode",
        "dev_mode",
        "utf8_mode",
        "warn_default_encoding",
        "safe_path",
        "interactive",
        "hash_randomization",
    )
    binary = {name: _bounded_flag(effective_flags, name, 1) for name in binary_names}
    optimize = _bounded_flag(effective_flags, "optimize", 2)
    bytes_warning = _bounded_flag(effective_flags, "bytes_warning", 2)
    debug = _bounded_flag(effective_flags, "debug", 255)
    inspect = _bounded_flag(effective_flags, "inspect", 1)
    verbose = _bounded_flag(effective_flags, "verbose", 255)
    quiet = _bounded_flag(effective_flags, "quiet", 255)
    int_digits = getattr(effective_flags, "int_max_str_digits", -1)
    valid_digits = (
        isinstance(int_digits, int)
        and not isinstance(int_digits, bool)
        and (int_digits in {-1, 0} or _MIN_INT_MAX_STR_DIGITS <= int_digits <= _MAX_INT_MAX_STR_DIGITS)
    )
    if (
        any(value is None for value in binary.values())
        or None in {optimize, bytes_warning, debug, inspect, verbose, quiet}
        or not valid_digits
        or (bool(binary["isolated"]) and not bool(binary["safe_path"]))
        or (bool(binary["interactive"]) and not bool(inspect))
    ):
        return None

    options: list[str] = []
    if effective_startup_state.unbuffered_stdio:
        options.append("-u")
    if binary["safe_path"]:
        options.append("-P")
    if binary["isolated"]:
        options.append("-I")
    else:
        if binary["ignore_environment"]:
            options.append("-E")
        if binary["no_user_site"]:
            options.append("-s")
    if binary["no_site"]:
        options.append("-S")
    for value, option in ((debug, "d"), (verbose, "v"), (quiet, "q")):
        if value:
            options.append("-" + option * value)
    if inspect and binary["interactive"]:
        options.append("-i")
    if optimize:
        options.append("-" + "O" * optimize)
    if binary["dont_write_bytecode"]:
        options.append("-B")
    if bytes_warning:
        options.append("-" + "b" * bytes_warning)
    if binary["dev_mode"]:
        options.extend(("-X", "dev"))
    options.extend(("-X", f"utf8={binary['utf8_mode']}"))
    if binary["warn_default_encoding"]:
        options.extend(("-X", "warn_default_encoding"))
    if int_digits != -1:
        options.extend(("-X", f"int_max_str_digits={int_digits}"))
    if effective_startup_state.faulthandler_enabled and "faulthandler" not in normalized_xoptions:
        options.extend(("-X", "faulthandler"))
    if effective_startup_state.tracemalloc_frames is not None and "tracemalloc" not in normalized_xoptions:
        options.extend(("-X", f"tracemalloc={effective_startup_state.tracemalloc_frames}"))
    options.extend(
        interpreter_xoption_arguments(
            normalized_xoptions,
            no_debug_ranges=effective_startup_state.no_debug_ranges,
        )
    )
    return tuple(options)


def _startup_warning_environment(warnoptions: Sequence[object]) -> tuple[bool, str | None]:
    """Render lossless pre-site warning policy for the effective child."""

    if not warnoptions or bool(getattr(sys.flags, "no_site", 0)):
        return True, None
    if bool(getattr(sys.flags, "ignore_environment", 0)) or bool(getattr(sys.flags, "isolated", 0)):
        return False, None
    if len(warnoptions) > MAX_WARN_OPTION_COUNT:
        return False, None
    options: list[str] = []
    characters = max(0, len(warnoptions) - 1)
    for option in warnoptions:
        if type(option) is not str or "\x00" in option or "," in option or len(option) > _MAX_STARTUP_WARNINGS_BYTES:
            return False, None
        characters += len(option)
        if characters > _MAX_STARTUP_WARNINGS_BYTES:
            return False, None
        options.append(option)
    encoded_size = max(0, len(options) - 1)
    try:
        for option in options:
            encoded = os.fsencode(option)
            if os.fsdecode(encoded) != option:
                return False, None
            encoded_size += len(encoded)
            if encoded_size > _MAX_STARTUP_WARNINGS_BYTES:
                return False, None
    except (UnicodeEncodeError, ValueError):
        return False, None
    return True, ",".join(options)


__all__: list[str] = []
