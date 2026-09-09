"""Bounded effective startup state for optional-module import probes."""

from __future__ import annotations

import _imp
import _io
import codecs
import io
import sys
from collections.abc import Mapping
from dataclasses import dataclass

from dpone.readiness.python_import_startup_surface import _setuptools_distutils_finder_active

_MAX_TRACEMALLOC_FRAMES = 65_535
_MIN_INT_MAX_STR_DIGITS = 640
_MAX_INT_MAX_STR_DIGITS = 2_147_483_647
_UNREPLAYABLE_STARTUP_CONTROLS = frozenset(
    {
        "__PYVENV_LAUNCHER__",
        "PYTHONEXECUTABLE",
        "PYTHONHOME",
        "PYTHONPLATLIBDIR",
    }
)
_UNATTESTED_STARTUP_CONTROLS = frozenset(
    {
        "PYTHONCASEOK",
        "PYTHONCOERCECLOCALE",
        "PYTHONLEGACYWINDOWSFSENCODING",
        "PYTHONLEGACYWINDOWSSTDIO",
        "PYTHONMALLOCSTATS",
        "PYTHONPERFSUPPORT",
        "PYTHONPROFILEIMPORTTIME",
    }
)


@dataclass(frozen=True, slots=True)
class StartupRuntimeState:
    """Effective state derived from selected startup-only environment controls."""

    faulthandler_enabled: bool
    tracemalloc_frames: int | None
    unbuffered_stdio: bool = False
    stdio_replayable: bool = True
    frozen_os: bool = True
    no_debug_ranges: bool = False
    int_max_str_digits: int = 0
    setuptools_distutils_finder: bool = False


def capture_startup_runtime_state(environment: Mapping[str, str]) -> StartupRuntimeState | None:
    """Capture observable effective state even after its startup control was removed."""

    faulthandler_enabled = False
    tracemalloc_frames: int | None = None
    try:
        import faulthandler
        import tracemalloc

        faulthandler_enabled = faulthandler.is_enabled()
        if tracemalloc.is_tracing():
            frames = tracemalloc.get_traceback_limit()
            if not isinstance(frames, int) or not 1 <= frames <= _MAX_TRACEMALLOC_FRAMES:
                return None
            tracemalloc_frames = frames
        unbuffered_state = _canonical_unbuffered_stdio_state()
        frozen_os = bool(_imp.is_frozen("os"))
        no_debug_ranges = _no_debug_ranges_enabled()
        int_max_str_digits = _runtime_int_max_str_digits()
        setuptools_distutils_finder = _setuptools_distutils_finder_active()
    except BaseException:
        return None
    if int_max_str_digits is None:
        return None
    return StartupRuntimeState(
        faulthandler_enabled=faulthandler_enabled,
        tracemalloc_frames=tracemalloc_frames,
        unbuffered_stdio=bool(unbuffered_state),
        stdio_replayable=unbuffered_state is not None,
        frozen_os=frozen_os,
        no_debug_ranges=no_debug_ranges,
        int_max_str_digits=int_max_str_digits,
        setuptools_distutils_finder=setuptools_distutils_finder,
    )


def startup_environment_replayable(
    environment: Mapping[str, str],
    runtime_state: StartupRuntimeState,
) -> bool:
    """Accept only bounded startup controls that a fresh child can reproduce."""

    if (
        not runtime_state.stdio_replayable
        or bool(getattr(sys.flags, "inspect", 0))
        or bool(getattr(sys.flags, "interactive", 0))
    ):
        return False
    if _UNREPLAYABLE_STARTUP_CONTROLS.intersection(environment):
        return False
    if bool(getattr(sys.flags, "ignore_environment", 0)):
        return _int_digits_replayable(None, runtime_state)
    if _UNATTESTED_STARTUP_CONTROLS.intersection(environment):
        return False
    return all(
        (
            _hash_seed_replayable(environment.get("PYTHONHASHSEED")),
            _allocator_replayable(environment.get("PYTHONMALLOC")),
            _faulthandler_replayable(environment.get("PYTHONFAULTHANDLER"), runtime_state),
            _tracemalloc_replayable(environment.get("PYTHONTRACEMALLOC"), runtime_state),
            _io_encoding_replayable(environment.get("PYTHONIOENCODING")),
            _int_digits_replayable(environment.get("PYTHONINTMAXSTRDIGITS"), runtime_state),
            _integer_flag_control_replayable(
                environment.get("PYTHONNOUSERSITE"),
                "no_user_site",
            ),
            _presence_flag_control_replayable(
                environment.get("PYTHONSAFEPATH"),
                "safe_path",
            ),
            _unbuffered_replayable(environment.get("PYTHONUNBUFFERED"), runtime_state),
            _utf8_mode_replayable(environment.get("PYTHONUTF8")),
        )
    )


def startup_xoptions_replayable(
    xoptions: Mapping[str, bool | str],
    runtime_state: StartupRuntimeState,
) -> bool:
    """Validate startup options whose observable runtime effects can drift."""

    if {"importtime", "perf"}.intersection(xoptions):
        return False
    frozen_modules = xoptions.get("frozen_modules")
    if runtime_state.frozen_os is not (frozen_modules != "off"):
        return False

    if "faulthandler" in xoptions and not runtime_state.faulthandler_enabled:
        return False
    tracemalloc_value = xoptions.get("tracemalloc")
    if tracemalloc_value is not None:
        expected_frames: int | None
        if tracemalloc_value is True:
            expected_frames = 1
        elif isinstance(tracemalloc_value, str):
            expected_frames = _bounded_decimal(tracemalloc_value, 0, 65_535)
        else:
            return False
        if expected_frames == 0:
            expected_frames = None
        if expected_frames is None and tracemalloc_value != "0":
            return False
        if runtime_state.tracemalloc_frames != expected_frames:
            return False
    return True


def _hash_seed_replayable(value: str | None) -> bool:
    if value is None:
        return bool(getattr(sys.flags, "hash_randomization", 1))
    if _bounded_decimal(value, 0, 0) == 0:
        return not bool(getattr(sys.flags, "hash_randomization", 1))
    return False


def _allocator_replayable(value: str | None) -> bool:
    return value is None


def _faulthandler_replayable(value: str | None, state: StartupRuntimeState) -> bool:
    return value is None or state.faulthandler_enabled is bool(value)


def _tracemalloc_replayable(value: str | None, state: StartupRuntimeState) -> bool:
    if value is None:
        return True
    parsed = _bounded_decimal(value, 0, _MAX_TRACEMALLOC_FRAMES)
    if value == "" or parsed == 0:
        expected = None
    else:
        expected = parsed
        if expected is None:
            return False
    return state.tracemalloc_frames == expected


def _io_encoding_replayable(value: str | None) -> bool:
    if value is None or value == "":
        return True
    encoding, separator, errors = value.partition(":")
    try:
        expected_encoding = None if not encoding else codecs.lookup(encoding).name
        stdout = sys.__stdout__
        stdin = sys.__stdin__
        if stdout is None or stdin is None:
            return False
        if expected_encoding is not None and any(
            codecs.lookup(str(getattr(stream, "encoding", ""))).name != expected_encoding for stream in (stdin, stdout)
        ):
            return False
    except (LookupError, TypeError, ValueError):
        return False
    if not separator:
        errors = "strict"
    if not errors:
        errors = "strict" if encoding else "surrogateescape"
    return all(getattr(stream, "errors", None) == errors for stream in (stdin, stdout))


def _int_digits_replayable(value: str | None, state: StartupRuntimeState) -> bool:
    flag_value = getattr(sys.flags, "int_max_str_digits", None)
    if not isinstance(flag_value, int) or isinstance(flag_value, bool):
        return False
    startup_value = getattr(sys.int_info, "default_max_str_digits", None) if flag_value == -1 else flag_value
    if not isinstance(startup_value, int) or state.int_max_str_digits != startup_value:
        return False
    if value is None or value == "":
        return True
    parsed = _bounded_decimal(value, 0, 0)
    if parsed is None:
        parsed = _bounded_decimal(value, _MIN_INT_MAX_STR_DIGITS, _MAX_INT_MAX_STR_DIGITS)
    return parsed is not None and startup_value == parsed


def _integer_flag_control_replayable(value: str | None, flag_name: str) -> bool:
    """Validate CPython controls whose decimal zero spelling disables a flag."""

    if value is None:
        return True
    return bool(getattr(sys.flags, flag_name, 0)) is _environment_integer_flag_enabled(value)


def _presence_flag_control_replayable(value: str | None, flag_name: str) -> bool:
    """Validate CPython controls whose every nonempty spelling enables a flag."""

    if value is None:
        return True
    return bool(getattr(sys.flags, flag_name, 0)) is bool(value)


def _unbuffered_replayable(value: str | None, state: StartupRuntimeState) -> bool:
    if value is None:
        return True
    return state.unbuffered_stdio is _environment_integer_flag_enabled(value)


def _canonical_unbuffered_stdio_state() -> bool | None:
    stdin = sys.__stdin__
    stdout = sys.__stdout__
    stderr = sys.__stderr__
    if not all(type(stream) is io.TextIOWrapper for stream in (stdin, stdout, stderr)):
        return None
    assert isinstance(stdin, io.TextIOWrapper)
    assert isinstance(stdout, io.TextIOWrapper)
    assert isinstance(stderr, io.TextIOWrapper)
    streams = (stdin, stdout, stderr)
    write_through = tuple(getattr(stream, "write_through", None) for stream in streams)
    if any(not isinstance(enabled, bool) for enabled in write_through) or len(set(write_through)) != 1:
        return None
    unbuffered = write_through[0]
    if type(stdin.buffer) is not io.BufferedReader:
        return None
    if unbuffered:
        raw_output_types: set[type[object]] = {io.FileIO}
        windows_console_type = getattr(_io, "_WindowsConsoleIO", None)
        if isinstance(windows_console_type, type):
            raw_output_types.add(windows_console_type)
        if any(type(stream.buffer) not in raw_output_types for stream in (stdout, stderr)):
            return None
    elif any(type(stream.buffer) is not io.BufferedWriter for stream in (stdout, stderr)):
        return None
    return unbuffered


def _no_debug_ranges_enabled() -> bool:
    positions_reader = getattr(_no_debug_ranges_enabled.__code__, "co_positions", None)
    if not callable(positions_reader):
        return False
    positions = tuple(positions_reader())
    return bool(positions) and all(left is None and right is None for _, _, left, right in positions)


def _runtime_int_max_str_digits() -> int | None:
    getter = getattr(sys, "get_int_max_str_digits", None)
    if not callable(getter):
        return None
    value = getter()
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    return value if value == 0 or _MIN_INT_MAX_STR_DIGITS <= value <= _MAX_INT_MAX_STR_DIGITS else None


def _environment_integer_flag_enabled(value: str | None) -> bool:
    if not value:
        return False
    stripped = value.lstrip(" \t\n\r\v\f")
    if stripped[:1] in {"+", "-"}:
        stripped = stripped[1:]
    return not stripped or any(character != "0" for character in stripped)


def _utf8_mode_replayable(value: str | None) -> bool:
    if value is None or value == "":
        return True
    if value not in {"0", "1"}:
        return False
    return bool(getattr(sys.flags, "utf8_mode", 0)) is (value == "1")


def _bounded_decimal(value: str, minimum: int, maximum: int) -> int | None:
    if len(value) > 32 or not value.isascii():
        return None
    candidate = value.lstrip(" \t\n\r\v\f")
    if candidate[:1] in {"+", "-"}:
        candidate = candidate[1:]
    if not candidate or not candidate.isdecimal():
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if minimum <= parsed <= maximum else None


__all__ = [
    "StartupRuntimeState",
    "capture_startup_runtime_state",
    "startup_environment_replayable",
    "startup_xoptions_replayable",
]
