"""Bounded runtime identity used by authenticated Python import probes."""

from __future__ import annotations

import sys
from dataclasses import dataclass, replace

from dpone.readiness.python_import_probe_child_runtime import IMMUTABLE_SYS_FLAG_NAMES

_MAX_TEXT_CHARACTERS = 64 * 1024
_MAX_FLAG_VALUE = 2_147_483_647
_MAX_HEXVERSION = 0xFFFFFFFF


@dataclass(frozen=True, slots=True)
class InterpreterFingerprint:
    """Target-visible interpreter identity and immutable startup flags."""

    implementation: str
    hexversion: int
    cache_tag: str | None
    abiflags: str | None
    executable: str
    base_executable: str | None
    prefix: str
    base_prefix: str
    exec_prefix: str
    base_exec_prefix: str
    platlibdir: str
    flags: tuple[int, ...]


def capture_interpreter_fingerprint() -> InterpreterFingerprint | None:
    """Capture one bounded identity without exposing it through argv or env."""

    implementation = getattr(sys.implementation, "name", None)
    cache_tag = getattr(sys.implementation, "cache_tag", None)
    hexversion = getattr(sys, "hexversion", None)
    missing = object()
    raw_abiflags = getattr(sys, "abiflags", missing)
    if raw_abiflags is missing:
        abiflags: str | None = None
    elif type(raw_abiflags) is str:
        abiflags = raw_abiflags
    else:
        return None
    executable = getattr(sys, "executable", None)
    base_executable = getattr(sys, "_base_executable", None)
    prefix = getattr(sys, "prefix", None)
    base_prefix = getattr(sys, "base_prefix", None)
    exec_prefix = getattr(sys, "exec_prefix", None)
    base_exec_prefix = getattr(sys, "base_exec_prefix", None)
    platlibdir = getattr(sys, "platlibdir", None)
    if (
        type(implementation) is not str
        or type(executable) is not str
        or type(prefix) is not str
        or type(base_prefix) is not str
        or type(exec_prefix) is not str
        or type(base_exec_prefix) is not str
        or type(platlibdir) is not str
        or type(hexversion) is not int
        or not 0 <= hexversion <= _MAX_HEXVERSION
    ):
        return None
    required_text = (
        implementation,
        executable,
        prefix,
        base_prefix,
        exec_prefix,
        base_exec_prefix,
        platlibdir,
    )
    if (
        any(not _safe_text(value) for value in required_text)
        or (abiflags is not None and not _safe_text(abiflags))
        or (cache_tag is not None and (type(cache_tag) is not str or not _safe_text(cache_tag)))
        or (base_executable is not None and (type(base_executable) is not str or not _safe_text(base_executable)))
    ):
        return None
    flags = _capture_flags()
    if flags is None:
        return None
    return InterpreterFingerprint(
        implementation=implementation,
        hexversion=hexversion,
        cache_tag=cache_tag,
        abiflags=abiflags,
        executable=executable,
        base_executable=base_executable,
        prefix=prefix,
        base_prefix=base_prefix,
        exec_prefix=exec_prefix,
        base_exec_prefix=base_exec_prefix,
        platlibdir=platlibdir,
        flags=flags,
    )


def hermetic_interpreter_fingerprint(
    fingerprint: InterpreterFingerprint,
) -> InterpreterFingerprint:
    """Project the flags imposed by the probe's explicit ``-I -S`` leg."""

    values = dict(zip(IMMUTABLE_SYS_FLAG_NAMES, fingerprint.flags, strict=True))
    values.update(
        {
            "hash_randomization": 1,
            "ignore_environment": 1,
            "isolated": 1,
            "no_site": 1,
            "no_user_site": 1,
            "safe_path": 1,
        }
    )
    flags = tuple(values[name] for name in IMMUTABLE_SYS_FLAG_NAMES)
    return replace(fingerprint, flags=flags)


def _capture_flags() -> tuple[int, ...] | None:
    values: list[int] = []
    for name in IMMUTABLE_SYS_FLAG_NAMES:
        value = getattr(sys.flags, name, None)
        if not isinstance(value, int) or not -1 <= value <= _MAX_FLAG_VALUE:
            return None
        values.append(int(value))
    return tuple(values)


def _safe_text(value: str) -> bool:
    return type(value) is str and len(value) <= _MAX_TEXT_CHARACTERS and "\x00" not in value
