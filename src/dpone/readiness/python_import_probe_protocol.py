"""Private, bounded transport for optional Python import health probes."""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from dpone.readiness.python_import_bounded import bounded_dict_items
from dpone.readiness.python_import_environment import capture_bounded_environment
from dpone.readiness.python_import_probe_child import (
    ALLOWED_XOPTIONS,
    BOOLEAN_XOPTIONS,
    IMPORT_FAILED_EXIT,
    IMPORT_NOT_INSTALLED_EXIT,
    IMPORT_PATH_INVALID_EXIT,
    IMPORT_POLICY_INVALID_EXIT,
    IMPORT_STARTUP_UNSUPPORTED_EXIT,
    MAX_PROBE_INPUT_BYTES,
    MAX_WARN_OPTION_COUNT,
    MAX_XOPTION_COUNT,
    RECEIPT_PREFIX,
    RECEIPT_TOKEN_BYTES,
    SANITIZED_STARTUP_ENVIRONMENT,
)
from dpone.readiness.python_import_probe_snapshots import ProbePathSnapshot, ProbePayloadError
from dpone.readiness.python_import_runtime_identity import (
    InterpreterFingerprint,
    capture_interpreter_fingerprint,
    hermetic_interpreter_fingerprint,
)
from dpone.readiness.python_import_startup_state import (
    StartupRuntimeState,
    capture_startup_runtime_state,
    startup_environment_replayable,
)
from dpone.readiness.python_import_user_site import UserSiteSnapshot, capture_user_site_snapshot

_MAX_PATH_BYTES = 64 * 1024
_MAX_POLICY_BYTES = 64 * 1024
_MAX_TEXT_CHARACTERS = max(_MAX_PATH_BYTES, _MAX_POLICY_BYTES)


@dataclass(frozen=True, slots=True)
class ProbeEnvironment:
    """One atomic environment snapshot for child startup and target import."""

    child: dict[str, str]
    target_startup: dict[str, str]
    interpreter: InterpreterFingerprint
    user_site: UserSiteSnapshot
    runtime_state: StartupRuntimeState
    startup_state_replayable: bool


@dataclass(frozen=True, slots=True)
class ProbePayload:
    """Bounded stdin frame and its expected positive completion receipt."""

    data: bytes
    receipt_token: bytes


def normalize_xoptions(raw: Mapping[str, object] | None = None) -> dict[str, bool | str] | None:
    """Accept only documented 3.11/3.12 options with bounded safe values."""

    source = sys._xoptions if raw is None else raw
    items = bounded_dict_items(source, MAX_XOPTION_COUNT)
    if items is None:
        return None
    normalized: dict[str, bool | str] = {}
    for key, value in items:
        if type(key) is not str or key not in ALLOWED_XOPTIONS:
            return None
        if key in BOOLEAN_XOPTIONS:
            if value is True:
                normalized_value: bool | str = True
            elif type(value) is str and _safe_text(value):
                normalized_value = value
            else:
                return None
        elif key == "frozen_modules":
            if value is True:
                normalized_value = True
            elif type(value) is str and value in {"on", "off"}:
                normalized_value = value
            else:
                return None
        elif key == "int_max_str_digits":
            if type(value) is not str or not _valid_int_max_str_digits(value):
                return None
            normalized_value = value
        elif key == "pycache_prefix":
            if type(value) is not str or not _safe_text(value):
                return None
            normalized_value = value
        elif key == "tracemalloc":
            if value is not True and (type(value) is not str or not _valid_decimal_range(value, 0, 65_535)):
                return None
            normalized_value = value if type(value) is str else True
        elif key == "utf8":
            if value is not True and value not in {"0", "1"}:
                return None
            normalized_value = value if type(value) is str else True
        else:
            return None
        normalized[key] = normalized_value
    return normalized


def capture_probe_environment() -> ProbeEnvironment | None:
    """Remove startup controls and classify state that cannot be replayed safely."""

    child = capture_bounded_environment()
    if child is None:
        return None
    runtime_state = capture_startup_runtime_state(child)
    interpreter = _capture_interpreter_fingerprint()
    if runtime_state is None or interpreter is None:
        return None
    target_startup: dict[str, str] = {}
    for name in SANITIZED_STARTUP_ENVIRONMENT:
        value = child.pop(name, None)
        if value is not None:
            if not _safe_text(value):
                return None
            target_startup[name] = value
    user_site = capture_user_site_snapshot()
    if user_site is None:
        return None
    return ProbeEnvironment(
        child=child,
        target_startup=target_startup,
        interpreter=interpreter,
        user_site=user_site,
        runtime_state=runtime_state,
        startup_state_replayable=startup_environment_replayable(target_startup, runtime_state),
    )


def build_probe_payload(
    receipt_token: bytes,
    *,
    environment: ProbeEnvironment,
    warnoptions: Sequence[object],
    xoptions: Mapping[str, bool | str],
    startup_warnings_replayed: bool,
    path: ProbePathSnapshot,
    pycache_prefix: object,
    hermetic: bool,
) -> ProbePayload:
    """Snapshot policy, cwd and import path into one length-framed stdin body."""

    if len(receipt_token) != RECEIPT_TOKEN_BYTES:
        raise ProbePayloadError("policy")
    policy = _policy_payload(
        environment,
        warnoptions,
        xoptions,
        startup_warnings_replayed=startup_warnings_replayed,
        pycache_prefix=pycache_prefix,
        hermetic=hermetic,
    )
    path_payload = _path_payload(path)
    data = _bytes_field(receipt_token) + policy + path_payload
    if len(data) > MAX_PROBE_INPUT_BYTES:
        raise ProbePayloadError("policy")
    return ProbePayload(
        data=data,
        receipt_token=receipt_token,
    )


def bind_probe_receipt(payload: ProbePayload, receipt_path: str) -> bytes:
    """Bind one attempt workspace path through bounded stdin, never argv/env."""

    receipt = _text_field(receipt_path, "path")
    if len(receipt) > MAX_PROBE_INPUT_BYTES - len(payload.data):
        raise ProbePayloadError("path")
    return receipt + payload.data


def expected_probe_receipt(receipt_token: bytes, outcome: int) -> bytes | None:
    """Return the exact authenticated receipt for one classified child outcome."""

    classified = {
        0,
        IMPORT_FAILED_EXIT,
        IMPORT_PATH_INVALID_EXIT,
        IMPORT_NOT_INSTALLED_EXIT,
        IMPORT_POLICY_INVALID_EXIT,
        IMPORT_STARTUP_UNSUPPORTED_EXIT,
    }
    if len(receipt_token) != RECEIPT_TOKEN_BYTES or outcome not in classified:
        return None
    return RECEIPT_PREFIX + receipt_token.hex().encode("ascii") + f":{outcome}\n".encode("ascii")


def interpreter_xoption_arguments(
    xoptions: Mapping[str, bool | str],
    *,
    no_debug_ranges: bool,
) -> tuple[str, ...]:
    """Render only non-sensitive startup options; path values stay on stdin."""

    arguments: list[str] = []
    flag_derived = {"dev", "int_max_str_digits", "utf8", "warn_default_encoding"}
    for key in sorted(xoptions):
        if key in flag_derived | {"no_debug_ranges", "pycache_prefix"}:
            continue
        value = xoptions[key]
        rendered = key if key in BOOLEAN_XOPTIONS or value is True else f"{key}={value}"
        arguments.extend(("-X", rendered))
    if no_debug_ranges:
        arguments.extend(("-X", "no_debug_ranges"))
    return tuple(arguments)


def _policy_payload(
    environment: ProbeEnvironment,
    warnoptions: Sequence[object],
    xoptions: Mapping[str, bool | str],
    *,
    startup_warnings_replayed: bool,
    pycache_prefix: object,
    hermetic: bool,
) -> bytes:
    payload = bytearray()
    interpreter = hermetic_interpreter_fingerprint(environment.interpreter) if hermetic else environment.interpreter
    _extend_text_field(payload, interpreter.implementation, "policy", _MAX_POLICY_BYTES)
    payload.extend(_size(interpreter.hexversion))
    _extend_optional_text(payload, interpreter.cache_tag, "policy", _MAX_POLICY_BYTES)
    _extend_optional_text(payload, interpreter.abiflags, "policy", _MAX_POLICY_BYTES)
    _extend_text_field(payload, interpreter.executable, "policy", _MAX_POLICY_BYTES)
    _extend_optional_text(payload, interpreter.base_executable, "policy", _MAX_POLICY_BYTES)
    for text_value in (
        interpreter.prefix,
        interpreter.base_prefix,
        interpreter.exec_prefix,
        interpreter.base_exec_prefix,
        interpreter.platlibdir,
    ):
        _extend_text_field(payload, text_value, "policy", _MAX_POLICY_BYTES)
    payload.extend(_size(0 if hermetic else 1))
    for flag_value in interpreter.flags:
        payload.extend(_size(flag_value + 1))
    runtime_state = environment.runtime_state
    payload.extend(_size(1 if runtime_state.unbuffered_stdio else 0))
    payload.extend(_size(1 if runtime_state.faulthandler_enabled else 0))
    if runtime_state.tracemalloc_frames is None:
        payload.extend(b"0\n")
    else:
        payload.extend(b"1\n")
        payload.extend(_size(runtime_state.tracemalloc_frames))
    payload.extend(_size(1 if runtime_state.frozen_os else 0))
    payload.extend(_size(1 if runtime_state.no_debug_ranges else 0))
    payload.extend(_size(runtime_state.int_max_str_digits))
    payload.extend(_size(0 if hermetic else int(runtime_state.setuptools_distutils_finder)))
    if len(warnoptions) > MAX_WARN_OPTION_COUNT:
        raise ProbePayloadError("policy")
    payload.extend(_size(len(warnoptions)))
    for option in warnoptions:
        if type(option) is not str:
            raise ProbePayloadError("policy")
        _extend_text_field(payload, option, "policy", _MAX_POLICY_BYTES)
    payload.extend(_size(1 if startup_warnings_replayed else 0))
    payload.extend(_size(len(environment.target_startup)))
    for name, environment_value in sorted(environment.target_startup.items()):
        _extend_text_field(payload, name, "policy", _MAX_POLICY_BYTES)
        _extend_text_field(payload, environment_value, "policy", _MAX_POLICY_BYTES)
    _extend_optional_text(payload, environment.user_site.user_base, "policy", _MAX_POLICY_BYTES)
    _extend_optional_text(payload, environment.user_site.user_site, "policy", _MAX_POLICY_BYTES)
    enabled_kind = {None: 0, False: 1, True: 2}[environment.user_site.enabled]
    payload.extend(_size(enabled_kind))
    payload.extend(_size(len(xoptions)))
    for key, xoption_value in sorted(xoptions.items()):
        _extend_text_field(payload, key, "policy", _MAX_POLICY_BYTES)
        if xoption_value is True:
            payload.extend(b"1\n")
        elif type(xoption_value) is str:
            payload.extend(b"2\n")
            _extend_text_field(payload, xoption_value, "policy", _MAX_POLICY_BYTES)
        else:
            raise ProbePayloadError("policy")
    if pycache_prefix is None:
        payload.extend(b"0\n")
    elif type(pycache_prefix) is str:
        payload.extend(b"1\n")
        _extend_text_field(payload, pycache_prefix, "policy", _MAX_POLICY_BYTES)
    else:
        raise ProbePayloadError("policy")
    if len(payload) > _MAX_POLICY_BYTES:
        raise ProbePayloadError("policy")
    return bytes(payload)


def _path_payload(path: ProbePathSnapshot) -> bytes:
    payload = bytearray()
    _extend_text_field(payload, path.cwd, "path", _MAX_PATH_BYTES)
    payload.extend(_size(len(path.entries)))
    for entry in path.entries:
        _extend_text_field(payload, entry, "path", _MAX_PATH_BYTES)
    if len(payload) > _MAX_PATH_BYTES:
        raise ProbePayloadError("path")
    return bytes(payload)


def _text_field(value: str, kind: Literal["path", "policy"]) -> bytes:
    if not _safe_text(value):
        raise ProbePayloadError(kind)
    return _bytes_field(value.encode("utf-8", "surrogatepass"))


def _extend_text_field(
    payload: bytearray,
    value: str,
    kind: Literal["path", "policy"],
    limit: int,
) -> None:
    if not _safe_text(value) or len(value) > limit - len(payload):
        raise ProbePayloadError(kind)
    encoded = value.encode("utf-8", "surrogatepass")
    framed = _bytes_field(encoded)
    if len(framed) > limit - len(payload):
        raise ProbePayloadError(kind)
    payload.extend(framed)


def _extend_optional_text(
    payload: bytearray,
    value: str | None,
    kind: Literal["path", "policy"],
    limit: int,
) -> None:
    marker = b"0\n" if value is None else b"1\n"
    if len(marker) > limit - len(payload):
        raise ProbePayloadError(kind)
    payload.extend(marker)
    if value is not None:
        _extend_text_field(payload, value, kind, limit)


def _bytes_field(value: bytes) -> bytes:
    return _size(len(value)) + value


def _size(value: int) -> bytes:
    return f"{value}\n".encode("ascii")


def _safe_text(value: str) -> bool:
    return type(value) is str and len(value) <= _MAX_TEXT_CHARACTERS and "\x00" not in value


def _capture_interpreter_fingerprint() -> InterpreterFingerprint | None:
    return capture_interpreter_fingerprint()


def _valid_int_max_str_digits(value: str) -> bool:
    return _valid_decimal_range(value, 0, 0) or _valid_decimal_range(value, 640, 2_147_483_647)


def _valid_decimal_range(value: str, minimum: int, maximum: int) -> bool:
    if len(value) > 32 or not value.isascii() or not value.isdecimal():
        return False
    try:
        parsed = int(value)
    except ValueError:
        return False
    return minimum <= parsed <= maximum
