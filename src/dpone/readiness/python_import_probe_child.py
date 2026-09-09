"""Dependency-light bootstrap executed by the Python import health child."""

from __future__ import annotations

from dpone.readiness import python_import_probe_startup_policy as _startup_policy
from dpone.readiness.python_import_probe_child_runtime import (
    IMMUTABLE_SYS_FLAG_NAMES,
    RUNTIME_ATTESTATION_HELPERS,
    RUNTIME_ATTESTATION_IMPORTS,
)

ALLOWED_XOPTIONS = _startup_policy.ALLOWED_XOPTIONS
BOOLEAN_XOPTIONS = _startup_policy.BOOLEAN_XOPTIONS
CONFINED_STARTUP_ENVIRONMENT = _startup_policy.CONFINED_STARTUP_ENVIRONMENT
EFFECTIVE_STARTUP_ENVIRONMENT = _startup_policy.EFFECTIVE_STARTUP_ENVIRONMENT
MAX_PATH_ENTRIES = _startup_policy.MAX_PATH_ENTRIES
MAX_WARN_OPTION_COUNT = _startup_policy.MAX_WARN_OPTION_COUNT
MAX_XOPTION_COUNT = _startup_policy.MAX_XOPTION_COUNT
SANITIZED_STARTUP_ENVIRONMENT = _startup_policy.SANITIZED_STARTUP_ENVIRONMENT
VALUE_XOPTIONS = _startup_policy.VALUE_XOPTIONS

IMPORT_FAILED_EXIT = 1
IMPORT_PATH_INVALID_EXIT = 2
IMPORT_NOT_INSTALLED_EXIT = 3
IMPORT_POLICY_INVALID_EXIT = 4
IMPORT_PROBE_UNAVAILABLE_EXIT = 5
IMPORT_STARTUP_UNSUPPORTED_EXIT = 6
MAX_PROBE_INPUT_BYTES = 132 * 1024 + 128
RECEIPT_TOKEN_BYTES = 32
RECEIPT_PREFIX = b"dpone-python-import-health-v2:"
RECEIPT_FILENAME = ".dpone-python-import-health.receipt"

IMPORT_COMMAND = f"""\
{RUNTIME_ATTESTATION_IMPORTS}
import os
import site
import sys
import warnings

terminate_process = os._exit
startup_warnoptions = tuple(sys.warnoptions)
receipt_path = ""
status = 0
receipt_token = b""
raw = sys.stdin.buffer.read({MAX_PROBE_INPUT_BYTES + 1})
cursor = 0

def read_size():
    global cursor
    line_end = raw.find(b"\\n", cursor)
    if line_end < 0:
        raise ValueError
    value = int(raw[cursor:line_end])
    cursor = line_end + 1
    if value < 0:
        raise ValueError
    return value

def read_bytes():
    global cursor
    size = read_size()
    end = cursor + size
    if end > len(raw):
        raise ValueError
    value = raw[cursor:end]
    cursor = end
    return value

def read_text():
    value = read_bytes().decode("utf-8", "surrogatepass")
    if "\\x00" in value:
        raise ValueError
    return value

{RUNTIME_ATTESTATION_HELPERS}

try:
    if len(raw) > {MAX_PROBE_INPUT_BYTES}:
        raise ValueError
    receipt_path = read_text()
    receipt_token = read_bytes()
    if len(receipt_token) != {RECEIPT_TOKEN_BYTES}:
        raise ValueError
    expected_implementation = read_text()
    expected_hexversion = read_size()
    cache_tag_kind = read_size()
    if cache_tag_kind == 0:
        expected_cache_tag = None
    elif cache_tag_kind == 1:
        expected_cache_tag = read_text()
    else:
        raise ValueError
    abiflags_kind = read_size()
    if abiflags_kind == 0:
        expected_abiflags = None
    elif abiflags_kind == 1:
        expected_abiflags = read_text()
    else:
        raise ValueError
    expected_executable = read_text()
    base_executable_kind = read_size()
    if base_executable_kind == 0:
        expected_base_executable = None
    elif base_executable_kind == 1:
        expected_base_executable = read_text()
    else:
        raise ValueError
    expected_prefix = read_text()
    expected_base_prefix = read_text()
    expected_exec_prefix = read_text()
    expected_base_exec_prefix = read_text()
    expected_platlibdir = read_text()
    proof_kind = read_size()
    if proof_kind not in {{0, 1}}:
        raise ValueError
    expected_flags = tuple(read_size() - 1 for _ in range({len(IMMUTABLE_SYS_FLAG_NAMES)}))
    observed_flags = tuple(getattr(sys.flags, name, None) for name in {IMMUTABLE_SYS_FLAG_NAMES!r})
    expected_unbuffered = read_size()
    expected_faulthandler = read_size()
    tracemalloc_kind = read_size()
    if tracemalloc_kind == 0:
        expected_tracemalloc_frames = None
    elif tracemalloc_kind == 1:
        expected_tracemalloc_frames = read_size()
    else:
        raise ValueError
    expected_frozen_os = read_size()
    expected_no_debug_ranges = read_size()
    expected_int_max_str_digits = read_size()
    expected_setuptools_distutils_finder = read_size()
    if any(value not in {{0, 1}} for value in (
        expected_unbuffered,
        expected_faulthandler,
        expected_frozen_os,
        expected_no_debug_ranges,
        expected_setuptools_distutils_finder,
    )):
        raise ValueError
    observed_tracemalloc_frames = (
        tracemalloc.get_traceback_limit() if tracemalloc.is_tracing() else None
    )
    runtime_state_matches = (
        canonical_unbuffered_stdio() is bool(expected_unbuffered)
        and faulthandler.is_enabled() is bool(expected_faulthandler)
        and observed_tracemalloc_frames == expected_tracemalloc_frames
        and bool(_imp.is_frozen("os")) is bool(expected_frozen_os)
        and no_debug_ranges_enabled() is bool(expected_no_debug_ranges)
        and runtime_int_max_str_digits() == expected_int_max_str_digits
        and setuptools_distutils_finder_active() is bool(expected_setuptools_distutils_finder)
        and startup_surface_is_standard()
    )
    core_interpreter_matches = (
        sys.implementation.name == expected_implementation
        and sys.hexversion == expected_hexversion
        and sys.implementation.cache_tag == expected_cache_tag
        and (
            (expected_abiflags is None and not hasattr(sys, "abiflags"))
            or (expected_abiflags is not None and getattr(sys, "abiflags", None) == expected_abiflags)
        )
        and getattr(sys, "_base_executable", None) == expected_base_executable
        and sys.base_prefix == expected_base_prefix
        and sys.base_exec_prefix == expected_base_exec_prefix
        and getattr(sys, "platlibdir", None) == expected_platlibdir
    )
    effective_identity_matches = (
        sys.executable == expected_executable
        and sys.prefix == expected_prefix
        and sys.exec_prefix == expected_exec_prefix
    )
    interpreter_matches = (
        core_interpreter_matches
        and observed_flags == expected_flags
        and runtime_state_matches
        and (proof_kind == 0 or effective_identity_matches)
    )
    warning_count = read_size()
    if warning_count > {MAX_WARN_OPTION_COUNT}:
        raise ValueError
    effective_warnoptions = [read_text() for _ in range(warning_count)]
    startup_warning_kind = read_size()
    if startup_warning_kind not in {{0, 1}}:
        raise ValueError
    startup_warnings_replayed = startup_warning_kind == 1
    environment_count = read_size()
    if environment_count > {len(SANITIZED_STARTUP_ENVIRONMENT)}:
        raise ValueError
    effective_environment = {{}}
    for _ in range(environment_count):
        key = read_text()
        if key not in {SANITIZED_STARTUP_ENVIRONMENT!r} or key in effective_environment:
            raise ValueError
        effective_environment[key] = read_text()
    user_base_kind = read_size()
    if user_base_kind == 0:
        effective_user_base = None
    elif user_base_kind == 1:
        effective_user_base = read_text()
    else:
        raise ValueError
    user_site_kind = read_size()
    if user_site_kind == 0:
        effective_user_site = None
    elif user_site_kind == 1:
        effective_user_site = read_text()
    else:
        raise ValueError
    user_site_enabled_kind = read_size()
    if user_site_enabled_kind == 0:
        effective_user_site_enabled = None
    elif user_site_enabled_kind == 1:
        effective_user_site_enabled = False
    elif user_site_enabled_kind == 2:
        effective_user_site_enabled = True
    else:
        raise ValueError
    xoption_count = read_size()
    if xoption_count > {MAX_XOPTION_COUNT}:
        raise ValueError
    effective_xoptions = {{}}
    for _ in range(xoption_count):
        key = read_text()
        if key not in {ALLOWED_XOPTIONS!r} or key in effective_xoptions:
            raise ValueError
        value_kind = read_size()
        if value_kind == 1:
            value = True
        elif value_kind == 2:
            value = read_text()
        else:
            raise ValueError
        effective_xoptions[key] = value
    pycache_kind = read_size()
    if pycache_kind == 0:
        effective_pycache_prefix = None
    elif pycache_kind == 1:
        effective_pycache_prefix = read_text()
    else:
        raise ValueError
except BaseException:
    status = {IMPORT_POLICY_INVALID_EXIT}

if status == 0 and not interpreter_matches:
    status = {IMPORT_STARTUP_UNSUPPORTED_EXIT}

if status == 0:
    try:
        effective_cwd = read_text()
        path_count = read_size()
        if path_count > {MAX_PATH_ENTRIES}:
            raise ValueError
        effective_path = [read_text() for _ in range(path_count)]
        if cursor != len(raw):
            raise ValueError
        os.chdir(effective_cwd)
        sys.path[:] = effective_path
    except BaseException:
        status = {IMPORT_PATH_INVALID_EXIT}

if status == 0:
    try:
        for key in {SANITIZED_STARTUP_ENVIRONMENT!r}:
            os.environ.pop(key, None)
        os.environ.update(effective_environment)
        site.USER_BASE = effective_user_base
        site.USER_SITE = effective_user_site
        site.ENABLE_USER_SITE = effective_user_site_enabled
        sys.pycache_prefix = effective_pycache_prefix
        if startup_warnings_replayed:
            if startup_warnoptions != tuple(effective_warnoptions):
                raise RuntimeError
        else:
            process_warning_options = getattr(warnings, "_processoptions", None)
            if not callable(process_warning_options):
                raise RuntimeError
            process_warning_options(effective_warnoptions)
        sys.warnoptions[:] = effective_warnoptions
        sys._xoptions.clear()
        sys._xoptions.update(effective_xoptions)
    except BaseException:
        status = {IMPORT_POLICY_INVALID_EXIT}

if status == 0:
    name = sys.argv[1]
    top_level_name = name.partition(".")[0]
    top_level_present = None
    try:
        top_level_present = any(
            finder.find_spec(top_level_name) is not None
            for finder in (
                importlib.machinery.BuiltinImporter,
                importlib.machinery.FrozenImporter,
            )
        ) or importlib.machinery.PathFinder.find_spec(top_level_name, sys.path) is not None
    except BaseException:
        top_level_present = None
    try:
        __import__(name)
    except ModuleNotFoundError as exc:
        missing = exc.name or ""
        target_is_missing = bool(missing) and (
            name == missing or name.startswith(missing + ".")
        )
        traceback_entered_loaded_code = exc.__traceback__ is not None and exc.__traceback__.tb_next is not None
        top_level_finder_miss = "." in name or top_level_present is False
        target_was_not_found = target_is_missing and not traceback_entered_loaded_code and top_level_finder_miss
        status = {IMPORT_NOT_INSTALLED_EXIT} if target_was_not_found else {IMPORT_FAILED_EXIT}
    except BaseException:
        status = {IMPORT_FAILED_EXIT}

if len(receipt_token) == {RECEIPT_TOKEN_BYTES} and status in {{
    0,
    {IMPORT_FAILED_EXIT},
    {IMPORT_PATH_INVALID_EXIT},
    {IMPORT_NOT_INSTALLED_EXIT},
    {IMPORT_POLICY_INVALID_EXIT},
    {IMPORT_STARTUP_UNSUPPORTED_EXIT},
}}:
    descriptor = -1
    try:
        flags = os.O_WRONLY | os.O_TRUNC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(receipt_path, flags)
        metadata = os.fstat(descriptor)
        if (metadata.st_mode & 0o170000) != 0o100000 or metadata.st_nlink != 1:
            raise OSError
        receipt = (
            {RECEIPT_PREFIX!r}
            + receipt_token.hex().encode("ascii")
            + b":"
            + str(status).encode("ascii")
            + b"\\n"
        )
        while receipt:
            written = os.write(descriptor, receipt)
            if written <= 0:
                raise OSError
            receipt = receipt[written:]
        os.fsync(descriptor)
    except BaseException:
        status = {IMPORT_PROBE_UNAVAILABLE_EXIT}
    finally:
        if descriptor >= 0:
            os.close(descriptor)
terminate_process(status)
"""
