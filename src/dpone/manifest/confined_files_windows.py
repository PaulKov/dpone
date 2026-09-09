"""Win32 handle-backed reads for project-confined build-plane inputs."""

from __future__ import annotations

import ctypes
import errno
import importlib
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from dpone.manifest.confined_files import ConfinedFileError

_GENERIC_READ = 0x80000000
_SHARE_ALL = 0x00000001 | 0x00000002 | 0x00000004
_OPEN_EXISTING = 3
_FILE_ATTRIBUTE_NORMAL = 0x00000080
_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
_FINAL_PATH_BUFFER = 32_768


@contextmanager
def open_confined_file_windows(root: Path, parts: tuple[str, ...]) -> Iterator[int]:
    """Open one regular file by Win32 handle and verify its final confined path."""

    candidate = root.joinpath(*parts)
    handle: int | None = None
    descriptor: int | None = None
    try:
        _reject_reparse_components(root, parts)
        handle = _create_file(candidate)
        _verify_final_path(handle, root=root, candidate=candidate)
        descriptor = _descriptor_from_handle(handle)
        handle = None
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ConfinedFileError("not_regular_file", "Project input is not a regular file.")
        yield descriptor
    except ConfinedFileError:
        raise
    except OSError as exc:
        code = (
            "file_not_found"
            if exc.errno == errno.ENOENT or getattr(exc, "winerror", None) in {2, 3}
            else "file_unavailable"
        )
        raise ConfinedFileError(code, "Project input could not be opened safely.") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        elif handle is not None:
            _close_handle(handle)


def _reject_reparse_components(root: Path, parts: tuple[str, ...]) -> None:
    current = root
    for part in parts:
        current /= part
        metadata = os.lstat(current)
        attributes = int(getattr(metadata, "st_file_attributes", 0))
        if attributes & _FILE_ATTRIBUTE_REPARSE_POINT:
            raise ConfinedFileError("symlink_forbidden", "Project input cannot traverse a reparse point.")


def _create_file(path: Path) -> int:
    kernel32 = _kernel32()
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    raw_handle = create_file(
        os.fspath(path),
        _GENERIC_READ,
        _SHARE_ALL,
        None,
        _OPEN_EXISTING,
        _FILE_ATTRIBUTE_NORMAL | _FILE_FLAG_OPEN_REPARSE_POINT,
        None,
    )
    handle = int(raw_handle) if raw_handle is not None else int(_INVALID_HANDLE_VALUE or -1)
    if handle == _INVALID_HANDLE_VALUE:
        raise OSError(_last_error(), "Win32 file open failed")
    return handle


def _verify_final_path(handle: int, *, root: Path, candidate: Path) -> None:
    final_path = _final_path(handle)
    normalized_root = _normalize_path(root)
    normalized_candidate = _normalize_path(candidate)
    normalized_final = _normalize_path(Path(final_path))
    try:
        confined = os.path.commonpath((normalized_root, normalized_final)) == normalized_root
    except ValueError:
        confined = False
    if not confined or normalized_final != normalized_candidate:
        raise ConfinedFileError("symlink_forbidden", "Project input resolves outside its confined path.")


def _final_path(handle: int) -> str:
    kernel32 = _kernel32()
    get_final_path = kernel32.GetFinalPathNameByHandleW
    get_final_path.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32]
    get_final_path.restype = ctypes.c_uint32
    buffer = ctypes.create_unicode_buffer(_FINAL_PATH_BUFFER)
    size = int(get_final_path(ctypes.c_void_p(handle), buffer, len(buffer), 0))
    if size == 0 or size >= len(buffer):
        raise OSError(_last_error(), "Win32 final path lookup failed")
    value = buffer.value
    if value.startswith("\\\\?\\UNC\\"):
        return "\\\\" + value[8:]
    if value.startswith("\\\\?\\"):
        return value[4:]
    return value


def _descriptor_from_handle(handle: int) -> int:
    msvcrt: Any = importlib.import_module("msvcrt")
    return int(msvcrt.open_osfhandle(handle, os.O_RDONLY))


def _close_handle(handle: int) -> None:
    close_handle = _kernel32().CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    close_handle(ctypes.c_void_p(handle))


def _normalize_path(path: Path) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def _kernel32() -> Any:
    return getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)


def _last_error() -> int:
    return int(getattr(ctypes, "get_last_error")())


__all__ = ["open_confined_file_windows"]
