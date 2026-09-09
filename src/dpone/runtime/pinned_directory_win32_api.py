"""Lazy ctypes adapter for the Win32 file and directory handle APIs."""

from __future__ import annotations

import ctypes
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

FILE_ATTRIBUTE_DIRECTORY = 0x00000010
FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400

_GENERIC_READ = 0x80000000
_GENERIC_WRITE = 0x40000000
_DELETE = 0x00010000
_FILE_TRAVERSE = 0x00000020
_FILE_READ_ATTRIBUTES = 0x00000080
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_OPEN_EXISTING = 3
_CREATE_NEW = 1
_FILE_ATTRIBUTE_NORMAL = 0x00000080
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
_FINAL_PATH_BUFFER = 32_768


@dataclass(frozen=True, slots=True)
class WindowsDirectoryIdentity:
    """Stable volume/file identity returned by the Win32 adapter."""

    volume: int
    file_index: int
    attributes: int
    creation_time: int = 0

    def same_file(self, other: WindowsDirectoryIdentity) -> bool:
        """Compare the kernel identity fields needed across a cleanup reopen."""

        return (
            self.volume,
            self.file_index,
            self.creation_time,
        ) == (
            other.volume,
            other.file_index,
            other.creation_time,
        )


class Win32DirectoryApiProtocol(Protocol):
    """Minimal Win32 boundary used by policy and platform-independent tests."""

    def open_directory(self, path: Path) -> int: ...

    def identity(self, handle: int) -> WindowsDirectoryIdentity: ...

    def require_final_path(self, handle: int, expected: Path) -> None: ...

    def create_exclusive_file(self, path: Path) -> int: ...

    def descriptor_identity(self, descriptor: int) -> WindowsDirectoryIdentity: ...

    def duplicate_read_lock(self, descriptor: int) -> int: ...

    def delete_exact_file(self, path: Path, *, expected: WindowsDirectoryIdentity) -> None: ...

    def close(self, handle: int) -> None: ...


class Win32DirectoryApi:
    """Small lazy adapter; importing the module remains platform-safe."""

    def open_directory(self, path: Path) -> int:
        return self._create_file(
            path,
            # Attribute-only access is exempt from Win32 share-mode checks.
            # FILE_TRAVERSE is the least-privilege directory right that makes
            # the omitted FILE_SHARE_DELETE deny concurrent rename/delete.
            access=_FILE_TRAVERSE | _FILE_READ_ATTRIBUTES,
            sharing=_FILE_SHARE_READ | _FILE_SHARE_WRITE,
            disposition=_OPEN_EXISTING,
            flags=_FILE_FLAG_BACKUP_SEMANTICS | _FILE_FLAG_OPEN_REPARSE_POINT,
        )

    def create_exclusive_file(self, path: Path) -> int:
        """Create a writer whose share reservation permits readers only."""

        handle = self._create_file(
            path,
            access=_GENERIC_READ | _GENERIC_WRITE,
            sharing=_FILE_SHARE_READ,
            disposition=_CREATE_NEW,
            flags=_FILE_ATTRIBUTE_NORMAL | _FILE_FLAG_OPEN_REPARSE_POINT,
            exists_as_file_error=True,
        )
        try:
            import msvcrt

            open_osfhandle = getattr(msvcrt, "open_osfhandle")
            return int(
                open_osfhandle(
                    handle,
                    os.O_WRONLY | int(getattr(os, "O_BINARY", 0)),
                )
            )
        except BaseException:
            self.close(handle)
            raise

    def descriptor_identity(self, descriptor: int) -> WindowsDirectoryIdentity:
        import msvcrt

        get_osfhandle = getattr(msvcrt, "get_osfhandle")
        return self.identity(int(get_osfhandle(descriptor)))

    def duplicate_read_lock(self, descriptor: int) -> int:
        """Retain the writer's no-write/no-delete share reservation as read-only."""

        import msvcrt

        get_osfhandle = getattr(msvcrt, "get_osfhandle")
        kernel32 = _kernel32()
        get_current_process = kernel32.GetCurrentProcess
        get_current_process.argtypes = []
        get_current_process.restype = ctypes.c_void_p
        duplicate_handle = kernel32.DuplicateHandle
        duplicate_handle.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.c_uint32,
            ctypes.c_int,
            ctypes.c_uint32,
        ]
        duplicate_handle.restype = ctypes.c_int
        process = get_current_process()
        duplicate = ctypes.c_void_p()
        if not duplicate_handle(
            process,
            ctypes.c_void_p(get_osfhandle(descriptor)),
            process,
            ctypes.byref(duplicate),
            _GENERIC_READ,
            0,
            0,
        ):
            raise OSError(_last_error(), "Win32 spool identity duplication failed")
        if duplicate.value is None:  # pragma: no cover - successful DuplicateHandle invariant.
            raise OSError("win32_spool_identity_duplication_empty")
        return int(duplicate.value)

    def identity(self, handle: int) -> WindowsDirectoryIdentity:
        information = _ByHandleFileInformation()
        get_information = _kernel32().GetFileInformationByHandle
        get_information.argtypes = [ctypes.c_void_p, ctypes.POINTER(_ByHandleFileInformation)]
        get_information.restype = ctypes.c_int
        if not get_information(ctypes.c_void_p(handle), ctypes.byref(information)):
            raise OSError(_last_error(), "Win32 directory identity lookup failed")
        return WindowsDirectoryIdentity(
            volume=int(information.volume_serial_number),
            file_index=(int(information.file_index_high) << 32) | int(information.file_index_low),
            attributes=int(information.file_attributes),
            creation_time=(int(information.creation_time.high) << 32) | int(information.creation_time.low),
        )

    def delete_exact_file(self, path: Path, *, expected: WindowsDirectoryIdentity) -> None:
        """Mark only the expected file object for deletion, never its pathname."""

        handle = self._create_file(
            path,
            access=_DELETE | _FILE_READ_ATTRIBUTES,
            sharing=_FILE_SHARE_READ | _FILE_SHARE_WRITE,
            disposition=_OPEN_EXISTING,
            flags=_FILE_ATTRIBUTE_NORMAL | _FILE_FLAG_OPEN_REPARSE_POINT,
        )
        try:
            observed = self.identity(handle)
            if observed.attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT):
                raise OSError("win32_cleanup_target_not_regular")
            if not observed.same_file(expected):
                raise OSError("win32_cleanup_target_identity_changed")
            self.require_final_path(handle, path)
            self._mark_delete_pending(handle)
        finally:
            self.close(handle)

    def require_final_path(self, handle: int, expected: Path) -> None:
        if _normalize_path(Path(self._final_path(handle))) != _normalize_path(expected):
            raise OSError("win32_pinned_directory_final_path_changed")

    def close(self, handle: int) -> None:
        close_handle = _kernel32().CloseHandle
        close_handle.argtypes = [ctypes.c_void_p]
        close_handle.restype = ctypes.c_int
        if not close_handle(ctypes.c_void_p(handle)):
            raise OSError(_last_error(), "Win32 directory lease close failed")

    @staticmethod
    def _create_file(
        path: Path,
        *,
        access: int,
        sharing: int,
        disposition: int,
        flags: int,
        exists_as_file_error: bool = False,
    ) -> int:
        create_file = _kernel32().CreateFileW
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
            access,
            sharing,
            None,
            disposition,
            flags,
            None,
        )
        handle = int(raw_handle) if raw_handle is not None else int(_INVALID_HANDLE_VALUE or -1)
        if handle != _INVALID_HANDLE_VALUE:
            return handle
        error = _last_error()
        if exists_as_file_error and error in {80, 183}:
            raise FileExistsError(error, "Win32 exclusive spool file already exists")
        raise OSError(error, "Win32 file lease open failed")

    @staticmethod
    def _final_path(handle: int) -> str:
        get_path = _kernel32().GetFinalPathNameByHandleW
        get_path.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32]
        get_path.restype = ctypes.c_uint32
        buffer = ctypes.create_unicode_buffer(_FINAL_PATH_BUFFER)
        size = int(get_path(ctypes.c_void_p(handle), buffer, len(buffer), 0))
        if size == 0 or size >= len(buffer):
            raise OSError(_last_error(), "Win32 directory final path lookup failed")
        value = buffer.value
        if value.startswith("\\\\?\\UNC\\"):
            return "\\\\" + value[8:]
        if value.startswith("\\\\?\\"):
            return value[4:]
        return value

    @staticmethod
    def _mark_delete_pending(handle: int) -> None:
        disposition = _FileDispositionInfo(delete_file=1)
        set_information = _kernel32().SetFileInformationByHandle
        set_information.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_uint32,
        ]
        set_information.restype = ctypes.c_int
        if not set_information(
            ctypes.c_void_p(handle),
            4,  # FileDispositionInfo
            ctypes.byref(disposition),
            ctypes.sizeof(disposition),
        ):
            raise OSError(_last_error(), "Win32 exact spool cleanup failed")


class _FileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]


class _ByHandleFileInformation(ctypes.Structure):
    _fields_ = [
        ("file_attributes", ctypes.c_uint32),
        ("creation_time", _FileTime),
        ("last_access_time", _FileTime),
        ("last_write_time", _FileTime),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    ]


class _FileDispositionInfo(ctypes.Structure):
    _fields_ = [("delete_file", ctypes.c_ubyte)]


def _normalize_path(path: Path) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def _kernel32() -> Any:
    return getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)


def _last_error() -> int:
    return int(getattr(ctypes, "get_last_error")())


__all__ = [
    "FILE_ATTRIBUTE_DIRECTORY",
    "FILE_ATTRIBUTE_REPARSE_POINT",
    "Win32DirectoryApi",
    "Win32DirectoryApiProtocol",
    "WindowsDirectoryIdentity",
]
