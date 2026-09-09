"""Fail-closed classification of startup-installed Python import machinery."""

from __future__ import annotations

import builtins
import importlib.machinery
import os
import site
import stat
import sys
import types
import zipimport
from collections.abc import Iterable

from dpone.readiness.python_import_bounded import bounded_dict_items, bounded_list

_MAX_SITE_DIRECTORIES = 32
_MAX_PTH_FILES = 256
_MAX_PTH_BYTES = 64 * 1024
_MAX_PATH_CHARACTERS = 64 * 1024
_MAX_RUNTIME_IMPORT_ENTRIES = 4_096
_MAX_SITE_SCAN_ENTRIES = 16_384
_MAX_CUSTOMIZER_ZIP_BYTES = 16 * 1024 * 1024
_VIRTUALENV_PTH_NAME = "_virtualenv.pth"
_VIRTUALENV_PTH_LINE = "import _virtualenv"
_SETUPTOOLS_PTH_NAME = "distutils-precedence.pth"
_SETUPTOOLS_PTH_LINE = (
    "import os; var = 'SETUPTOOLS_USE_DISTUTILS'; enabled = "
    "os.environ.get(var, 'local') == 'local'; enabled and "
    "__import__('_distutils_hack').add_shim(); "
)
_CUSTOMIZER_NAMES = ("sitecustomize", "usercustomize")
_IMPORT_SUFFIXES = tuple(importlib.machinery.all_suffixes())


def startup_import_surface_replayable() -> bool:
    """Accept only standard, inspectable startup import machinery."""

    try:
        if type(sys.modules) is not dict:
            return False
        if {"sitecustomize", "usercustomize"}.intersection(sys.modules):
            return False
        if not _builtin_import_is_standard():
            return False
        meta_path = bounded_list(sys.meta_path, _MAX_RUNTIME_IMPORT_ENTRIES)
        path_hooks = bounded_list(sys.path_hooks, _MAX_RUNTIME_IMPORT_ENTRIES)
        importer_items = bounded_dict_items(sys.path_importer_cache, _MAX_RUNTIME_IMPORT_ENTRIES)
        importer_cache = None if importer_items is None else tuple(value for _, value in importer_items)
        if any(value is None for value in (meta_path, path_hooks, importer_cache)):
            return False
        assert meta_path is not None and path_hooks is not None and importer_cache is not None
        if not all(_meta_path_entry_is_supported(entry) for entry in meta_path):
            return False
        if not all(_path_hook_is_standard(hook) for hook in path_hooks):
            return False
        if not all(_importer_is_standard(finder) for finder in importer_cache):
            return False
        if bool(getattr(sys.flags, "no_site", 0)):
            return True
        if not _customizers_absent():
            return False
        return _site_files_are_replayable(_automatic_site_directories())
    except BaseException:
        return False


def _builtin_import_is_standard() -> bool:
    importer = builtins.__import__
    return (
        isinstance(importer, types.BuiltinFunctionType)
        and getattr(importer, "__module__", None) == "builtins"
        and getattr(importer, "__name__", None) == "__import__"
    )


def _meta_path_entry_is_supported(entry: object) -> bool:
    if any(
        entry is standard
        for standard in (
            importlib.machinery.BuiltinImporter,
            importlib.machinery.FrozenImporter,
            importlib.machinery.PathFinder,
        )
    ):
        return True
    if _setuptools_distutils_finder_is_exact(entry):
        return True
    entry_type = type(entry)
    virtualenv_module = sys.modules.get("_virtualenv")
    virtualenv_finder_type = None if virtualenv_module is None else getattr(virtualenv_module, "_Finder", None)
    return isinstance(virtualenv_finder_type, type) and entry_type is virtualenv_finder_type


def _setuptools_distutils_finder() -> object | None:
    module = sys.modules.get("_distutils_hack")
    return None if module is None else getattr(module, "DISTUTILS_FINDER", None)


def _setuptools_distutils_finder_active() -> bool:
    finder = _setuptools_distutils_finder()
    entries = bounded_list(sys.meta_path, _MAX_RUNTIME_IMPORT_ENTRIES)
    return (
        finder is not None
        and entries is not None
        and _setuptools_distutils_finder_is_exact(finder)
        and any(entry is finder for entry in entries)
    )


def _setuptools_distutils_finder_is_exact(entry: object) -> bool:
    finder = _setuptools_distutils_finder()
    entry_type = type(entry)
    return (
        entry is finder
        and entry_type.__module__ == "_distutils_hack"
        and entry_type.__qualname__ == "DistutilsMetaFinder"
    )


def _path_hook_is_standard(hook: object) -> bool:
    if hook is zipimport.zipimporter:
        return True
    return (
        isinstance(hook, types.FunctionType)
        and hook.__module__ == "_frozen_importlib_external"
        and hook.__qualname__ == "FileFinder.path_hook.<locals>.path_hook_for_FileFinder"
    )


def _importer_is_standard(finder: object) -> bool:
    if finder is None or isinstance(finder, zipimport.zipimporter):
        return True
    finder_type = type(finder)
    return finder_type.__module__ == "_frozen_importlib_external" and finder_type.__qualname__ == "FileFinder"


def _automatic_site_directories() -> tuple[str, ...]:
    import_path = bounded_list(sys.path, _MAX_RUNTIME_IMPORT_ENTRIES)
    if import_path is None or any(type(entry) is not str for entry in import_path):
        raise ValueError
    active_prefixes = _bounded_prefixes((sys.prefix, sys.exec_prefix))
    base_prefixes = _bounded_prefixes((sys.base_prefix, sys.base_exec_prefix))
    active_sites = _site_packages_for_prefixes(active_prefixes)
    base_sites = _site_packages_for_prefixes(base_prefixes)
    raw = list(active_sites)
    raw.extend(path for path in base_sites if path in import_path)
    if not bool(getattr(sys.flags, "no_user_site", 0)):
        raw.extend(_user_site_candidates())
    directories: list[str] = []
    for value in raw:
        if type(value) is not str or len(value) > _MAX_PATH_CHARACTERS or "\x00" in value:
            raise ValueError
        if value not in directories:
            directories.append(value)
            if len(directories) > _MAX_SITE_DIRECTORIES:
                raise ValueError
    return tuple(directories)


def _bounded_prefixes(values: tuple[object, ...]) -> tuple[str, ...]:
    prefixes: list[str] = []
    for value in values:
        if type(value) is not str or len(value) > _MAX_PATH_CHARACTERS or "\x00" in value:
            raise ValueError
        if value and value not in prefixes:
            prefixes.append(value)
    return tuple(prefixes)


def _site_packages_for_prefixes(prefixes: tuple[str, ...]) -> tuple[str, ...]:
    getter = site.getsitepackages
    if (
        not isinstance(getter, types.FunctionType)
        or getter.__module__ != "site"
        or getter.__qualname__ != "getsitepackages"
    ):
        raise ValueError
    discovered = bounded_list(getter(prefixes=prefixes), _MAX_SITE_DIRECTORIES)
    if discovered is None or any(type(value) is not str for value in discovered):
        raise ValueError
    return tuple(value for value in discovered if type(value) is str)


def _user_site_candidates() -> tuple[str, ...]:
    getter = site.getusersitepackages
    if (
        not isinstance(getter, types.FunctionType)
        or getter.__module__ != "site"
        or getter.__qualname__ != "getusersitepackages"
    ):
        raise ValueError
    raw: list[object] = [getattr(site, "USER_SITE", None)]
    raw.append(getter())
    candidates: list[str] = []
    for value in raw:
        if isinstance(value, list):
            discovered = bounded_list(value, _MAX_SITE_DIRECTORIES)
            if discovered is None:
                raise ValueError
            values = discovered
        else:
            values = (value,)
        for candidate in values:
            if candidate is None:
                continue
            if type(candidate) is not str or len(candidate) > _MAX_PATH_CHARACTERS or "\x00" in candidate:
                raise ValueError
            if candidate not in candidates:
                candidates.append(candidate)
                if len(candidates) > _MAX_SITE_DIRECTORIES:
                    raise ValueError
    return tuple(candidates)


def _site_files_are_replayable(directories: Iterable[str]) -> bool:
    seen = 0
    scanned = 0
    for directory in directories:
        try:
            with os.scandir(directory) as iterator:
                entries: list[os.DirEntry[str]] = []
                for entry in iterator:
                    scanned += 1
                    if scanned > _MAX_SITE_SCAN_ENTRIES:
                        return False
                    if not entry.name.endswith(".pth"):
                        continue
                    entries.append(entry)
                    if seen + len(entries) > _MAX_PTH_FILES:
                        return False
        except FileNotFoundError:
            continue
        except OSError:
            return False
        entries.sort(key=lambda entry: entry.name)
        for entry in entries:
            seen += 1
            if not _pth_file_is_replayable(entry):
                return False
    return True


def _customizers_absent() -> bool:
    paths = bounded_list(sys.path, _MAX_RUNTIME_IMPORT_ENTRIES)
    if paths is None or any(type(path) is not str for path in paths):
        return False
    return _paths_have_no_customizer(
        tuple(path for path in paths if type(path) is str),
        _CUSTOMIZER_NAMES,
    )


def _paths_have_no_customizer(paths: tuple[str, ...], names: tuple[str, ...]) -> bool:
    scanned = 0
    zip_bytes = 0
    for raw_path in paths:
        if len(raw_path) > _MAX_PATH_CHARACTERS or "\x00" in raw_path:
            return False
        path = raw_path or os.getcwd()
        try:
            metadata = os.stat(path)
        except FileNotFoundError:
            try:
                os.lstat(path)
            except FileNotFoundError:
                continue
            except OSError:
                return False
            return False
        except OSError:
            return False
        if stat.S_ISDIR(metadata.st_mode):
            try:
                with os.scandir(path) as entries:
                    for entry in entries:
                        scanned += 1
                        if scanned > _MAX_SITE_SCAN_ENTRIES or _is_customizer_name(entry.name, names):
                            return False
            except OSError:
                return False
            continue
        if not stat.S_ISREG(metadata.st_mode):
            return False
        zip_bytes += metadata.st_size
        if metadata.st_size > _MAX_CUSTOMIZER_ZIP_BYTES or zip_bytes > _MAX_CUSTOMIZER_ZIP_BYTES:
            return False
        try:
            importer = zipimport.zipimporter(path)
        except zipimport.ZipImportError:
            continue
        try:
            if any(importer.find_spec(name) is not None for name in names):
                return False
        except BaseException:
            return False
    return True


def _is_customizer_name(filename: str, names: tuple[str, ...]) -> bool:
    case_insensitive = os.name == "nt" or "PYTHONCASEOK" in os.environ
    normalize = str.casefold if case_insensitive else str
    candidate = normalize(filename)
    return any(
        candidate == normalize(name) or any(candidate == normalize(name + suffix) for suffix in _IMPORT_SUFFIXES)
        for name in names
    )


def _pth_file_is_replayable(entry: os.DirEntry[str]) -> bool:
    flags = os.O_RDONLY
    for name in ("O_NONBLOCK", "O_CLOEXEC", "O_BINARY"):
        flags |= int(getattr(os, name, 0))
    try:
        descriptor = os.open(entry.path, flags)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_PTH_BYTES:
                return False
            with os.fdopen(descriptor, "rb") as stream:
                descriptor = -1
                raw = stream.read(_MAX_PTH_BYTES + 1)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
    except OSError:
        return False
    if len(raw) > _MAX_PTH_BYTES:
        return False
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        return False
    executable = [line for line in lines if line.startswith(("import ", "import\t"))]
    if not executable:
        return True
    return (entry.name, tuple(executable)) in {
        (_VIRTUALENV_PTH_NAME, (_VIRTUALENV_PTH_LINE,)),
        (_SETUPTOOLS_PTH_NAME, (_SETUPTOOLS_PTH_LINE,)),
    }


__all__ = ["startup_import_surface_replayable"]
