"""Self-contained runtime-attestation fragments for the import probe child."""

from __future__ import annotations

IMMUTABLE_SYS_FLAG_NAMES = (
    "debug",
    "inspect",
    "interactive",
    "optimize",
    "dont_write_bytecode",
    "no_user_site",
    "no_site",
    "ignore_environment",
    "verbose",
    "bytes_warning",
    "quiet",
    "hash_randomization",
    "isolated",
    "dev_mode",
    "utf8_mode",
    "warn_default_encoding",
    "safe_path",
    "int_max_str_digits",
)

RUNTIME_ATTESTATION_IMPORTS = """\
import _imp
import _io
import builtins
import faulthandler
import importlib.machinery
import io
import stat
import tracemalloc
import types
import zipimport
from itertools import islice
"""

RUNTIME_ATTESTATION_HELPERS = """\
def canonical_unbuffered_stdio():
    streams = (sys.__stdin__, sys.__stdout__, sys.__stderr__)
    if not all(type(stream) is io.TextIOWrapper for stream in streams):
        return None
    write_through = tuple(stream.write_through for stream in streams)
    if len(set(write_through)) != 1 or type(sys.__stdin__.buffer) is not io.BufferedReader:
        return None
    if write_through[0]:
        output_types = {io.FileIO}
        console_type = getattr(_io, "_WindowsConsoleIO", None)
        if isinstance(console_type, type):
            output_types.add(console_type)
        if any(type(stream.buffer) not in output_types for stream in (sys.__stdout__, sys.__stderr__)):
            return None
    elif any(type(stream.buffer) is not io.BufferedWriter for stream in (sys.__stdout__, sys.__stderr__)):
        return None
    return write_through[0]

def no_debug_ranges_enabled():
    positions_reader = getattr(no_debug_ranges_enabled.__code__, "co_positions", None)
    if not callable(positions_reader):
        return False
    positions = tuple(positions_reader())
    return bool(positions) and all(left is None and right is None for _, _, left, right in positions)

def runtime_int_max_str_digits():
    getter = getattr(sys, "get_int_max_str_digits", None)
    if not callable(getter):
        return None
    try:
        return getter()
    except BaseException:
        return None

def meta_path_entry_is_supported(entry):
    if any(entry is standard for standard in (
        importlib.machinery.BuiltinImporter,
        importlib.machinery.FrozenImporter,
        importlib.machinery.PathFinder,
    )):
        return True
    if setuptools_distutils_finder_is_exact(entry):
        return True
    entry_type = type(entry)
    virtualenv_module = sys.modules.get("_virtualenv")
    virtualenv_finder_type = None if virtualenv_module is None else getattr(virtualenv_module, "_Finder", None)
    return isinstance(virtualenv_finder_type, type) and entry_type is virtualenv_finder_type

def setuptools_distutils_finder():
    module = sys.modules.get("_distutils_hack")
    return None if module is None else getattr(module, "DISTUTILS_FINDER", None)

def setuptools_distutils_finder_active():
    finder = setuptools_distutils_finder()
    entries = bounded_list(sys.meta_path)
    return finder is not None and entries is not None and setuptools_distutils_finder_is_exact(finder) and any(
        entry is finder for entry in entries
    )

def setuptools_distutils_finder_is_exact(entry):
    finder = setuptools_distutils_finder()
    entry_type = type(entry)
    return (
        entry is finder
        and entry_type.__module__ == "_distutils_hack"
        and entry_type.__qualname__ == "DistutilsMetaFinder"
    )

def path_hook_is_standard(hook):
    return hook is zipimport.zipimporter or (
        isinstance(hook, types.FunctionType)
        and hook.__module__ == "_frozen_importlib_external"
        and hook.__qualname__ == "FileFinder.path_hook.<locals>.path_hook_for_FileFinder"
    )

def importer_is_standard(finder):
    if finder is None or isinstance(finder, zipimport.zipimporter):
        return True
    finder_type = type(finder)
    return finder_type.__module__ == "_frozen_importlib_external" and finder_type.__qualname__ == "FileFinder"

def bounded_list(values):
    if type(values) is not list:
        return None
    try:
        entries = tuple(islice(iter(values), 4097))
    except BaseException:
        return None
    return entries if len(entries) <= 4096 else None

def bounded_dict_values(values):
    if type(values) is not dict:
        return None
    try:
        entries = tuple(islice(iter(values.items()), 4097))
    except BaseException:
        return None
    return tuple(value for _, value in entries) if len(entries) <= 4096 else None

def customizers_absent(paths):
    names = ("sitecustomize", "usercustomize")
    suffixes = tuple(importlib.machinery.all_suffixes())
    case_insensitive = os.name == "nt" or "PYTHONCASEOK" in os.environ
    normalize = str.casefold if case_insensitive else str
    scanned = 0
    zip_bytes = 0
    for raw_path in paths:
        if type(raw_path) is not str or len(raw_path) > 65536 or "\\x00" in raw_path:
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
                        candidate = normalize(entry.name)
                        if scanned > 16384 or any(
                            candidate == normalize(name)
                            or any(candidate == normalize(name + suffix) for suffix in suffixes)
                            for name in names
                        ):
                            return False
            except OSError:
                return False
            continue
        if not stat.S_ISREG(metadata.st_mode):
            return False
        zip_bytes += metadata.st_size
        if metadata.st_size > 16777216 or zip_bytes > 16777216:
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

def startup_surface_is_standard():
    if type(sys.modules) is not dict:
        return False
    importer = builtins.__import__
    meta_path = bounded_list(sys.meta_path)
    path_hooks = bounded_list(sys.path_hooks)
    importer_cache = bounded_dict_values(sys.path_importer_cache)
    paths = bounded_list(sys.path)
    return (
        not {"sitecustomize", "usercustomize"}.intersection(sys.modules)
        and isinstance(importer, types.BuiltinFunctionType)
        and getattr(importer, "__module__", None) == "builtins"
        and getattr(importer, "__name__", None) == "__import__"
        and meta_path is not None
        and path_hooks is not None
        and importer_cache is not None
        and paths is not None
        and all(type(path) is str for path in paths)
        and all(meta_path_entry_is_supported(entry) for entry in meta_path)
        and all(path_hook_is_standard(hook) for hook in path_hooks)
        and all(importer_is_standard(finder) for finder in importer_cache)
        and (bool(sys.flags.no_site) or customizers_absent(paths))
    )
"""

__all__ = ["IMMUTABLE_SYS_FLAG_NAMES", "RUNTIME_ATTESTATION_HELPERS", "RUNTIME_ATTESTATION_IMPORTS"]
