"""Source registry support (Step 9).

Why this exists
---------------
Some naming/metadata conventions (e.g. landing/raw) require additional source
attributes such as {host} and (optionally) {type}. Writing these variables in
every batch manifest is noisy.

This module introduces *registries*:
- A registry is a YAML file that maps (src_system, src_database) to a set of
  variables (host/type/...) that will be merged into manifest vars as defaults.
- User-defined vars in the manifest always win.

Registry can be provided:
- via manifest fields `registry` / `registries` (relative to the manifest file)
- via loader/CLI extra paths (e.g. --registry sources.yaml)
- via env var DPONE_SOURCES_REGISTRY (see dpone.config.env)

The format is intentionally simple and universal.

Supported YAML formats
----------------------

1) Flat `entries` list (recommended):

    version: 1
    entries:
      - src_system: monolite
        src_database: example_db
        host: 203.0.113.10
        type: postgres

2) A plain list (alias of entries):

    - src_system: monolite
      src_database: example_db
      host: 203.0.113.10
      type: postgres

3) A `sources` table-like structure (best-effort):

    version: 1
    sources:
      - code: monolite
        type: postgres
        host: 203.0.113.10
        db: [example_db]

All entry fields except src_system/src_database are treated as vars.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.batch_merge import deep_merge
from dpone.manifest.errors import ManifestConfigurationError


@dataclass(frozen=True, slots=True)
class RegistryEntry:
    src_system: str
    src_database: str  # exact or '*'
    vars: dict[str, Any]

    def key(self) -> tuple[str, str]:
        return (self.src_system.lower(), self.src_database.lower())


class SourceRegistry:
    """A lookup table for (src_system, src_database) -> vars."""

    def __init__(self, entries: Sequence[RegistryEntry]) -> None:
        # We keep an ordered mapping: later entries override earlier ones.
        merged: dict[tuple[str, str], dict[str, Any]] = {}
        for e in entries:
            merged[e.key()] = dict(e.vars)
        self._map = merged

    def lookup(self, *, src_system: str, src_database: str) -> dict[str, Any] | None:
        key_exact = (str(src_system).lower(), str(src_database).lower())
        if key_exact in self._map:
            return dict(self._map[key_exact])

        key_wild = (str(src_system).lower(), "*")
        if key_wild in self._map:
            return dict(self._map[key_wild])

        return None

    def available_keys(self) -> Sequence[str]:
        return [f"{k[0]}::{k[1]}" for k in sorted(self._map.keys())]


def apply_registries(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    extra_registry_paths: Sequence[Path] = (),
) -> dict[str, Any]:
    """Applies source registries to a raw manifest.

    Merge semantics:
    - registry-derived vars are merged first
    - then manifest vars override them (user wins)

    Registry paths are resolved in this order:
    1) extra_registry_paths (e.g. from CLI/env)
    2) manifest `registry` / `registries` (relative to manifest dir)
    If the same key appears in multiple registries, the later one wins.
    """

    # Collect registry paths
    reg_paths: list[Path] = [Path(p) for p in extra_registry_paths if str(p)]
    reg_paths.extend(_extract_manifest_registry_paths(raw, manifest_path=manifest_path))
    reg_paths = [p for p in reg_paths if p]

    if not reg_paths:
        return dict(raw)

    vars_block = raw.get("vars") or {}
    if not isinstance(vars_block, Mapping):
        raise ManifestConfigurationError(f"vars должен быть объектом, чтобы применить registry ({manifest_path})")

    src_system = vars_block.get("src_system")
    src_database = vars_block.get("src_database")
    if not isinstance(src_system, str) or not src_system.strip():
        raise ManifestConfigurationError(
            f"Чтобы применить registry, необходимо задать vars.src_system в {manifest_path}"
        )
    if not isinstance(src_database, str) or not src_database.strip():
        raise ManifestConfigurationError(
            f"Чтобы применить registry, необходимо задать vars.src_database в {manifest_path}"
        )

    registry = _load_registries(reg_paths)
    reg_vars = registry.lookup(src_system=src_system.strip(), src_database=src_database.strip())
    if reg_vars is None:
        keys = ", ".join(registry.available_keys())
        raise ManifestConfigurationError(
            f"Registry не содержит запись для src_system='{src_system}', src_database='{src_database}' ({manifest_path}). "
            f"Доступные ключи: {keys or '—'}"
        )

    # Merge registry vars as defaults; manifest vars win.
    new_vars = deep_merge(reg_vars, dict(vars_block))
    out = dict(raw)
    out["vars"] = new_vars
    return out


def _extract_manifest_registry_paths(raw: Mapping[str, Any], *, manifest_path: Path) -> Iterable[Path]:
    reg = raw.get("registry")
    if isinstance(reg, str) and reg.strip():
        yield (manifest_path.parent / reg.strip()).resolve()

    regs = raw.get("registries")
    if isinstance(regs, Sequence) and not isinstance(regs, str | bytes):
        for item in regs:
            if isinstance(item, str) and item.strip():
                yield (manifest_path.parent / item.strip()).resolve()


# --------------------------- explain helpers (Step 14) ---------------------------


@dataclass(frozen=True, slots=True)
class RegistryResolution:
    """Resolved registry entry used for (src_system, src_database).

    Used by `dpone manifest explain`.
    """

    registry_paths: tuple[Path, ...]
    src_system: str
    src_database: str
    matched_key: tuple[str, str]
    entry_source: Path
    vars: dict[str, Any]


def resolve_registry(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    extra_registry_paths: Sequence[Path] = (),
) -> RegistryResolution | None:
    """Resolve the effective registry entry for the manifest vars.

    Semantics match apply_registries():
    - registry paths are collected as: extra paths, then manifest registry/registries
    - later entries override earlier ones
    - exact (src_system, src_database) wins over wildcard (src_system, '*')
    """

    reg_paths: list[Path] = [Path(p) for p in extra_registry_paths if str(p)]
    reg_paths.extend(_extract_manifest_registry_paths(raw, manifest_path=manifest_path))
    reg_paths = [p for p in reg_paths if p]
    if not reg_paths:
        return None

    vars_block = raw.get("vars") or {}
    if not isinstance(vars_block, Mapping):
        raise ManifestConfigurationError(f"vars должен быть объектом, чтобы применить registry ({manifest_path})")

    src_system = vars_block.get("src_system")
    src_database = vars_block.get("src_database")
    if not isinstance(src_system, str) or not src_system.strip():
        raise ManifestConfigurationError(
            f"Чтобы применить registry, необходимо задать vars.src_system в {manifest_path}"
        )
    if not isinstance(src_database, str) or not src_database.strip():
        raise ManifestConfigurationError(
            f"Чтобы применить registry, необходимо задать vars.src_database в {manifest_path}"
        )

    sys_key = src_system.strip().lower()
    db_key = src_database.strip().lower()

    last_exact: tuple[Path, RegistryEntry] | None = None
    last_wild: tuple[Path, RegistryEntry] | None = None

    for p in reg_paths:
        entries = _load_registry_file(p)
        for e in entries:
            k = e.key()
            if k[0] != sys_key:
                continue
            if k[1] == db_key:
                last_exact = (p, e)
            elif k[1] == "*":
                last_wild = (p, e)

    if last_exact is not None:
        p, e = last_exact
        return RegistryResolution(
            registry_paths=tuple(reg_paths),
            src_system=src_system.strip(),
            src_database=src_database.strip(),
            matched_key=(e.src_system, e.src_database),
            entry_source=p,
            vars=dict(e.vars),
        )

    if last_wild is not None:
        p, e = last_wild
        return RegistryResolution(
            registry_paths=tuple(reg_paths),
            src_system=src_system.strip(),
            src_database=src_database.strip(),
            matched_key=(e.src_system, e.src_database),
            entry_source=p,
            vars=dict(e.vars),
        )

    return None


def load_registries(paths: Sequence[Path]) -> SourceRegistry:
    """Loads and merges registry files.

    Public helper used by lint/CLI.
    Later entries override earlier ones.
    """

    return _load_registries(paths)


def _load_registries(paths: Sequence[Path]) -> SourceRegistry:
    entries: list[RegistryEntry] = []
    for p in paths:
        entries.extend(_load_registry_file(p))
    return SourceRegistry(entries)


def _load_registry_file(path: Path) -> Sequence[RegistryEntry]:
    if not path.exists():
        raise ManifestConfigurationError(f"Registry файл не найден: {path}")

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ManifestConfigurationError(f"Ошибка чтения registry {path}: {exc}") from exc

    if data is None:
        return []

    # Format A: {entries: [...]}
    if isinstance(data, Mapping) and "entries" in data:
        entries_raw = data.get("entries")
        if not isinstance(entries_raw, list):
            raise ManifestConfigurationError(f"registry.entries должен быть массивом: {path}")
        return [_parse_entry(e, path) for e in entries_raw]

    # Format B: {sources: [...]}
    if isinstance(data, Mapping) and "sources" in data:
        sources_raw = data.get("sources")
        if not isinstance(sources_raw, list):
            raise ManifestConfigurationError(f"registry.sources должен быть массивом: {path}")
        out: list[RegistryEntry] = []
        for src in sources_raw:
            out.extend(_parse_source_row(src, path))
        return out

    # Format C: a plain list
    if isinstance(data, list):
        return [_parse_entry(e, path) for e in data]

    raise ManifestConfigurationError(
        f"Неподдерживаемый формат registry {path}. Ожидается dict(entries|sources) или list"
    )


def _parse_source_row(row: Any, path: Path) -> Sequence[RegistryEntry]:
    if not isinstance(row, Mapping):
        raise ManifestConfigurationError(f"registry.sources элементы должны быть объектами: {path}")

    src_system = row.get("src_system") or row.get("code") or row.get("source")
    if not isinstance(src_system, str) or not src_system.strip():
        raise ManifestConfigurationError(f"registry.sources: отсутствует code/src_system: {path}")

    host = row.get("host")
    typ = row.get("type") or row.get("src_type")

    dbs = row.get("src_database") or row.get("db") or row.get("database") or row.get("databases")
    db_list: list[str] = []
    if isinstance(dbs, str) and dbs.strip():
        db_list = [dbs.strip()]
    elif isinstance(dbs, list):
        for item in dbs:
            if isinstance(item, str) and item.strip():
                db_list.append(item.strip())
            elif isinstance(item, Mapping):
                name = item.get("name") or item.get("db") or item.get("src_database")
                if isinstance(name, str) and name.strip():
                    db_list.append(name.strip())
    elif dbs is None:
        # Allow wildcard if db is not specified.
        db_list = ["*"]

    if not db_list:
        raise ManifestConfigurationError(f"registry.sources: не удалось определить db/src_database: {path}")

    out: list[RegistryEntry] = []
    for db in db_list:
        vars_dict: dict[str, Any] = {}
        if host is not None:
            vars_dict["host"] = host
        if typ is not None:
            vars_dict["type"] = typ
        # keep any additional fields under vars
        extra = {
            k: v
            for k, v in row.items()
            if k
            not in {
                "src_system",
                "code",
                "source",
                "host",
                "type",
                "src_type",
                "db",
                "database",
                "databases",
                "src_database",
            }
        }
        if extra:
            vars_dict = deep_merge(vars_dict, extra)
        out.append(RegistryEntry(src_system=str(src_system).strip(), src_database=str(db).strip(), vars=vars_dict))
    return out


def _parse_entry(entry: Any, path: Path) -> RegistryEntry:
    if not isinstance(entry, Mapping):
        raise ManifestConfigurationError(f"registry entries должны быть объектами: {path}")

    src_system = entry.get("src_system") or entry.get("code") or entry.get("source")
    src_database = entry.get("src_database") or entry.get("db") or entry.get("database")
    if not isinstance(src_system, str) or not src_system.strip():
        raise ManifestConfigurationError(f"registry entry: отсутствует src_system/code: {path}")
    if not isinstance(src_database, str) or not str(src_database).strip():
        raise ManifestConfigurationError(f"registry entry: отсутствует src_database/db: {path}")

    src_system = src_system.strip()
    src_database = str(src_database).strip()

    vars_dict: dict[str, Any] = {}

    # if entry has a nested vars block - merge it
    nested_vars = entry.get("vars")
    if nested_vars is not None:
        if not isinstance(nested_vars, Mapping):
            raise ManifestConfigurationError(f"registry entry.vars должен быть объектом: {path}")
        vars_dict = deep_merge(vars_dict, dict(nested_vars))

    # convenience flat fields -> vars
    for k in ("host", "type"):
        if k in entry and k not in vars_dict:
            vars_dict[k] = entry.get(k)

    # any other fields (except identifiers) are also treated as vars defaults
    extra: dict[str, Any] = {
        k: v
        for k, v in entry.items()
        if k not in {"src_system", "code", "source", "src_database", "db", "database", "vars"}
        and k not in {"host", "type"}
    }
    if extra:
        vars_dict = deep_merge(vars_dict, extra)

    return RegistryEntry(src_system=src_system, src_database=src_database, vars=vars_dict)
