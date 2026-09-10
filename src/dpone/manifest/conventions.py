"""Built-in conventions / presets for manifests (Step 8).

Why this exists
--------------
Variant C is powerful but can feel "empty" unless users have a convenient way
to bootstrap naming + metadata conventions.

This module introduces *conventions*:
- A convention is a reusable YAML fragment (vars/naming/defaults/validation)
  that is deep-merged into a manifest as *defaults*.
- User-defined values in the manifest always win.

Design goals:
- Universal framework: users can keep writing explicit naming/defaults without
  conventions.
- Good UX for standardization: a single `convention: landing_raw_v1` enables
  a ready-to-use landing/raw setup (naming + required labels/description).
- Extensible without code: any string that is not a known built-in convention
  is treated as a *relative YAML path* to a custom preset file.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.airflow_resources import reject_process_airflow_resources, reject_process_resource_declaration
from dpone.manifest.batch_merge import deep_merge
from dpone.manifest.errors import ManifestConfigurationError

# --------------------------- built-in presets ---------------------------


# Landing/raw naming convention v1 (Experience-Landing - Naming Convention)
# - dataset: landing__{source}__{db}
# - table:   {schema}__{table}
# - labels:  mandatory keys (layer/src/db/schema/host/type/ingest)
# - description: original path + owner/contact/SLA placeholders
_LANDING_RAW_V1_PATCH: dict[str, Any] = {
    # default vars used by templates
    "vars": {
        "layer": "landing",
        "ingest": "dpone",
    },
    # naming templates (users can override any of them)
    "naming": {
        "sink_dataset": "{{ layer|default('landing')|dpone_snake }}__{{ src_system|dpone_snake }}__{{ src_database|dpone_snake }}",
        "sink_table": "{{ src_schema|default('default')|dpone_snake }}__{{ src_table|dpone_snake }}",
        "task_group": "{{ src_system|dpone_snake }}__{{ src_database|dpone_snake }}",
        "process_name": "{{ src_schema|default('default')|dpone_snake }}_{{ src_table|dpone_snake }}__{{ sink.strategy.mode }}",
        "labels": {
            "layer": "{{ layer|default('landing')|dpone_snake }}",
            "src": "{{ src_system|dpone_snake }}",
            "db": "{{ src_database|dpone_snake }}",
            "schema": "{{ src_schema|default('default')|dpone_snake }}",
            # host is required by the standard, but is rarely present in legacy manifests.
            # Prefer explicit vars.host; otherwise fall back to 'unknown'.
            "host": "{{ host|default('unknown')|dpone_snake }}",
            # Prefer explicit vars.type (e.g. from source registry); fallback to source.type.
            "type": "{{ type|default(source.type|default('unknown'))|dpone_snake }}",
            "ingest": "{{ ingest|default('dpone')|dpone_snake }}",
            # Optional labels
            "pii": "{{ pii|default(None) }}",
            "gdpr": "{{ gdpr|default(None) }}",
        },
        "sink_table_description": """Источник: {{ host|default('unknown') }}/{{ src_database }}/{{ source.table.schema|default(src_schema|default('default')) }}/{{ source.table.name|default(src_table) }}
Владелец: {{ owner_team|default('unknown') }}
Контакт: {{ owner_contact|default('unknown') }}
SLA: {{ sla|default('unknown') }}""",
    },
    # Ensure metadata is applied not only on CREATE TABLE but also for existing tables.
    "defaults": {
        "sink": {
            "options": {
                "apply_table_metadata": True,
                # Prefer tri-state UX over boolean:
                # required | optional | forbidden
                "technical_columns": "required",
            }
        }
    },
    # Convention-level validation (so `dpone manifest validate` works without extra flags)
    "validation": {
        "dataset_pattern": r"^landing__[a-z][a-z0-9_]*__[a-z][a-z0-9_]*$",
        "table_pattern": r"^[a-z][a-z0-9_]*__[a-z][a-z0-9_]*$",
        "required_labels": ["layer", "src", "db", "schema", "host", "type", "ingest"],
        "require_table_description": True,
        # New UX: tri-state policy instead of boolean
        "technical_columns": "required",
        "technical_columns_severity": "ERROR",
        "missing_labels_severity": "ERROR",
        "missing_description_severity": "ERROR",
        # Additional strictness: forbid placeholder values for mandatory labels
        "invalid_label_value_severity": "ERROR",
        "forbidden_label_values": {
            "host": ["unknown", "tbd", "todo", "unset", "none", "null", "n_a", "na"],
            "type": ["unknown", "tbd", "todo", "unset", "none", "null", "n_a", "na"],
        },
        "label_value_patterns": {
            "layer": r"^[a-z0-9_]+$",
            "src": r"^[a-z0-9_]+$",
            "db": r"^[a-z0-9_]+$",
            "schema": r"^[a-z0-9_]+$",
            "host": r"^[a-z0-9_]+$",
            "type": r"^[a-z0-9_]+$",
            "ingest": r"^[a-z0-9_]+$",
        },
        # Description requirements (metadata must be meaningful, not placeholders)
        # See: "Experience-Landing - Naming Convention" (section "Метаданные")
        "description_required_regex": [
            r"(?im)^Источник:\s*.+$",
            r"(?im)^Владелец:\s*.+$",
            r"(?im)^Контакт:\s*.+$",
            r"(?im)^SLA:\s*.+$",
        ],
        "description_forbidden_regex": [
            r"(?im)^Источник:\s*(unknown|tbd|todo|unset|none|null|n_a|na)(/|\s|$)",
            r"(?im)^Владелец:\s*(unknown|tbd|todo|unset|none|null|n_a|na)\s*$",
            r"(?im)^Контакт:\s*(unknown|tbd|todo|unset|none|null|n_a|na)\s*$",
            r"(?im)^SLA:\s*(unknown|tbd|todo|unset|none|null|n_a|na)\s*$",
        ],
        "require_source_path_in_description": True,
        "invalid_description_severity": "ERROR",
    },
}


def _builtin_patch(name: str) -> dict[str, Any] | None:
    normalized = (name or "").strip().lower()
    if normalized in {"landing", "landing_raw", "landing_raw_v1"}:
        return dict(_LANDING_RAW_V1_PATCH)
    return None


# --------------------------- public API ---------------------------


def apply_conventions(raw: Mapping[str, Any], *, manifest_path: Path) -> dict[str, Any]:
    """Applies conventions to a raw manifest.

    Supported fields (both are optional):
    - convention: <string>
    - conventions: [<string>, ...]

    Each string is either:
    - a built-in convention name (e.g. landing_raw_v1)
    - a relative YAML file path to a custom preset

    Merge semantics:
    - convention patches are merged first (in listed order)
    - then the user manifest is merged on top (user wins)
    """

    convs = list(_extract_conventions(raw))
    if not convs:
        return dict(raw)

    patch: dict[str, Any] = {}
    for c in convs:
        layer = _load_patch(c, manifest_path=manifest_path)
        reject_process_resource_declaration(layer, field=f"conventions[{c}]")
        reject_process_airflow_resources(layer, field=f"conventions[{c}]")
        patch = deep_merge(patch, layer)

    # user manifest overrides convention defaults
    return deep_merge(patch, dict(raw))


def _extract_conventions(raw: Mapping[str, Any]) -> Iterable[str]:
    conv = raw.get("convention")
    if isinstance(conv, str) and conv.strip():
        yield conv.strip()

    convs = raw.get("conventions")
    if isinstance(convs, Sequence) and not isinstance(convs, str | bytes):
        for item in convs:
            if isinstance(item, str) and item.strip():
                yield item.strip()


def _load_patch(name_or_path: str, *, manifest_path: Path) -> dict[str, Any]:
    # 1) built-in
    builtin = _builtin_patch(name_or_path)
    if builtin is not None:
        return builtin

    # 2) custom preset file (relative to manifest)
    preset_path = (manifest_path.parent / name_or_path).resolve()
    if not preset_path.exists():
        raise ManifestConfigurationError(
            f"Неизвестная convention '{name_or_path}'. "
            f"Ожидается built-in (landing_raw_v1) или путь к YAML preset (относительно {manifest_path.parent})."
        )

    try:
        data = yaml.safe_load(preset_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise ManifestConfigurationError(f"Ошибка чтения convention preset {preset_path}: {exc}") from exc

    if not isinstance(data, Mapping):
        raise ManifestConfigurationError(f"Convention preset должен быть YAML объектом (dict): {preset_path}")

    return dict(data)


# --------------------------- explain helpers (Step 14) ---------------------------


@dataclass(frozen=True, slots=True)
class ConventionLayer:
    """A single resolved convention layer.

    Used by `dpone manifest explain` to show provenance.
    """

    name: str
    source: str  # 'builtin' or a resolved preset file path
    patch: dict[str, Any]


def resolve_convention_layers(raw: Mapping[str, Any], *, manifest_path: Path) -> list[ConventionLayer]:
    """Resolve convention(s) referenced by a manifest.

    Returns layers in the same order as they are applied.
    """

    layers: list[ConventionLayer] = []
    for c in _extract_conventions(raw):
        builtin = _builtin_patch(c)
        if builtin is not None:
            layers.append(ConventionLayer(name=c, source="builtin", patch=builtin))
            continue

        preset_path = (manifest_path.parent / c).resolve()
        if not preset_path.exists():
            raise ManifestConfigurationError(
                f"Неизвестная convention '{c}'. Ожидается built-in (landing_raw_v1) "
                f"или путь к YAML preset (относительно {manifest_path.parent})."
            )

        try:
            data = yaml.safe_load(preset_path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            raise ManifestConfigurationError(f"Ошибка чтения convention preset {preset_path}: {exc}") from exc

        if not isinstance(data, Mapping):
            raise ManifestConfigurationError(f"Convention preset должен быть YAML объектом (dict): {preset_path}")

        layers.append(ConventionLayer(name=c, source=str(preset_path), patch=dict(data)))

    return layers
