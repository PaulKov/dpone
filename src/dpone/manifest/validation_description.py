from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.models import ProcessSpec
    from dpone.manifest.validation_models import Severity


import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from dpone.manifest.validation_models import ValidationIssue


def _validate_description_source_path(
    spec: ProcessSpec,
    *,
    manifest_path: Path,
    selector: str,
    description: str,
    labels: Mapping[str, Any],
    severity: Severity,
) -> Iterable[ValidationIssue]:
    """Validates that description contains original source path.

    The landing naming convention requires description to contain:
      {host}/{db}/{schema}/{table_original}

    We validate this by checking the 'Источник:' line contains a host segment
    followed by expected db/schema/table values.

    Notes:
    - host may contain dots/hyphens; we do not enforce its exact value here.
    - db is taken from labels['db'] if available.
    - schema/table_original are taken from raw_config.source.table.* where possible.
    """

    raw = spec.raw_config or {}
    src_table_schema = None
    src_table_name = None
    try:
        src = raw.get("source") or {}
        tbl = (src.get("table") or {}) if isinstance(src, Mapping) else {}
        if isinstance(tbl, Mapping):
            if isinstance(tbl.get("schema"), str) and tbl.get("schema"):
                src_table_schema = str(tbl.get("schema"))
            if isinstance(tbl.get("name"), str) and tbl.get("name"):
                src_table_name = str(tbl.get("name"))
    except Exception:
        # best-effort only
        pass

    db = labels.get("db")
    if isinstance(db, str) and db.strip():
        db_str = db.strip()
    else:
        # Try to infer db from dataset name: landing__{src}__{db}
        lc = getattr(spec.config, "load_config", None)
        dataset = getattr(lc, "target_schema", None) if lc else None
        db_str = ""
        if isinstance(dataset, str) and dataset.count("__") >= 2:
            parts = dataset.split("__")
            # landing__src__db
            if len(parts) >= 3:
                db_str = parts[2]

    schema = src_table_schema or labels.get("schema")
    schema_str = str(schema).strip() if schema is not None else ""

    table = src_table_name
    if not table:
        # Fallback to target table second segment in "schema__table" if possible
        lc = getattr(spec.config, "load_config", None)
        target_table = getattr(lc, "target_table", None) if lc else None
        if isinstance(target_table, str) and "__" in target_table:
            table = target_table.split("__", 1)[1]
        elif isinstance(target_table, str):
            table = target_table

    if not (db_str and schema_str and table):
        return [
            ValidationIssue(
                severity=severity,
                code="META_DESCRIPTION_SOURCE_PATH_UNDETERMINED",
                message="cannot validate source path in description: missing db/schema/table context",
                manifest_path=manifest_path,
                selector=selector,
            )
        ]

    # Build strict regex for the source path line.
    # Example: "Источник: 203.0.113.10/example_db/public/core_events"
    db_re = re.escape(db_str)
    schema_re = re.escape(schema_str)
    table_re = re.escape(str(table))

    rx = re.compile(
        rf"(?im)^Источник:\s*[^/\s]+/{db_re}/{schema_re}/{table_re}\s*$",
        flags=re.MULTILINE,
    )

    if not rx.search(description):
        return [
            ValidationIssue(
                severity=severity,
                code="META_DESCRIPTION_SOURCE_PATH",
                message=(
                    "table description must contain original source path: "
                    f"'{{host}}/{db_str}/{schema_str}/{table}' (line starting with 'Источник:')"
                ),
                manifest_path=manifest_path,
                selector=selector,
            )
        ]

    return []
