"""Provider-neutral schema contract versioning and compatibility gates."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone as datetime_timezone
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

CONTRACT_VERSION_SCHEMA = "dpone.schema_contract_version.v1"
COMPATIBILITY_PLAN_SCHEMA = "dpone.schema_contract_compatibility_plan.v1"
CONSUMER_BINDING_SCHEMA = "dpone.schema_contract_consumer_binding.v1"
CONTRACT_GATE_SCHEMA = "dpone.schema_contract_gate.v1"
REGISTRY_SCHEMA = "dpone.schema_contract_registry.v1"

_BUMP_ORDER = {"patch": 0, "minor": 1, "major": 2}


@dataclass(frozen=True, slots=True)
class SchemaContractVersionBuilder:
    """Builds canonical logical contract versions from manifest evidence."""

    def build(self, *, manifest: Mapping[str, Any], source_schema: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
        sink = _mapping(manifest.get("sink"))
        options = _mapping(sink.get("options"))
        raw = _mapping(options.get("schema_contract"))
        identity = _mapping(options.get("schema_identity"))
        columns = _columns(raw, identity, source_schema)
        contract_id = str(raw.get("id") or _table_key(sink))
        version = str(raw.get("version") or "1.0.0")
        payload: dict[str, Any] = {
            "schema_version": CONTRACT_VERSION_SCHEMA,
            "contract_id": contract_id,
            "version": version,
            "owner": str(raw.get("owner") or ""),
            "status": "published",
            "compatibility": str(raw.get("compatibility") or "backward"),
            "enforcement": str(raw.get("enforcement") or "strict"),
            "target": {"sink_type": str(sink.get("type") or "unknown"), "table": _table_key(sink)},
            "columns": columns,
            "deprecation": _mapping(raw.get("deprecation")),
            "registry": _registry(raw),
            "versioning": _mapping(raw.get("versioning")),
            "consumers": _consumer_list(raw),
            "created_at": _utc_now(),
        }
        payload["contract_fingerprint"] = stable_fingerprint(
            {
                "contract_id": contract_id,
                "columns": columns,
                "compatibility": payload["compatibility"],
                "enforcement": payload["enforcement"],
                "deprecation": payload["deprecation"],
            }
        )
        payload["contract_version_id"] = stable_fingerprint(
            {"contract_id": contract_id, "version": version, "fingerprint": payload["contract_fingerprint"]}
        )
        return payload


@dataclass(frozen=True, slots=True)
class SchemaContractComparator:
    """Diffs contract versions by stable identity first and column name second."""

    def diff(self, *, base: Mapping[str, Any], head: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
        base_cols = _column_index(base)
        head_cols = _column_index(head)
        changes: list[dict[str, Any]] = []
        for key, base_col in base_cols.items():
            head_col = head_cols.get(key)
            if head_col is None:
                changes.append(_change("column_removed", base_col, None, "major"))
                continue
            changes.extend(_column_changes(base_col, head_col))
        for key, head_col in head_cols.items():
            if key not in base_cols:
                bump = "minor" if bool(head_col.get("nullable", True)) else "major"
                changes.append(_change("column_added", None, head_col, bump))
        return tuple(sorted(changes, key=lambda item: (item["path"], item["change_type"])))

    def compare(self, *, base: Mapping[str, Any], head: Mapping[str, Any], compatibility: str) -> dict[str, Any]:
        return SchemaCompatibilityClassifier().classify(
            base=base,
            head=head,
            changes=self.diff(base=base, head=head),
            compatibility=compatibility,
            semver_mode="advisory",
        )


@dataclass(frozen=True, slots=True)
class SchemaCompatibilityClassifier:
    """Assigns compatibility, risk and semver decisions."""

    def classify(
        self,
        *,
        base: Mapping[str, Any],
        head: Mapping[str, Any],
        changes: Sequence[Mapping[str, Any]],
        compatibility: str,
        semver_mode: str = "strict",
    ) -> dict[str, Any]:
        required = _required_bump(changes)
        blockers = list(_compatibility_blockers(changes, compatibility))
        if semver_mode == "strict" and not _declared_bump_satisfies(
            str(base.get("version")), str(head.get("version")), required
        ):
            blockers.append(f"schema_contract.semver_bump_required:{required}")
        risk_tags = ["schema_contract.compatibility_breaking"] if required == "major" else []
        status = "blocked" if blockers else "breaking" if required == "major" else "compatible"
        payload: dict[str, Any] = {
            "schema_version": COMPATIBILITY_PLAN_SCHEMA,
            "status": status,
            "contract_id": head.get("contract_id"),
            "base_version": base.get("version"),
            "head_version": head.get("version"),
            "base_contract_version_id": base.get("contract_version_id"),
            "head_contract_version_id": head.get("contract_version_id"),
            "compatibility": compatibility,
            "required_bump": required,
            "changes": [dict(item) for item in changes],
            "risk_tags": risk_tags,
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": [],
        }
        payload["compatibility_plan_id"] = stable_fingerprint(payload)
        return payload


@dataclass(frozen=True, slots=True)
class SchemaConsumerCompatibilityGate:
    """Checks declared consumer constraints against one compatibility plan."""

    def evaluate(
        self,
        *,
        contract_version: Mapping[str, Any],
        compatibility_plan: Mapping[str, Any],
        consumers: Sequence[Mapping[str, Any]],
        unknown_consumer: str = "warn",
    ) -> dict[str, Any]:
        blockers: list[str] = []
        warnings: list[str] = []
        removed = {
            str(item.get("column"))
            for item in compatibility_plan.get("changes", [])
            if isinstance(item, Mapping) and item.get("change_type") == "column_removed"
        }
        version = str(contract_version.get("version") or "")
        if not consumers and unknown_consumer in {"warn", "block"}:
            (blockers if unknown_consumer == "block" else warnings).append("schema_contract.unknown_consumers")
        bindings: list[dict[str, Any]] = []
        for consumer in consumers:
            consumer_id = str(consumer.get("id") or "unknown")
            reads = _string_set(_mapping(consumer.get("reads")).get("columns", []))
            consumer_blockers: list[str] = []
            if not _constraint_allows(str(consumer.get("version_constraint") or ""), version):
                consumer_blockers.append(f"schema_contract.consumer_version_incompatible:{consumer_id}")
            for column in sorted(reads & removed):
                consumer_blockers.append(f"schema_contract.consumer_column_removed:{consumer_id}:{column}")
            blockers.extend(consumer_blockers)
            bindings.append(
                {
                    "schema_version": CONSUMER_BINDING_SCHEMA,
                    "consumer_id": consumer_id,
                    "owner": consumer.get("owner"),
                    "version_constraint": consumer.get("version_constraint"),
                    "reads": sorted(reads),
                    "status": "blocked" if consumer_blockers else "compatible",
                    "blockers": consumer_blockers,
                }
            )
        status = "blocked" if blockers else "warning" if warnings else "allowed"
        payload: dict[str, Any] = {
            "schema_version": CONTRACT_GATE_SCHEMA,
            "status": status,
            "contract_id": contract_version.get("contract_id"),
            "contract_version_id": contract_version.get("contract_version_id"),
            "version": version,
            "consumer_bindings": bindings,
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
        }
        payload["contract_gate_id"] = stable_fingerprint(payload)
        return payload


def _columns(
    raw: Mapping[str, Any], identity: Mapping[str, Any], source_schema: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    declared = _mapping(raw.get("columns"))
    identity_columns = _mapping(identity.get("columns"))
    names = set(declared) | {str(item.get("name")) for item in source_schema if isinstance(item, Mapping)}
    columns: list[dict[str, Any]] = []
    for name in sorted(item for item in names if item and item != "None"):
        config = _mapping(declared.get(name))
        ident = _mapping(identity_columns.get(name))
        columns.append(
            {
                "name": name,
                "identity_id": ident.get("id"),
                "logical_type": str(config.get("type", config.get("logical_type", "string"))),
                "precision": config.get("precision"),
                "scale": config.get("scale"),
                "nullable": bool(config.get("nullable", True)),
                "aliases": list(_aliases(ident)),
                "remove_after": config.get("remove_after"),
                "deprecated": bool(config.get("deprecated", False)),
            }
        )
    return columns


def _column_index(version: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {_column_key(column): column for column in version.get("columns", []) if isinstance(column, Mapping)}


def _column_key(column: Mapping[str, Any]) -> str:
    return str(column.get("identity_id") or column.get("name")).lower()


def _column_changes(base: Mapping[str, Any], head: Mapping[str, Any]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    if str(base.get("logical_type")) != str(head.get("logical_type")):
        changes.append(_change("column_type_changed", base, head, "major"))
    if bool(base.get("nullable", True)) and not bool(head.get("nullable", True)):
        changes.append(_change("column_nullability_narrowed", base, head, "major"))
    if set(_aliases(base)) != set(_aliases(head)):
        changes.append(_change("alias_changed", base, head, "minor"))
    if base.get("remove_after") != head.get("remove_after") or base.get("deprecated") != head.get("deprecated"):
        changes.append(_change("deprecation_changed", base, head, "patch"))
    return changes


def _change(kind: str, base: Mapping[str, Any] | None, head: Mapping[str, Any] | None, bump: str) -> dict[str, Any]:
    column = str((head or base or {}).get("name") or "")
    return {"change_type": kind, "column": column, "path": f"columns.{column}", "bump": bump}


def _required_bump(changes: Sequence[Mapping[str, Any]]) -> str:
    bump = "patch"
    for change in changes:
        candidate = str(change.get("bump") or "patch")
        if _BUMP_ORDER[candidate] > _BUMP_ORDER[bump]:
            bump = candidate
    return bump


def _compatibility_blockers(changes: Sequence[Mapping[str, Any]], compatibility: str) -> tuple[str, ...]:
    del changes, compatibility
    return ()


def _declared_bump_satisfies(base: str, head: str, required: str) -> bool:
    base_v = _semver(base)
    head_v = _semver(head)
    if required == "major":
        return head_v[0] > base_v[0]
    if required == "minor":
        return head_v[0] == base_v[0] and head_v[1] > base_v[1]
    return head_v >= base_v


def _constraint_allows(constraint: str, version: str) -> bool:
    if not constraint:
        return True
    value = _semver(version)
    if constraint.endswith(".x"):
        return value[0] == int(constraint[:-2])
    for part in [item.strip() for item in constraint.split(",") if item.strip()]:
        if part.startswith(">="):
            if value < _semver(part[2:]):
                return False
            continue
        if part.startswith(">"):
            if value <= _semver(part[1:]):
                return False
            continue
        if part.startswith("<="):
            if value > _semver(part[2:]):
                return False
            continue
        if part.startswith("<"):
            if value >= _semver(part[1:]):
                return False
            continue
        if part.startswith("==") and value != _semver(part[2:]):
            return False
    return True


def _semver(raw: str) -> tuple[int, int, int]:
    parts = [int(part) for part in raw.split(".")[:3]]
    return tuple((parts + [0, 0, 0])[:3])  # type: ignore[return-value]


def _table_key(sink: Mapping[str, Any]) -> str:
    table = sink.get("table")
    if isinstance(table, Mapping):
        return ".".join(str(item) for item in (table.get("schema"), table.get("name")) if item)
    return str(table or sink.get("target_table") or "unknown")


def _registry(raw: Mapping[str, Any]) -> dict[str, Any]:
    registry = _mapping(raw.get("registry"))
    return {
        "enabled": bool(registry.get("enabled", False)),
        "mode": str(registry.get("mode") or "gate"),
        "store_backend": str(registry.get("store_backend") or "local_json"),
        "store_uri": str(registry.get("store_uri") or ".dpone/schema-contracts/registry.json"),
    }


def _consumer_list(raw: Mapping[str, Any]) -> list[dict[str, Any]]:
    manual = _mapping(raw.get("consumers")).get("manual", [])
    return [dict(item) for item in manual if isinstance(item, Mapping)]


def _aliases(identity: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(item.get("name")) for item in identity.get("aliases", []) if isinstance(item, Mapping))


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _string_set(raw: object) -> set[str]:
    return {str(item) for item in raw} if isinstance(raw, list | tuple | set) else set()


def _utc_now() -> str:
    return datetime.now(datetime_timezone.utc).replace(microsecond=0).isoformat()  # noqa: UP017
