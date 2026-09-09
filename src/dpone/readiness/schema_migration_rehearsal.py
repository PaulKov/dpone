"""Provider-neutral schema migration rehearsal contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.schema_migration_rehearsal_execution import (
    REHEARSAL_RUN_SCHEMA,
    MigrationRehearsalRunner,
)
from dpone.readiness.schema_migration_rehearsal_quality import quality_evidence

REHEARSAL_PLAN_SCHEMA = "dpone.schema_migration_rehearsal_plan.v1"
REHEARSAL_CERTIFICATE_SCHEMA = "dpone.schema_migration_rehearsal_certificate.v1"
_PROD_ENVIRONMENTS = frozenset({"prod", "production"})
_PROFILES = frozenset({"advisory", "stage", "prod_strict", "regulated"})


@dataclass(frozen=True, slots=True)
class MigrationRehearsalPlanner:
    """Builds immutable rehearsal plans from migration packs and bundle evidence."""

    def plan(
        self,
        *,
        pack: Mapping[str, Any],
        bundle: Mapping[str, Any] | None,
        environment: str,
        target_connection: Mapping[str, Any],
    ) -> dict[str, Any]:
        migration_pack = MigrationPack.from_mapping(dict(pack))
        blockers = list(_plan_blockers(migration_pack, bundle, environment, target_connection))
        rollback_operations = _rollback_operations(migration_pack)
        deferred_phases = _deferred_phases(migration_pack)
        warnings = _plan_warnings(migration_pack, bundle, deferred_phases)
        operations = _operations(migration_pack, deferred_phases=deferred_phases)
        target = migration_pack.target.to_dict()
        payload: dict[str, Any] = {
            "schema_version": REHEARSAL_PLAN_SCHEMA,
            "status": "blocked" if blockers else "planned",
            "pack_id": migration_pack.pack_id,
            "bundle_id": bundle.get("bundle_id") if bundle else None,
            "environment": environment,
            "target": target,
            "target_connection": _connection_identity(target_connection),
            "checks": _plan_checks(bundle, migration_pack, blockers, rollback_operations),
            "operations": operations,
            "rollback_operations": rollback_operations,
            "deferred_phases": list(deferred_phases),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
        }
        payload["rehearsal_plan_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "bundle_id": payload["bundle_id"],
                "environment": environment,
                "target": target,
                "connection": payload["target_connection"],
                "operations": operations,
                "rollback_operations": rollback_operations,
                "deferred_phases": payload["deferred_phases"],
                "checks": payload["checks"],
                "blockers": payload["blockers"],
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class MigrationRehearsalCertifier:
    """Converts rehearsal run evidence into go/no-go certificates."""

    def certify(
        self,
        *,
        run: Mapping[str, Any],
        profile: str = "stage",
        fixture_build: Mapping[str, Any] | None = None,
        before_profile: Mapping[str, Any] | None = None,
        after_profile: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_profile = profile.strip().lower()
        evidence = quality_evidence(
            run=run,
            fixture_build=fixture_build,
            before_profile=before_profile,
            after_profile=after_profile,
        )
        blockers = list(_certificate_blockers(run, normalized_profile))
        blockers.extend(evidence["blockers"])
        warnings = list(_certificate_warnings(run, normalized_profile))
        warnings.extend(evidence["warnings"])
        checks = _certificate_checks(run, normalized_profile, blockers, warnings)
        checks.extend(evidence["checks"])
        status = "blocked" if blockers else "warning" if warnings else "certified"
        payload: dict[str, Any] = {
            "schema_version": REHEARSAL_CERTIFICATE_SCHEMA,
            "pack_id": run.get("pack_id"),
            "bundle_id": run.get("bundle_id"),
            "environment": run.get("environment"),
            "target": dict(run.get("target", {})) if isinstance(run.get("target"), Mapping) else {},
            "status": status,
            "profile": normalized_profile,
            "checks": checks,
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "metrics": dict(run.get("metrics", {})) if isinstance(run.get("metrics"), Mapping) else {},
            "fixture_build_id": evidence["fixture_build_id"],
            "before_profile_id": evidence["before_profile_id"],
            "after_profile_id": evidence["after_profile_id"],
        }
        payload["certificate_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "run_id": run.get("rehearsal_run_id"),
                "profile": normalized_profile,
                "fixture_build_id": payload["fixture_build_id"],
                "before_profile_id": payload["before_profile_id"],
                "after_profile_id": payload["after_profile_id"],
                "checks": checks,
                "blockers": payload["blockers"],
                "warnings": payload["warnings"],
            }
        )
        return payload


def render_rehearsal_markdown(payload: Mapping[str, Any]) -> str:
    title = "Schema Migration Rehearsal Certificate"
    if payload.get("schema_version") == REHEARSAL_PLAN_SCHEMA:
        title = "Schema Migration Rehearsal Plan"
    if payload.get("schema_version") == REHEARSAL_RUN_SCHEMA:
        title = "Schema Migration Rehearsal Run"
    lines = [
        f"# {title}",
        "",
        f"- status: {payload.get('status')}",
        f"- pack_id: {payload.get('pack_id')}",
        f"- bundle_id: {payload.get('bundle_id')}",
        f"- environment: {payload.get('environment')}",
    ]
    target = payload.get("target", {})
    if isinstance(target, Mapping):
        lines.append(f"- target: {target.get('sink_type')}.{target.get('table')}")
    for key in ("blockers", "warnings"):
        values = _strings(payload.get(key, []))
        if values:
            lines.extend(["", f"## {key.title()}", "", *[f"- {item}" for item in values]])
    return "\n".join(lines) + "\n"


def render_rehearsal_table(payload: Mapping[str, Any]) -> str:
    rows = [
        "field | value",
        "--- | ---",
        f"status | {payload.get('status')}",
        f"pack_id | {payload.get('pack_id')}",
        f"bundle_id | {payload.get('bundle_id')}",
        f"environment | {payload.get('environment')}",
    ]
    return "\n".join(rows) + "\n"


def _plan_blockers(
    pack: MigrationPack,
    bundle: Mapping[str, Any] | None,
    environment: str,
    target_connection: Mapping[str, Any],
) -> tuple[str, ...]:
    blockers: list[str] = []
    if bundle and bundle.get("pack_id") != pack.pack_id:
        blockers.append("schema_migration_rehearsal.bundle_pack_id_mismatch")
    if pack.blockers:
        blockers.append("schema_migration_rehearsal.pack_blocked")
        blockers.extend(pack.blockers)
    sink_type = str(target_connection.get("type", target_connection.get("sink_type", pack.target.sink_type))).lower()
    if sink_type != pack.target.sink_type.lower():
        blockers.append("schema_migration_rehearsal.target_connection_type_mismatch")
    if pack.target.sink_type.lower() != "clickhouse":
        blockers.append(f"schema_migration_rehearsal.unsupported_target:{pack.target.sink_type}")
    connection_env = str(target_connection.get("environment", environment)).strip().lower()
    if connection_env in _PROD_ENVIRONMENTS or environment.strip().lower() in _PROD_ENVIRONMENTS:
        if not bool(target_connection.get("allow_prod_rehearsal")):
            blockers.append("schema_migration_rehearsal.prod_environment_blocked")
    return tuple(blockers)


def _plan_warnings(
    pack: MigrationPack,
    bundle: Mapping[str, Any] | None,
    deferred_phases: Sequence[str],
) -> tuple[str, ...]:
    warnings = list(pack.warnings)
    if bundle is None:
        warnings.append("schema_migration_rehearsal.bundle_not_provided")
    if deferred_phases:
        warnings.append("schema_migration_rehearsal.contract_deferred_for_rollback_check")
    return tuple(warnings)


def _operations(pack: MigrationPack, *, deferred_phases: Sequence[str]) -> list[dict[str, Any]]:
    if pack.phases:
        operations: list[dict[str, Any]] = []
        for phase in pack.phases:
            phase_name = str(phase.get("name", "phase"))
            if phase_name in deferred_phases:
                continue
            for operation in _phase_operations(phase):
                operations.append({**operation, "phase": phase_name})
        return operations
    return [
        {"name": f"ddl_{index}", "operation_type": "sql", "sql": statement}
        for index, statement in enumerate(pack.ddl, 1)
    ]


def _rollback_operations(pack: MigrationPack) -> list[dict[str, Any]]:
    rollback = pack.rollback if isinstance(pack.rollback, Mapping) else {}
    if not rollback.get("supported"):
        return []
    return [
        {"name": f"rollback_{index}", "operation_type": "rollback", "sql": statement}
        for index, statement in enumerate(_strings(rollback.get("ddl", [])), 1)
    ]


def _deferred_phases(pack: MigrationPack) -> tuple[str, ...]:
    rollback = pack.rollback if isinstance(pack.rollback, Mapping) else {}
    supported_until = str(rollback.get("supported_until_phase", "")).strip()
    if rollback.get("supported") and supported_until:
        return (supported_until,)
    return ()


def _phase_operations(phase: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(item) for key in ("operations", "validations") for item in phase.get(key, []) if isinstance(item, Mapping)
    ]


def _connection_identity(connection: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: connection.get(key)
        for key in ("type", "sink_type", "environment", "database", "host", "port", "path")
        if connection.get(key) is not None
    }


def _plan_checks(
    bundle: Mapping[str, Any] | None,
    pack: MigrationPack,
    blockers: Sequence[str],
    rollback_operations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        _check("bundle_binding", "passed" if bundle and bundle.get("pack_id") == pack.pack_id else "warning", ()),
        _check("pack_blockers", "blocked" if pack.blockers else "passed", pack.blockers),
        _check(
            "sandbox_guard",
            "blocked" if "schema_migration_rehearsal.prod_environment_blocked" in blockers else "passed",
            (),
        ),
        _check("rollback_available", "passed" if rollback_operations else "warning", ()),
    ]


def _certificate_blockers(run: Mapping[str, Any], profile: str) -> tuple[str, ...]:
    blockers: list[str] = []
    if profile not in _PROFILES:
        blockers.append(f"schema_migration_rehearsal.unknown_profile:{profile}")
    if run.get("status") == "blocked":
        blockers.append("schema_migration_rehearsal.run_blocked")
        blockers.extend(_strings(run.get("blockers", [])))
    if profile in {"stage", "prod_strict", "regulated"} and not run.get("executed"):
        blockers.append("schema_migration_rehearsal.execution_required")
    return tuple(blockers)


def _certificate_warnings(run: Mapping[str, Any], profile: str) -> tuple[str, ...]:
    warnings = _strings(run.get("warnings", []))
    if not run.get("executed") and profile == "advisory":
        warnings.append("schema_migration_rehearsal.dry_run_certificate")
    return tuple(warnings)


def _certificate_checks(
    run: Mapping[str, Any],
    profile: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> list[dict[str, Any]]:
    return [
        _check("profile", "passed" if profile in _PROFILES else "blocked", ()),
        _check(
            "run_status", "blocked" if run.get("status") == "blocked" else "passed", _strings(run.get("blockers", []))
        ),
        _check("execution", "passed" if run.get("executed") else "warning", ()),
        _check(
            "policy", "blocked" if blockers else "warning" if warnings else "passed", tuple(blockers) + tuple(warnings)
        ),
    ]


def _strings(raw: object) -> list[str]:
    return [str(item) for item in raw if str(item)] if isinstance(raw, list | tuple) else []


def _check(name: str, status: str, details: Sequence[str]) -> dict[str, Any]:
    return {"name": name, "status": status, "details": list(details)}


__all__ = [
    "MigrationRehearsalCertifier",
    "MigrationRehearsalPlanner",
    "MigrationRehearsalRunner",
    "REHEARSAL_CERTIFICATE_SCHEMA",
    "REHEARSAL_PLAN_SCHEMA",
    "REHEARSAL_RUN_SCHEMA",
    "render_rehearsal_markdown",
    "render_rehearsal_table",
]
