"""Provider-neutral approval authority, quorum and SOD decisions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.data_product_authority_gate import AUTHORITY_GATE_SCHEMA, AuthorityGate
from dpone.readiness.migration_control import stable_fingerprint

AUTHORITY_REGISTRY_SCHEMA = "dpone.data_product_authority_registry.v1"
AUTHORITY_CHECK_SCHEMA = "dpone.data_product_authority_check.v1"
APPROVAL_QUORUM_SCHEMA = "dpone.data_product_approval_quorum.v1"


class AuthorityRegistryBuilder:
    """Build a deterministic local authority registry from a manifest."""

    def build(self, *, manifest: Mapping[str, Any]) -> dict[str, Any]:
        product = _product(manifest)
        options = _authority_options(product)
        if not options.get("enabled"):
            payload = _registry_payload(
                status="disabled",
                product=product,
                options=options,
                identities=(),
                roles=(),
                blockers=(),
                warnings=(),
            )
            payload["authority_registry_id"] = stable_fingerprint(payload)
            return payload
        identities = _identity_items(options)
        roles = _role_items(options)
        blockers = _registry_blockers(identities=identities, roles=roles)
        payload = _registry_payload(
            status="blocked" if blockers else "ready",
            product=product,
            options=options,
            identities=identities,
            roles=roles,
            blockers=blockers,
            warnings=(),
        )
        payload["authority_registry_id"] = stable_fingerprint(payload)
        return payload


class AuthorityCheckEvaluator:
    """Evaluate whether an actor can perform an action on an evidence subject."""

    def check(
        self,
        *,
        registry: Mapping[str, Any],
        actor: str,
        action: str,
        subject: Mapping[str, Any],
    ) -> dict[str, Any]:
        blockers: list[str] = []
        warnings: list[str] = []
        identity = _identity(registry, actor)
        roles = _role_map(registry)
        granted_roles: list[str] = []
        if not actor.strip():
            blockers.append("data_product_authority.actor_required")
        elif identity is None:
            _append_unknown_actor(registry, actor, blockers, warnings)
        else:
            granted_roles = _granted_roles(identity, roles, action)
            if not granted_roles:
                blockers.append(f"data_product_authority.action_not_granted:{actor}:{action}")
        if identity is not None and _is_approval_action(action):
            blockers.extend(_sod_blockers(registry, actor=actor, subject=subject))
        status = "blocked" if blockers else "warning" if warnings else "allowed"
        payload: dict[str, Any] = {
            "schema_version": AUTHORITY_CHECK_SCHEMA,
            "status": status,
            "authority_registry_id": registry.get("authority_registry_id"),
            "product": dict(registry.get("product", {})) if isinstance(registry.get("product"), Mapping) else {},
            "actor": actor,
            "action": action,
            "subject": _subject_ref(subject),
            "granted_roles": sorted(granted_roles),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
        }
        payload["authority_check_id"] = stable_fingerprint(payload)
        return payload


class ApprovalQuorumVerifier:
    """Verify distinct approval artifacts satisfy role quorum rules."""

    def verify(
        self,
        *,
        registry: Mapping[str, Any],
        request: Mapping[str, Any],
        approvals: Sequence[Mapping[str, Any]],
        quorum_name: str = "policy_waiver",
        action: str = "policy_waiver.approve",
    ) -> dict[str, Any]:
        blockers: list[str] = []
        warnings: list[str] = []
        seen: set[str] = set()
        approved_actors: list[str] = []
        satisfied_roles: set[str] = set()
        checks: list[dict[str, Any]] = []
        for approval in approvals:
            actor = _approval_actor(approval)
            if not actor:
                blockers.append("data_product_authority.approval_actor_required")
                continue
            if actor in seen:
                blockers.append(f"data_product_authority.duplicate_approval_actor:{actor}")
                continue
            seen.add(actor)
            if _approval_mismatches_request(approval, request):
                blockers.append(f"data_product_authority.approval_request_mismatch:{actor}")
                continue
            check = AuthorityCheckEvaluator().check(registry=registry, actor=actor, action=action, subject=request)
            checks.append(check)
            if check.get("status") == "blocked":
                blockers.extend(str(item) for item in check.get("blockers", []) if str(item))
                continue
            approved_actors.append(actor)
            satisfied_roles.update(str(item) for item in check.get("granted_roles", []) if str(item))
            warnings.extend(str(item) for item in check.get("warnings", []) if str(item))
        rule = _quorum_rule(registry, quorum_name)
        min_approvals = _positive_int(rule.get("min_approvals"), default=1)
        required_roles = set(_strings(rule.get("required_roles")))
        if len(approved_actors) < min_approvals:
            blockers.append(f"data_product_authority.quorum_min_approvals:{len(approved_actors)}:{min_approvals}")
        missing = sorted(required_roles - satisfied_roles)
        blockers.extend(f"data_product_authority.quorum_missing_role:{role}" for role in missing)
        status = "blocked" if blockers else "warning" if warnings else "allowed"
        payload: dict[str, Any] = {
            "schema_version": APPROVAL_QUORUM_SCHEMA,
            "status": status,
            "authority_registry_id": registry.get("authority_registry_id"),
            "waiver_request_id": request.get("waiver_request_id"),
            "request_schema_version": request.get("schema_version"),
            "quorum": {"name": quorum_name, "min_approvals": min_approvals, "required_roles": sorted(required_roles)},
            "approved_actors": approved_actors,
            "satisfied_roles": sorted(satisfied_roles),
            "authority_checks": [_compact_check(item) for item in checks],
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
        }
        payload["approval_quorum_id"] = stable_fingerprint(payload)
        return payload


def _registry_payload(
    *,
    status: str,
    product: Mapping[str, Any],
    options: Mapping[str, Any],
    identities: Sequence[Mapping[str, Any]],
    roles: Sequence[Mapping[str, Any]],
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    return {
        "schema_version": AUTHORITY_REGISTRY_SCHEMA,
        "status": status,
        "mode": str(options.get("mode") or "gate"),
        "profile": str(options.get("profile") or "prod_strict"),
        "product": _product_ref(product),
        "store_backend": str(options.get("store_backend") or "local_json"),
        "store_uri": options.get("store_uri"),
        "unknown_actor": str(options.get("unknown_actor") or "block"),
        "identities": [dict(item) for item in identities],
        "roles": [dict(item) for item in roles],
        "approval": dict(options.get("approval", {})) if isinstance(options.get("approval"), Mapping) else {},
        "separation_of_duties": dict(options.get("separation_of_duties", {}))
        if isinstance(options.get("separation_of_duties"), Mapping)
        else {},
        "signing": dict(options.get("signing", {})) if isinstance(options.get("signing"), Mapping) else {},
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }


def _registry_blockers(*, identities: Sequence[Mapping[str, Any]], roles: Sequence[Mapping[str, Any]]) -> list[str]:
    blockers: list[str] = []
    role_ids = set()
    for role in roles:
        role_id = str(role.get("id") or "")
        if not role_id:
            blockers.append("data_product_authority.role_id_required")
            continue
        if role_id in role_ids:
            blockers.append(f"data_product_authority.duplicate_role:{role_id}")
        role_ids.add(role_id)
        if not _strings(role.get("grants")):
            blockers.append(f"data_product_authority.empty_grants:{role_id}")
    identity_ids: set[str] = set()
    for identity in identities:
        identity_id = str(identity.get("id") or "")
        if not identity_id:
            blockers.append("data_product_authority.identity_id_required")
            continue
        if identity_id in identity_ids:
            blockers.append(f"data_product_authority.duplicate_identity:{identity_id}")
        identity_ids.add(identity_id)
        for role_id in _strings(identity.get("roles")):
            if role_id not in role_ids:
                blockers.append(f"data_product_authority.unknown_role:{identity_id}:{role_id}")
    return list(dict.fromkeys(blockers))


def _product(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    sink = manifest.get("sink")
    options = sink.get("options") if isinstance(sink, Mapping) else {}
    product = options.get("data_product") if isinstance(options, Mapping) else {}
    return product if isinstance(product, Mapping) else {}


def _authority_options(product: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = product.get("authority")
    return raw if isinstance(raw, Mapping) else {}


def _product_ref(product: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": product.get("id"),
        "owner": product.get("owner"),
        "tier": product.get("tier"),
        "criticality": product.get("criticality"),
    }


def _identity_items(options: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in options.get("identities", []) if isinstance(item, Mapping))


def _role_items(options: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in options.get("roles", []) if isinstance(item, Mapping))


def _identity(registry: Mapping[str, Any], actor: str) -> Mapping[str, Any] | None:
    for item in registry.get("identities", []) if isinstance(registry.get("identities"), list) else ():
        if isinstance(item, Mapping) and item.get("id") == actor:
            return item
    return None


def _role_map(registry: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(item.get("id")): item for item in registry.get("roles", []) if isinstance(item, Mapping) and item.get("id")
    }


def _granted_roles(identity: Mapping[str, Any], roles: Mapping[str, Mapping[str, Any]], action: str) -> list[str]:
    granted: list[str] = []
    for role_id in _strings(identity.get("roles")):
        if action in _strings(roles.get(role_id, {}).get("grants")):
            granted.append(role_id)
    return granted


def _append_unknown_actor(registry: Mapping[str, Any], actor: str, blockers: list[str], warnings: list[str]) -> None:
    finding = f"data_product_authority.unknown_actor:{actor}"
    policy = str(registry.get("unknown_actor") or "block")
    if policy == "allow":
        return
    if policy == "warn":
        warnings.append(finding)
    else:
        blockers.append(finding)


def _sod_blockers(registry: Mapping[str, Any], *, actor: str, subject: Mapping[str, Any]) -> list[str]:
    sod = registry.get("separation_of_duties")
    if not isinstance(sod, Mapping) or not sod.get("prevent_self_approval"):
        return []
    fields = _strings(sod.get("disallow_same_actor_for")) or ("requested_by", "implemented_by", "approved_by")
    return [f"data_product_authority.sod_self_approval:{field}" for field in fields if subject.get(field) == actor]


def _is_approval_action(action: str) -> bool:
    return action.endswith(".approve")


def _subject_ref(subject: Mapping[str, Any]) -> dict[str, Any]:
    result = {"schema_version": subject.get("schema_version")}
    for key, value in subject.items():
        if str(key).endswith("_id") and value:
            result[str(key)] = value
    return result


def _approval_actor(approval: Mapping[str, Any]) -> str:
    return str(approval.get("actor") or approval.get("approved_by") or "")


def _approval_mismatches_request(approval: Mapping[str, Any], request: Mapping[str, Any]) -> bool:
    return bool(
        approval.get("waiver_request_id") and approval.get("waiver_request_id") != request.get("waiver_request_id")
    )


def _quorum_rule(registry: Mapping[str, Any], quorum_name: str) -> Mapping[str, Any]:
    approval = registry.get("approval")
    quorum = approval.get("quorum") if isinstance(approval, Mapping) else {}
    rule = quorum.get(quorum_name) if isinstance(quorum, Mapping) else {}
    return rule if isinstance(rule, Mapping) else {}


def _compact_check(check: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "authority_check_id": check.get("authority_check_id"),
        "actor": check.get("actor"),
        "status": check.get("status"),
        "granted_roles": list(check.get("granted_roles", [])) if isinstance(check.get("granted_roles"), list) else [],
    }


def _positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item) for item in value if str(item))
    return ()


__all__ = [
    "APPROVAL_QUORUM_SCHEMA",
    "AUTHORITY_CHECK_SCHEMA",
    "AUTHORITY_GATE_SCHEMA",
    "AUTHORITY_REGISTRY_SCHEMA",
    "ApprovalQuorumVerifier",
    "AuthorityCheckEvaluator",
    "AuthorityGate",
    "AuthorityRegistryBuilder",
]
