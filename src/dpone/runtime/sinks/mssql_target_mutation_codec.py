"""Bounded canonical originals for existing MSSQL target mutation plans.

The existing plan digest is unchanged. A separate document hash must come from
trusted retained storage; neither decoding nor a matching hash grants DDL
permission. SQL validation delegates to the existing target-bound plan builder.
"""

from __future__ import annotations

import re
from dataclasses import asdict
from hashlib import sha256
from typing import Any

from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import MssqlTargetCatalogExpectation
from dpone.runtime.sinks.mssql_target_mutation_plan import MssqlTargetMutationAction, MssqlTargetMutationPlan

MAX_RETAINED_PLAN_BYTES = 1024 * 1024
_SCHEMA = "dpone.mssql-target-mutation-plan.v1"
_PLAN_FIELDS = {
    "target_identity",
    "target_database",
    "target_schema",
    "target_table",
    "actions",
    "expectations",
    "version",
}


class MssqlRetainedPlanError(ValueError):
    """Malformed or mismatched retained original, without supplied SQL in errors."""


def retained_document(document: bytes, expected_sha256: bytes) -> dict[str, Any]:
    """Read a canonical bounded object pinned to an external binary SHA256."""
    try:
        if (
            type(document) is not bytes
            or not 0 < len(document) <= MAX_RETAINED_PLAN_BYTES
            or type(expected_sha256) is not bytes
            or len(expected_sha256) != 32
            or sha256(document).digest() != expected_sha256
        ):
            raise ValueError
        value = strict_json_object(document)
        if canonical_json_bytes(value) != document:
            raise ValueError
        return value
    except (ValueError, TypeError, RecursionError):
        raise MssqlRetainedPlanError("mssql_retained_plan_document") from None


def binary_digest(value: Any) -> bytes:
    """Decode a canonical lowercase 32-byte hexadecimal digest."""
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise MssqlRetainedPlanError("mssql_retained_plan_digest")
    return bytes.fromhex(value)


def _text(value: Any) -> None:
    if type(value) is not str or not value or "\x00" in value:
        raise ValueError


def _payload(plan: MssqlTargetMutationPlan) -> dict[str, Any]:
    value = asdict(plan)
    value["target_identity"] = plan.target_identity.hex()
    value["actions"] = [asdict(action) for action in plan.actions]
    value["expectations"] = [
        {
            **asdict(expectation),
            "before_sha256": expectation.before_sha256.hex(),
            "after_sha256": expectation.after_sha256.hex(),
        }
        for expectation in plan.expectations
    ]
    return value


def _decode_plan(value: Any) -> MssqlTargetMutationPlan:
    if (
        type(value) is not dict
        or set(value) != _PLAN_FIELDS
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError
    for field in ("target_database", "target_schema", "target_table"):
        _text(value[field])
    if type(value["actions"]) is not list or type(value["expectations"]) is not list:
        raise ValueError
    expectations = []
    for raw in value["expectations"]:
        if type(raw) is not dict or set(raw) != {"kind", "representation", "before_sha256", "after_sha256"}:
            raise ValueError
        allowed = {
            "schema_columns": {"exact_sys_catalog_v5", "legacy_columns_v1"},
            "physical_design": {"physical_state_v1"},
        }
        if raw["kind"] not in allowed or raw["representation"] not in allowed[raw["kind"]]:
            raise ValueError
        expectations.append(
            MssqlTargetCatalogExpectation(
                raw["kind"],
                binary_digest(raw["before_sha256"]),
                binary_digest(raw["after_sha256"]),
                raw["representation"],
            )
        )
    base = MssqlTargetMutationPlan(
        binary_digest(value["target_identity"]), value["target_database"], value["target_schema"], value["target_table"]
    )
    actions = []
    for raw in value["actions"]:
        if (
            type(raw) is not dict
            or set(raw) != {"kind", "sql", "statement_type"}
            or raw["kind"] not in {"schema_evolution", "physical_design"}
        ):
            raise ValueError
        _text(raw["sql"])
        action = MssqlTargetMutationAction(**raw)
        kind = "schema_columns" if action.kind == "schema_evolution" else "physical_design"
        expectation = next((item for item in expectations if item.kind == kind), None)
        checked = base.append(action.kind, [action.sql], expectation=expectation)
        if checked.actions != (action,):
            raise ValueError
        actions.append(action)
    return MssqlTargetMutationPlan(
        base.target_identity,
        base.target_database,
        base.target_schema,
        base.target_table,
        tuple(actions),
        tuple(expectations),
        value["version"],
    )


def encode_mssql_target_mutation_plan(plan: MssqlTargetMutationPlan) -> bytes:
    """Retain every existing action and ordered before/after expectation."""
    try:
        if type(plan) is not MssqlTargetMutationPlan:
            raise ValueError
        payload = _payload(plan)
        decoded = _decode_plan(payload)
        if decoded != plan or sha256(canonical_json_bytes(payload)).digest() != plan.digest:
            raise ValueError
        document = canonical_json_bytes(
            {"schema": _SCHEMA, "mutation_plan": payload, "mutation_plan_sha256": plan.digest.hex()}
        )
        if len(document) > MAX_RETAINED_PLAN_BYTES:
            raise ValueError
        return document
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise MssqlRetainedPlanError("mssql_retained_mutation_plan") from None


def decode_mssql_target_mutation_plan(document: bytes, expected_sha256: bytes) -> MssqlTargetMutationPlan:
    """Verify an external document hash and the unchanged embedded plan digest."""
    try:
        value = retained_document(document, expected_sha256)
        if set(value) != {"schema", "mutation_plan", "mutation_plan_sha256"} or value["schema"] != _SCHEMA:
            raise ValueError
        plan = _decode_plan(value["mutation_plan"])
        if (
            plan.digest != binary_digest(value["mutation_plan_sha256"])
            or encode_mssql_target_mutation_plan(plan) != document
        ):
            raise ValueError
        return plan
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise MssqlRetainedPlanError("mssql_retained_mutation_plan") from None
