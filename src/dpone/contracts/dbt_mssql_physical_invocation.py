"""Closed pre-reserve managed locators consumed from actual retained command bytes.

No endpoint, command or future reservation appears in this envelope. Parsing
neither grants graph membership nor changes a retained argv template.
"""

from dataclasses import dataclass
from typing import Any, cast

from dpone.contracts.dbt_mssql_physical_validation import require_physical_uuid
from dpone.contracts.native_delivery_json import NativeJsonValue, decode_native_delivery_json
from dpone.contracts.native_generation_invocation import TrustedDbtCommandPlan, decode_trusted_dbt_command_plan
from dpone.contracts.native_identity import OriginalRef


@dataclass(frozen=True, slots=True)
class PhysicalManagedInvocation:
    """Four exact locators; the full admitted invocation is resolved by SQL."""

    generation_id: str
    invocation_id: str
    plan_set: OriginalRef
    runtime_registration_id: str

    def __post_init__(self) -> None:
        for name in ("generation_id", "invocation_id", "runtime_registration_id"):
            require_physical_uuid(getattr(self, name), name)
        if type(self.plan_set) is not OriginalRef:
            raise ValueError("managed invocation requires an exact plan original reference")
        self.plan_set.__post_init__()

    def to_dict(self) -> dict[str, NativeJsonValue]:
        """Detach the four-field transport without adding future identities."""
        self.__post_init__()
        return dict(
            generation_id=self.generation_id,
            invocation_id=self.invocation_id,
            plan_set=dict(locator=self.plan_set.locator, sha256=self.plan_set.sha256),
            runtime_registration_id=self.runtime_registration_id,
        )


def decode_managed_invocation(value: object) -> PhysicalManagedInvocation:
    """Require exact closed shapes, canonical UUIDs and full plan reference."""
    if type(value) is not dict or set(value) != {
        "generation_id",
        "invocation_id",
        "plan_set",
        "runtime_registration_id",
    }:
        raise ValueError("managed invocation requires exactly four fields")
    raw = cast(dict[str, Any], value)
    ref = raw["plan_set"]
    if type(ref) is not dict or set(ref) != {"locator", "sha256"}:
        raise ValueError("managed plan reference requires its closed shape")
    return PhysicalManagedInvocation(
        raw["generation_id"], raw["invocation_id"], OriginalRef(**ref), raw["runtime_registration_id"]
    )


def require_managed_command(payload: bytes, *, expected: PhysicalManagedInvocation) -> TrustedDbtCommandPlan:
    """Read actual canonical command bytes; compare every parse/ls/build vars slot.

    The existing command codec owns argv/deadline/phase validation. Native JSON
    rejects duplicate keys including reserved collisions. All three vars strings
    must be byte-identical; no canonicalization or post-reserve rewrite occurs.
    """
    if type(expected) is not PhysicalManagedInvocation:
        raise ValueError("managed command requires an exact expected invocation")
    expected.__post_init__()
    plan = decode_trusted_dbt_command_plan(payload)
    if plan.phase != "BUILD" or str(plan.executor_invocation_id) != expected.invocation_id:
        raise ValueError("managed command differs from the build invocation")
    previous = None
    for command in plan.commands:
        args = command.argv_template
        if args.count("--vars") != 1 or any(arg.startswith("--vars=") for arg in args):
            raise ValueError("managed command requires one literal --vars argument")
        index = args.index("--vars") + 1
        if index == len(args):
            raise ValueError("managed command vars value is missing")
        text = args[index]
        raw = decode_native_delivery_json(text.encode("utf-8"))
        if decode_managed_invocation(raw.get("__dpone_managed")) != expected:
            raise ValueError("retained managed invocation differs from enrollment")
        if previous is not None and text != previous:
            raise ValueError("managed parse/ls/build vars differ")
        previous = text
    return plan
