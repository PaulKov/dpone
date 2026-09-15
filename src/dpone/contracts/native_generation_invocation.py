"""Pure identity and locked argument policy for authenticated native invocations.

These decisions require authenticated input records. They neither read originals
nor observe filesystem state, qualify a runtime, or authorize command dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import NativeSourceCustodyError, SourceExecutorBinding, TrustedDbtCommandPlan
from dpone.contracts.native_trusted_dbt_environment import TrustedDbtOwnedRoot, TrustedDbtQualification


@dataclass(frozen=True, slots=True)
class AuthenticatedInvocationPlan:
    """Freshly authenticated plan and its three role-specific root identities."""

    plan: TrustedDbtCommandPlan
    project: TrustedDbtOwnedRoot
    output: TrustedDbtOwnedRoot
    profile: TrustedDbtOwnedRoot


def require_admitted_command(executor: SourceExecutorBinding, command: OriginalRef) -> None:
    """Reject a substituted command before any original acquisition."""
    if executor.command != command:
        raise NativeSourceCustodyError("command differs from the admitted executor")


def require_qualified_invocation(
    qualified: TrustedDbtQualification,
    plan: TrustedDbtCommandPlan,
    *,
    command: OriginalRef,
    toolchain: OriginalRef,
    executor: SourceExecutorBinding,
) -> None:
    """Check qualification first, then invocation, before acquiring owned roots."""
    if (qualified.command_plan, qualified.toolchain, qualified.profile) != (command, toolchain, executor.profile):
        raise NativeSourceCustodyError("qualification differs from the exact admitted plan/toolchain/profile")
    if plan.executor_invocation_id != executor.invocation_id:
        raise NativeSourceCustodyError("command plan differs from the admitted invocation")


def require_owned_root_identities(roots: tuple[TrustedDbtOwnedRoot, ...], executor: SourceExecutorBinding) -> None:
    """Validate roles and distinct identities after all three roots are acquired."""
    for root, role in zip(roots, ("PROJECT", "OUTPUT", "PROFILE"), strict=True):
        if root.role != role or root.executor_invocation_id != executor.invocation_id:
            raise NativeSourceCustodyError("owned root role or invocation differs from admission")
    if len({(root.device, root.inode) for root in roots}) != 3:
        raise NativeSourceCustodyError("project, output and profile roots must be distinct directories")


def invocation_path_slots(
    authenticated: AuthenticatedInvocationPlan, *, position: int, args: tuple[str, ...]
) -> dict[str, int]:
    """Validate literal arguments and return only the four permitted path slots."""
    template = authenticated.plan.commands[position].argv_template
    if type(args) is not tuple or len(args) != len(template) or any(type(arg) is not str for arg in args):
        raise NativeSourceCustodyError("command arguments differ from the locked plan")
    substitutions = {
        "--project-dir": "PROJECT_DIR",
        "--profiles-dir": "PROFILE_DIR",
        "--target-path": "TARGET_DIR",
        "--log-path": "LOG_DIR",
    }
    slots: dict[str, int] = {}
    for flag in substitutions:
        if template.count(flag) != 1:
            raise NativeSourceCustodyError("locked command requires each explicit owned path exactly once")
        index = template.index(flag) + 1
        if index == len(template):
            raise NativeSourceCustodyError("locked command path argument is missing")
        slots[flag] = index
    for index, (expected, actual) in enumerate(zip(template, args, strict=True)):
        placeholder = next((substitutions[flag] for flag, slot in slots.items() if slot == index), None)
        if expected != actual and expected != placeholder:
            raise NativeSourceCustodyError("command arguments differ from the locked plan")
    return slots
