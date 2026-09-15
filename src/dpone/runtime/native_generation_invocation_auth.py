"""Exact original authentication and owned-path checks for a trusted invocation.

The injected original index is an authority, not a caller-supplied lookup table.
The application must admit the actual runtime and upstream qualification before
publishing generation-local attestations. This consumer verifies their immutable
membership; it does not manufacture qualification from a successful dbt exit.
"""

from __future__ import annotations

import stat
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeGenerationOriginalSubject,
    NativeOriginalKind,
    NativeOriginalSubject,
    decode_native_original_binding,
    decode_native_original_subject,
    encode_native_original_binding,
    encode_native_original_subject,
)
from dpone.contracts.native_source_custody import NativeSourceCustodyError, SourceExecutorBinding, TrustedDbtCommandPlan
from dpone.contracts.native_source_custody_codec import decode_trusted_dbt_command_plan
from dpone.contracts.native_trusted_dbt_environment import TrustedDbtOwnedRoot
from dpone.contracts.native_trusted_dbt_environment_codec import (
    decode_trusted_dbt_owned_root,
    decode_trusted_dbt_qualification,
    decode_trusted_dbt_toolchain,
)
from dpone.ports.native_originals import NativeOriginalBindingPort, NativeOriginalReaderPort


class InvocationOriginalReader:
    """Fresh full binding and bounded exact-version read at each trust boundary."""

    def __init__(
        self,
        *,
        originals: NativeOriginalReaderPort,
        bindings: NativeOriginalBindingPort,
        subject: NativeOriginalSubject,
        max_bytes: int,
    ) -> None:
        if type(max_bytes) is not int or max_bytes <= 0:
            raise NativeSourceCustodyError("invocation metadata bound must be an exact positive integer")
        self._originals = originals
        self._bindings = bindings
        self._subject_bytes = encode_native_original_subject(subject)
        self.max_bytes = max_bytes

    def require_generation(self, executor: SourceExecutorBinding) -> None:
        """Bind the recorder to the admitted generation, never another scope."""
        subject = decode_native_original_subject(self._subject_bytes)
        if type(subject) is not NativeGenerationOriginalSubject or subject.generation_id != executor.generation_id:
            raise NativeSourceCustodyError("invocation subject differs from the admitted generation")

    def read(self, reference: OriginalRef, kind: NativeOriginalKind) -> bytes:
        """Reject substituted subject/kind/locator/digest even from a faulty port."""
        reference = OriginalRef(reference.locator, reference.sha256)
        subject = decode_native_original_subject(self._subject_bytes)
        resolved = self._bindings.resolve(reference, expected_subject=subject, expected_kind=kind)
        binding = decode_native_original_binding(encode_native_original_binding(resolved))
        if (
            encode_native_original_subject(binding.subject) != self._subject_bytes
            or binding.kind != kind
            or binding.locator != reference.locator
            or binding.payload_sha256 != reference.sha256
        ):
            raise NativeSourceCustodyError("invocation original binding differs from admitted identity")
        payload = self._originals.read(
            binding.object_ref,
            expected_subject=decode_native_original_subject(self._subject_bytes),
            expected_kind=kind,
            max_bytes=self.max_bytes,
        )
        if (
            type(payload) is not bytes
            or len(payload) > self.max_bytes
            or "sha256:" + sha256(payload).hexdigest() != reference.sha256
        ):
            raise NativeSourceCustodyError("invocation original bytes differ from admitted identity or bound")
        return payload


@dataclass(frozen=True, slots=True)
class AuthenticatedInvocationPlan:
    """Freshly authenticated plan and its three role-specific root identities."""

    plan: TrustedDbtCommandPlan
    project: TrustedDbtOwnedRoot
    output: TrustedDbtOwnedRoot
    profile: TrustedDbtOwnedRoot


def authenticate_invocation(
    reader: InvocationOriginalReader,
    *,
    executor: SourceExecutorBinding,
    command: OriginalRef,
    toolchain: OriginalRef,
    qualification: OriginalRef,
) -> AuthenticatedInvocationPlan:
    """Authenticate retained admission records without observing a new runtime."""
    reader.require_generation(executor)
    if executor.command != command:
        raise NativeSourceCustodyError("command differs from the admitted executor")
    plan = decode_trusted_dbt_command_plan(reader.read(command, "trusted_dbt_command_plan_v1"))
    decode_trusted_dbt_toolchain(reader.read(toolchain, "trusted_dbt_toolchain_v1"))
    qualified = decode_trusted_dbt_qualification(reader.read(qualification, "trusted_dbt_qualification_v1"))
    if (qualified.command_plan, qualified.toolchain, qualified.profile) != (command, toolchain, executor.profile):
        raise NativeSourceCustodyError("qualification differs from the exact admitted plan/toolchain/profile")
    if plan.executor_invocation_id != executor.invocation_id:
        raise NativeSourceCustodyError("command plan differs from the admitted invocation")
    roots = tuple(
        decode_trusted_dbt_owned_root(reader.read(ref, "trusted_dbt_owned_root_v1"))
        for ref in (
            plan.project_root,
            plan.output_root,
            plan.profile_root,
        )
    )
    for root, role in zip(roots, ("PROJECT", "OUTPUT", "PROFILE"), strict=True):
        if root.role != role or root.executor_invocation_id != executor.invocation_id:
            raise NativeSourceCustodyError("owned root role or invocation differs from admission")
    if len({(root.device, root.inode) for root in roots}) != 3:
        raise NativeSourceCustodyError("project, output and profile roots must be distinct directories")
    return AuthenticatedInvocationPlan(plan, *roots)


def verify_invocation_paths(
    authenticated: AuthenticatedInvocationPlan,
    *,
    position: int,
    args: tuple[str, ...],
    cwd: Path,
    previous_profile: Path | None,
) -> Path:
    """Check only four explicit path argument slots; all other bytes stay literal.

    The bootstrap holds its roots throughout execution. This check rejects path
    traversal, links, replaced root identities and profile-directory changes. It
    does not replace bootstrap isolation or claim a pathname is a lifetime lease.
    """
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
    paths = {flag: _directory(args[index]) for flag, index in slots.items()}
    project = paths["--project-dir"]
    if _directory(str(cwd)) != project or not _identity(project, authenticated.project):
        raise NativeSourceCustodyError("command project differs from its authenticated owned root")
    for flag in ("--target-path", "--log-path"):
        if not any(_identity(parent, authenticated.output) for parent in paths[flag].parents):
            raise NativeSourceCustodyError("command output is outside its authenticated owned root")
    profile = paths["--profiles-dir"]
    if not _identity(profile.parent, authenticated.profile) or (
        previous_profile is not None and profile != previous_profile
    ):
        raise NativeSourceCustodyError("command profile is outside its owned root or changed during invocation")
    return profile


def _directory(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or str(path) != raw or ".." in path.parts:
        raise NativeSourceCustodyError("invocation directory must be an exact absolute path without traversal")
    for component in (*reversed(path.parents), path):
        if not stat.S_ISDIR(component.lstat().st_mode):
            raise NativeSourceCustodyError("invocation directory cannot contain symlinks or nondirectories")
    return path


def _identity(path: Path, root: TrustedDbtOwnedRoot) -> bool:
    observed = path.lstat()
    return stat.S_ISDIR(observed.st_mode) and (observed.st_dev, observed.st_ino) == (root.device, root.inode)
