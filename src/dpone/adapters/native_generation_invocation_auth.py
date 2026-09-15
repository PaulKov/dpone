"""Exact original authentication and owned-path checks for a trusted invocation.

The injected original index is an authority, not a caller-supplied lookup table.
The application must admit the actual runtime and upstream qualification before
publishing generation-local attestations. This consumer verifies their immutable
membership; it does not manufacture qualification from a successful dbt exit.
"""

from __future__ import annotations

import stat
from hashlib import sha256
from pathlib import Path

from dpone.contracts.native_generation_invocation import (
    AuthenticatedInvocationPlan,
    invocation_path_slots,
    require_admitted_command,
    require_owned_root_identities,
    require_qualified_invocation,
)
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
from dpone.contracts.native_source_custody import NativeSourceCustodyError, SourceExecutorBinding
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
    require_admitted_command(executor, command)
    plan = decode_trusted_dbt_command_plan(reader.read(command, "trusted_dbt_command_plan_v1"))
    decode_trusted_dbt_toolchain(reader.read(toolchain, "trusted_dbt_toolchain_v1"))
    qualified = decode_trusted_dbt_qualification(reader.read(qualification, "trusted_dbt_qualification_v1"))
    require_qualified_invocation(qualified, plan, command=command, toolchain=toolchain, executor=executor)
    roots = tuple(
        decode_trusted_dbt_owned_root(reader.read(ref, "trusted_dbt_owned_root_v1"))
        for ref in (
            plan.project_root,
            plan.output_root,
            plan.profile_root,
        )
    )
    require_owned_root_identities(roots, executor)
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
    slots = invocation_path_slots(authenticated, position=position, args=args)
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
