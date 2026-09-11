"""Derive every protected dbt dispatch value from verified sources only.

One admitted attempt owns exactly one authority instance. No caller may supply a
dispatch intent, an expected status, a materialization contract, an evidence
digest, an issued principal or a command identity: each value is derived here
from the verified execution pack, the root-owned preflight manifest, the
immutable run identity, the protected allocation, the inspected toolchain and
the exact issued login SID.

Every callback reopens the root-owned preflight manifest through the injected
protected reader and hashes its exact bytes. The first observation pins that
digest, so a stale or mutated original rejects instead of authorizing a build,
and the durable registration can never bind bytes that no longer exist.

Command derivation stays with the runtime policy that owns dbt argv, but the
derived commands are not trusted blindly: this authority re-checks their phase
closure, working directory, timeout and every path flag against the protected
allocation before any of them can become a dispatch intent.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.adapters.composition_dbt_command_runner import TrustedDbtCommand
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_dbt_materialization import (
    DbtMaterializationContract,
    derive_dbt_materializations,
)
from dpone.contracts.composition_dbt_outcome import (
    MAX_ARTIFACT_BYTES,
    DbtCaptureError,
    DbtDispatchIntent,
    DbtOutcomeExpectation,
)
from dpone.contracts.dbt_graph_contract import dbt_graph_contract_sha256, expected_dbt_run_result_ids
from dpone.contracts.dbt_runtime import (
    AirflowRunIdentity,
    DbtExecutionPack,
    dbt_target_binding_sha256,
    dbt_target_identity_sha256,
)
from dpone.contracts.strict_json import strict_json_object
from dpone.ports.dbt_publishing import DbtInstalledToolchain

PREFLIGHT_MANIFEST_ARTIFACT = "preflight/target/manifest.json"
EXECUTION_EVIDENCE_ARTIFACT = "execution-evidence.json"
ManifestReader = Callable[[], bytes]
CommandFactory = Callable[[], tuple[TrustedDbtCommand, ...]]


@dataclass(frozen=True, slots=True)
class CompositionDbtChildAllocation:
    """One attempt's already reserved protected paths and child identity.

    ``supervisor_uid`` is the root supervisor that owns the output root, and the
    child identity is the permanently reserved pair observed on the supervisor
    mount. Nothing here allocates, chowns or widens an identity.
    """

    output_directory: Path
    preflight_target: Path
    preflight_logs: Path
    target: Path
    logs: Path
    profile_path: Path
    child_uid: int
    child_gid: int
    supervisor_uid: int = 0

    def __post_init__(self) -> None:
        for path in (
            self.output_directory,
            self.preflight_target,
            self.preflight_logs,
            self.target,
            self.logs,
            self.profile_path,
        ):
            if not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts:
                raise DbtCaptureError("capture_allocation_path")
        if self.supervisor_uid != 0 or min(self.child_uid, self.child_gid) <= 0:
            raise DbtCaptureError("capture_child_identity")
        for path in (self.preflight_target, self.preflight_logs, self.target, self.logs):
            if self.output_directory not in path.parents:
                raise DbtCaptureError("capture_allocation_path")

    @property
    def artifact_paths(self) -> tuple[tuple[str, str], ...]:
        """Project the exact capture-relative originals of this allocation."""
        target = self.target.relative_to(self.output_directory).as_posix()
        return (
            ("preflight_manifest", PREFLIGHT_MANIFEST_ARTIFACT),
            ("build_manifest", f"{target}/manifest.json"),
            ("run_results", f"{target}/run_results.json"),
            ("execution_evidence", EXECUTION_EVIDENCE_ARTIFACT),
        )


class CompositionDbtCaptureAuthority:
    """Single source of truth for one attempt's dispatch and outcome authority."""

    def __init__(
        self,
        *,
        attempt: CompositionAttemptIdentity,
        pack: DbtExecutionPack,
        run_identity: AirflowRunIdentity,
        allocation: CompositionDbtChildAllocation,
        toolchain: DbtInstalledToolchain,
        manifest_reader: ManifestReader,
        commands: CommandFactory,
        sql_principal_sid: str,
        project_directory: Path,
        dbt_executable: str,
    ) -> None:
        attempt.__post_init__()
        allocation.__post_init__()
        if attempt.constituent_id != "native" or attempt.plan_sha256 != pack.pack_sha256:
            raise DbtCaptureError("capture_registration_subject")
        installed = (toolchain.dbt_core_version, toolchain.adapter_name, toolchain.adapter_version)
        if installed != (pack.dbt_core_version, pack.profile.adapter_type, pack.dbt_adapter_version):
            raise DbtCaptureError("capture_manifest_toolchain")
        if not isinstance(project_directory, Path) or not project_directory.is_absolute():
            raise DbtCaptureError("capture_working_directory")
        if not dbt_executable.startswith("/"):
            raise DbtCaptureError("capture_argv")
        self._attempt = attempt
        self._pack = pack
        self._run_identity = run_identity
        self._allocation = allocation
        self._toolchain = toolchain
        self._read_manifest = manifest_reader
        self._commands = commands
        self._sid = sql_principal_sid
        self._project = project_directory
        self._executable = dbt_executable
        self._pinned: str | None = None

    def trusted_commands(self) -> tuple[TrustedDbtCommand, ...]:
        """Return the checked runtime invocations for exactly this allocation."""
        commands = self._commands()
        if type(commands) is not tuple or not commands:
            raise DbtCaptureError("capture_trusted_command")
        phases = tuple(command.phase for command in commands)
        if phases.count("build") != 1 or set(phases) - {"build", "preflight"}:
            raise DbtCaptureError("capture_trusted_command")
        for command in commands:
            command.__post_init__()
            self._require_command_paths(command)
        return commands

    def intent(self) -> DbtDispatchIntent:
        """Bind the exact build argv to freshly rehashed preflight bytes."""
        digest = self._manifest()[1]
        build = next(command for command in self.trusted_commands() if command.phase == "build")
        result = DbtDispatchIntent(
            self._attempt,
            (self._executable, *build.args[1:]),
            self._pack.selection_lock.toolchain_sha256,
            self._sid,
            str(self._allocation.output_directory),
            self._allocation.supervisor_uid,
            self._allocation.child_uid,
            self._allocation.child_gid,
            self._allocation.artifact_paths,
            digest,
            working_directory=str(self._project),
            timeout_seconds=self._pack.timeout_seconds,
        )
        result.__post_init__()
        if build.args[0] != "dbt" or str(build.cwd) != result.working_directory:
            raise DbtCaptureError("capture_trusted_command")
        if build.timeout_seconds != result.timeout_seconds:
            raise DbtCaptureError("capture_trusted_command")
        return result

    def expectation(self) -> DbtOutcomeExpectation:
        """Derive the selected graph, toolchain and evidence subject obligations."""
        manifest = self._manifest()[0]
        selected = self._pack.selection_lock.selected_graph_unique_ids
        try:
            graph = dbt_graph_contract_sha256(manifest, selected)
            expected = expected_dbt_run_result_ids(manifest, selected)
        except (KeyError, TypeError, ValueError):
            raise DbtCaptureError("capture_manifest_selection") from None
        result = DbtOutcomeExpectation(
            graph,
            selected,
            expected,
            self._toolchain.dbt_core_version,
            self._pack.manifest_schema_version,
            self._pack.run_results_schema_version,
            self._evidence_subject(),
            tuple(row.expectation for row in self._contracts(manifest)),
            self._pack.dbt_warning_policy,
        )
        result.__post_init__()
        return result

    def verify_source(self, attempt: CompositionAttemptIdentity) -> tuple[DbtDispatchIntent, DbtOutcomeExpectation]:
        """Serve the protected registration store; never accept a supplied DTO."""
        self._require_attempt(attempt)
        return self.intent(), self.expectation()

    def materialization_contracts(self, attempt: CompositionAttemptIdentity) -> tuple[DbtMaterializationContract, ...]:
        """Reopen declared obligations for the actual SQL observer, per call."""
        self._require_attempt(attempt)
        return self._contracts(self._manifest()[0])

    def _require_attempt(self, attempt: CompositionAttemptIdentity) -> None:
        if type(attempt) is not CompositionAttemptIdentity or attempt != self._attempt:
            raise DbtCaptureError("capture_attempt")

    def _manifest(self) -> tuple[Mapping[str, Any], str]:
        """Reopen and rehash the root-owned preflight original on every call."""
        payload = self._read_manifest()
        if type(payload) is not bytes or not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
            raise DbtCaptureError("capture_preflight_original")
        digest = "sha256:" + sha256(payload).hexdigest()
        if self._pinned is not None and self._pinned != digest:
            raise DbtCaptureError("capture_preflight_changed")
        try:
            manifest = strict_json_object(payload)
        except ValueError:
            raise DbtCaptureError("capture_preflight_original") from None
        self._pinned = digest
        return manifest, digest

    def _contracts(self, manifest: Mapping[str, Any]) -> tuple[DbtMaterializationContract, ...]:
        return derive_dbt_materializations(project_path=self._pack.project_subdir, pack=self._pack, manifest=manifest)

    def _evidence_subject(self) -> tuple[tuple[str, str], ...]:
        pack, run_identity = self._pack, self._run_identity
        return tuple(
            sorted(
                {
                    "release_id": run_identity.release_id,
                    "deployment_id": run_identity.deployment_id,
                    "workload_pack_sha256": run_identity.workload_pack.sha256,
                    "project_bundle_sha256": pack.project_bundle_sha256,
                    "manifest_sha256": pack.selection_lock.manifest_sha256,
                    "selection_sha256": pack.selection_lock.selection_sha256,
                    "toolchain_sha256": pack.selection_lock.toolchain_sha256,
                    "invocation_context_sha256": pack.invocation_context.invocation_context_sha256,
                    "logical_target_sha256": dbt_target_identity_sha256(pack.profile),
                    "target_binding_sha256": dbt_target_binding_sha256(pack, run_identity),
                    "adapter_policy_sha256": pack.adapter_policy.adapter_policy_sha256,
                    "graph_policy_sha256": pack.selection_lock.graph_policy_sha256,
                    "workflow_id": pack.workflow_id,
                }.items()
            )
        )

    def _require_command_paths(self, command: TrustedDbtCommand) -> None:
        """Require every derived invocation to stay inside this allocation."""
        allocation = self._allocation
        preflight = command.phase == "preflight"
        expected = {
            "--project-dir": str(self._project),
            "--profiles-dir": str(allocation.profile_path.parent),
            "--target-path": str(allocation.preflight_target if preflight else allocation.target),
            "--log-path": str(allocation.preflight_logs if preflight else allocation.logs),
        }
        if str(command.cwd) != str(self._project):
            raise DbtCaptureError("capture_trusted_command")
        for flag, value in expected.items():
            if command.args.count(flag) != 1 or command.args[command.args.index(flag) + 1] != value:
                raise DbtCaptureError("capture_trusted_command")


__all__ = [
    "EXECUTION_EVIDENCE_ARTIFACT",
    "PREFLIGHT_MANIFEST_ARTIFACT",
    "CompositionDbtCaptureAuthority",
    "CompositionDbtChildAllocation",
]
