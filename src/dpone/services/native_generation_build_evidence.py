"""Once-only publication of validated actual build artifacts.

Bootstrap owns dispatch and root lifetime. This writer has no command runner or
SQL mutation capability, and a published cohort is not permission to freeze.
"""

from __future__ import annotations

from collections.abc import Callable
from hashlib import sha256
from pathlib import Path
from threading import Lock

from dpone.adapters.native_generation_invocation_auth import (
    InvocationOriginalReader,
    authenticate_invocation,
    verify_invocation_paths,
)
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.dbt_execution_evidence import DbtExecutionEvidence, canonical_dbt_execution_evidence_bytes
from dpone.contracts.dbt_execution_pack import DbtExecutionPack
from dpone.contracts.native_generation_build_cohort import (
    NativeBuildArtifact,
    NativeBuildArtifactInventory,
    encode_native_build_artifact_inventory,
)
from dpone.contracts.native_generation_build_validation import (
    validate_native_build_evidence,
    validate_native_build_termination,
)
from dpone.contracts.native_generation_invocation import decode_trusted_dbt_invocation_completion
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_original_kinds import NativeOriginalKind
from dpone.contracts.native_source_custody import (
    NativeSourceCustodyError,
    SourceExecutorBinding,
    SourceTrustedBuildCompletion,
    decode_source_executor_binding,
    encode_source_executor_binding,
)
from dpone.contracts.native_trusted_dbt_environment_codec import decode_trusted_dbt_toolchain
from dpone.contracts.strict_json import strict_json_object
from dpone.ports.dbt_publishing import (
    DbtExecutionEvidenceWriter,
    DbtManifestSchemaValidator,
    DbtRunResultsSchemaValidator,
)
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader
from dpone.ports.native_originals import BoundNativeOriginalPublisher


class NativeGenerationBuildEvidenceWriter:
    """Capture one positive cohort, reconcile acknowledgements without redispatch.

    The admission and generation readers retain their separately bound subjects.
    The fixed pack reference must already be authenticated and published by the
    bootstrap; passing a typed pack alone is insufficient.
    """

    def __init__(
        self,
        *,
        delegate: DbtExecutionEvidenceWriter,
        executor: SourceExecutorBinding,
        execution_pack: DbtExecutionPack,
        execution_pack_ref: OriginalRef,
        run_identity: AirflowRunIdentity,
        toolchain: OriginalRef,
        qualification: OriginalRef,
        admission_reader: InvocationOriginalReader,
        generation_reader: InvocationOriginalReader,
        build_argv: tuple[str, ...],
        project_directory: Path,
        output_root: Path,
        artifact_reader: ConfinedReleaseFileReader,
        require_termination: Callable[[], OriginalRef],
        publish_original: BoundNativeOriginalPublisher,
        manifest_validator: DbtManifestSchemaValidator,
        results_validator: DbtRunResultsSchemaValidator,
        max_artifact_bytes: int,
        max_metadata_bytes: int,
    ) -> None:
        if any(type(n) is not int or n <= 0 for n in (max_artifact_bytes, max_metadata_bytes)):
            raise NativeSourceCustodyError("build cohort bounds must be exact positive integers")
        self._executor = decode_source_executor_binding(encode_source_executor_binding(executor))
        admission_reader.require_generation(self._executor)
        generation_reader.require_generation(self._executor)
        self._admission, self._generation = admission_reader, generation_reader
        self._toolchain, self._qualification = toolchain, qualification
        authenticated = self._authenticate()
        if authenticated.plan.phase != "BUILD":
            raise NativeSourceCustodyError("build cohort requires the admitted BUILD command")
        verify_invocation_paths(
            authenticated,
            position=len(authenticated.plan.commands) - 1,
            args=build_argv,
            cwd=project_directory,
            previous_profile=None,
        )
        target = Path(build_argv[build_argv.index("--target-path") + 1])
        relative = target.relative_to(output_root)
        info = output_root.lstat()
        if (info.st_dev, info.st_ino) != (authenticated.output.device, authenticated.output.inode):
            raise NativeSourceCustodyError("captured output differs from authenticated root")
        self._root, self._relative = output_root, relative
        self._pack_ref = execution_pack_ref
        self._pack_payload = generation_reader.read(execution_pack_ref, "generation_execution_pack_v1")
        if len(self._pack_payload) > max_metadata_bytes:
            raise NativeSourceCustodyError("build execution pack exceeds metadata bound")
        self._pack = DbtExecutionPack.from_mapping(strict_json_object(self._pack_payload))
        if self._pack.to_dict() != execution_pack.to_dict():
            raise NativeSourceCustodyError("build execution pack differs from its authenticated original")
        admitted_toolchain = decode_trusted_dbt_toolchain(
            admission_reader.read(toolchain, "trusted_dbt_toolchain_v1")
        ).dbt_contract
        if (
            admitted_toolchain.sha256 != self._pack.selection_lock.toolchain_sha256
            or admitted_toolchain.dbt_core_version != self._pack.dbt_core_version
            or admitted_toolchain.adapter_version != self._pack.dbt_adapter_version
            or admitted_toolchain.manifest_schema_version != self._pack.manifest_schema_version
            or admitted_toolchain.run_results_schema_version != self._pack.run_results_schema_version
        ):
            raise NativeSourceCustodyError("execution pack differs from the admitted dbt toolchain")
        self._identity, self._delegate = run_identity, delegate
        self._read_file, self._terminate, self._publish = artifact_reader, require_termination, publish_original
        self._manifest_validator, self._results_validator = manifest_validator, results_validator
        self._artifact_bound, self._metadata_bound = max_artifact_bytes, max_metadata_bytes
        self._lock = Lock()
        self._input: bytes | None = None
        self._captured: tuple[bytes, bytes, OriginalRef] | None = None
        self._completion: SourceTrustedBuildCompletion | None = None
        self._attempted: dict[str, tuple[NativeOriginalKind, bytes, OriginalRef]] = {}

    def require_executor(self, executor: SourceExecutorBinding) -> None:
        """Compare the complete bound identity without reading or publishing."""
        if decode_source_executor_binding(encode_source_executor_binding(executor)) != self._executor:
            raise NativeSourceCustodyError("build writer executor differs from its bound identity")

    def write(self, evidence: DbtExecutionEvidence) -> Path:
        """Preserve failed evidence; publish positive documents only after validation."""
        if type(evidence) is not DbtExecutionEvidence:
            raise NativeSourceCustodyError("build writer requires exact execution evidence")
        payload = canonical_dbt_execution_evidence_bytes(evidence.to_dict())
        with self._lock:
            if self._input is not None and self._input != payload:
                raise NativeSourceCustodyError("build writer cannot replace its first evidence")
            self._input = payload
            if evidence.status != "passed":
                return self._delegate.write(evidence)
            if len(payload) > self._metadata_bound:
                raise NativeSourceCustodyError("build evidence exceeds metadata bound")
            if self._captured is None:
                self._captured = self._capture(evidence)
            manifest, results, termination = self._captured
            build_ref = self._emit("dbt_build_evidence_v1", "evidence.json", payload, self._metadata_bound)
            manifest_ref = self._emit("dbt_build_manifest_v1", "manifest.json", manifest, self._artifact_bound)
            results_ref = self._emit("dbt_build_run_results_v1", "run_results.json", results, self._artifact_bound)
            inventory = NativeBuildArtifactInventory(
                self._executor,
                self._executor.command,
                self._toolchain,
                termination,
                self._pack_ref,
                build_ref,
                evidence.invocation_id or "",
                (
                    NativeBuildArtifact(
                        "MANIFEST", (self._relative / "manifest.json").as_posix(), len(manifest), manifest_ref
                    ),
                    NativeBuildArtifact(
                        "RUN_RESULTS", (self._relative / "run_results.json").as_posix(), len(results), results_ref
                    ),
                ),
            )
            inventory_ref = self._emit(
                "native_build_artifact_inventory_v1",
                "inventory.json",
                encode_native_build_artifact_inventory(inventory),
                self._metadata_bound,
            )
            candidate = SourceTrustedBuildCompletion(
                self._executor, self._executor.command, self._toolchain, build_ref, inventory_ref, termination
            )
            path = self._delegate.write(evidence)
            self._completion = candidate
            return path

    def require_build_completion(self) -> SourceTrustedBuildCompletion:
        """Independently read every retained original; no SQL state is advanced."""
        with self._lock:
            if self._completion is None or self._captured is None:
                raise NativeSourceCustodyError("build writer has no complete positive cohort")
            if self._generation.read(self._pack_ref, "generation_execution_pack_v1") != self._pack_payload:
                raise NativeSourceCustodyError("retained execution pack changed")
            self._authenticate()
            self._check_termination(self._captured[2])
            for kind, payload, reference in self._attempted.values():
                if self._generation.read(reference, kind) != payload:
                    raise NativeSourceCustodyError("retained build cohort differs from validated bytes")
            return self._completion

    def _authenticate(self):
        return authenticate_invocation(
            self._admission,
            executor=self._executor,
            command=self._executor.command,
            toolchain=self._toolchain,
            qualification=self._qualification,
        )

    def _capture(self, evidence: DbtExecutionEvidence) -> tuple[bytes, bytes, OriginalRef]:
        manifest = self._read_file(
            self._root, (self._relative / "manifest.json").as_posix(), max_bytes=self._artifact_bound
        )
        results = self._read_file(
            self._root, (self._relative / "run_results.json").as_posix(), max_bytes=self._artifact_bound
        )
        if any(type(raw) is not bytes or not 0 < len(raw) <= self._artifact_bound for raw in (manifest, results)):
            raise NativeSourceCustodyError("build artifact reader violated its byte contract")
        validate_native_build_evidence(
            evidence,
            pack=self._pack,
            run_identity=self._identity,
            manifest=strict_json_object(manifest),
            run_results=strict_json_object(results),
            manifest_validator=self._manifest_validator,
            results_validator=self._results_validator,
        )
        termination = self._terminate()
        self._check_termination(termination)
        return manifest, results, termination

    def _check_termination(self, reference: OriginalRef) -> None:
        plan = self._authenticate().plan
        terminal = decode_trusted_dbt_invocation_completion(
            self._admission.read(reference, "trusted_dbt_invocation_completion_v1")
        )
        validate_native_build_termination(
            terminal,
            plan=plan,
            executor=self._executor,
            toolchain=self._toolchain,
            qualification=self._qualification,
        )

    def _emit(self, kind: NativeOriginalKind, filename: str, payload: bytes, maximum: int) -> OriginalRef:
        if len(payload) > maximum:
            raise NativeSourceCustodyError("build cohort document exceeds its byte allowance")
        locator = (
            f"generations/{self._executor.generation_id}/invocations/{self._executor.invocation_id}/build/{filename}"
        )
        reference = OriginalRef(locator, "sha256:" + sha256(payload).hexdigest())
        previous = self._attempted.get(locator)
        if previous is not None and previous != (kind, payload, reference):
            raise NativeSourceCustodyError("build original cannot change after publication was attempted")
        if previous is None:
            self._attempted[locator] = (kind, payload, reference)
            try:
                returned = self._publish(kind=kind, locator=locator, payload=payload, max_bytes=maximum)
                if type(returned) is not OriginalRef or returned != reference:
                    raise NativeSourceCustodyError("publication returned a different build original")
            except Exception:
                # Reconciliation reads the complete original; never republish.
                pass
        if self._generation.read(reference, kind) != payload:
            raise NativeSourceCustodyError("build original independent readback differs")
        return reference
