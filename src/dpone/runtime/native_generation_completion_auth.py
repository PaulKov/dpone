"""Fresh original authentication before durable positive source completion.

This verifier has no SQL, dispatch, credential or publication capability. The
composition root supplies the same fixed authorities as the admitted build.
Qualification issuance remains an upstream premise, not an inferred success.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.dbt_execution_evidence import DbtExecutionEvidence, canonical_dbt_execution_evidence_bytes
from dpone.contracts.dbt_execution_pack import DbtExecutionPack
from dpone.contracts.native_generation_build_cohort import decode_native_build_artifact_inventory
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_original_kinds import NativeOriginalKind
from dpone.contracts.native_source_custody import (
    NativeSourceCustodyError,
    SourceExecutorBinding,
    SourceTrustedBuildCompletion,
)
from dpone.contracts.native_source_custody_codec import (
    decode_source_executor_binding,
    decode_source_trusted_build_completion,
    decode_trusted_dbt_invocation_completion,
    encode_source_executor_binding,
    encode_source_trusted_build_completion,
)
from dpone.contracts.native_trusted_dbt_environment_codec import decode_trusted_dbt_toolchain
from dpone.contracts.strict_json import strict_json_object
from dpone.ports.dbt_publishing import DbtManifestSchemaValidator, DbtRunResultsSchemaValidator
from dpone.runtime.native_generation_build_validation import (
    validate_native_build_evidence,
    validate_native_build_termination,
)
from dpone.runtime.native_generation_invocation_auth import InvocationOriginalReader, authenticate_invocation


class NativeGenerationCompletionAuthenticator:
    """Read every original again; successful verification grants no dispatch.

    The SQL caller must use these exact completion values in its next revision
    checked operation. No current-pointer lookup or cached positive result is used.
    The existing evidence parser is injected to avoid a second wire decoder.
    """

    def __init__(
        self,
        *,
        executor: SourceExecutorBinding,
        execution_pack_ref: OriginalRef,
        run_identity: AirflowRunIdentity,
        qualification: OriginalRef,
        admission_reader: InvocationOriginalReader,
        metadata_reader: InvocationOriginalReader,
        artifact_reader: InvocationOriginalReader,
        evidence_decoder: Callable[[Mapping[str, Any]], DbtExecutionEvidence],
        manifest_validator: DbtManifestSchemaValidator,
        results_validator: DbtRunResultsSchemaValidator,
    ) -> None:
        self._executor = decode_source_executor_binding(encode_source_executor_binding(executor))
        self._pack_ref = OriginalRef(execution_pack_ref.locator, execution_pack_ref.sha256)
        self._qualification = OriginalRef(qualification.locator, qualification.sha256)
        self._identity = AirflowRunIdentity.from_mapping(run_identity.to_dict())
        for reader in (admission_reader, metadata_reader, artifact_reader):
            reader.require_generation(self._executor)
        self._admission, self._metadata, self._artifacts = admission_reader, metadata_reader, artifact_reader
        self._decode_evidence = evidence_decoder
        self._manifest_validator, self._results_validator = manifest_validator, results_validator

    def read_completion(self, reference: OriginalRef) -> SourceTrustedBuildCompletion:
        """Resolve an exact retained record, then authenticate its entire cohort.

        Recovery callers need not retain a Python completion object. Each call
        reads the originals anew and grants no authority to mutate the ledger.
        """
        completion = decode_source_trusted_build_completion(
            self._metadata.read(reference, "trusted_source_build_completion_v1")
        )
        self.authenticate(completion, reference)
        return completion

    def authenticate(self, completion: SourceTrustedBuildCompletion, completion_ref: OriginalRef) -> None:
        """Reject incomplete, substituted or nonpositive cohorts before SQL CAS."""
        supplied = encode_source_trusted_build_completion(completion)
        original = self._metadata.read(completion_ref, "trusted_source_build_completion_v1")
        if original != supplied:
            raise NativeSourceCustodyError("positive completion differs from its exact original")
        accepted = decode_source_trusted_build_completion(original)
        if accepted.executor != self._executor:
            raise NativeSourceCustodyError("positive completion differs from the admitted executor")
        authenticated = authenticate_invocation(
            self._admission,
            executor=self._executor,
            command=accepted.command,
            toolchain=accepted.toolchain,
            qualification=self._qualification,
        )
        terminal = decode_trusted_dbt_invocation_completion(
            self._admission.read(accepted.termination, "trusted_dbt_invocation_completion_v1")
        )
        validate_native_build_termination(
            terminal,
            plan=authenticated.plan,
            executor=self._executor,
            toolchain=accepted.toolchain,
            qualification=self._qualification,
        )
        inventory = decode_native_build_artifact_inventory(
            self._metadata.read(accepted.artifact_inventory, "native_build_artifact_inventory_v1")
        )
        if (
            inventory.executor,
            inventory.command,
            inventory.toolchain,
            inventory.termination,
            inventory.execution_pack,
            inventory.build_evidence,
        ) != (
            self._executor,
            accepted.command,
            accepted.toolchain,
            accepted.termination,
            self._pack_ref,
            accepted.build_evidence,
        ):
            raise NativeSourceCustodyError("build inventory differs from fixed completion authorities")
        pack = DbtExecutionPack.from_mapping(
            strict_json_object(self._metadata.read(self._pack_ref, "generation_execution_pack_v1"))
        )
        toolchain = decode_trusted_dbt_toolchain(
            self._admission.read(accepted.toolchain, "trusted_dbt_toolchain_v1")
        ).dbt_contract
        if (
            toolchain.sha256,
            toolchain.dbt_core_version,
            toolchain.adapter_version,
            toolchain.manifest_schema_version,
            toolchain.run_results_schema_version,
        ) != (
            pack.selection_lock.toolchain_sha256,
            pack.dbt_core_version,
            pack.dbt_adapter_version,
            pack.manifest_schema_version,
            pack.run_results_schema_version,
        ):
            raise NativeSourceCustodyError("retained pack differs from admitted toolchain")
        raw_evidence = self._metadata.read(accepted.build_evidence, "dbt_build_evidence_v1")
        evidence = self._decode_evidence(strict_json_object(raw_evidence))
        if type(evidence) is not DbtExecutionEvidence or (
            canonical_dbt_execution_evidence_bytes(evidence.to_dict()) != raw_evidence
            or evidence.invocation_id != inventory.dbt_invocation_id
        ):
            raise NativeSourceCustodyError("parsed build evidence differs from its canonical observed original")
        payloads = []
        artifact_kinds: tuple[NativeOriginalKind, ...] = ("dbt_build_manifest_v1", "dbt_build_run_results_v1")
        for entry, kind in zip(inventory.artifacts, artifact_kinds, strict=True):
            if entry.size_bytes > self._artifacts.max_bytes:
                raise NativeSourceCustodyError("retained artifact exceeds the configured read bound")
            raw = self._artifacts.read(entry.original, kind)
            if len(raw) != entry.size_bytes:
                raise NativeSourceCustodyError("retained artifact differs from its inventoried size")
            payloads.append(strict_json_object(raw))
        validate_native_build_evidence(
            evidence,
            pack=pack,
            run_identity=self._identity,
            manifest=payloads[0],
            run_results=payloads[1],
            manifest_validator=self._manifest_validator,
            results_validator=self._results_validator,
        )
