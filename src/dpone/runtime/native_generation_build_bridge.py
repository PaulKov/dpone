"""Reserved build orchestration with actual positive evidence provenance.

This bridge is scoped to one admitted execution. It neither creates admission
authority nor releases ownership, including on failed or unknown outcomes.
"""

from __future__ import annotations

from threading import Lock
from typing import Literal

from dpone.adapters.native_generation_invocation_auth import InvocationOriginalReader
from dpone.contracts.dbt_execution_evidence import DbtExecutionEvidence, canonical_dbt_execution_evidence_bytes
from dpone.contracts.native_delivery import GenerationBuildReceipt, GenerationReservation
from dpone.contracts.native_originals import (
    NativeGenerationOriginalSubject,
    NativeOriginalSubject,
    decode_native_original_subject,
    encode_native_original_subject,
)
from dpone.contracts.native_source_custody import NativeSourceCustodyError, SourceExecutorBinding
from dpone.ports.dbt_publishing import (
    DbtCommandRunner,
    DbtExecutionEvidenceWriter,
    DbtExecutionOutcome,
    DbtProfileRenderer,
)
from dpone.ports.native_generation_build import NativeGenerationBuildPort
from dpone.ports.native_originals import (
    BoundNativeOriginalPublisher,
    NativeOriginalBindingPort,
    NativeOriginalReaderPort,
)
from dpone.runtime.native_generation_build_evidence import NativeGenerationBuildEvidenceWriter
from dpone.runtime.native_generation_execution import TrustedDbtInvocationRecorder


class NativeGenerationBuildRejected(ValueError):
    """Actual unsuccessful local outcome; its classification is not cleanup authority."""

    def __init__(self, outcome: DbtExecutionOutcome) -> None:
        self.outcome = outcome
        self.state: Literal["FAILED", "UNKNOWN"] = (
            "UNKNOWN"
            if outcome.evidence.code == "COMMIT_UNKNOWN" or outcome.evidence.recovery is not None
            else "FAILED"
        )
        super().__init__(f"native generation build reported {self.state}")


class ReservedDbtBuildBridge:
    """Run once with the exact recorder and return only its authenticated cohort."""

    def __init__(
        self,
        *,
        build: NativeGenerationBuildPort,
        command_runner: DbtCommandRunner,
        profile_renderer: DbtProfileRenderer,
        evidence_writer: DbtExecutionEvidenceWriter,
        originals: NativeOriginalReaderPort,
        bindings: NativeOriginalBindingPort,
        subject: NativeOriginalSubject,
        publish_original: BoundNativeOriginalPublisher,
        invocation: TrustedDbtInvocationRecorder,
    ) -> None:
        if command_runner is not invocation or not isinstance(invocation, TrustedDbtInvocationRecorder):
            raise NativeSourceCustodyError("build runner must be the actual bound invocation recorder")
        if not isinstance(evidence_writer, NativeGenerationBuildEvidenceWriter):
            raise NativeSourceCustodyError("build requires the actual positive cohort writer")
        admitted_subject = decode_native_original_subject(encode_native_original_subject(subject))
        if not isinstance(admitted_subject, NativeGenerationOriginalSubject):
            raise NativeSourceCustodyError("build requires a generation original subject")
        self._subject = admitted_subject
        self._build, self._runner, self._renderer = build, command_runner, profile_renderer
        self._writer, self._invocation = evidence_writer, invocation
        self._originals, self._bindings = originals, bindings
        self._lock, self._attempted = Lock(), False
        # Retained constructor contract: the actual writer owns publication.
        # This bridge must never republish the cohort or create a second receipt kind.

    def __call__(self, reservation: GenerationReservation, executor: SourceExecutorBinding) -> GenerationBuildReceipt:
        with self._lock:
            if self._attempted:
                raise NativeSourceCustodyError("reserved build cannot repeat an execution attempt")
            self._attempted = True
        if type(reservation) is not GenerationReservation or type(executor) is not SourceExecutorBinding:
            raise NativeSourceCustodyError("build requires exact reservation and executor values")
        reservation.__post_init__()
        executor.__post_init__()
        if (reservation.generation_id, reservation.guard_epoch, reservation.reservation) != (
            executor.generation_id,
            executor.guard_epoch,
            executor.reservation,
        ) or self._subject.generation_id != executor.generation_id:
            raise NativeSourceCustodyError("build reservation differs from the admitted executor")
        self._invocation.require_executor(executor)
        self._writer.require_executor(executor)
        self._invocation.validate_before_credentials()
        outcome = self._build.execute(
            command_runner=self._runner, profile_renderer=self._renderer, evidence_writer=self._writer
        )
        if type(outcome) is not DbtExecutionOutcome or type(outcome.evidence) is not DbtExecutionEvidence:
            raise NativeSourceCustodyError("build returned an invalid execution outcome")
        if outcome.evidence.status == "failed":
            raise NativeGenerationBuildRejected(outcome)
        if outcome.evidence.status != "passed" or any(
            type(value) is not int or value != 0 for value in (outcome.exit_code, outcome.evidence.dbt_exit_code)
        ):
            raise NativeSourceCustodyError("build returned inconsistent positive evidence")
        completion = self._writer.require_build_completion()
        if completion.executor != executor or self._invocation.require_completion() != completion.termination:
            raise NativeSourceCustodyError("build completion differs from the actual invocation")
        expected = canonical_dbt_execution_evidence_bytes(outcome.evidence.to_dict())
        reader = InvocationOriginalReader(
            originals=self._originals, bindings=self._bindings, subject=self._subject, max_bytes=len(expected)
        )
        reader.require_generation(executor)
        if reader.read(completion.build_evidence, "dbt_build_evidence_v1") != expected:
            raise NativeSourceCustodyError("build outcome differs from the actual writer evidence")
        return GenerationBuildReceipt(
            generation_id=reservation.generation_id,
            guard_epoch=reservation.guard_epoch,
            reservation=reservation.reservation,
            executor_invocation_id=executor.invocation_id,
            build_evidence=completion.build_evidence,
            artifact_inventory=completion.artifact_inventory,
            termination=completion.termination,
            outcome="SUCCEEDED",
        )
