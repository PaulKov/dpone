"""Complete immutable request for protected generation admission under existing P."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

from dpone.contracts.dbt_workspace_activation import DbtWorkspaceGuardEpoch
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest
from dpone.contracts.native_delivery import NativeGenerationContractError
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativeGenerationOriginalSubject, encode_native_original_subject


@dataclass(frozen=True, slots=True)
class VerifiedGenerationRequest:
    """Authenticated application input; construction itself confers no authority.

    The enclosing reservation reference addresses request_bytes(), which excludes
    that reference to avoid a self-referential digest. The ledger independently
    verifies retained original identity, active physical ownership and capacity.
    """

    subject: NativeGenerationOriginalSubject
    workspace_attempt: DbtWorkspaceAttemptRequest
    guard: DbtWorkspaceGuardEpoch
    reservation: OriginalRef
    profile: OriginalRef
    command: OriginalRef
    requested_bytes: int

    def __post_init__(self) -> None:
        if type(self.reservation) is not OriginalRef:
            raise NativeGenerationContractError("generation request requires an exact reservation reference")
        self.reservation.__post_init__()
        if self.reservation.sha256 != "sha256:" + sha256(self.request_bytes()).hexdigest():
            raise NativeGenerationContractError("reservation reference does not address the complete canonical request")

    def request_bytes(self) -> bytes:
        """Serialize the request body without its enclosing reference."""
        return generation_admission_request_bytes(
            subject=self.subject,
            workspace_attempt=self.workspace_attempt,
            guard=self.guard,
            profile=self.profile,
            command=self.command,
            requested_bytes=self.requested_bytes,
        )


def generation_admission_request_bytes(
    *,
    subject: NativeGenerationOriginalSubject,
    workspace_attempt: DbtWorkspaceAttemptRequest,
    guard: DbtWorkspaceGuardEpoch,
    profile: OriginalRef,
    command: OriginalRef,
    requested_bytes: int,
) -> bytes:
    """Validate and encode before publishing the enclosing reservation original.

    Publish these bytes through the verified original store, then construct
    VerifiedGenerationRequest with the returned reference. This pure producer
    neither enrolls an authority nor reserves capacity.
    """
    if type(subject) is not NativeGenerationOriginalSubject:
        raise NativeGenerationContractError("generation admission requires an exact generation subject")
    if type(workspace_attempt) is not DbtWorkspaceAttemptRequest or type(guard) is not DbtWorkspaceGuardEpoch:
        raise NativeGenerationContractError("generation admission requires existing workspace attempt and guard values")
    subject.__post_init__()
    workspace_attempt.__post_init__()
    guard.__post_init__()
    if type(guard.fencing_epoch) is not int or not 1 <= guard.fencing_epoch <= 9223372036854775807:
        raise NativeGenerationContractError("generation admission requires a positive SQL bigint epoch")
    if type(requested_bytes) is not int or not 1 <= requested_bytes <= 9223372036854775807:
        raise NativeGenerationContractError("generation capacity must be a positive SQL bigint")
    for reference in (profile, command):
        if type(reference) is not OriginalRef:
            raise NativeGenerationContractError("generation request requires exact original references")
        reference.__post_init__()
    attempt = workspace_attempt
    return encode_native_delivery_json(
        {
            "schema": "dpone.native-generation-admission.v1",
            "subject": decode_native_delivery_json(encode_native_original_subject(subject)),
            "workspace_attempt": {
                "activation_id": attempt.activation_id,
                "attempt_id": attempt.attempt_id,
                "workflow_id": attempt.workflow_id,
                "write_subjects": list(attempt.write_subjects),
                "request_sha256": attempt.request_sha256,
            },
            "guard": {"guard_id": guard.guard_id, "fencing_epoch": guard.fencing_epoch},
            "profile": {"locator": profile.locator, "sha256": profile.sha256},
            "command": {"locator": command.locator, "sha256": command.sha256},
            "requested_bytes": requested_bytes,
        }
    )
