"""Pure exact-origin validation for the private P10b launch service."""

from __future__ import annotations

import math
from hashlib import sha256
from inspect import getattr_static
from typing import Any, Never, cast

from dpone.services.mssql_tds_writer_authority import SqlClientWriterAdmitted, _AdmissionPlan
from dpone.services.mssql_tds_writer_authority_proof import assert_writer_authority
from dpone.services.mssql_tds_writer_contracts import (
    NativeBulkTransportPolicy,
    SqlClientEvidenceBinding,
    SqlClientInputDescriptor,
    SqlClientLaunch,
    SqlClientReady,
    SqlClientRegistration,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    attempt_identity_digest,
    canonical_json_bytes,
    encode_input_descriptor,
    encode_stage_identity,
    input_descriptor_digest,
    launch_digest,
    validate_original_record,
    validate_ready,
)

ERROR = "mssql_native.sqlclient_writer_launch_unknown"


def _invalid() -> Never:
    raise ValueError(ERROR)


def _class_property(value: object, name: str) -> Any:
    descriptor = getattr_static(type(value), name)
    if type(descriptor) is not property:
        _invalid()
    return descriptor.__get__(value, type(value))


def _validate_policy(plan: _AdmissionPlan, attempt: TdsAttemptSnapshot) -> tuple[str, int, int]:
    policy = plan.policy
    if type(policy) is not NativeBulkTransportPolicy:
        _invalid()
    names = (
        "backend",
        "input",
        "max_worker_address_space_bytes",
        "batch_rows",
        "startup_timeout_seconds",
        "operation_timeout_seconds",
        "terminate_timeout_seconds",
        "drop_timeout_seconds",
    )
    record = {name: object.__getattribute__(policy, name) for name in names}
    record["max_input_batch_bytes"] = object.__getattribute__(policy, "max_input_batch_bytes")
    values = (record["input"], record["batch_rows"], record["max_input_batch_bytes"])
    if (
        record["backend"] != "mssql_sqlclient"
        or type(values[0]) is not str
        or values[0] not in ("rows", "arrow")
        or type(values[1]) is not int
        or type(values[2]) is not int
        or canonical_json_bytes(record) != plan.policy_snapshot
        or sha256(plan.policy_snapshot).hexdigest() != attempt.state.identity.policy_sha256
    ):
        _invalid()
    return cast(tuple[str, int, int], values)


def validate_launch_plan(admitted: SqlClientWriterAdmitted, claim: object, plan: _AdmissionPlan) -> TdsAttemptSnapshot:
    """Rebind P10a snapshots to the producer-sealed writer authority."""
    exact = type(admitted)._assert_p10b_claim(admitted, claim)
    if exact is not plan:
        _invalid()
    _, _, grant_receipt, grant_digest = assert_writer_authority(
        plan.authority_proof,
        terminal=plan.terminal,
        owner=plan.p9_owner,
        transition=plan.transition,
        fence=plan.authority_fence,
    )
    attempt, directory = plan.attempt, plan.directory
    installation_digest = object.__getattribute__(plan.installation, "build_sha256")
    if (
        grant_receipt is not plan.grant_result_receipt
        or grant_digest != plan.grant_result_sha256
        or type(attempt) is not TdsAttemptSnapshot
        or attempt.state.phase is not TdsAttemptPhase.PREPARED
        or attempt != plan.attempt_value
        or directory != plan.directory_value
        or encode_stage_identity(plan.stage) != plan.stage_snapshot
        or type(plan.input_descriptor) is not SqlClientInputDescriptor
        or encode_input_descriptor(plan.input_descriptor) != plan.input_snapshot
        or input_descriptor_digest(plan.input_descriptor) != plan.input_binding_sha256
        or type(installation_digest) is not str
        or installation_digest != plan.build_sha256
        or attempt.state.identity.implementation_sha256 != plan.implementation_sha256
    ):
        _invalid()
    validate_original_record((attempt, plan.attempt_value, directory, plan.directory_value))
    _validate_policy(plan, attempt)
    return attempt


def build_registration(process: Any, plan: _AdmissionPlan, attempt: TdsAttemptSnapshot) -> SqlClientRegistration:
    """Validate exact post-startup bindings and construct canonical registration."""
    launch = _class_property(process, "declared_launch")
    bound_input = _class_property(process, "bound_input")
    ready = _class_property(process, "startup_receipt")
    attempt_sha256 = attempt_identity_digest(attempt.state.identity)
    if (
        type(launch) is not SqlClientLaunch
        or type(ready) is not SqlClientReady
        or type(bound_input) is not SqlClientInputDescriptor
        or _class_property(process, "identity") != launch.process
        or launch.attempt_sha256 != attempt_sha256
        or launch.build_sha256 != plan.build_sha256
        or launch.input_binding_sha256 != input_descriptor_digest(bound_input)
        or launch.address_space_bytes != plan.max_worker_address_space_bytes
        or launch.operation_deadline_ns != int(math.nextafter(plan.operation_deadline, 0.0) * 10**9)
        or launch.startup_deadline_ns != int(math.nextafter(plan.startup_deadline, 0.0) * 10**9)
        or attempt.state.object_identity is None
    ):
        _invalid()
    validate_ready(launch, ready, now_ns=0)
    mode, batch_rows, max_bytes = _validate_policy(plan, attempt)
    binding = SqlClientEvidenceBinding(
        launch_digest(launch),
        attempt_sha256,
        attempt.state.identity,
        attempt.state.ownership,
        launch.process,
        attempt.state.object_identity,
        launch.input_binding_sha256,
        launch.build_sha256,
        launch.operation_deadline_ns,
    )
    return SqlClientRegistration(
        binding=binding,
        launch=launch,
        ready=ready,
        input=bound_input,
        input_mode=mode,
        batch_rows=batch_rows,
        max_input_batch_bytes=max_bytes,
    )


__all__ = ("build_registration", "validate_launch_plan")
