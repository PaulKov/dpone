"""Canonical enrollment fixtures with real codec bytes; no admission authority."""

from dataclasses import replace
from hashlib import sha256
from uuid import UUID

from dpone.contracts.dbt_mssql_physical_invocation import PhysicalManagedInvocation
from dpone.contracts.dbt_mssql_physical_wire import decode_physical_plan_set, encode_physical_plan_set
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from dpone.contracts.native_generation_invocation import encode_trusted_dbt_command_plan
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativeGenerationOriginalSubject
from tests.support.dbt_mssql_physical import canonical, plan_set_document
from tests.support.dbt_mssql_physical_registration import registration_inputs
from tests.test_native_generation_admission import executor
from tests.test_native_trusted_dbt_invocation import command_plan


def digest(payload):
    return "sha256:" + sha256(payload).hexdigest()


def components():
    plan = decode_physical_plan_set(canonical(plan_set_document()))
    plan_bytes = encode_physical_plan_set(plan)
    plan_ref = OriginalRef("fixture/physical-plan", digest(plan_bytes))
    writer = replace(
        executor(), generation_id=UUID(plan.generation_id), guard_epoch=plan.guard.fencing_epoch, profile=plan.profile
    )
    invocation = PhysicalManagedInvocation(
        plan.generation_id, str(writer.invocation_id), plan_ref, plan.runtime_registration_id
    )
    vars_text = encode_native_delivery_json({"__dpone_managed": invocation.to_dict()}).decode()
    command = command_plan()
    command = replace(
        command,
        executor_invocation_id=writer.invocation_id,
        commands=tuple(
            replace(entry, argv_template=("dbt", entry.verb, "--vars", vars_text)) for entry in command.commands
        ),
    )
    command_bytes = encode_trusted_dbt_command_plan(command)
    writer = replace(writer, command=OriginalRef("fixture/physical-command", digest(command_bytes)))
    subject = NativeGenerationOriginalSubject(registration_inputs()["platform_subject"].authority, writer.generation_id)
    from dpone.contracts.native_generation_admission import generation_admission_request_bytes

    reservation = generation_admission_request_bytes(
        subject=subject,
        workspace_attempt=plan.workspace_attempt,
        guard=plan.guard,
        profile=writer.profile,
        command=writer.command,
        requested_bytes=100,
    )
    writer = replace(writer, reservation=OriginalRef(writer.reservation.locator, digest(reservation)))
    return subject, plan_ref, plan_bytes, writer, command_bytes


def reservation_payload(value):
    from dpone.contracts.native_generation_admission import generation_admission_request_bytes

    return generation_admission_request_bytes(
        subject=value.subject,
        workspace_attempt=value.plan.workspace_attempt,
        guard=value.plan.guard,
        profile=value.executor.profile,
        command=value.executor.command,
        requested_bytes=100,
    )
