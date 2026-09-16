"""Closed invocation membership and positive-only completion contracts."""

from dataclasses import replace

import pytest

from dpone.contracts.native_generation_invocation import (
    TrustedDbtCommandEntry,
    TrustedDbtCommandPlan,
    TrustedDbtInvocationCompletion,
    decode_trusted_dbt_command_plan,
    decode_trusted_dbt_invocation_completion,
    encode_trusted_dbt_command_plan,
    encode_trusted_dbt_invocation_completion,
)
from dpone.contracts.native_source_custody import NativeSourceCustodyError
from tests.test_native_generation_admission import executor


def command_plan(phase="BUILD"):
    binding = executor()
    verbs = ("parse", "ls", "build") if phase == "BUILD" else ("test",)
    return TrustedDbtCommandPlan(
        schema="dpone.trusted-dbt-command-plan.v1",
        executor_invocation_id=binding.invocation_id,
        phase=phase,
        commands=tuple(TrustedDbtCommandEntry(index, verb, ("dbt", verb), 10, 2) for index, verb in enumerate(verbs)),
        project_root=binding.reservation,
        output_root=binding.command,
        profile_root=binding.profile,
        total_termination_budget_seconds=36,
    )


def completion():
    binding = executor()
    return TrustedDbtInvocationCompletion(
        schema="dpone.trusted-dbt-invocation-completion.v1",
        executor=binding,
        command=binding.command,
        toolchain=binding.profile,
        qualification=binding.reservation,
        started_at="2026-09-15T10:00:00.000001Z",
        finished_at="2026-09-15T10:00:01.000001Z",
        elapsed_microseconds=1000000,
        exit_code=0,
        command_count=3,
    )


def test_build_and_quality_plans_have_closed_ordered_membership():
    assert tuple(entry.verb for entry in command_plan().commands) == ("parse", "ls", "build")
    assert tuple(entry.verb for entry in command_plan("QUALITY").commands) == ("test",)
    with pytest.raises(NativeSourceCustodyError):
        replace(command_plan(), commands=command_plan().commands[::-1])
    with pytest.raises(NativeSourceCustodyError):
        replace(command_plan(), commands=command_plan().commands[:2])


@pytest.mark.parametrize("field", ["position", "command_timeout_seconds", "termination_allowance_seconds"])
def test_entry_integer_fields_reject_boolean(field):
    with pytest.raises(NativeSourceCustodyError):
        replace(command_plan().commands[0], **{field: True})


@pytest.mark.parametrize(
    "field,value",
    [
        ("exit_code", False),
        ("exit_code", 1),
        ("elapsed_microseconds", True),
        ("elapsed_microseconds", -1),
        ("command_count", 2),
        ("command_count", True),
    ],
)
def test_positive_completion_cannot_encode_abnormal_or_coerced_outcome(field, value):
    with pytest.raises(NativeSourceCustodyError):
        replace(completion(), **{field: value})


@pytest.mark.parametrize(
    "timestamp",
    ["2026-09-15T10:00:00Z", "2026-09-15T10:00:00.000001+00:00", "2026-09-15T10:00:00.000001+03:00", "invalid"],
)
def test_completion_requires_exact_utc_microsecond_timestamps(timestamp):
    with pytest.raises(NativeSourceCustodyError):
        replace(completion(), started_at=timestamp)


def test_wall_clock_reversal_does_not_override_positive_monotonic_elapsed():
    value = replace(completion(), finished_at="2026-09-15T09:59:59.000001Z")
    assert value.elapsed_microseconds == 1000000


@pytest.mark.parametrize(
    "factory,encode,decode",
    [
        (command_plan, encode_trusted_dbt_command_plan, decode_trusted_dbt_command_plan),
        (completion, encode_trusted_dbt_invocation_completion, decode_trusted_dbt_invocation_completion),
    ],
)
def test_invocation_wire_roundtrip_requires_exact_canonical_fields(factory, encode, decode):
    value = factory()
    payload = encode(value)
    assert decode(payload) == value
    with pytest.raises(NativeSourceCustodyError):
        decode(b" " + payload)
    with pytest.raises(NativeSourceCustodyError):
        decode(payload[:-1] + b',"success":true}')
