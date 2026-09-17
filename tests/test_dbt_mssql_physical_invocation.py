"""Retained command vars must carry one exact pre-reserve physical invocation."""

from dataclasses import replace

import pytest

from dpone.contracts.dbt_mssql_physical_invocation import PhysicalManagedInvocation, require_managed_command
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from dpone.contracts.native_generation_invocation import encode_trusted_dbt_command_plan
from dpone.contracts.native_identity import OriginalRef
from tests.test_native_trusted_dbt_invocation import command_plan


def invocation():
    return PhysicalManagedInvocation(
        "10000000-0000-0000-0000-000000000001",
        str(command_plan().executor_invocation_id),
        OriginalRef("plan", "sha256:" + "a" * 64),
        "20000000-0000-0000-0000-000000000001",
    )


def payload(vars_text=None):
    value = command_plan()
    text = (
        vars_text
        or encode_native_delivery_json({"__dpone_managed": invocation().to_dict(), "start": "2026-09-16"}).decode()
    )
    return encode_trusted_dbt_command_plan(
        replace(
            value,
            commands=tuple(
                replace(entry, argv_template=("dbt", entry.verb, "--vars", text)) for entry in value.commands
            ),
        )
    )


def test_actual_retained_command_is_not_rewritten():
    original = payload()
    result = require_managed_command(original, expected=invocation())
    assert encode_trusted_dbt_command_plan(result) == original


@pytest.mark.parametrize(
    "mode", ["missing", "future", "duplicate", "wrong_identity", "different_vars", "extra_vars", "alias"]
)
def test_managed_command_rejects_closed_shape_or_sequence_mismatch(mode):
    raw = {"__dpone_managed": invocation().to_dict()}
    if mode == "missing":
        raw = {}
    if mode == "future":
        raw["__dpone_managed"]["reservation"] = {"locator": "x", "sha256": "sha256:" + "b" * 64}
    if mode == "wrong_identity":
        raw["__dpone_managed"]["generation_id"] = "30000000-0000-0000-0000-000000000001"
    text = encode_native_delivery_json(raw).decode()
    if mode == "duplicate":
        text = text[:-1] + ',"__dpone_managed":' + encode_native_delivery_json(invocation().to_dict()).decode() + "}"
    original = payload(text)
    if mode in {"different_vars", "extra_vars", "alias"}:
        from dpone.contracts.native_generation_invocation import decode_trusted_dbt_command_plan

        plan = decode_trusted_dbt_command_plan(original)
        commands = list(plan.commands)
        args = commands[1].argv_template
        args = (
            args + ("--vars", text)
            if mode == "extra_vars"
            else ("dbt", "ls", "--vars=" + text)
            if mode == "alias"
            else args[:-1] + (" " + text,)
        )
        commands[1] = replace(commands[1], argv_template=args)
        original = encode_trusted_dbt_command_plan(replace(plan, commands=tuple(commands)))
    with pytest.raises(ValueError):
        require_managed_command(original, expected=invocation())
