"""Authentication ordering and compatibility; synthetic originals are not qualification."""

from dataclasses import replace
from importlib import import_module
from uuid import UUID

import pytest

from dpone.adapters.native_generation_invocation_auth import InvocationOriginalReader, authenticate_invocation
from dpone.contracts.native_source_custody import NativeSourceCustodyError
from dpone.contracts.native_trusted_dbt_environment_codec import (
    decode_trusted_dbt_owned_root,
    decode_trusted_dbt_qualification,
    encode_trusted_dbt_owned_root,
    encode_trusted_dbt_qualification,
)
from tests.native_trusted_dbt_fixtures import InvocationFixture

LOCATORS = (
    "invocation/command",
    "invocation/toolchain",
    "invocation/qualification",
    "roots/PROJECT",
    "roots/OUTPUT",
    "roots/PROFILE",
)


def authentication(fixture, monkeypatch):
    reader = InvocationOriginalReader(
        originals=fixture.store, bindings=fixture.store, subject=fixture.store.subject, max_bytes=65536
    )
    trace = []
    read = reader.read

    def observed(reference, kind):
        trace.append(reference.locator)
        return read(reference, kind)

    monkeypatch.setattr(reader, "read", observed)
    return (
        reader,
        trace,
        {
            "executor": fixture.executor,
            "command": fixture.executor.command,
            "toolchain": fixture.recorder._toolchain,
            "qualification": fixture.recorder._qualification,
        },
    )


def test_authentication_reads_each_original_fresh_in_order(tmp_path, monkeypatch):
    fixture = InvocationFixture(tmp_path)
    reader, trace, arguments = authentication(fixture, monkeypatch)
    authenticate_invocation(reader, **arguments)
    authenticate_invocation(reader, **arguments)
    assert trace == list(LOCATORS) * 2
    bound = fixture.store.bound[LOCATORS[1]]
    fixture.store.bound[LOCATORS[1]] = replace(bound, kind="trusted_dbt_owned_root_v1")
    with pytest.raises(NativeSourceCustodyError, match="binding differs"):
        authenticate_invocation(reader, **arguments)
    assert trace == list(LOCATORS) * 2 + list(LOCATORS[:2])
    assert fixture.delegate.calls == []


@pytest.mark.parametrize("mismatch", ["generation", "command"])
def test_admission_mismatch_precedes_any_read(tmp_path, monkeypatch, mismatch):
    fixture = InvocationFixture(tmp_path)
    reader, trace, arguments = authentication(fixture, monkeypatch)
    if mismatch == "generation":
        arguments["executor"] = replace(fixture.executor, generation_id=UUID(int=99))
    else:
        arguments["command"] = fixture.executor.profile
    with pytest.raises(NativeSourceCustodyError):
        authenticate_invocation(reader, **arguments)
    assert trace == []


def test_qualification_mismatch_precedes_invocation_and_root_checks(tmp_path, monkeypatch):
    fixture = InvocationFixture(tmp_path)
    reader, trace, arguments = authentication(fixture, monkeypatch)
    qualified = decode_trusted_dbt_qualification(fixture.store.documents[LOCATORS[2]])
    arguments["qualification"] = fixture.store.add(
        "trusted_dbt_qualification_v1",
        LOCATORS[2],
        encode_trusted_dbt_qualification(replace(qualified, profile=fixture.executor.reservation)),
    )
    arguments["executor"] = replace(fixture.executor, invocation_id=UUID(int=99))
    with pytest.raises(NativeSourceCustodyError, match="qualification differs"):
        authenticate_invocation(reader, **arguments)
    assert trace == list(LOCATORS[:3])


def test_all_roots_are_acquired_before_role_validation(tmp_path, monkeypatch):
    fixture = InvocationFixture(tmp_path)
    reader, trace, arguments = authentication(fixture, monkeypatch)
    original_read = reader.read

    def changed_root(reference, kind):
        payload = original_read(reference, kind)
        if reference == fixture.plan.project_root:
            root = decode_trusted_dbt_owned_root(payload)
            return encode_trusted_dbt_owned_root(replace(root, executor_invocation_id=UUID(int=99)))
        return payload

    # The read port is deliberately substituted to isolate post-decode policy order.
    monkeypatch.setattr(reader, "read", changed_root)
    with pytest.raises(NativeSourceCustodyError, match="root role or invocation"):
        authenticate_invocation(reader, **arguments)
    assert trace == list(LOCATORS)


@pytest.mark.parametrize(
    "symbol",
    [
        "InvocationOriginalReader",
        "authenticate_invocation",
        "verify_invocation_paths",
        "AuthenticatedInvocationPlan",
    ],
)
def test_invocation_symbols_have_resolvable_canonical_owners(symbol):
    layer = (
        "contracts.native_generation_invocation"
        if symbol == "AuthenticatedInvocationPlan"
        else "adapters.native_generation_invocation_auth"
    )
    canonical = import_module("dpone." + layer)
    assert getattr(canonical, symbol).__module__ == "dpone." + layer


def test_literal_argument_failure_precedes_filesystem_observation(tmp_path, monkeypatch):
    from pathlib import Path

    from dpone.adapters.native_generation_invocation_auth import verify_invocation_paths

    fixture = InvocationFixture(tmp_path)
    reader, _, arguments = authentication(fixture, monkeypatch)
    admitted = authenticate_invocation(reader, **arguments)
    observations = []
    lstat = Path.lstat

    def observed(path, *args, **kwargs):
        observations.append(path)
        return lstat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", observed)
    argv = ("unadmitted-executable", *fixture.args(0)[1:])
    with pytest.raises(NativeSourceCustodyError, match="arguments differ"):
        verify_invocation_paths(admitted, position=0, args=argv, cwd=fixture.project, previous_profile=None)
    assert observations == []
