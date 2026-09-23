"""P10f terminal projection is exact, one-shot, and credential-free."""

from dataclasses import fields, replace

import pytest

from dpone.contracts.mssql_sqlclient_evidence_types import evidence_name
from dpone.contracts.mssql_sqlclient_native_chunk import SqlClientNativeChunkProjection
from dpone.contracts.mssql_sqlclient_stage_identity import stage_object_identity
from dpone.contracts.mssql_tds_directory import directory_key
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.services.mssql_tds_writer_execution import execute_sqlclient_writer
from dpone.services.mssql_tds_writer_settlement import (
    SqlClientTerminalProjectionUnknown,
    project_sqlclient_native_chunk,
    settle_sqlclient_writer,
)
from tests.test_mssql_tds_writer_execution import _install_process, _ready, _result
from tests.test_mssql_tds_writer_launch import setup as setup
from tests.test_mssql_tds_writer_settlement import Verifier, _observation


def _project(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    exited = execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    class LazyVerifier(Verifier):
        def __init__(self):
            super().__init__(None)

        def observe(self, **kwargs):
            owner = type("OwnerView", (), kwargs)()
            owner.observation = kwargs["writer_observation"]
            owner.writer_admission = kwargs["writer_admission"]
            owner.stage = kwargs["stage"]
            owner.content_expectation = kwargs["expectation"]
            return _observation(owner)

    terminal = settle_sqlclient_writer(exited, LazyVerifier())
    return terminal, project_sqlclient_native_chunk(terminal)


def test_projection_binds_exact_attempt_stage_input_evidence_and_directory(setup, monkeypatch):
    terminal, projection = _project(setup, monkeypatch)
    identity = setup.plan.attempt.state.identity
    expected = setup.plan.input_descriptor.expected

    assert type(projection) is SqlClientNativeChunkProjection
    assert projection.attempt == identity
    assert projection.attempt_sha256 == attempt_identity_digest(identity)
    assert projection.object_identity == stage_object_identity(projection.stage)
    assert (projection.rows, projection.encoded_bytes, projection.file_sha256) == (
        expected.rows,
        expected.encoded_bytes,
        expected.file_sha256,
    )
    assert projection.typed_sum == 0
    assert projection.registration_receipt.attempt_sha256 == projection.attempt_sha256
    assert projection.verification_receipt.payload_sha256 == setup.lifecycle.snapshot.state.verification_sha256
    assert projection.lifecycle_revision == setup.lifecycle.snapshot.revision
    assert projection.directory_key == directory_key(identity)
    assert projection.directory_coordinate.ordinal == identity.ordinal
    assert projection.implementation_sha256 == identity.implementation_sha256
    assert projection.worker_build_sha256 == setup.plan.build_sha256
    assert repr(terminal) == "SqlClientWriterVerified(<opaque>)"
    assert not any(hasattr(terminal, name) for name in ("_record", "_receipt", "_state", "_projection"))
    forbidden = {"credential", "password", "descriptor", "process", "resource", "handle", "authority"}
    assert not forbidden.intersection(field.name for field in fields(projection))


@pytest.mark.parametrize(
    "nested",
    ["attempt", "stage", "object", "receipt", "directory", "lifecycle", "implementation"],
)
def test_projection_rejects_every_nested_binding_substitution(setup, monkeypatch, nested):
    _, projection = _project(setup, monkeypatch)
    with pytest.raises(ValueError, match="terminal_projection_invalid"):
        if nested == "attempt":
            replace(projection, attempt=replace(projection.attempt, ordinal=projection.attempt.ordinal + 1))
        elif nested == "stage":
            replace(projection, stage=replace(projection.stage, table_name=projection.stage.table_name + "x"))
        elif nested == "object":
            replace(projection, object_identity=replace(projection.object_identity, fingerprint="f" * 64))
        elif nested == "receipt":
            receipt = replace(
                projection.verification_receipt,
                payload_sha256="e" * 64,
                relative_name=evidence_name(projection.attempt_sha256, projection.verification_receipt.kind, "e" * 64),
            )
            replace(projection, verification_receipt=receipt)
        elif nested == "directory":
            replace(
                projection,
                directory_coordinate=replace(
                    projection.directory_coordinate,
                    attempt=(projection.directory_coordinate.attempt + 1) % 3,
                ),
            )
        elif nested == "lifecycle":
            replace(projection, lifecycle_revision=projection.lifecycle_revision + 1)
        else:
            replace(projection, helper_implementation_sha256="d" * 64)


def test_projection_consumes_exact_terminal_once(setup, monkeypatch):
    terminal, projection = _project(setup, monkeypatch)
    assert projection.rows >= 0
    with pytest.raises(SqlClientTerminalProjectionUnknown):
        project_sqlclient_native_chunk(terminal)
    with pytest.raises(SqlClientTerminalProjectionUnknown):
        project_sqlclient_native_chunk(object())  # type: ignore[arg-type]
