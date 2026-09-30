"""Redacted original status is inspectable, never importable execution authority."""

from __future__ import annotations

import json

import pytest

from dpone.contracts.clickhouse_authority import AuthorityError
from tests.test_clickhouse_candidate_sqlite import complete_create, enrollment, mutation, request, store


def test_status_exports_originals_atomically_without_secrets_or_overwrite(tmp_path):
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    authority = store(private)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        writes = complete_create(authority, invocation)
        grant = writes.register(invocation, mutation(invocation, "insert", 1))
        writes.begin_send(grant)
        authority.retain(value.operation_id, "candidate_request_unclosed")
    report = tmp_path / "status.json"
    authority.write_diagnostics(value.operation_id, report)
    payload = report.read_text()
    data = json.loads(payload)
    assert data["schema_version"] == "dpone.clickhouse.protected-publication-status.v1"
    assert data["safe_to_retry"] is False
    assert data["owner_retained"] is True
    assert data["accepted_requests"] == 2
    assert data["succeeded_requests"] == 1
    assert data["uncertain_requests"] == 1
    assert data["reason"] == "candidate_request_unclosed"
    assert data["observation_kind"] == "not_observed"
    assert data["sealed"] is False
    assert "secret" not in payload and "grant_hash" not in payload and "invocation_hash" not in payload
    assert grant._secret not in payload and invocation._secret not in payload
    with pytest.raises(AuthorityError):
        authority.write_diagnostics(value.operation_id, report)
    assert report.read_text() == payload
    assert authority.inspect(value.operation_id).lifecycle == "retained"


def test_report_cannot_overwrite_authority_or_sidecar(tmp_path):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)):
        for name in ("authority.db", "authority.db-wal", "report.json"):
            with pytest.raises(AuthorityError):
                authority.write_diagnostics(value.operation_id, tmp_path / name)
    assert authority.inspect(value.operation_id).binding.operation_id == value.operation_id


def test_retained_reason_is_a_code_not_raw_sdk_error(tmp_path):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)):
        with pytest.raises(AuthorityError):
            authority.retain(value.operation_id, "password=do-not-persist-this")
    assert authority.inspect(value.operation_id).retained_reason is None
