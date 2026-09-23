"""Contract fixtures and exact-result checks for P9a."""

from dataclasses import replace
from datetime import datetime
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_restricted_writer_verify import (
    RestrictedWriterVerifyDomainOperations,
    SqlClientEffectivePermission,
    SqlClientRestrictedWriterVerifyRequest,
    SqlClientTokenRow,
    restricted_writer_fingerprint,
    validate_verify_result,
)
from dpone.contracts.mssql_tds_session import TdsRestrictedRemoteSessionIdentity
from tests.test_mssql_sqlclient_permission_grant import request as grant_request


def verify_request():
    grant = grant_request()
    return SqlClientRestrictedWriterVerifyRequest(
        parent=grant.parent,
        stage=grant.stage,
        writer=grant.writer,
        writer_login=grant.writer_login,
        login_token=(SqlClientTokenRow(30, b"writer".hex(), "writer", "SQL LOGIN", "GRANT OR DENY"),),
        user_token=(SqlClientTokenRow(5, b"writer".hex(), "writer", "SQL USER", "GRANT OR DENY"),),
        server_permissions=(SqlClientEffectivePermission(None, None, "CONNECT SQL"),),
        database_permissions=(SqlClientEffectivePermission(None, None, "CONNECT"),),
        operation_id=UUID(int=41),
        implementation_sha256="e" * 64,
    )


def verify_result(request=None):
    request = verify_request() if request is None else request
    operations = RestrictedWriterVerifyDomainOperations()
    remote = TdsRestrictedRemoteSessionIdentity(UUID(int=7), 51, datetime(2026, 1, 2), b"n" * 32, b"a" * 32)
    context = operations.context(
        remote,
        (
            request.writer_login.name,
            request.writer_login.sid,
            request.writer_login.name,
            request.writer_login.sid,
            request.writer_login.authenticating_database_id,
            request.stage.database_id,
            request.stage.database_name,
            request.writer.principal_id,
            request.writer.name,
            request.writer.sid,
            False,
            False,
            False,
            1,
            0,
            0,
            0,
            False,
        ),
    )
    return operations.result(
        opening=context,
        login_token=operations.tokens(
            tuple((v.principal_id, v.sid, v.name, v.type, v.usage) for v in request.login_token)
        ),
        user_token=operations.tokens(
            tuple((v.principal_id, v.sid, v.name, v.type, v.usage) for v in request.user_token)
        ),
        server_permissions=operations.permissions(
            tuple((v.entity_name, v.subentity_name, v.permission_name) for v in request.server_permissions)
        ),
        database_permissions=operations.permissions(
            tuple((v.entity_name, v.subentity_name, v.permission_name) for v in request.database_permissions)
        ),
        stage_permissions=("INSERT", "SELECT", "VIEW DEFINITION"),
        stage=request.stage,
        empty_count=0,
        closing=context,
        closing_stage_permissions=("INSERT", "SELECT", "VIEW DEFINITION"),
        closing_stage=request.stage,
        closing_empty_count=0,
    )


def test_exact_result_is_accepted():
    validate_verify_result(verify_request(), verify_result())


def test_autocommit_select_may_report_committable_xact_state_without_open_transaction():
    request, result = verify_request(), verify_result()
    context = replace(result.opening, xact_state=1)
    validate_verify_result(request, replace(result, opening=context, closing=context))
    with pytest.raises(ValueError, match="restricted_writer_verify_invalid"):
        doomed = replace(context, xact_state=-1)
        validate_verify_result(request, replace(result, opening=doomed, closing=doomed))


def test_name_fingerprints_are_field_domain_separated():
    value = "same-sensitive-name"
    assert len({restricted_writer_fingerprint(kind, value) for kind in ("login", "database", "user", "token")}) == 4


@pytest.mark.parametrize(
    "fault",
    (
        "extra-update",
        "extra-delete",
        "extra-alter",
        "nonempty",
        "changed-session",
        "changed-original",
        "changed-token",
        "overprivileged",
    ),
)
def test_result_drift_is_rejected(fault):
    request, result = verify_request(), verify_result()
    with pytest.raises(ValueError, match="restricted_writer_verify_invalid"):
        if fault.startswith("extra-"):
            result = replace(result, stage_permissions=result.stage_permissions + (fault[6:].upper(),))
        elif fault == "nonempty":
            result = replace(result, empty_count=1)
        elif fault == "changed-session":
            result = replace(result, closing=replace(result.closing, request_count=2))
        elif fault == "changed-original":
            result = replace(result, opening=replace(result.opening, original_login_name_sha256="f" * 64))
        elif fault == "changed-token":
            result = replace(result, login_token=result.login_token + result.login_token)
        else:
            result = replace(result, opening=replace(result.opening, is_sysadmin=True))
        validate_verify_result(request, result)


def test_stage_permission_sql_has_no_allow_list_and_reads_overflow_row():
    from dpone.adapters.mssql_sqlclient_restricted_writer_verify_sql import STAGE_PERMISSIONS_SQL

    assert "WHERE" not in STAGE_PERMISSIONS_SQL.upper()
    assert "SELECT DISTINCT TOP (4)" in " ".join(STAGE_PERMISSIONS_SQL.upper().split())


def test_live_sql_canonicalizes_sid_and_empty_permission_fields():
    from dpone.adapters.mssql_sqlclient_restricted_writer_verify_sql import (
        CONTEXT_SQL,
        DATABASE_PERMISSIONS_SQL,
        LOGIN_TOKEN_SQL,
        SERVER_PERMISSIONS_SQL,
        USER_TOKEN_SQL,
    )

    assert CONTEXT_SQL.upper().count("LOWER(CONVERT(VARCHAR(170)") == 3
    for sql in (LOGIN_TOKEN_SQL, USER_TOKEN_SQL):
        assert "LOWER(CONVERT(VARCHAR(170),SID,2))" in "".join(sql.upper().split())
    for sql in (SERVER_PERMISSIONS_SQL, DATABASE_PERMISSIONS_SQL):
        compact = "".join(sql.upper().split())
        assert "NULLIF(ENTITY_NAME,N'')" in compact
        assert "NULLIF(SUBENTITY_NAME,N'')" in compact


def test_adapter_returns_fourth_permission_instead_of_filtering_it(monkeypatch):
    from dpone.adapters.mssql_sqlclient_restricted_writer_verify import SqlClientRestrictedWriterVerify
    from dpone.app.mssql_sqlclient_restricted_writer_verify_request import RestrictedWriterVerifySqlOperations

    verifier = object.__new__(SqlClientRestrictedWriterVerify)
    verifier._contract = RestrictedWriterVerifySqlOperations()
    monkeypatch.setattr(
        verifier,
        "_query",
        lambda sql, args, deadline, limit: (("INSERT",), ("SELECT",), ("UPDATE",), ("VIEW DEFINITION",)),
    )
    assert verifier._stage_permissions(verify_request(), 10.0) == (
        "INSERT",
        "SELECT",
        "UPDATE",
        "VIEW DEFINITION",
    )
