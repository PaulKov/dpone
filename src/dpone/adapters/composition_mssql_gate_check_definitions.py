"""Generated from the controlled SQL CHECK round trip; do not hand edit."""

CHECK_DDL_SHA256: str = "sha256:3e251fbe1a24008ccd6e51284f1d8da432c84dcf94b7bc90e9966aa5dee8daf5"
CHECK_DATABASE_COLLATION: str = "SQL_Latin1_General_CP1_CI_AS"
CHECK_DEFINITIONS: dict[str, str] = {
    "ck_c_login_closed": "([gate_state]<>'CLOSED' OR [disabled_evidence_sha256] IS NOT NULL)",
    "ck_c_login_family": "([operation_family]='execution' AND datalength([operation_family])=(9))",
    "ck_c_login_state": "([gate_state]='CLOSED' OR [gate_state]='CLOSING' OR [gate_state]='READY' OR [gate_state]='JOURNALED')",
    "ck_c_mssql_database_id": "([database_id]>(4))",
    "ck_c_mssql_gate_evidence_document": "(datalength([evidence_document])>=(1) AND datalength([evidence_document])<=(8388608))",
}
CHECK_METADATA: dict[str, tuple[int, int]] = {
    "ck_c_login_closed": (0, 1),
    "ck_c_login_family": (2, 1),
    "ck_c_login_state": (5, 1),
    "ck_c_mssql_database_id": (3, 1),
    "ck_c_mssql_gate_evidence_document": (3, 1),
}
