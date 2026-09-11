"""Fixed optional SQL gate layout, associated with the shared execution journal.

Lengths are physical catalog bytes. Gate and evidence associations retain old
proof originals; a family FK prevents qualification from borrowing SQL issuance.
Enrollment remains externally provisioned on fresh exclusive physical databases.
"""

from dpone.adapters.composition_mssql_layout import (
    CompositionCheck,
    CompositionColumn,
    CompositionForeignKey,
    CompositionKey,
    CompositionTable,
)


def _digest(name: str, *, nullable: bool = False) -> CompositionColumn:
    return CompositionColumn(name, "varchar", 71, nullable)


def _name(name: str, length: int = 128) -> CompositionColumn:
    return CompositionColumn(name, "nvarchar", length * 2)


COMPOSITION_GATE_TABLES = (
    CompositionTable(
        "login_gates",
        (
            _digest("operation_key"),
            CompositionColumn("operation_family", "varchar", 13),
            CompositionColumn("login_sid", "binary", 16),
            _name("login_name"),
            CompositionColumn("gate_state", "varchar", 16),
            _digest("disabled_evidence_sha256", nullable=True),
        ),
        (
            CompositionKey("pk_c_login_gates", ("operation_key",), True),
            CompositionKey("uq_c_login_sid", ("login_sid",)),
            CompositionKey("uq_c_login_name", ("login_name",)),
        ),
        (
            CompositionForeignKey(
                "fk_c_login_operation",
                ("operation_key", "operation_family"),
                "operations",
                ("operation_key", "operation_family"),
            ),
        ),
        (
            CompositionCheck(
                "ck_c_login_family", "[operation_family]='execution' AND DATALENGTH([operation_family])=9"
            ),
            CompositionCheck("ck_c_login_state", "[gate_state] IN ('JOURNALED', 'READY', 'CLOSING', 'CLOSED')"),
            CompositionCheck("ck_c_login_closed", "[gate_state]<>'CLOSED' OR [disabled_evidence_sha256] IS NOT NULL"),
        ),
    ),
    CompositionTable(
        "mssql_enrollments",
        (
            _digest("guard_id"),
            _name("database_name"),
            CompositionColumn("database_id", "int", 4),
            CompositionColumn("database_guid", "uniqueidentifier", 16),
            _name("database_create_token", 33),
            _name("writer_role"),
        ),
        (
            CompositionKey("pk_c_mssql_enrollments", ("guard_id",), True),
            CompositionKey("uq_c_mssql_database_name", ("database_name",)),
            CompositionKey("uq_c_mssql_database_id", ("database_id",)),
        ),
        (CompositionForeignKey("fk_c_mssql_enrollment_domain", ("guard_id",), "domains", ("guard_id",)),),
        (CompositionCheck("ck_c_mssql_database_id", "[database_id]>4"),),
    ),
    CompositionTable(
        "mssql_managed_schemas",
        (_digest("guard_id"), _name("schema_name")),
        (CompositionKey("pk_c_mssql_managed_schemas", ("guard_id", "schema_name"), True),),
        (CompositionForeignKey("fk_c_mssql_schema_enrollment", ("guard_id",), "mssql_enrollments", ("guard_id",)),),
    ),
    CompositionTable(
        "mssql_gate_evidence",
        (
            _digest("evidence_sha256"),
            _digest("operation_key"),
            CompositionColumn("evidence_document", "varbinary", -1),
        ),
        (CompositionKey("pk_c_mssql_gate_evidence", ("evidence_sha256",), True),),
        (
            CompositionForeignKey(
                "fk_c_mssql_gate_evidence_operation", ("operation_key",), "operations", ("operation_key",)
            ),
        ),
        (
            CompositionCheck(
                "ck_c_mssql_gate_evidence_document", "DATALENGTH([evidence_document]) BETWEEN 1 AND 8388608"
            ),
        ),
    ),
)
