from types import SimpleNamespace

from dpone.readiness.mssql_sqlclient import MssqlSqlClientReadinessService


def test_doctor_reports_verified_public_identities_without_command_or_credentials(monkeypatch):
    companion = SimpleNamespace(
        package_version="0.88.0",
        protocol="dpone.mssql-sqlclient.ipc.v1",
        runtime_major=10,
        artifact_sha256="a" * 64,
        writer_identity_sha256="b" * 64,
        runtime_identity_sha256="c" * 64,
    )
    monkeypatch.setattr(
        "dpone.readiness.mssql_sqlclient.import_module",
        lambda name: SimpleNamespace(locate=lambda: companion),
    )
    payload = MssqlSqlClientReadinessService().doctor()
    assert payload["ready"] is True
    assert payload["artifact_sha256"] == "a" * 64
    assert "command" not in payload and "credentials" not in payload


def test_doctor_reports_closed_optional_package_blocker(monkeypatch):
    def missing(name):
        raise ImportError(name)

    monkeypatch.setattr("dpone.readiness.mssql_sqlclient.import_module", missing)
    payload = MssqlSqlClientReadinessService().doctor()
    assert payload["ready"] is False
    assert payload["blocker_codes"] == ["mssql_sqlclient.optional_package_required"]
