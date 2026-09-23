"""The permission evidence gateway remains deliberately narrow."""

from dpone.ports.mssql_sqlclient_permission_grant_evidence import SqlClientPermissionGrantEvidenceGateway


def test_gateway_has_only_observation_write_and_close():
    public = {name for name in vars(SqlClientPermissionGrantEvidenceGateway) if not name.startswith("_")}
    assert public == {"observation", "write", "close"}
