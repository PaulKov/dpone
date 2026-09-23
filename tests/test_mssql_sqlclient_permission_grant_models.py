from __future__ import annotations

import pickle


def test_legacy_grant_exports_preserve_leaf_class_identity() -> None:
    from dpone.contracts import mssql_sqlclient_permission_grant as legacy
    from dpone.contracts import mssql_sqlclient_permission_grant_models as models

    assert legacy.SqlClientPermissionGrantRequest is models.SqlClientPermissionGrantRequest
    assert legacy.SqlClientDirectPermission is models.SqlClientDirectPermission
    assert legacy.SqlClientPermissionGrantEvidence is models.SqlClientPermissionGrantEvidence


def test_legacy_departure_exports_preserve_leaf_class_identity() -> None:
    from dpone.contracts import mssql_sqlclient_permission_grant_departure as legacy
    from dpone.contracts import mssql_sqlclient_permission_grant_models as models

    for name in (
        "SqlClientPermissionGrantDeparturePlan",
        "SqlClientPermissionGrantDepartureRequest",
        "SqlClientPermissionGrantDepartureResult",
        "PermissionGrantDepartureCompletion",
        "SqlClientPermissionGrantDepartureCredentials",
        "PermissionGrantDepartureEvidenceContext",
    ):
        assert getattr(legacy, name) is getattr(models, name)


def test_leaf_models_are_pickle_round_trip_compatible() -> None:
    from dpone.contracts import mssql_sqlclient_permission_grant_models as models

    permission = models.SqlClientDirectPermission(
        1,
        42,
        0,
        None,
        "G",
        "SELECT",
        5,
        "writer",
        "01",
        "SQL_USER",
        6,
        "manager",
        "02",
        "SQL_USER",
    )

    restored = pickle.loads(pickle.dumps(permission))
    assert restored == permission
    assert type(restored) is models.SqlClientDirectPermission
