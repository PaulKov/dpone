from __future__ import annotations

import pickle


def test_departure_model_surface_preserves_exact_class_and_enum_identity() -> None:
    from dpone.contracts import mssql_sqlclient_departure_models as models
    from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
        SqlClientDepartureEvidenceKind,
        SqlClientDepartureEvidenceReceipt,
    )
    from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
    from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
    from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsChildExit, TdsProcessIdentity

    assert models.SqlClientDepartureEvidenceKind is SqlClientDepartureEvidenceKind
    assert models.SqlClientDepartureEvidenceReceipt is SqlClientDepartureEvidenceReceipt
    assert models.SqlClientObserverAdmission is SqlClientObserverAdmission
    assert models.TdsCoordinatorStartup is TdsCoordinatorStartup
    assert models.TdsAttemptIdentity is TdsAttemptIdentity
    assert models.TdsChildExit is TdsChildExit
    assert models.TdsProcessIdentity is TdsProcessIdentity


def test_departure_model_surface_keeps_pickle_lookup_stable() -> None:
    from dpone.contracts import mssql_sqlclient_departure_models as models
    from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind

    value = models.SqlClientDepartureEvidenceKind.LOCAL_EXIT
    restored = pickle.loads(pickle.dumps(value))
    assert restored is value
    assert type(restored) is SqlClientDepartureEvidenceKind
