import pickle


def test_observe_departure_models_are_reexported_with_legacy_pickle_identity() -> None:
    from dpone.contracts import mssql_sqlclient_observe_departure as legacy
    from dpone.contracts import mssql_sqlclient_observe_departure_models as models

    names = (
        "SqlClientObserveDeparturePlan",
        "SqlClientObserveDepartureRequest",
        "SqlClientObserveDepartureResult",
        "SqlClientObserveContainment",
        "SqlClientObserveContainmentReceipt",
        "SqlClientObserveContainmentObservation",
    )
    for name in names:
        model = getattr(models, name)
        assert getattr(legacy, name) is model
        legacy_global = f"cdpone.contracts.mssql_sqlclient_observe_departure\n{name}\n.".encode()
        assert pickle.loads(legacy_global) is model
