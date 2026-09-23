"""Compatibility facade for original-OBSERVE departure contracts."""

from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    ERROR as ERROR,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    FAILURES as FAILURES,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    SqlClientObserveContainment as SqlClientObserveContainment,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    SqlClientObserveContainmentObservation as SqlClientObserveContainmentObservation,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    SqlClientObserveContainmentReceipt as SqlClientObserveContainmentReceipt,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    SqlClientObserveDeparturePlan as SqlClientObserveDeparturePlan,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    SqlClientObserveDepartureRequest as SqlClientObserveDepartureRequest,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    SqlClientObserveDepartureResult as SqlClientObserveDepartureResult,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    _observation as _observation,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    _operation as _operation,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    _uuid as _uuid,
)
from dpone.contracts.mssql_sqlclient_observe_departure_models import (
    validate_observe_departure_binding as validate_observe_departure_binding,
)

__all__ = [
    "SqlClientObserveContainment",
    "SqlClientObserveContainmentObservation",
    "SqlClientObserveContainmentReceipt",
    "SqlClientObserveDeparturePlan",
    "SqlClientObserveDepartureRequest",
    "SqlClientObserveDepartureResult",
    "validate_observe_departure_binding",
]
