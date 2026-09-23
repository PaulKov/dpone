"""Stable contract surface for the bounded MSSQL TDS capability."""

from dpone.contracts.bounded_window import WindowContractError  # noqa: F401,E402
from dpone.contracts.mssql_native_chunks import (  # noqa: F401,E402
    EncodedNativeFile,
    NativeBulkTransportPolicy,
    NativeChunkPlan,
    NativeChunkReceipt,
    NativeStageComplete,
)
from dpone.contracts.mssql_native_parent_journal import (  # noqa: F401,E402
    NativeParentAuthority,
    canonical_digest,
)

# Observe settlement is one capability boundary.  Re-export its closed contract
# vocabulary here so application services depend on that boundary rather than
# on the storage layout of each individual record type.
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (  # noqa: F401,E402
    SqlClientDepartureEvidenceKind,
    SqlClientDepartureEvidenceObservation,
    SqlClientDepartureEvidenceReceipt,
)
from dpone.contracts.mssql_sqlclient_native_chunk import (  # noqa: F401,E402
    SqlClientDirectoryCoordinate,
    SqlClientNativeChunkProjection,
    bind_sqlclient_native_chunk,
)
from dpone.contracts.mssql_sqlclient_observation import session_authority_digest  # noqa: F401,E402
from dpone.contracts.mssql_sqlclient_observe_departure import (  # noqa: F401,E402
    SqlClientObserveContainment,
    SqlClientObserveContainmentObservation,
    SqlClientObserveDeparturePlan,
    SqlClientObserveDepartureRequest,
    SqlClientObserveDepartureResult,
)
from dpone.contracts.mssql_sqlclient_registration import decode_registration  # noqa: F401,E402
from dpone.contracts.mssql_sqlclient_writer_settlement import decode_writer_settlement  # noqa: F401,E402
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup  # noqa: F401,E402
from dpone.contracts.mssql_tds_directory import (  # noqa: F401,E402
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    directory_key,  # noqa: F401,E402
    record_local_containment,
    record_remote_settlement,
)
from dpone.contracts.mssql_tds_directory_codec import process_identity_digest  # noqa: F401,E402
from dpone.contracts.mssql_tds_result import attempt_identity_digest  # noqa: F401,E402
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds  # noqa: F401,E402
from dpone.contracts.mssql_tds_worker import TdsChildExit  # noqa: F401,E402

from .part_1 import *  # noqa: F403
from .part_1 import _context_records as _context_records  # noqa: F401
from .part_1 import _enum as _enum  # noqa: F401
from .part_1 import _identity_body as _identity_body  # noqa: F401
from .part_1 import _original as _original  # noqa: F401
from .part_1 import _startup as _startup  # noqa: F401
from .part_1 import _typed as _typed  # noqa: F401
from .part_2 import *  # noqa: F403
from .part_2 import _construct as _construct  # noqa: F401
from .part_2 import _contracts as _contracts  # noqa: F401
from .part_2 import _identity as _identity  # noqa: F401
from .part_2 import _integer as _integer  # noqa: F401
from .part_2 import _shape as _shape  # noqa: F401
from .part_2 import _text as _text  # noqa: F401
from .part_2 import _uuid as _uuid  # noqa: F401
