"""Immutable operation-origin observation, not a recovery authorization.

The selected authority supplies this value only after acknowledging its read
transaction. Consumers still need current ownership, physical/DDL observation
and authenticated governance before taking any recovery action.
"""

from dataclasses import dataclass
from datetime import datetime

from dpone.contracts.clickhouse_cluster_publication import VersionedAuthorityRecord


@dataclass(frozen=True, slots=True)
class NativePublicationPreparation:
    binding_digest: str
    prepared: VersionedAuthorityRecord
    current: VersionedAuthorityRecord
    prepared_at: datetime
