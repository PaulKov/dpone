"""Detached catalog facts accepted by acquisition, without model admission."""

from collections.abc import Mapping
from dataclasses import dataclass

from dpone.contracts.dbt_mssql_physical_catalog_rows import CatalogRow
from dpone.contracts.dbt_mssql_physical_source_identity import PhysicalSourceIdentity


@dataclass(frozen=True, slots=True)
class PhysicalCatalogObservation:
    """Observation-time match, not a receipt, durable lock, or execution grant.

    The reader returns immutable row tuples in a read-only mapping after its
    owned read transaction commits. Direct construction does not authenticate
    facts or establish that acquisition/settlement happened.
    """

    source: PhysicalSourceIdentity
    results: Mapping[str, tuple[CatalogRow, ...]]
