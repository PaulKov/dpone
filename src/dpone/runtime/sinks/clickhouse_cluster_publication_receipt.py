"""Serializable staged-load receipt for ClickHouse cluster publication."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from dpone.ports.clickhouse_cluster_publication import contracts
from dpone.runtime.sinks.clickhouse_full_refresh_contract import FullRefreshPublicationMarker

CLUSTER_RECEIPT_VERSION = "dpone.clickhouse.cluster-full-refresh-receipt.v1"


@dataclass(frozen=True, slots=True)
class ClusterFullRefreshReceipt:
    """Proof used by staged-load cleanup and machine-readable evidence."""

    marker: FullRefreshPublicationMarker
    authority: contracts.AuthorityRecord
    authority_version: int
    cluster: str
    schema_version: str = CLUSTER_RECEIPT_VERSION

    @classmethod
    def from_authority(
        cls,
        current: contracts.VersionedAuthorityRecord,
        cluster: str,
    ) -> ClusterFullRefreshReceipt:
        """Build the stable receipt from the currently verified authority row."""

        record = current.record
        marker = FullRefreshPublicationMarker.create(
            operation_id=record.operation_id,
            database=record.database,
            target=record.target,
            candidate=record.candidate,
            predecessor_uuid=record.predecessor.uuid if record.predecessor else None,
            desired_uuid=record.desired.uuid,
            staged_rows=record.staged_rows,
        )
        return cls(
            marker=marker,
            authority=record,
            authority_version=current.version,
            cluster=cluster,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "marker": asdict(self.marker),
            "authority": json.loads(self.authority.payload),
            "authority_version": self.authority_version,
            "cluster": self.cluster,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ClusterFullRefreshReceipt:
        if value.get("schema_version") != CLUSTER_RECEIPT_VERSION:
            raise contracts.ClusterPublicationError(
                "DPONE_CLICKHOUSE_CLUSTER_RECEIPT_INVALID", "schema version mismatch"
            )
        authority = authority_from_mapping(_mapping(value.get("authority")))
        marker = FullRefreshPublicationMarker(**_mapping(value.get("marker")))
        return cls(
            marker=marker,
            authority=authority,
            authority_version=int(value["authority_version"]),
            cluster=str(value["cluster"]),
        )


def authority_from_mapping(value: dict[str, Any]) -> contracts.AuthorityRecord:
    value["phase"] = contracts.AuthorityPhase(value["phase"])
    value["desired"] = contracts.GenerationIdentity(**_mapping(value["desired"]))
    if value.get("predecessor") is not None:
        value["predecessor"] = contracts.GenerationIdentity(**_mapping(value["predecessor"]))
    return contracts.AuthorityRecord(**value)


def _mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise contracts.ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_RECEIPT_INVALID", "mapping required")
    return dict(value)
