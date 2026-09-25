"""External I/O boundaries for a bounded protected workspace handover cycle.

Implementations verify artifacts outside SQL transactions. These interfaces do
not confer database registration or mutation authority by themselves.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment, DesiredStateRevision
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest
from dpone.contracts.dbt_workspace_channel import WorkspaceChannel, WorkspaceHandoverError
from dpone.contracts.dbt_workspace_handover import WorkspaceHandoverClaim
from dpone.contracts.dbt_workspace_registration_baseline import WorkspaceAdoptedCurrent
from dpone.ports.dbt_workspace_handover import WorkspaceChannelReadback, WorkspaceStoredOccurrence


@dataclass(frozen=True, slots=True)
class VerifiedWorkspaceDesired:
    """Exact verified remote bytes with their opaque storage revision.

    Shape validation is not signature verification. Only the trusted driver creates
    this value after verifying the retained artifacts and current capabilities.
    """

    desired_json: str
    revision: str

    def __post_init__(self) -> None:
        if not isinstance(self.desired_json, str):
            raise WorkspaceHandoverError("remote_desired_bytes")
        AirflowDesiredDeployment.from_json(self.desired_json)
        DesiredStateRevision(self.revision)

    @property
    def desired(self) -> AirflowDesiredDeployment:
        """Parse the same bytes; do not fetch a second remote publication."""
        return AirflowDesiredDeployment.from_json(self.desired_json)


class WorkspaceHandoverExecutionPort(Protocol):
    """Verified remote artifacts, physical observation and local replica I/O.

    No method may call the legacy workspace prepare/activate coordinator. Replica
    materialization explicitly disables external activation coordination. Historical
    publication authorization and present credential/capability revocation are
    distinct checks: the former must not require yesterday's Git SHA to be today's
    head or substitute a different replica's watcher-specific authority digest.
    """

    def verified_latest(self, channel: WorkspaceChannel) -> VerifiedWorkspaceDesired:
        """Read exact latest bytes/revision and verify supported native closure.

        Reject composed/non-workspace artifacts before any retirement. Remote
        unavailability raises non-success; cached content cannot prove latest.
        """
        ...

    def remote_revision_matches(self, latest: VerifiedWorkspaceDesired) -> bool:
        """Independently reread remote revision, immediately before a claim."""
        ...

    def build_claim(
        self, readback: WorkspaceChannelReadback, latest: VerifiedWorkspaceDesired
    ) -> WorkspaceHandoverClaim:
        """Bind verified closure to the applied predecessor, not publication lineage."""
        ...

    def verify_snapshot(
        self, channel: WorkspaceChannel, snapshot: WorkspaceHandoverClaim | WorkspaceAdoptedCurrent
    ) -> None:
        """Reauthorize exact retained signatures/authorization digest and capabilities.

        Verify channel, artifact integrity and supported native-only closure before
        mutation or replication. Adopted baselines need the same verification as
        claims; do not manufacture a claim or ignore current revocation.
        """
        ...

    def observe_successor(self, claim: WorkspaceHandoverClaim) -> DbtWorkspaceActivationRequest:
        """Observe physical closure only after protected predecessor RETIRED.

        The proposal is not authority. If another writer prepared first, only the
        exact original request returned by the store may be used subsequently.
        """
        ...

    def local_matches(self, occurrence: WorkspaceStoredOccurrence) -> bool:
        """Verify pointer, complete immutable artifacts, UUID and original request."""
        ...

    def replicate(self, occurrence: WorkspaceStoredOccurrence) -> None:
        """Materialize exact artifacts and pointer only; never reserve or activate.

        Keep original UUID/request/epochs, validate credential projection and
        precommit authority, and leave protected completion to the store.
        """
        ...
