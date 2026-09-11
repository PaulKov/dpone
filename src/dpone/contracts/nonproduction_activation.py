"""Exact execution-grant binding for the isolated synthetic activation family.

This request preserves the existing physical guards, runtime context and
attempt/receipt shapes. It is a distinct wire family: the production v1 reader
must reject it. Its digest hashes the exact canonical UTF-8 bytes, including the
mandatory execution-grant digest, without legacy path normalization.

These immutable claims cannot authenticate a grant or authorize physical access.
Protected admission must separately verify the original signature and pinned
trust, reconstruct native/source/scope subjects, enforce enrollment and consume
shared campaign budgets atomically before issuing execution authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Any

from dpone.contracts.composition_activation import (
    CompositionActivationRequest,
    CompositionOccurrenceContext,
    CompositionPhysicalResource,
    CompositionWorkloadAdmission,
)
from dpone.contracts.composition_persistence import activation_request_fields
from dpone.contracts.nonproduction_grants import NonproductionExecutionGrant
from dpone.contracts.nonproduction_scope import (
    NonproductionAuthorityError,
    canonical_document,
    digest,
    document_sha256,
    exact_fields,
    parse_document,
)
from dpone.contracts.strict_json import strict_json_object

SCHEMA = "dpone.composition-activation-request.nonproduction.v1"
_CONSTITUENTS = {
    "sqlserver_dbt_v1": "native",
    "mssql_clickhouse_full_refresh_v1": "native",
    "postgres_mssql_full_refresh_v1": "standalone",
}


@dataclass(frozen=True, slots=True)
class NonproductionCompositionActivationRequest(CompositionActivationRequest):
    """Complete three-cell parent with at most 64 workloads and one exact grant.

    The inherited validator retains the complete, unique physical-write
    partition. Source effects and physical isolation remain independently
    reconstructed admission inputs; cell labels and digests are not evidence.
    ``runtime_context_sha256`` keeps its existing meaning and carries no grant.
    """

    execution_grant_sha256: str

    def __post_init__(self) -> None:
        digest(self.execution_grant_sha256)
        if (
            type(self.context) is not CompositionOccurrenceContext
            or type(self.workloads) is not tuple
            or not 1 <= len(self.workloads) <= 64
            or any(type(row) is not CompositionWorkloadAdmission for row in self.workloads)
            or type(self.resources) is not tuple
            or any(type(row) is not CompositionPhysicalResource for row in self.resources)
        ):
            raise NonproductionAuthorityError("activation_shape")
        CompositionActivationRequest.__post_init__(self)
        if {row.execution_cell for row in self.workloads} != set(_CONSTITUENTS) or any(
            row.constituent_id != _CONSTITUENTS[row.execution_cell] for row in self.workloads
        ):
            raise NonproductionAuthorityError("complete_execution_cells")

    def to_dict(self) -> dict[str, Any]:
        """Return a detached closed JSON object within the NP 1 MiB bound."""
        self.__post_init__()
        return strict_json_object(canonical_document({"schema": SCHEMA, **asdict(self)}))

    def to_bytes(self) -> bytes:
        """Return the original canonical wire format for this explicit family."""
        return canonical_document(self.to_dict())

    @property
    def request_sha256(self) -> str:
        """Digest of exact canonical bytes, including the execution grant."""
        return document_sha256(self.to_dict())

    @classmethod
    def from_bytes(cls, raw: bytes) -> NonproductionCompositionActivationRequest:
        """Decode only canonical NP bytes; no legacy or coercing fallback.

        The computed digest is a value, not an authenticated readback. Protected
        persistence must compare it with the independently expected request key.
        """
        try:
            body = exact_fields(parse_document(raw, SCHEMA), {"schema", *(field.name for field in fields(cls))})
            value = cls(**activation_request_fields(body), execution_grant_sha256=body["execution_grant_sha256"])
            if value.to_bytes() != raw:
                raise NonproductionAuthorityError("activation_document")
            return value
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
            raise NonproductionAuthorityError("activation_document") from None

    def require_execution_grant(self, grant: NonproductionExecutionGrant) -> None:
        """Compare exact hash, parent and complete workload membership only.

        This does not check signatures, clocks, revocation, native ancestry,
        source closure, enrolled scope or consumption. A matching caller DTO
        grants no permission; trusted authentication and admission remain
        mandatory. Workload limits still require protected runtime enforcement.
        """
        self.__post_init__()
        if type(grant) is not NonproductionExecutionGrant:
            raise NonproductionAuthorityError("grant_phase")
        grant.__post_init__()
        if len(self.workloads) > grant.limits.max_workloads:
            raise NonproductionAuthorityError("workload_budget")
        if (
            self.execution_grant_sha256,
            self.release_id,
            self.deployment_id,
            self.activation_id,
            tuple((row.workload_id, row.constituent_id, row.pack_sha256) for row in self.workloads),
        ) != (
            grant.grant_sha256,
            grant.parent_release_id,
            grant.deployment_id,
            grant.activation_id,
            tuple((row.workload_id, row.constituent_id, row.pack_sha256) for row in grant.workloads),
        ):
            raise NonproductionAuthorityError("execution_grant_subject")
