"""Self-binding checkpoint CAS adapter for a retired native parent."""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters import mssql_native_publication_journal as publication

CheckpointCas = Callable[[str, str, int, str], tuple[int, str]]
CheckpointObserve = Callable[[str, str], tuple[int, str] | None]


class SqlClientCheckpointCas:
    """Convert an injected durable CAS acknowledgement into a closed receipt."""

    def __init__(self, *, cas: CheckpointCas, observe: CheckpointObserve) -> None:
        self._cas = cas
        self._observe = observe

    def commit(
        self,
        retirement: publication.NativeParentRetirementReceipt,
        *,
        target_id: str,
        window_fingerprint: str,
        fence: int,
    ) -> publication.NativeCheckpointReceipt:
        """CAS once; reconcile an unknown acknowledgement by exact observation."""
        self._validate(retirement, target_id, window_fingerprint, fence)
        digest = retirement.digest
        try:
            revision, proof = self._cas(target_id, window_fingerprint, fence, digest)
        except Exception as error:
            observed = self._observe(target_id, window_fingerprint)
            if observed is None:
                raise RuntimeError("mssql_sqlclient.checkpoint_outcome_unknown") from error
            revision, proof = observed
        receipt = publication.NativeCheckpointReceipt(digest, target_id, window_fingerprint, fence, revision, proof)
        if proof != self._proof(receipt):
            raise ValueError("mssql_sqlclient.checkpoint_binding_mismatch")
        return receipt

    @staticmethod
    def proof(
        retirement: publication.NativeParentRetirementReceipt,
        *,
        target_id: str,
        window_fingerprint: str,
        fence: int,
        revision: int,
    ) -> str:
        return publication.canonical_digest(
            {
                "parent_retirement_digest": retirement.digest,
                "target_id": target_id,
                "window_fingerprint": window_fingerprint,
                "fence": fence,
                "checkpoint_cas_revision": revision,
            }
        )

    @staticmethod
    def _proof(receipt: publication.NativeCheckpointReceipt) -> str:
        return publication.canonical_digest(
            {
                "parent_retirement_digest": receipt.parent_retirement_digest,
                "target_id": receipt.target_id,
                "window_fingerprint": receipt.window_fingerprint,
                "fence": receipt.fence,
                "checkpoint_cas_revision": receipt.checkpoint_cas_revision,
            }
        )

    @staticmethod
    def _validate(retirement: publication.NativeParentRetirementReceipt, target: str, window: str, fence: int) -> None:
        if (
            type(retirement) is not publication.NativeParentRetirementReceipt
            or type(target) is not str
            or not target
            or type(window) is not str
            or not window
            or type(fence) is not int
            or fence < 1
        ):
            raise ValueError("mssql_sqlclient.checkpoint_request_invalid")


__all__ = ("SqlClientCheckpointCas",)
