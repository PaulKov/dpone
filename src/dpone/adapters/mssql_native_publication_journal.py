"""Publication lifecycle over a shared fenced native journal revision.

This view owns preparation, target receipt, rollback proof and evidence/state
ordering. Its injected storage callbacks retain a single CAS owner shared with
chunk tracking; the view never reloads or independently advances revisions.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict
from typing import Any

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_parent_journal import (
    NativeCheckpointReceipt,
    NativeChunkRetirementReceipt,
    NativeParentAuthority,
    NativeParentRetirementReceipt,
    canonical_digest,
    checkpoint_matches,
    settled_parent_authority,
    validate_v4_publication,
)


def validate_publication_state(value: dict[str, Any]) -> None:
    """Validate the versioned projection without rewriting historical records."""
    if not isinstance(value["rollback_history"], list) or any(
        not isinstance(item, dict) for item in value["rollback_history"]
    ):
        raise ValueError("invalid rollback history")
    publication = value["publication"]
    if publication is not None:
        if value["version"] == 4:
            validate_v4_publication(value, publication)
            return
        if (
            not isinstance(publication, dict)
            or set(publication) != {"phase", "prepared", "receipt"}
            or publication["phase"]
            not in ("preparing", "prepared", "publishing", "published", "evidence-complete", "succeeded")
            or not isinstance(publication["prepared"], dict)
            or not publication["prepared"]
            or value["phase"] != "stage_complete"
        ):
            raise ValueError("invalid publication")
        if publication["phase"] in ("published", "evidence-complete", "succeeded") and not isinstance(
            publication["receipt"], dict
        ):
            raise ValueError("missing publication receipt")


class NativePublicationJournal:
    """Persist publication transitions after complete source-free stage authority."""

    def __init__(
        self,
        *,
        snapshot: Callable[[], dict[str, Any] | None],
        save: Callable[[dict[str, Any]], None],
        completed: Callable[[], object | None],
        fence: Callable[[], int],
    ) -> None:
        self._snapshot = snapshot
        self._save = save
        self._completed = completed
        self._fence = fence

    def state(self) -> dict[str, Any] | None:
        """Detached prepared/publication authority; unknown outcomes must reconcile."""
        data = self._snapshot()
        return None if data is None else json.loads(json.dumps(data["publication"]))

    def prepared(self, binding: dict[str, Any]) -> None:
        """Persist prepared stage/artifact identity before publication intent."""
        if self._completed() is None or not binding:
            raise WindowContractError("mssql_native.preparation_requires_complete_staging")
        prior = self.state()
        if prior is not None:
            if self._v4() and prior["phase"] not in ("preparing", "prepared"):
                raise WindowContractError("mssql_native.terminal_parent_authority")
            if prior["phase"] == "preparing":
                if any(binding.get(key) != value for key, value in prior["prepared"].items()):
                    raise WindowContractError("mssql_native.prepared_identity_changed")
                self._publication_save(
                    self._new_v4("prepared", binding)
                    if self._v4()
                    else dict(phase="prepared", prepared=binding, receipt=None)
                )
                return
            if prior["prepared"] != binding:
                raise WindowContractError("mssql_native.prepared_identity_changed")
            return
        self._publication_save(
            self._new_v4("prepared", binding) if self._v4() else dict(phase="prepared", prepared=binding, receipt=None)
        )

    def publication_started(self, binding: dict[str, Any]) -> None:
        """Persist intent before transaction; restart may reconcile, never blindly replay."""
        prior = self.state()
        if prior is None or prior["prepared"] != binding or prior["phase"] not in ("prepared", "publishing"):
            raise WindowContractError("mssql_native.invalid_publication_intent")
        self._publication_save(dict(prior, phase="publishing"))

    def publication_confirmed(self, receipt: dict[str, Any]) -> NativeParentAuthority | None:
        """Caller supplies the authoritative same-transaction target receipt."""
        prior = self.state()
        if prior is None or prior["phase"] not in ("publishing", "published") or not receipt:
            raise WindowContractError("mssql_native.invalid_publication_confirmation")
        if prior["receipt"] is not None and prior["receipt"] != receipt:
            raise WindowContractError("mssql_native.publication_receipt_changed")
        if self._v4():
            if prior["phase"] == "published":
                authority = self.authority()
                if authority is None or authority.kind != "published":
                    raise WindowContractError("mssql_native.missing_parent_authority")
                return authority
            authority = self._settled_authority("published", receipt)
            self._publication_save(dict(prior, phase="published", receipt=receipt, authority=asdict(authority)))
            return authority
        self._publication_save(dict(prior, phase="published", receipt=receipt))
        return None

    def evidence_complete(self) -> None:
        """Only acknowledge after durable evidence production has succeeded."""
        if self._v4():
            raise WindowContractError("mssql_native.v4_retirement_required")
        self._publication_advance("published", "evidence-complete")

    def succeeded(self, checkpoint_receipt: NativeCheckpointReceipt | None = None) -> None:
        """Only acknowledge after fenced source-state advancement has succeeded."""
        if self._v4():
            prior = self.state()
            if (
                prior is None
                or prior["phase"] not in ("checkpoint_required", "succeeded")
                or checkpoint_receipt is None
            ):
                raise WindowContractError("mssql_native.checkpoint_receipt_required")
            checkpoint = checkpoint_receipt.to_dict()
            if prior["phase"] == "succeeded":
                if prior["checkpoint_receipt"] != checkpoint:
                    raise WindowContractError("mssql_native.checkpoint_receipt_changed")
                return
            data = self._snapshot()
            retirement = self.retirement_receipt()
            if (
                data is None
                or retirement is None
                or checkpoint_receipt.fence != self._fence()
                or not checkpoint_matches(data, checkpoint_receipt, retirement)
                or (prior["checkpoint_receipt"] is not None and prior["checkpoint_receipt"] != checkpoint)
            ):
                raise WindowContractError("mssql_native.checkpoint_receipt_changed")
            self._publication_save(
                dict(
                    prior,
                    phase="succeeded",
                    checkpoint_receipt=checkpoint,
                    checkpoint_receipt_digest=canonical_digest(checkpoint),
                )
            )
            return
        self._publication_advance("evidence-complete", "succeeded")

    def _publication_advance(self, expected: str, phase: str) -> None:
        prior = self.state()
        if prior is None or prior["phase"] not in (expected, phase):
            raise WindowContractError("mssql_native.publication_phase_order")
        self._publication_save(dict(prior, phase=phase))

    def _publication_save(self, publication: dict[str, Any]) -> None:
        data = self._snapshot()
        if data is None or data["phase"] != "stage_complete":
            raise WindowContractError("mssql_native.publication_requires_complete_staging")
        self._save(dict(data, publication=publication))

    def preparation_started(self, binding: dict[str, Any]) -> None:
        """Bind the owned preparation object before creation to make crash cleanup safe."""
        prior = self.state()
        if prior is not None and prior["phase"] == "preparing" and prior["prepared"] == binding:
            return
        if self._completed() is None or not binding or prior is not None:
            raise WindowContractError("mssql_native.invalid_preparation_intent")
        self._publication_save(
            self._new_v4("preparing", binding)
            if self._v4()
            else dict(phase="preparing", prepared=binding, receipt=None)
        )

    def publication_rolled_back(self, rollback_receipt: dict[str, Any]) -> None:
        """Use only with adapter proof of no commit, never a failed rollback attempt."""
        prior = self.state()
        if self._v4():
            if prior is None or prior["phase"] != "abort_required":
                raise WindowContractError("mssql_native.rollback_proof_required")
            self.abort_confirmed(rollback_receipt)
            return
        if prior is None or prior["phase"] != "publishing" or not rollback_receipt:
            raise WindowContractError("mssql_native.rollback_proof_required")
        data = self._snapshot()
        # Retain the proof in metadata; it never becomes a publication receipt.
        if data is None:
            raise WindowContractError("mssql_native.missing_journal")
        self._save(
            dict(
                data,
                rollback_history=[*data["rollback_history"], rollback_receipt],
                publication=dict(prior, phase="prepared", receipt=None),
            )
        )

    def abort_required(self) -> None:
        """Persist the v4 abort decision before obtaining rollback/no-commit proof."""
        prior = self._require_v4_state("prepared", "publishing", "abort_required")
        self._publication_save(dict(prior, phase="abort_required"))

    def abort_confirmed(self, receipt: dict[str, Any]) -> NativeParentAuthority:
        """Persist terminal abort authority; it can never reopen as prepared."""
        prior = self._require_v4_state("abort_required", "aborted")
        if not receipt or (prior["receipt"] is not None and prior["receipt"] != receipt):
            raise WindowContractError("mssql_native.rollback_proof_required")
        if prior["phase"] == "aborted":
            authority = self.authority()
            if authority is None or authority.kind != "aborted":
                raise WindowContractError("mssql_native.missing_parent_authority")
            return authority
        authority = self._settled_authority("aborted", receipt)
        self._publication_save(dict(prior, phase="aborted", receipt=receipt, authority=asdict(authority)))
        return authority

    def authority(self) -> NativeParentAuthority | None:
        """Return settled v4 authority only; uncertain phases grant nothing."""
        prior = self.state()
        if not self._v4() or prior is None or prior["authority"] is None:
            return None
        return NativeParentAuthority(**prior["authority"])

    def retirement_required(self, authority: NativeParentAuthority) -> None:
        prior = self._require_v4_state("published", "aborted", "retirement_required")
        if self.authority() != authority:
            raise WindowContractError("mssql_native.parent_authority_changed")
        self._publication_save(dict(prior, phase="retirement_required"))

    def retiring(self) -> None:
        prior = self._require_v4_state("retirement_required", "retiring")
        self._publication_save(dict(prior, phase="retiring"))

    def chunk_retired(self, receipt: NativeChunkRetirementReceipt) -> None:
        prior = self._require_v4_state("retiring")
        existing = prior["chunk_retirements"]
        if receipt.ordinal < len(existing):
            if existing[receipt.ordinal] != receipt.to_dict():
                raise WindowContractError("mssql_native.retirement_receipt_changed")
            return
        authority = self.authority()
        data = self._snapshot()
        expected = None if data is None else data["chunks"].get(str(receipt.ordinal))
        if (
            receipt.ordinal != len(existing)
            or authority is None
            or receipt.parent_authority_digest != authority.digest
            or not isinstance(expected, dict)
            or not isinstance(expected.get("receipt"), dict)
            or receipt.attempt_id != expected["receipt"].get("attempt_id")
            or receipt.verification_receipt_sha256 != canonical_digest(expected["receipt"])
        ):
            raise WindowContractError("mssql_native.retirement_order")
        self._publication_save(dict(prior, chunk_retirements=[*existing, receipt.to_dict()]))

    def retired(self) -> NativeParentRetirementReceipt:
        prior = self._require_v4_state("retiring", "retired")
        if prior["phase"] == "retired":
            return NativeParentRetirementReceipt.from_dict(prior["retirement_receipt"])
        data = self._snapshot()
        authority = self.authority()
        if data is None or authority is None or len(prior["chunk_retirements"]) != len(data["chunks"]):
            raise WindowContractError("mssql_native.incomplete_retirement")
        receipt = NativeParentRetirementReceipt(
            authority.digest,
            tuple(NativeChunkRetirementReceipt(**item) for item in prior["chunk_retirements"]),
        )
        self._publication_save(dict(prior, phase="retired", retirement_receipt=receipt.to_dict()))
        return receipt

    def retirement_receipt(self) -> NativeParentRetirementReceipt | None:
        prior = self.state()
        if not self._v4() or prior is None or prior["retirement_receipt"] is None:
            return None
        return NativeParentRetirementReceipt.from_dict(prior["retirement_receipt"])

    def checkpoint_required(self) -> None:
        prior = self._require_v4_state("retired", "checkpoint_required")
        self._publication_save(dict(prior, phase="checkpoint_required"))

    def _v4(self) -> bool:
        data = self._snapshot()
        return data is not None and data["version"] == 4

    def _require_v4_state(self, *phases: str) -> dict[str, Any]:
        prior = self.state()
        if not self._v4() or prior is None or prior["phase"] not in phases:
            raise WindowContractError("mssql_native.v4_publication_phase_order")
        return prior

    @staticmethod
    def _new_v4(phase: str, binding: dict[str, Any]) -> dict[str, Any]:
        return dict(
            phase=phase,
            prepared=binding,
            receipt=None,
            authority=None,
            chunk_retirements=[],
            retirement_receipt=None,
            checkpoint_receipt=None,
            checkpoint_receipt_digest=None,
        )

    def _settled_authority(self, kind: str, receipt: dict[str, Any]) -> NativeParentAuthority:
        data = self._snapshot()
        if data is None:
            raise WindowContractError("mssql_native.missing_journal")
        return settled_parent_authority(data, kind, receipt, self._fence())
