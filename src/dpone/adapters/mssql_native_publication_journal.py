"""Publication lifecycle over a shared fenced native journal revision.

This view owns preparation, target receipt, rollback proof and evidence/state
ordering. Its injected storage callbacks retain a single CAS owner shared with
chunk tracking; the view never reloads or independently advances revisions.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from dpone.contracts.bounded_window import WindowContractError


def validate_publication_state(value: dict[str, Any]) -> None:
    """Validate the existing v1 publication projection without changing its shape."""
    if not isinstance(value["rollback_history"], list) or any(
        not isinstance(item, dict) for item in value["rollback_history"]
    ):
        raise ValueError("invalid rollback history")
    publication = value["publication"]
    if publication is not None:
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
    ) -> None:
        self._snapshot = snapshot
        self._save = save
        self._completed = completed

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
            if prior["phase"] == "preparing":
                if any(binding.get(key) != value for key, value in prior["prepared"].items()):
                    raise WindowContractError("mssql_native.prepared_identity_changed")
                self._publication_save(dict(phase="prepared", prepared=binding, receipt=None))
                return
            if prior["prepared"] != binding:
                raise WindowContractError("mssql_native.prepared_identity_changed")
            return
        self._publication_save(dict(phase="prepared", prepared=binding, receipt=None))

    def publication_started(self, binding: dict[str, Any]) -> None:
        """Persist intent before transaction; restart may reconcile, never blindly replay."""
        prior = self.state()
        if prior is None or prior["prepared"] != binding or prior["phase"] not in ("prepared", "publishing"):
            raise WindowContractError("mssql_native.invalid_publication_intent")
        self._publication_save(dict(prior, phase="publishing"))

    def publication_confirmed(self, receipt: dict[str, Any]) -> None:
        """Caller supplies the authoritative same-transaction target receipt."""
        prior = self.state()
        if prior is None or prior["phase"] not in ("publishing", "published") or not receipt:
            raise WindowContractError("mssql_native.invalid_publication_confirmation")
        if prior["receipt"] is not None and prior["receipt"] != receipt:
            raise WindowContractError("mssql_native.publication_receipt_changed")
        self._publication_save(dict(prior, phase="published", receipt=receipt))

    def evidence_complete(self) -> None:
        """Only acknowledge after durable evidence production has succeeded."""
        self._publication_advance("published", "evidence-complete")

    def succeeded(self) -> None:
        """Only acknowledge after fenced source-state advancement has succeeded."""
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
        self._publication_save(dict(phase="preparing", prepared=binding, receipt=None))

    def publication_rolled_back(self, rollback_receipt: dict[str, Any]) -> None:
        """Use only with adapter proof of no commit, never a failed rollback attempt."""
        prior = self.state()
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
