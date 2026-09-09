"""Durable local store for replayable deployment-cache retention outcomes."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.adapters.deployment_cache_files import (
    DeploymentCacheError,
    atomic_write_json,
    read_regular_json_object,
)
from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.deployment_cache_retention_receipt import (
    MAX_RETENTION_APPLY_RECEIPT_BYTES,
    MAX_RETENTION_APPLY_RECEIPTS,
    DeploymentCacheRetentionReceiptError,
    build_retention_apply_receipt,
    build_retention_apply_receipt_v2,
    parse_retention_apply_receipt,
    retention_operation_id,
)
from dpone.contracts.posix_permissions import has_posix_access_mode

_RECEIPT_DIR = ".retention-apply-receipts"
_MAX_RECEIPT_INVENTORY_BYTES = 64 * 1024 * 1024


class DeploymentCacheRetentionReceiptStore:
    """Persist per-operation outcomes before returning public apply evidence."""

    @staticmethod
    def operation_id(
        *,
        environment: str,
        reviewed_plan_sha256: str,
        review_id: str | None = None,
    ) -> str:
        """Preserve the historical concrete-adapter helper outside the active port."""

        return retention_operation_id(
            environment=environment,
            reviewed_plan_sha256=reviewed_plan_sha256,
            review_id=review_id,
        )

    def __init__(self, cache_root: Path) -> None:
        self._root = cache_root
        self._receipt_root = cache_root / _RECEIPT_DIR

    def load(self, operation_id: str) -> dict[str, Any] | None:
        path = self._path(operation_id)
        try:
            self._receipt_root.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise self._error("deployment cache retention receipt root is unavailable") from exc
        self._ensure_private_root(create=False)
        inventory = self._validated_inventory()
        return inventory.get(path)

    def incomplete_operation_ids(self) -> tuple[str, ...]:
        try:
            self._receipt_root.lstat()
        except FileNotFoundError:
            return ()
        except OSError as exc:
            raise self._error("deployment cache retention receipt root is unavailable") from exc
        self._ensure_private_root(create=False)
        return tuple(
            sorted(
                str(receipt["operation_id"])
                for receipt in self._validated_inventory().values()
                if receipt["status"] == "applying"
            )
        )

    def begin(
        self,
        *,
        operation_id: str,
        environment: str,
        promoted_by: str,
        current_deployment_id: str | None,
        reviewed_plan_sha256: str,
        review_id: str,
        reviewed_protected_deployment_ids: Sequence[str],
        reviewed_recovery_revision: str | None,
        activation_history_revision: str,
        items: Sequence[Mapping[str, object]],
    ) -> dict[str, Any]:
        existing = self.load(operation_id)
        if existing is not None:
            return existing
        self._ensure_private_root(create=True)
        inventory = self._validated_inventory()
        if len(inventory) >= MAX_RETENTION_APPLY_RECEIPTS:
            raise self._error("deployment cache retention receipt capacity is exhausted")
        payload = self._build(
            operation_id=operation_id,
            status="applying",
            environment=environment,
            promoted_by=promoted_by,
            current_deployment_id=current_deployment_id,
            reviewed_plan_sha256=reviewed_plan_sha256,
            review_id=review_id,
            reviewed_protected_deployment_ids=reviewed_protected_deployment_ids,
            reviewed_recovery_revision=reviewed_recovery_revision,
            activation_history_revision=activation_history_revision,
            items=items,
        )
        self._write(payload)
        return payload

    def mark_deleted(self, receipt: dict[str, Any], deployment_id: str) -> dict[str, Any]:
        items = []
        found = False
        for item in receipt["items"]:
            updated = dict(item)
            if item["deployment_id"] == deployment_id:
                found = True
                if item["action"] not in {"pending", "deleted"}:
                    raise self._error("retention receipt outcome transition is invalid")
                updated["action"] = "deleted"
            items.append(updated)
        if not found:
            raise self._error("retention receipt does not contain the committed deployment")
        return self._replace(receipt, status="applying", items=items)

    def abort(
        self,
        receipt: dict[str, Any],
        *,
        committed_deployment_ids: tuple[str, ...],
        restored_deployment_ids: tuple[str, ...],
        reason: str,
        error_code: str,
    ) -> dict[str, Any]:
        committed = frozenset(committed_deployment_ids)
        restored = frozenset(restored_deployment_ids)
        items: list[dict[str, Any]] = []
        matched = False
        for item in receipt["items"]:
            updated = dict(item)
            if item["action"] == "pending":
                deployment_id = item["deployment_id"]
                if deployment_id in committed:
                    updated["action"] = "deleted"
                else:
                    updated["action"] = "skipped"
                    updated["reason"] = reason
                    updated["error_code"] = error_code
                if deployment_id in restored:
                    matched = True
            items.append(updated)
        if restored and not matched:
            raise self._error("restored deployment does not match the incomplete retention receipt")
        return self._replace(receipt, status="aborted", items=items)

    def commit(self, receipt: dict[str, Any]) -> dict[str, Any]:
        if any(item["action"] == "pending" for item in receipt["items"]):
            raise self._error("retention receipt cannot commit with pending outcomes")
        return self._replace(receipt, status="committed", items=receipt["items"])

    @staticmethod
    def committed(receipt: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_retention_apply_receipt(receipt)
        except DeploymentCacheRetentionReceiptError as exc:
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID",
                "deployment cache retention receipt is invalid",
                details={"state_may_have_changed": True},
            ) from exc
        if parsed["status"] != "committed":
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INCOMPLETE",
                "deployment cache retention receipt is not committed",
            )
        return parsed

    def _replace(self, receipt: dict[str, Any], *, status: str, items: Sequence[dict[str, Any]]) -> dict[str, Any]:
        kwargs: dict[str, Any] = dict(
            operation_id=receipt["operation_id"],
            status=status,
            environment=receipt["environment"],
            promoted_by=receipt["promoted_by"],
            current_deployment_id=receipt["current_deployment_id"],
            reviewed_plan_sha256=receipt["reviewed_plan_sha256"],
            activation_history_revision=receipt["activation_history_revision"],
            items=items,
        )
        if "review_id" in receipt:
            kwargs.update(
                review_id=receipt["review_id"],
                reviewed_protected_deployment_ids=receipt["reviewed_protected_deployment_ids"],
                reviewed_recovery_revision=receipt["reviewed_recovery_revision"],
            )
        payload = self._build(**kwargs)
        self._write(payload)
        return payload

    def _build(self, **kwargs: Any) -> dict[str, Any]:
        try:
            if "review_id" in kwargs:
                return build_retention_apply_receipt_v2(**kwargs)
            return build_retention_apply_receipt(**kwargs)
        except DeploymentCacheRetentionReceiptError as exc:
            raise self._error("deployment cache retention receipt transition is invalid") from exc

    def _write(self, payload: dict[str, Any]) -> None:
        encoded_size = len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        if encoded_size > MAX_RETENTION_APPLY_RECEIPT_BYTES:
            raise self._error("deployment cache retention receipt exceeds its byte capacity")
        try:
            atomic_write_json(self._path(str(payload["operation_id"])), payload)
        except OSError as exc:
            raise self._error("deployment cache retention receipt could not be committed durably") from exc

    def _ensure_private_root(self, *, create: bool) -> None:
        try:
            if create:
                self._receipt_root.mkdir(mode=0o700, parents=True, exist_ok=True)
            metadata = self._receipt_root.lstat()
        except OSError as exc:
            raise self._error("deployment cache retention receipt root is unavailable") from exc
        wrong_owner = hasattr(os, "geteuid") and metadata.st_uid != os.geteuid()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or self._receipt_root.is_symlink()
            or not has_posix_access_mode(metadata.st_mode, 0o700)
            or wrong_owner
        ):
            raise self._error("deployment cache retention receipt root is unsafe")

    def _path(self, operation_id: str) -> Path:
        if not is_canonical_sha256_digest(operation_id):
            raise self._error("deployment cache retention operation id is invalid")
        return self._receipt_root / f"{operation_id.replace(':', '-', 1)}.json"

    def _validated_inventory(self) -> dict[Path, dict[str, Any]]:
        inventory: dict[Path, dict[str, Any]] = {}
        observed_bytes = 0
        try:
            for path in self._receipt_root.iterdir():
                if len(inventory) >= MAX_RETENTION_APPLY_RECEIPTS:
                    raise self._error("deployment cache retention receipt capacity is exhausted")
                metadata = path.lstat()
                observed_bytes += metadata.st_size
                if observed_bytes > _MAX_RECEIPT_INVENTORY_BYTES:
                    raise self._error("deployment cache retention receipt inventory exceeds its byte capacity")
                operation_id = path.name.removesuffix(".json").replace("sha256-", "sha256:", 1)
                wrong_owner = hasattr(os, "geteuid") and metadata.st_uid != os.geteuid()
                if (
                    path.is_symlink()
                    or not stat.S_ISREG(metadata.st_mode)
                    or stat.S_IMODE(metadata.st_mode) != 0o600
                    or wrong_owner
                    or self._path(operation_id) != path
                ):
                    raise self._error("deployment cache retention receipt inventory is unsafe", path=path)
                payload = read_regular_json_object(
                    path,
                    missing_code="DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID",
                    invalid_code="DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID",
                    label="deployment cache retention apply receipt",
                    root=self._receipt_root,
                    max_bytes=MAX_RETENTION_APPLY_RECEIPT_BYTES,
                    required_owner_uid=os.geteuid() if hasattr(os, "geteuid") else None,
                    forbid_group_world_permissions=True,
                )
                parsed = parse_retention_apply_receipt(payload)
                if parsed["operation_id"] != operation_id:
                    raise self._error(
                        "deployment cache retention receipt identity does not match its filename",
                        path=path,
                    )
                inventory[path] = parsed
        except DeploymentCacheError:
            raise
        except (DeploymentCacheRetentionReceiptError, OSError) as exc:
            raise self._error("deployment cache retention receipt inventory is unavailable") from exc
        return inventory

    def _error(self, message: str, *, path: Path | None = None) -> DeploymentCacheError:
        return DeploymentCacheError(
            "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID",
            message,
            path=(path or self._receipt_root).as_posix(),
            details={"state_may_have_changed": True},
        )


__all__ = [
    "DeploymentCacheRetentionReceiptStore",
]
