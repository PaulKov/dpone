"""Shared non-staging verification operations for MSSQL authority verifiers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from typing import Any, ClassVar


class MssqlDatabaseAuthorityVerifierSupportMixin:
    """Keep reusable master verification and identity evidence separate from lease admission."""

    target_connection: Any
    state_connection: Any
    target_database: str
    staging_database: str
    state_database: str
    target_authorities: Any
    state_authorities: Any
    master_connector_factory: Callable[[Any], Any]
    _authority_support: ClassVar[Any]

    @staticmethod
    def _enter_query_timeout_scope(stack: ExitStack, connector: Any, query_timeout: int | None) -> None:
        """Enter the verifier-owned timeout scope for a master session."""

        raise NotImplementedError

    def _verify_master_pins(self, target_master: Any, state_master: Any) -> None:
        self._authority_support.require_same_session_authority(
            self._authority_support.session_identity(target_master, role="target"),
            self._authority_support.session_identity(state_master, role="state"),
            code="mssql_transaction.target_state_database_topology_mismatch",
        )
        target_pin = self.target_authorities.require(self.target_database, capability="target")
        staging_pin = self.target_authorities.require(self.staging_database, capability="staging")
        state_pin = self.state_authorities.require(self.state_database, capability="state")
        self._authority_support.verify_pin(target_master, target_pin, role="target")
        if staging_pin != target_pin:
            self._authority_support.verify_pin(target_master, staging_pin, role="staging")
        self._authority_support.verify_pin(state_master, state_pin, role="state")

    @contextmanager
    def _master_connectors(self, *, query_timeout: int | None = None) -> Iterator[tuple[Any, Any]]:
        with ExitStack() as stack:
            target_master = self.master_connector_factory(self.target_connection)
            stack.callback(self._authority_support.close_connector, target_master)
            self._enter_query_timeout_scope(stack, target_master, query_timeout)
            state_master = self.master_connector_factory(self.state_connection)
            stack.callback(self._authority_support.close_connector, state_master)
            self._enter_query_timeout_scope(stack, state_master, query_timeout)
            yield target_master, state_master

    @property
    def authority_sha256(self) -> str:
        """Hash exact role-bound pins for route and receipt replay identity."""

        document = {
            "version": 1,
            "roles": {
                "target": self._authority_support.pin_document(
                    self.target_authorities.require(self.target_database, capability="target")
                ),
                "staging": self._authority_support.pin_document(
                    self.target_authorities.require(self.staging_database, capability="staging")
                ),
                "state": self._authority_support.pin_document(
                    self.state_authorities.require(self.state_database, capability="state")
                ),
            },
        }
        encoded = json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        return "sha256:" + hashlib.sha256(encoded).hexdigest()
