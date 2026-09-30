"""Owned SQL-session and read-only catalog boundaries for publication authority.

Vendor DBAPI modules stay outside the port. A session factory must return a
fresh independently owned connection to the admitted binding on every call.
"""

from collections.abc import Callable, Sequence
from typing import Any, Protocol

from dpone.contracts.publication_authority_binding import (
    PublicationAuthorityBinding as PublicationAuthorityBinding,
)
from dpone.contracts.publication_authority_binding import (
    publication_binding_digest as publication_binding_digest,
)
from dpone.contracts.publication_authority_binding import (
    publication_slot_key as publication_slot_key,
)


class PublicationCatalogReader(Protocol):
    def get_records(self, query: str, params: Sequence[Any]) -> Sequence[Sequence[Any]]: ...


class PublicationSqlCursor(Protocol):
    description: Any

    def execute(self, query: str, params: tuple[Any, ...]) -> Any: ...
    def fetchall(self) -> Sequence[Sequence[Any]]: ...
    def nextset(self) -> bool | None: ...
    def close(self) -> None: ...


class PublicationSqlSession(Protocol):
    autocommit: bool

    def cursor(self) -> PublicationSqlCursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


PublicationSessionFactory = Callable[[], PublicationSqlSession]
