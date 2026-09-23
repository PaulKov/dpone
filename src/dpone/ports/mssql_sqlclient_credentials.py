"""Memory-only one-shot SqlClient credential holder.

The holder performs no I/O, waiting, callback or secret serialization.  Release
clears its only credential reference before returning the value to the caller.
"""

from __future__ import annotations

from threading import Lock

from dpone.contracts.mssql_tds_api import SqlClientCredentialProfile, SqlClientCredentials, validate_credentials

ERROR = "mssql_native.sqlclient_credentials_unavailable"
_TOKEN = object()


class PreloadedSqlClientCredentials:
    """Final one-shot value holder; construction is restricted to the preload helper."""

    __slots__ = ("__credentials", "__lock", "__profile", "__released")

    def __init_subclass__(cls, **kwargs: object) -> None:
        raise TypeError(ERROR)

    def __init__(
        self,
        token: object,
        profile: SqlClientCredentialProfile,
        credentials: SqlClientCredentials | None,
    ) -> None:
        if token is not _TOKEN or type(profile) is not SqlClientCredentialProfile:
            raise ValueError(ERROR)
        if credentials is not None:
            validate_credentials(profile, credentials)
        self.__profile = profile
        self.__credentials = credentials
        self.__released = False
        self.__lock = Lock()

    def __repr__(self) -> str:
        return "PreloadedSqlClientCredentials(<opaque>)"

    def assert_profile(self, profile: SqlClientCredentialProfile) -> None:
        """Check the exact nonsecret profile without touching secret material."""
        if self.__profile is not profile:
            raise ValueError(ERROR)

    def release_once(self, profile: SqlClientCredentialProfile) -> SqlClientCredentials:
        """Move the preloaded value out exactly once, clearing retained custody."""
        with self.__lock:
            if self.__released or self.__profile is not profile or self.__credentials is None:
                raise ValueError(ERROR)
            credentials = self.__credentials
            self.__credentials = None
            self.__released = True
        validate_credentials(profile, credentials)
        return credentials


def preload_sqlclient_credentials(
    profile: SqlClientCredentialProfile,
    credentials: SqlClientCredentials | None,
) -> PreloadedSqlClientCredentials:
    """Materialize a callback-free holder before the P10c claim boundary."""
    return PreloadedSqlClientCredentials(_TOKEN, profile, credentials)


__all__ = ("PreloadedSqlClientCredentials", "preload_sqlclient_credentials")
