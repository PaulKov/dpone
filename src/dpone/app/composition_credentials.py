"""Issued SQL login material confined to memory and the dbt child environment.

The composition root supplies a physically verified target. Only endpoint/TLS
fields are copied from that binding: ambient credentials and driver options
cannot become fallback authority. Profiles contain dbt secret env references.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import yaml

from dpone.adapters.dbt_runtime_profile import RuntimeDbtProfileRenderer
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.ports.dbt_publishing import RenderedDbtProfile
from dpone.runtime.credentials.config import CredentialsConfig

if TYPE_CHECKING:
    from dpone.contracts.composition_persistence import CompositionAttemptIdentity
    from dpone.contracts.dbt_publishing import DbtProfileSpec, DbtSqlServerRuntimePolicy

_USER_ENV = "DBT_ENV_SECRET_DPONE_COMPOSITION_USER"
_PASSWORD_ENV = "DBT_ENV_SECRET_DPONE_COMPOSITION_PASSWORD"


class IssuedSqlCredentials(Protocol):
    """Read-only one-time material supplied by the protected issuer."""

    @property
    def login_name(self) -> str: ...

    @property
    def login_sid(self) -> bytes: ...

    @property
    def password(self) -> str: ...


def _require_issued(attempt: CompositionAttemptIdentity, credentials: IssuedSqlCredentials) -> None:
    attempt.__post_init__()
    if (
        credentials.login_name != "dpone_v3_" + attempt.attempt_sha256[7:]
        or type(credentials.login_sid) is not bytes
        or len(credentials.login_sid) != 16
        or not isinstance(credentials.password, str)
        or not 1 <= len(credentials.password) <= 128
    ):
        raise CompositionAdmissionError("worker_issued_identity")


class IssuedDbtProfileRenderer:
    """Render exact target coordinates, replacing secrets with environment refs."""

    def __init__(
        self,
        *,
        attempt: CompositionAttemptIdentity,
        connection_ref: str,
        target: ResolvedBindingConnection,
        credentials: IssuedSqlCredentials,
    ) -> None:
        _require_issued(attempt, credentials)
        self._ref = connection_ref
        original = target.credentials
        self._target = ResolvedBindingConnection(
            credentials=CredentialsConfig(
                host=original.host,
                port=original.port,
                database=original.database,
                schema=original.schema,
                driver=original.driver,
                encrypt=original.encrypt,
                trust_server_certificate=original.trust_server_certificate,
                username=credentials.login_name,
                password=credentials.password,
            ),
            safe_metadata={"resolver": "composition-issued-login"},
            descriptor=target.descriptor,
        )

    def resolve(self, connection_ref: str) -> ResolvedBindingConnection:
        """Supply only the admitted target; no delegate or ambient fallback."""
        if connection_ref != self._ref:
            raise CompositionAdmissionError("worker_credential_scope")
        return self._target

    def render(self, profile: DbtProfileSpec, adapter_runtime: DbtSqlServerRuntimePolicy) -> RenderedDbtProfile:
        rendered = RuntimeDbtProfileRenderer(self).render(profile, adapter_runtime)
        document = yaml.safe_load(rendered.content)
        output = document[profile.profile_name]["outputs"][profile.target_name]
        output["user"] = "{{ env_var('" + _USER_ENV + "') }}"
        output["password"] = "{{ env_var('" + _PASSWORD_ENV + "') }}"
        return replace(rendered, content=yaml.safe_dump(document, sort_keys=True).encode("utf-8"))


def issued_dbt_child_environment(
    *, attempt: CompositionAttemptIdentity, credentials: IssuedSqlCredentials, home: Path
) -> Callable[[], Mapping[str, str]]:
    """Bind issued secrets to one bounded, explicit child environment.

    The returned provider is scoped to exactly one attempt by construction. It
    supplies the same pinned invocation environment the shared dbt runner uses,
    plus the two dbt secret variables the rendered profile references. ``home``
    must be a child-writable directory of this attempt, because dbt creates its
    user state below ``HOME``. Nothing here mutates ``os.environ`` and no value
    is journaled, logged or persisted.
    """
    _require_issued(attempt, credentials)
    pinned = DbtInvocationContext.canonical().environment(home=str(home))

    def environment() -> Mapping[str, str]:
        return {
            **pinned,
            _USER_ENV: credentials.login_name,
            _PASSWORD_ENV: credentials.password,
        }

    return environment


def issued_dbt_process_factory(
    popen: Callable[..., Any], *, attempt: CompositionAttemptIdentity, credentials: IssuedSqlCredentials
) -> Callable[..., Any]:
    """Bind issued secrets to a detached explicit child environment only.

    Inject the returned callable into SubprocessDbtCommandRunner.popen_factory.
    That runner supplies its existing bounded, pinned environment. Never mutate
    os.environ or accept an implicit ambient environment.
    """
    _require_issued(attempt, credentials)

    def launch(*args: Any, **kwargs: Any) -> Any:
        environment = kwargs.get("env")
        if not isinstance(environment, dict):
            raise CompositionAdmissionError("worker_child_environment")
        kwargs["env"] = {**environment, _USER_ENV: credentials.login_name, _PASSWORD_ENV: credentials.password}
        return popen(*args, **kwargs)

    return launch
