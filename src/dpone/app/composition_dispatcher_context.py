"""Load one provisioned dispatcher context without accepting caller authority.

The startup configuration pins an authority-to-original-digest catalog. Each
root-owned original pins the init-fetch plan and parent activation identity;
requests select entries by digest only. Loading verifies runtime documents and
reopens the producer-backed composition plan before exposing its lazy resolver.
It does not prove a RUNNING attempt or current SQL ownership: the business
handler must reopen those authorities for every operation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Any

from dpone.adapters.composition_dispatcher_context_files import DispatcherContextFiles
from dpone.app.composition_pack_execution_dispatcher import reopen_composition_plan
from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity
from dpone.contracts.composition_activation import CompositionOccurrenceContext
from dpone.contracts.composition_dispatcher_binding import (
    CompositionDispatcherBinding,
    require_dispatcher_connection_ref,
)
from dpone.contracts.composition_execution import CompositionExecutionPlan
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.runtime.credentials.runtime_context import (
    RUNTIME_CONNECTION_CONTEXT_ENV,
    RUNTIME_INIT_FETCH_PLAN_B64_ENV,
    RUNTIME_INIT_FETCH_PLAN_SHA256_ENV,
    RuntimeConnectionContext,
    RuntimeConnectionContextLoader,
)
from dpone.runtime.init_fetch_contract import cache_relative_path
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan

_FIELDS = {
    "schema",
    "runtime_authority_sha256",
    "deployment_identity",
    "init_fetch_plan_b64",
    "init_fetch_plan_sha256",
    "plan_sha256",
}


@dataclass(frozen=True, slots=True)
class StagedDispatcherContext:
    """Verified immutable coordinates plus a lazy, non-serialized runtime resolver."""

    occurrence: CompositionOccurrenceContext
    binding: CompositionDispatcherBinding
    plan: CompositionExecutionPlan = field(repr=False)
    runtime: RuntimeConnectionContext = field(repr=False)
    cache_root: Path = field(repr=False)
    target_binding_ref: str


def _registry_entry(runtime: RuntimeConnectionContext, alias: str) -> Mapping[str, Any]:
    """Read signed metadata without invoking credential resolution."""
    require_dispatcher_connection_ref(alias)
    reference = runtime.binding_set["bindings"][alias]["connection_ref"]
    require_dispatcher_connection_ref(reference)
    entry = runtime.connection_registry["connections"][reference]
    if not isinstance(entry, Mapping):
        raise CompositionAdmissionError("dispatcher_context")
    return entry


class StagedDispatcherContextLoader:
    """Resolve only startup-pinned originals; no ambient environment fallback.

    ``staged_authorities`` maps the exact runtime authority digest to the whole
    staged metadata digest. Its caller must obtain this catalog from protected
    service configuration, never an HTTP request. The runtime loader may be
    injected to supply the service's existing lazy Vault/Kubernetes readers.
    """

    def __init__(
        self,
        *,
        root: Path,
        dispatcher_gid: int,
        dispatcher_id: str,
        configuration_sha256: str,
        staged_authorities: Mapping[str, str],
        runtime_loader: RuntimeConnectionContextLoader | None = None,
    ) -> None:
        identity = CompositionDispatcherBinding(dispatcher_id, "dispatcher", configuration_sha256)
        if not staged_authorities or len(staged_authorities) > 1024:
            raise CompositionAdmissionError("dispatcher_context")
        for selector, original in staged_authorities.items():
            require_digest(selector)
            require_digest(original)
        self._catalog = MappingProxyType(dict(staged_authorities))
        self._identity = identity
        self._files = DispatcherContextFiles(root, dispatcher_gid=dispatcher_gid)
        self._runtime_loader = runtime_loader if runtime_loader is not None else RuntimeConnectionContextLoader()

    def load(
        self,
        runtime_authority_sha256: str,
        *,
        dispatcher_id: str,
        configuration_sha256: str,
        plan_sha256: str,
        target_binding_ref: str,
    ) -> StagedDispatcherContext:
        """Verify staged parent, source plan and signed dispatcher before resolution."""
        try:
            require_digest(runtime_authority_sha256)
            require_digest(plan_sha256)
            require_dispatcher_connection_ref(target_binding_ref)
            if (dispatcher_id, configuration_sha256) != (
                self._identity.dispatcher_id,
                self._identity.service_configuration_sha256,
            ):
                raise ValueError
            expected = self._catalog[runtime_authority_sha256]
            directory = runtime_authority_sha256.removeprefix("sha256:")
            original = self._files.read(directory + "/context.json")
            if "sha256:" + sha256(original).hexdigest() != expected:
                raise ValueError
            body = strict_json_object(original)
            if (
                set(body) != _FIELDS
                or canonical_json_bytes(body) != original
                or body["schema"] != "dpone.composition-dispatcher-context.v1"
                or body["runtime_authority_sha256"] != runtime_authority_sha256
                or body["plan_sha256"] != plan_sha256
            ):
                raise ValueError
            identity = AirflowDeploymentIdentity.from_mapping(body["deployment_identity"])
            init_plan, _ = decode_runtime_init_fetch_plan(body["init_fetch_plan_b64"], body["init_fetch_plan_sha256"])
            if (init_plan.release_id, init_plan.deployment_id) != (identity.release_id, identity.deployment_id):
                raise ValueError
            cache = self._files.require_tree(directory + "/cache")
            context_root = cache / "payload" / cache_relative_path(init_plan.binding_set.artifact_ref).parent
            runtime = self._runtime_loader.load(
                {
                    RUNTIME_CONNECTION_CONTEXT_ENV: str(context_root),
                    RUNTIME_INIT_FETCH_PLAN_B64_ENV: body["init_fetch_plan_b64"],
                    RUNTIME_INIT_FETCH_PLAN_SHA256_ENV: body["init_fetch_plan_sha256"],
                }
            )
            if runtime is None or (
                runtime.authority_subject_sha256,
                runtime.release_id,
                runtime.deployment_id,
                runtime.environment,
            ) != (runtime_authority_sha256, identity.release_id, identity.deployment_id, init_plan.environment):
                raise ValueError
            plan = reopen_composition_plan(cache, identity.release_id)
            if plan.sources.subject_sha256 != plan_sha256 or not any(
                write.connector == "clickhouse"
                and write.kind == "transfer"
                and write.connection_ref == target_binding_ref
                for write in plan.writes
            ):
                raise ValueError
            target = _registry_entry(runtime, target_binding_ref)
            if target["type"] != "clickhouse":
                raise ValueError
            binding = CompositionDispatcherBinding.from_mapping(target["connection"]["composition_dispatcher"])
            if (binding.dispatcher_id, binding.service_configuration_sha256) != (
                dispatcher_id,
                configuration_sha256,
            ) or _registry_entry(runtime, binding.connection_ref)["type"] != "api":
                raise ValueError
            occurrence = CompositionOccurrenceContext(
                identity.activation_id,
                runtime.environment,
                identity.release_id,
                identity.deployment_id,
                None,
                runtime_authority_sha256,
            )
            return StagedDispatcherContext(occurrence, binding, plan, runtime, cache, target_binding_ref)
        except Exception:
            raise CompositionAdmissionError("dispatcher_context_unverified") from None
