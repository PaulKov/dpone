"""Provider-owned local verification and deterministic base-only Secret mounts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_artifact_contract import CacheActivationIdentity, read_confined_cache_file_with_identity
from dpone_airflow_pack.credential_projection_contract import (
    PROJECTION_FILENAME,
    CredentialProjectionError,
    parse_credential_projection,
    require_registry_parity,
)
from dpone_airflow_pack.init_fetch_contract import InitFetchDeliveryContext, InitFetchProviderError


def load_credential_projection_context(
    context: InitFetchDeliveryContext,
    *,
    index_path: Path,
    cache_root: Path,
    activation: CacheActivationIdentity | None,
) -> InitFetchDeliveryContext:
    """Load only bounded local metadata and preserve the captured active identity."""
    descriptor = context.credential_projection
    if descriptor is None:
        return context
    try:

        def read(name: str, sha256: str, size: int) -> bytes:
            observed = read_confined_cache_file_with_identity(
                index_path.parent / name,
                cache_root=cache_root,
                max_bytes=size,
                allow_current_pointer=True,
            )
            if (
                observed.activation != activation
                or len(observed.content) != size
                or "sha256:" + hashlib.sha256(observed.content).hexdigest() != sha256
            ):
                raise CredentialProjectionError("MISMATCH")
            return observed.content

        body = read(PROJECTION_FILENAME, descriptor.sha256, descriptor.bytes)
        projection = parse_credential_projection(
            body,
            descriptor=descriptor.to_dict(),
            environment=context.environment,
            release_id=context.release_id,
            binding_set_sha256=context.binding_set.sha256,
            runtime_registry_sha256=context.connection_registry.sha256,
        )
        binding = json.loads(read("binding-set.json", context.binding_set.sha256, context.binding_set.bytes))
        registry = json.loads(
            read("connection-registry.ref", context.connection_registry.sha256, context.connection_registry.bytes)
        )
        require_registry_parity(projection, binding_set=binding, registry=registry)
        if activation is not None:
            projection.require_authority(control_ref=activation.workspace_authority_connection_ref or "")
        if {row.workload_id for row in projection.workloads} != {pack.id for pack in context.workload_packs}:
            raise CredentialProjectionError("MISMATCH")
        return replace(context, credential_projection_data=projection)
    except CredentialProjectionError as exc:
        raise InitFetchProviderError(exc.code, str(exc)) from None


def project_credential_pod(
    pod: Mapping[str, Any],
    *,
    context: InitFetchDeliveryContext,
    workload_id: str,
    control_ref: object,
    native_empty_projection: bool,
) -> dict[str, Any]:
    """Refuse missing authority and project only reviewed keys on base.

    Group shared keys into one Secret volume with distinct alias/uri items.
    Per-alias subPath mounts avoid overlapping roots between Secret sources;
    rotation takes effect on the next Pod, as with workload-start resolution.
    """
    result = deepcopy(dict(pod))
    if context.credential_projection is None:
        if native_empty_projection and control_ref:
            raise InitFetchProviderError(
                "DPONE_RUNTIME_CREDENTIAL_PROJECTION_REQUIRED", "native credential projection is required"
            )
        return result
    try:
        projection = context.credential_projection_data
        if projection is None:
            raise CredentialProjectionError("REQUIRED")
        projection.require_authority(control_ref=control_ref if isinstance(control_ref, str) else "")
        sources = projection.selected_sources(workload_id)
        spec = result["spec"]
        base = next(container for container in spec["containers"] if container["name"] == "base")
        mounts = base.setdefault("volumeMounts", [])
        volumes = spec.setdefault("volumes", [])
        groups: dict[str, list[Any]] = {}
        for source in sources:
            groups.setdefault(source.secret_name, []).append(source)
        for secret, entries in sorted(groups.items()):
            name = "dpone-credentials-" + hashlib.sha256(secret.encode()).hexdigest()[:16]
            if any(volume.get("name") == name for volume in volumes):
                raise CredentialProjectionError("MISMATCH")
            volumes.append(
                {
                    "name": name,
                    "secret": {
                        "secretName": secret,
                        "optional": False,
                        "items": [
                            {"key": source.secret_key, "path": f"{source.registry_ref}/{source.filename}"}
                            for source in entries
                        ],
                    },
                }
            )
            for source in entries:
                if any(_overlaps(str(mount.get("mountPath", "")), source.mount_path) for mount in mounts):
                    raise CredentialProjectionError("MISMATCH")
                mounts.append(
                    {"name": name, "mountPath": source.mount_path, "subPath": source.registry_ref, "readOnly": True}
                )
        return result
    except CredentialProjectionError as exc:
        raise InitFetchProviderError(exc.code, str(exc)) from None


def _overlaps(left: str, right: str) -> bool:
    return left == right or left.startswith(right + "/") or right.startswith(left.rstrip("/") + "/")
