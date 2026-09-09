from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from dpone.readiness.safe_sample_auto_live_runtime import (
    AutoLiveSafeSampleRuntime,
    prepare_auto_live_safe_sample_runtime,
)
from dpone.readiness.safe_sample_live_errors import LiveSafeSampleRuntimeAssemblyError
from dpone.readiness.safe_sample_live_input_discovery import (
    LiveSafeSampleInputDiscovery,
    LiveSafeSampleInputPaths,
)


def test_auto_live_runtime_preserves_network_free_handoff_when_overlay_is_absent(tmp_path: Path) -> None:
    calls: list[dict[str, Any]] = []

    result = prepare_auto_live_safe_sample_runtime(
        object(),
        pipeline_source_path="pipelines/orders/pipeline.yaml",
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
        discovery=lambda *args, **kwargs: LiveSafeSampleInputDiscovery(
            status="not_configured",
            deployment_id="sha256:" + "d" * 64,
            pipeline_id="orders",
        ),
        assembly_builder=lambda **kwargs: calls.append(kwargs),
    )

    assert result.execution_mode == "local_handoff"
    assert result.assembly is None
    assert result.errors == ()
    assert calls == []


def test_auto_live_runtime_delegates_complete_inputs_to_existing_live_assembly(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    marker = object()
    plan = object()
    verifier = object()
    calls: list[dict[str, Any]] = []

    def build(**kwargs: Any) -> object:
        calls.append(kwargs)
        return marker

    result = prepare_auto_live_safe_sample_runtime(
        plan,
        pipeline_source_path="pipelines/orders/pipeline.yaml",
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
        process_name="orders",
        signature_verifier=verifier,
        discovery=lambda *args, **kwargs: LiveSafeSampleInputDiscovery(
            status="ready",
            deployment_id="sha256:" + "d" * 64,
            pipeline_id="orders",
            paths=paths,
        ),
        assembly_builder=build,
    )

    assert result.execution_mode == "live_copy"
    assert result.assembly is marker
    assert result.errors == ()
    assert calls == [
        {
            "plan": plan,
            "pipeline_source_path": "pipelines/orders/pipeline.yaml",
            "binding_set_path": paths.binding_set,
            "connection_registry_path": paths.connection_registry,
            "credential_runtime_path": paths.credential_runtime,
            "route_attestation_path": paths.route_attestation,
            "route_attestation_bundle_path": paths.route_attestation_bundle,
            "route_certification_bundle_path": paths.route_certification_bundle,
            "route_attestation_policy_path": paths.route_attestation_policy,
            "cache_root": tmp_path / ".dpone-cache",
            "source_root": tmp_path,
            "process_name": "orders",
            "route_attestation_signature_verifier": verifier,
        }
    ]


def test_auto_live_runtime_does_not_call_assembly_for_incomplete_overlay(tmp_path: Path) -> None:
    error = {
        "schema": "dpone.error.v1",
        "code": "DPONE_SAFE_SAMPLE_LIVE_INPUTS_INCOMPLETE",
        "stage": "safe_sample_live_input_discovery",
        "severity": "error",
        "message": "required file is missing",
        "fixes": [],
    }
    result = prepare_auto_live_safe_sample_runtime(
        object(),
        pipeline_source_path="pipelines/orders/pipeline.yaml",
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
        discovery=lambda *args, **kwargs: LiveSafeSampleInputDiscovery(
            status="incomplete",
            deployment_id="sha256:" + "d" * 64,
            pipeline_id="orders",
            errors=(error,),
        ),
        assembly_builder=lambda **kwargs: (_ for _ in ()).throw(AssertionError("assembly must not run")),
    )

    assert result.execution_mode == "blocked"
    assert result.assembly is None
    assert result.errors == (error,)
    assert result.to_dict()["status"] == "incomplete"


def test_auto_live_runtime_redacts_live_assembly_failure(tmp_path: Path) -> None:
    paths = _paths(tmp_path)

    def fail(**kwargs: Any) -> object:
        del kwargs
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID",
            "signature failed password=must-not-leak vault_token=must-not-leak",
        )

    result = prepare_auto_live_safe_sample_runtime(
        object(),
        pipeline_source_path="pipelines/orders/pipeline.yaml",
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
        discovery=lambda *args, **kwargs: LiveSafeSampleInputDiscovery(
            status="ready",
            deployment_id="sha256:" + "d" * 64,
            pipeline_id="orders",
            paths=paths,
        ),
        assembly_builder=fail,
    )

    assert result.execution_mode == "blocked"
    assert result.assembly is None
    assert result.errors[0]["code"] == "DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID"
    assert result.errors[0]["stage"] == "safe_sample_auto_live_runtime"
    assert result.errors[0]["message"] == (
        "Live safe-sample authorization is blocked; inspect the structured code and platform configuration."
    )
    assert "must-not-leak" not in str(result.to_dict())


def test_auto_live_runtime_classifies_unexpected_assembly_failure_as_internal(tmp_path: Path) -> None:
    paths = _paths(tmp_path)

    def fail(**kwargs: Any) -> object:
        del kwargs
        raise RuntimeError("password=must-not-leak")

    result = prepare_auto_live_safe_sample_runtime(
        object(),
        pipeline_source_path="pipelines/orders/pipeline.yaml",
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
        discovery=lambda *args, **kwargs: LiveSafeSampleInputDiscovery(
            status="ready",
            deployment_id="sha256:" + "d" * 64,
            pipeline_id="orders",
            paths=paths,
        ),
        assembly_builder=fail,
    )

    assert result.execution_mode == "blocked"
    assert result.to_dict()["status"] == "ready"
    assert result.errors[0]["code"] == "DPONE_INTERNAL_SAFE_SAMPLE_LIVE_ASSEMBLY_FAILED"
    assert "must-not-leak" not in str(result.to_dict())


@pytest.mark.parametrize(
    ("mode", "assembly", "errors"),
    [
        ("live_copy", None, ()),
        ("local_handoff", object(), ()),
        ("blocked", None, ()),
        ("unknown", None, ()),
    ],
)
def test_auto_live_runtime_rejects_inconsistent_selection_state(
    mode: str,
    assembly: object | None,
    errors: tuple[dict[str, Any], ...],
) -> None:
    with pytest.raises(ValueError):
        AutoLiveSafeSampleRuntime(
            execution_mode=mode,
            discovery=LiveSafeSampleInputDiscovery(
                status="not_configured",
                deployment_id="sha256:" + "d" * 64,
                pipeline_id="orders",
            ),
            assembly=assembly,  # type: ignore[arg-type]
            errors=errors,
        )


def _paths(root: Path) -> LiveSafeSampleInputPaths:
    return LiveSafeSampleInputPaths(
        binding_set=root / "binding-set.yaml",
        connection_registry=root / "connection-registry.yaml",
        credential_runtime=root / "credential-runtime.yaml",
        route_attestation=root / "route-attestation.json",
        route_attestation_bundle=root / "route-attestation.sigstore.json",
        route_certification_bundle=root / "route-certification-bundle.json",
        route_attestation_policy=root / "route-attestation-policy.json",
    )
