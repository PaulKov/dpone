from __future__ import annotations

import json
from pathlib import Path

from dpone.runtime.route_capabilities import (
    CapabilityEvidence,
    CapabilityEvidencePublisher,
    CapabilityProbeRegistry,
    CapabilityRequirement,
    RouteAlternativeAdvisor,
    RouteCandidate,
    RouteCapabilityPlanner,
    RuntimeCapability,
)


def test_route_capability_taxonomy_is_connector_neutral() -> None:
    candidate = RouteCandidate(
        route_id="object_storage_pull",
        requirements=(
            CapabilityRequirement(
                id="storage.runtime.write",
                domain=RuntimeCapability.INTERMEDIATE_STORAGE,
                required_for="object_storage_pull",
                blocker_code="storage.runtime_put_denied",
            ),
            CapabilityRequirement(
                id="format.columnar.read",
                domain=RuntimeCapability.FILE_FORMAT,
                required_for="object_storage_pull",
                blocker_code="format.columnar_read_unsupported",
            ),
        ),
        priority=10,
        expected_performance_class="high",
    )

    assert candidate.route_id == "object_storage_pull"
    assert {requirement.domain for requirement in candidate.requirements} == {
        RuntimeCapability.INTERMEDIATE_STORAGE,
        RuntimeCapability.FILE_FORMAT,
    }
    assert "mssql" not in repr(candidate).lower()
    assert "clickhouse" not in repr(candidate).lower()


def test_missing_capability_blocks_only_candidates_that_require_it() -> None:
    planner = RouteCapabilityPlanner()
    fast = RouteCandidate(
        route_id="object_storage_pull",
        requirements=(
            CapabilityRequirement(
                id="storage.runtime.write",
                domain=RuntimeCapability.INTERMEDIATE_STORAGE,
                required_for="object_storage_pull",
                blocker_code="storage.runtime_put_denied",
            ),
        ),
        priority=10,
    )
    fallback = RouteCandidate(
        route_id="typed_binary_streaming",
        requirements=(
            CapabilityRequirement(
                id="transport.push_stream",
                domain=RuntimeCapability.TRANSPORT,
                required_for="typed_binary_streaming",
            ),
        ),
        priority=20,
    )

    decision = planner.decide(
        candidates=(fast, fallback),
        evidence={
            "storage.runtime.write": CapabilityEvidence.failure(
                requirement_id="storage.runtime.write",
                domain=RuntimeCapability.INTERMEDIATE_STORAGE,
                blockers=("storage.runtime_put_denied",),
            ),
            "transport.push_stream": CapabilityEvidence.success(
                requirement_id="transport.push_stream",
                domain=RuntimeCapability.TRANSPORT,
            ),
        },
        mode="auto",
    )

    assert decision.selected_route_id == "typed_binary_streaming"
    assert decision.fallback_reason == "storage.runtime_put_denied"
    assert decision.rejected_routes[0]["route_id"] == "object_storage_pull"
    assert decision.should_start_source_io is True


def test_required_route_fails_before_source_io_with_recommendation() -> None:
    requirement = CapabilityRequirement(
        id="sink.auth.named_collection",
        domain=RuntimeCapability.AUTH,
        required_for="object_storage_pull",
        blocker_code="sink.auth.named_collection_missing",
        alternatives=("use_connection_mode_for_dev",),
    )
    decision = RouteCapabilityPlanner(advisor=RouteAlternativeAdvisor()).decide(
        candidates=(RouteCandidate(route_id="object_storage_pull", requirements=(requirement,), priority=10),),
        evidence={
            "sink.auth.named_collection": CapabilityEvidence.failure(
                requirement_id="sink.auth.named_collection",
                domain=RuntimeCapability.AUTH,
                server_version="24.8.1",
                blockers=("sink.auth.named_collection_missing",),
            )
        },
        mode="required",
        requested_route_id="object_storage_pull",
    )

    assert decision.selected_route_id == "blocked"
    assert decision.should_start_source_io is False
    assert decision.recommendations == (
        "Create the required named collection or allow a non-production credential mode explicitly.",
    )
    assert decision.to_evidence()["schema_version"] == "dpone.runtime.route_capabilities.v1"


def test_warn_only_keeps_requested_route_but_marks_warnings() -> None:
    requirement = CapabilityRequirement(
        id="sink.cluster.pull",
        domain=RuntimeCapability.CLUSTER,
        required_for="object_storage_pull",
        blocker_code="sink.cluster_pull_unsupported",
    )

    decision = RouteCapabilityPlanner().decide(
        candidates=(RouteCandidate(route_id="object_storage_pull", requirements=(requirement,), priority=10),),
        evidence={
            "sink.cluster.pull": CapabilityEvidence.failure(
                requirement_id="sink.cluster.pull",
                domain=RuntimeCapability.CLUSTER,
                blockers=("sink.cluster_pull_unsupported",),
            )
        },
        mode="warn_only",
        requested_route_id="object_storage_pull",
    )

    assert decision.selected_route_id == "object_storage_pull"
    assert decision.blockers == ()
    assert decision.warnings == ("sink.cluster_pull_unsupported",)


def test_probe_registry_maps_requirements_to_domain_and_specific_probes() -> None:
    registry = CapabilityProbeRegistry()
    registry.register_domain(
        RuntimeCapability.TRANSPORT,
        lambda requirement, context: CapabilityEvidence.success(
            requirement_id=requirement.id,
            domain=requirement.domain,
            checks=("domain_probe", str(context["route"])),
        ),
    )
    registry.register(
        "transport.native_tcp",
        lambda requirement, context: CapabilityEvidence.failure(
            requirement_id=requirement.id,
            domain=requirement.domain,
            blockers=("transport.native_tcp_missing",),
        ),
    )

    generic = registry.probe(
        CapabilityRequirement(
            id="transport.push_stream",
            domain=RuntimeCapability.TRANSPORT,
            required_for="typed_raw_streaming",
        ),
        context={"route": "typed_raw_streaming"},
    )
    specific = registry.probe(
        CapabilityRequirement(
            id="transport.native_tcp",
            domain=RuntimeCapability.TRANSPORT,
            required_for="native_tcp",
        ),
        context={"route": "native_tcp"},
    )

    assert generic.passed is True
    assert generic.checks == ("domain_probe", "typed_raw_streaming")
    assert specific.passed is False
    assert specific.blockers == ("transport.native_tcp_missing",)


def test_capability_evidence_publisher_records_json_safe_decision() -> None:
    publisher = CapabilityEvidencePublisher()
    decision = RouteCapabilityPlanner().decide(
        candidates=(
            RouteCandidate(
                route_id="typed_raw_streaming",
                requirements=(
                    CapabilityRequirement(
                        id="transport.push_stream",
                        domain=RuntimeCapability.TRANSPORT,
                        required_for="typed_raw_streaming",
                    ),
                ),
                priority=10,
            ),
        ),
        evidence={
            "transport.push_stream": CapabilityEvidence.success(
                requirement_id="transport.push_stream",
                domain=RuntimeCapability.TRANSPORT,
            )
        },
    )

    publisher.publish(decision)

    assert publisher.records[0]["decision_id"] == "runtime.route_capabilities"
    assert publisher.records[0]["selected_route_id"] == "typed_raw_streaming"


def test_manifest_schema_documents_runtime_capability_control() -> None:
    config_schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))
    batch_runtime = batch_schema["definitions"]["runtime_control"]["properties"]

    runtime_blocks = (
        config_schema["properties"]["runtime"]["properties"],
        batch_runtime,
    )
    for runtime in runtime_blocks:
        capabilities = runtime["capabilities"]["properties"]
        assert capabilities["mode"]["enum"] == ["auto", "required", "warn_only"]
        assert capabilities["mode"]["default"] == "auto"
        assert capabilities["explain_alternatives"]["default"] is True
    assert batch_schema["properties"]["runtime"]["$ref"] == "#/definitions/runtime_control"
    assert (
        batch_schema["definitions"]["process_fragment"]["properties"]["runtime"]["$ref"]
        == "#/definitions/runtime_control"
    )
