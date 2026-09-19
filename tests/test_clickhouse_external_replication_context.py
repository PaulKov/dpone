from __future__ import annotations

from dpone.contracts.clickhouse_external_replication import (
    ExternalArtifactReceipt,
    ExternalPublicationRequest,
)
from dpone.runtime.governance.ports import StagedLoadHandle, staged_load_handle_details
from dpone.runtime.governance.validation_snapshot import snapshot_staged_validation_values
from dpone.runtime.sinks.clickhouse_external_replication_context import ExternalStagedContext
from dpone.runtime.sinks.clickhouse_external_replication_receipt import ExternalReplicationReceipt


def test_external_context_survives_validation_without_entering_evidence() -> None:
    request = ExternalPublicationRequest(
        cluster="analytics_cluster",
        database="analytics",
        target="target_table",
        scheduler_invocation="scheduled-run",
        plan_sha256="f" * 64,
        artifact=ExternalArtifactReceipt(
            artifact_id="artifact-v1",
            sha256="a" * 64,
            byte_size=128,
            row_count=2,
            schema_sha256="b" * 64,
            content_sha256="c" * 64,
            replayable=True,
        ),
    )
    receipt = ExternalReplicationReceipt(
        target_key=request.target_key,
        operation_id=request.operation_id,
        generation_id=request.generation_id,
        inventory_digest="d" * 64,
        plan_digest=request.plan_sha256,
        artifact_sha256=request.artifact.sha256,
        member_ids=("member-a", "member-b"),
        authority_version=4,
        phase="STAGED",
    )
    context = ExternalStagedContext(
        request=request,
        staged_receipt=receipt,
        candidate_name="target_table__dpone_ext_candidate",
    )
    handle = StagedLoadHandle(
        staging_config=None,
        payload_schema=(),
        staged_rows=2,
        metadata={"external_publication": receipt.to_dict()},
        sink_state=context,
    )

    (snapshot,) = snapshot_staged_validation_values(handle)

    assert snapshot.sink_state == context
    assert "sink_state" not in staged_load_handle_details(snapshot)
