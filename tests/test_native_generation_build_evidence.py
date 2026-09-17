"""Actual result membership is checked against the fixed pack, never inferred."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_execution_evidence import DbtNodeOutcome
from dpone.contracts.dbt_execution_pack import dbt_target_identity_sha256
from dpone.contracts.dbt_runtime import dbt_target_binding_sha256
from dpone.contracts.native_generation_build_validation import validate_native_build_evidence
from tests.test_dbt_dev_evidence_runtime_writer import _evidence
from tests.test_dbt_runtime_execution import _pack, _preflight_manifest, _run_identity


def observations():
    pack = _pack()
    identity = _run_identity(pack)
    nodes = tuple(
        DbtNodeOutcome(uid, "pass" if uid.startswith("test.") else "success", 1.0)
        for uid in pack.selection_lock.expected_run_result_unique_ids
    )
    evidence = replace(
        _evidence(),
        workflow_id=pack.workflow_id,
        release_id=identity.release_id,
        deployment_id=identity.deployment_id,
        workload_pack_sha256=identity.workload_pack.sha256,
        project_bundle_sha256=pack.project_bundle_sha256,
        manifest_sha256=pack.selection_lock.manifest_sha256,
        selection_sha256=pack.selection_lock.selection_sha256,
        toolchain_sha256=pack.selection_lock.toolchain_sha256,
        invocation_context_sha256=pack.invocation_context.invocation_context_sha256,
        logical_target_sha256=dbt_target_identity_sha256(pack.profile),
        target_binding_sha256=dbt_target_binding_sha256(pack, identity),
        adapter_runtime=pack.adapter_runtime,
        adapter_policy_sha256=pack.adapter_policy.adapter_policy_sha256,
        graph_policy_sha256=pack.selection_lock.graph_policy_sha256,
        dbt_schema_version="https://schemas.getdbt.com/dbt/run-results/v6.json",
        nodes=nodes,
    )
    manifest = deepcopy(_preflight_manifest())
    manifest["metadata"]["invocation_id"] = evidence.invocation_id
    results = {
        "metadata": {
            "dbt_schema_version": evidence.dbt_schema_version,
            "dbt_version": pack.dbt_core_version,
            "generated_at": evidence.finished_at,
            "invocation_id": evidence.invocation_id,
            "env": {},
        },
        "elapsed_time": 2.0,
        "results": [
            {
                "unique_id": node.unique_id,
                "status": node.status,
                "execution_time": node.execution_time,
                "timing": [],
                "adapter_response": {},
            }
            for node in nodes
        ],
    }
    return pack, identity, evidence, manifest, results


class AcceptSchema:
    """Unit boundary only; actual official schema validators require live fixtures."""

    def validate(self, payload, *, version):
        return ()


def validate(pack, identity, evidence, manifest, results):
    return validate_native_build_evidence(
        evidence,
        pack=pack,
        run_identity=identity,
        manifest=manifest,
        run_results=results,
        manifest_validator=AcceptSchema(),
        results_validator=AcceptSchema(),
    )


def test_actual_results_match_fixed_expected_membership():
    values = observations()
    parsed = validate(*values)
    assert parsed.invocation_id == values[2].invocation_id
    assert len(parsed.nodes) == 2


def test_build_rejects_other_workload_identity_with_same_digest():
    pack, identity, evidence, manifest, results = observations()
    identity = replace(identity, workload_pack=replace(identity.workload_pack, id="dbt__other"))
    with pytest.raises(DbtPublishingError):
        validate(pack, identity, evidence, manifest, results)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_node",
        "duplicate_node",
        "failed_node",
        "invocation",
        "manifest_invocation",
        "selection",
        "deployment",
        "evidence_node",
    ],
)
def test_cohort_rejects_inconsistent_actual_results(mutation):
    pack, identity, evidence, manifest, results = observations()
    if mutation == "missing_node":
        results["results"].pop()
    elif mutation == "duplicate_node":
        results["results"][1] = results["results"][0]
    elif mutation == "failed_node":
        results["results"][0]["status"] = "error"
    elif mutation == "invocation":
        results["metadata"]["invocation_id"] = "other"
    elif mutation == "manifest_invocation":
        manifest["metadata"]["invocation_id"] = "other"
    elif mutation in {"selection", "deployment"}:
        evidence = replace(
            evidence, **{("selection_sha256" if mutation == "selection" else "deployment_id"): "sha256:" + "f" * 64}
        )
    else:
        evidence = replace(evidence, nodes=(replace(evidence.nodes[0], execution_time=2.0), evidence.nodes[1]))
    expected_error = DbtPublishingError if mutation in {"missing_node", "duplicate_node"} else ValueError
    with pytest.raises(expected_error):
        validate(pack, identity, evidence, manifest, results)


def writer_fixture(tmp_path, *, wrong_toolchain=False):
    from dpone.adapters.dbt_artifacts import LocalDbtExecutionEvidenceWriter
    from dpone.adapters.native_generation_invocation_auth import InvocationOriginalReader
    from dpone.contracts.dbt_contract_validation import artifact_json_bytes
    from dpone.contracts.native_identity import OriginalRef
    from dpone.contracts.native_trusted_dbt_environment_codec import decode_trusted_dbt_owned_root
    from dpone.runtime.native_generation_build_artifacts import CapturedBuildArtifactReader
    from dpone.services.native_generation_build_evidence import NativeGenerationBuildEvidenceWriter
    from tests.native_trusted_dbt_fixtures import InvocationFixture

    fixture = InvocationFixture(tmp_path)
    pack, identity, evidence, manifest, results = observations()
    if wrong_toolchain:
        from dpone.contracts.native_trusted_dbt_environment_codec import (
            decode_trusted_dbt_qualification,
            decode_trusted_dbt_toolchain,
            encode_trusted_dbt_qualification,
            encode_trusted_dbt_toolchain,
        )

        toolchain = decode_trusted_dbt_toolchain(fixture.store.documents["invocation/toolchain"])
        toolchain = replace(
            toolchain, dbt_contract=replace(toolchain.dbt_contract, contract_id="different-admitted-contract")
        )
        new_ref = fixture.store.add(
            "trusted_dbt_toolchain_v1", "invocation/toolchain", encode_trusted_dbt_toolchain(toolchain)
        )
        qualification = decode_trusted_dbt_qualification(fixture.store.documents["invocation/qualification"])
        fixture.store.add(
            "trusted_dbt_qualification_v1",
            "invocation/qualification",
            encode_trusted_dbt_qualification(replace(qualification, toolchain=new_ref)),
        )
    pack_ref = fixture.store.add(
        "generation_execution_pack_v1", "build/execution-pack.json", artifact_json_bytes(pack.to_dict())
    )
    output = decode_trusted_dbt_owned_root(fixture.store.documents["roots/OUTPUT"])
    admission = InvocationOriginalReader(
        originals=fixture.store, bindings=fixture.store, subject=fixture.store.subject, max_bytes=1048576
    )
    generation = InvocationOriginalReader(
        originals=fixture.store, bindings=fixture.store, subject=fixture.store.subject, max_bytes=1048576
    )

    def ref(locator):
        return OriginalRef(locator, fixture.store.bound[locator].payload_sha256)

    writer = NativeGenerationBuildEvidenceWriter(
        delegate=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json"),
        executor=fixture.executor,
        execution_pack=pack,
        execution_pack_ref=pack_ref,
        run_identity=identity,
        toolchain=ref("invocation/toolchain"),
        qualification=ref("invocation/qualification"),
        admission_reader=admission,
        generation_reader=generation,
        build_argv=fixture.args(2),
        project_directory=fixture.project,
        output_root=fixture.target.parent,
        artifact_reader=CapturedBuildArtifactReader(
            root=fixture.target.parent, target=fixture.target, identity=output, max_bytes=1048576
        ),
        require_termination=fixture.recorder.require_completion,
        publish_original=fixture.store.publish,
        manifest_validator=AcceptSchema(),
        results_validator=AcceptSchema(),
        max_artifact_bytes=1048576,
        max_metadata_bytes=1048576,
    )
    if wrong_toolchain:
        return writer, fixture, evidence
    fixture.complete()
    (fixture.target / "manifest.json").write_bytes(json.dumps(manifest).encode())
    (fixture.target / "run_results.json").write_bytes(json.dumps(results).encode())
    return writer, fixture, evidence


def test_writer_publishes_observed_cohort_once_and_independently_reads_it(tmp_path):
    writer, fixture, evidence = writer_fixture(tmp_path)
    path = writer.write(evidence)
    assert path.is_file()
    positive = writer.require_build_completion()
    assert positive.executor == fixture.executor
    publications = fixture.store.publications
    assert writer.write(evidence) == path
    assert writer.require_build_completion() == positive
    assert fixture.store.publications == publications
    assert len(fixture.delegate.calls) == 3


def test_writer_missing_actual_artifact_cannot_publish_positive_cohort(tmp_path):
    writer, fixture, evidence = writer_fixture(tmp_path)
    (fixture.target / "run_results.json").unlink()
    before = fixture.store.publications
    with pytest.raises(OSError):
        writer.write(evidence)
    assert fixture.store.publications == before
    with pytest.raises(ValueError):
        writer.require_build_completion()


def test_writer_lost_publication_ack_reconciles_without_another_write(tmp_path):
    writer, fixture, evidence = writer_fixture(tmp_path)
    fixture.store.lose_ack = True
    before = fixture.store.publications
    writer.write(evidence)
    positive = writer.require_build_completion()
    assert fixture.store.publications == before + 4
    writer.write(evidence)
    assert writer.require_build_completion() == positive
    assert fixture.store.publications == before + 4
    assert len(fixture.delegate.calls) == 3


def test_writer_failed_evidence_preserves_delegate_without_positive_originals(tmp_path):
    writer, fixture, evidence = writer_fixture(tmp_path)
    failed = replace(evidence, status="failed", code="DPONE_DBT_EXECUTION_FAILED", dbt_exit_code=1)
    before = fixture.store.publications
    assert writer.write(failed).is_file()
    assert fixture.store.publications == before
    with pytest.raises(ValueError):
        writer.require_build_completion()
    with pytest.raises(ValueError):
        writer.write(evidence)


def test_writer_readback_unavailable_cannot_return_cached_positive_claim(tmp_path):
    writer, fixture, evidence = writer_fixture(tmp_path)
    writer.write(evidence)
    fixture.store.fail_reads = True
    with pytest.raises(OSError):
        writer.require_build_completion()


def test_writer_rejects_corrupted_stored_artifact_on_independent_readback(tmp_path):
    writer, fixture, evidence = writer_fixture(tmp_path)
    writer.write(evidence)
    locator = next(key for key in fixture.store.documents if key.endswith("/build/run_results.json"))
    fixture.store.documents[locator] = b"{}"
    with pytest.raises(ValueError):
        writer.require_build_completion()


def test_writer_rejects_pack_not_bound_to_admitted_toolchain(tmp_path):
    with pytest.raises(ValueError):
        writer_fixture(tmp_path, wrong_toolchain=True)


def completion_consumer_fixture(tmp_path):
    from dpone.adapters.native_generation_invocation_auth import InvocationOriginalReader
    from dpone.contracts.native_identity import OriginalRef
    from dpone.contracts.native_source_custody import encode_source_trusted_build_completion
    from dpone.services.dbt_dev_evidence_contracts import validate_dbt_execution_evidence_contract
    from dpone.services.native_generation_completion_auth import NativeGenerationCompletionAuthenticator

    writer, fixture, evidence = writer_fixture(tmp_path)
    writer.write(evidence)
    completion = writer.require_build_completion()
    reference = fixture.store.add(
        "trusted_source_build_completion_v1",
        "build/completion.json",
        encode_source_trusted_build_completion(completion),
    )

    def ref(locator):
        return OriginalRef(locator, fixture.store.bound[locator].payload_sha256)

    def reader():
        return InvocationOriginalReader(
            originals=fixture.store,
            bindings=fixture.store,
            subject=fixture.store.subject,
            max_bytes=1048576,
        )

    consumer = NativeGenerationCompletionAuthenticator(
        executor=fixture.executor,
        execution_pack_ref=ref("build/execution-pack.json"),
        run_identity=observations()[1],
        qualification=ref("invocation/qualification"),
        admission_reader=reader(),
        metadata_reader=reader(),
        artifact_reader=reader(),
        evidence_decoder=validate_dbt_execution_evidence_contract,
        manifest_validator=AcceptSchema(),
        results_validator=AcceptSchema(),
    )
    return consumer, fixture, completion, reference


def test_completion_consumer_independently_authenticates_without_writer_or_dispatch(tmp_path):
    consumer, fixture, completion, reference = completion_consumer_fixture(tmp_path)
    before = fixture.store.publications
    consumer.authenticate(completion, reference)
    consumer.authenticate(completion, reference)
    assert len(fixture.delegate.calls) == 3
    assert fixture.store.publications == before


def test_completion_reader_authenticates_referenced_original_without_cached_value(tmp_path):
    consumer, fixture, completion, reference = completion_consumer_fixture(tmp_path)
    before = fixture.store.publications
    assert consumer.read_completion(reference) == completion
    fixture.store.fail_reads = True
    with pytest.raises(OSError):
        consumer.read_completion(reference)
    assert fixture.store.publications == before
    assert len(fixture.delegate.calls) == 3


def test_completion_reader_rejects_broken_cohort_even_when_top_level_record_is_valid(tmp_path):
    consumer, fixture, _, reference = completion_consumer_fixture(tmp_path)
    locator = next(key for key in fixture.store.documents if key.endswith("/build/run_results.json"))
    fixture.store.documents[locator] = b"{}"
    with pytest.raises(ValueError):
        consumer.read_completion(reference)


@pytest.mark.parametrize("suffix", ["/manifest.json", "/run_results.json", "/evidence.json", "/inventory.json"])
def test_completion_consumer_rejects_changed_retained_original(tmp_path, suffix):
    consumer, fixture, completion, reference = completion_consumer_fixture(tmp_path)
    locator = next(key for key in fixture.store.documents if "/build/" in key and key.endswith(suffix))
    fixture.store.documents[locator] = b"{}"
    with pytest.raises(ValueError):
        consumer.authenticate(completion, reference)


@pytest.mark.parametrize("field", ["size", "invocation", "pack", "termination"])
def test_completion_consumer_rejects_rebound_inconsistent_inventory(tmp_path, field):
    from dpone.contracts.native_generation_build_cohort import (
        decode_native_build_artifact_inventory,
        encode_native_build_artifact_inventory,
    )
    from dpone.contracts.native_identity import OriginalRef
    from dpone.contracts.native_source_custody import encode_source_trusted_build_completion

    consumer, fixture, completion, reference = completion_consumer_fixture(tmp_path)
    inventory = decode_native_build_artifact_inventory(fixture.store.documents[completion.artifact_inventory.locator])
    if field == "size":
        entry = replace(inventory.artifacts[0], size_bytes=inventory.artifacts[0].size_bytes + 1)
        inventory = replace(inventory, artifacts=(entry, inventory.artifacts[1]))
    elif field == "invocation":
        inventory = replace(inventory, dbt_invocation_id="another-dbt-invocation")
    else:
        name = "execution_pack" if field == "pack" else "termination"
        inventory = replace(inventory, **{name: OriginalRef("another/original", "sha256:" + "f" * 64)})
    inventory_ref = fixture.store.add(
        "native_build_artifact_inventory_v1",
        completion.artifact_inventory.locator,
        encode_native_build_artifact_inventory(inventory),
    )
    completion = replace(completion, artifact_inventory=inventory_ref)
    reference = fixture.store.add(
        "trusted_source_build_completion_v1",
        reference.locator,
        encode_source_trusted_build_completion(completion),
    )
    with pytest.raises(ValueError):
        consumer.authenticate(completion, reference)


@pytest.mark.parametrize("excess", [0, 1])
def test_completion_consumer_checks_rebound_terminal_at_admitted_budget(tmp_path, excess):
    from dpone.contracts.native_generation_build_cohort import (
        decode_native_build_artifact_inventory,
        encode_native_build_artifact_inventory,
    )
    from dpone.contracts.native_generation_invocation import (
        decode_trusted_dbt_invocation_completion,
        encode_trusted_dbt_invocation_completion,
    )
    from dpone.contracts.native_source_custody import encode_source_trusted_build_completion

    consumer, fixture, completion, reference = completion_consumer_fixture(tmp_path)
    terminal = decode_trusted_dbt_invocation_completion(fixture.store.documents[completion.termination.locator])
    terminal = replace(terminal, elapsed_microseconds=fixture.plan.total_termination_budget_seconds * 1000000 + excess)
    terminal_ref = fixture.store.add(
        "trusted_dbt_invocation_completion_v1",
        completion.termination.locator,
        encode_trusted_dbt_invocation_completion(terminal),
    )
    inventory = decode_native_build_artifact_inventory(fixture.store.documents[completion.artifact_inventory.locator])
    inventory_ref = fixture.store.add(
        "native_build_artifact_inventory_v1",
        completion.artifact_inventory.locator,
        encode_native_build_artifact_inventory(replace(inventory, termination=terminal_ref)),
    )
    completion = replace(completion, termination=terminal_ref, artifact_inventory=inventory_ref)
    reference = fixture.store.add(
        "trusted_source_build_completion_v1",
        reference.locator,
        encode_source_trusted_build_completion(completion),
    )
    if excess:
        with pytest.raises(ValueError, match="termination budget"):
            consumer.authenticate(completion, reference)
    else:
        consumer.authenticate(completion, reference)
