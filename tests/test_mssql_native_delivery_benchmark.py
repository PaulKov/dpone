"""Hermetic producer/consumer fixtures never establish live certification."""

import json
from dataclasses import asdict
from pathlib import Path

import pytest

from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.runtime.native_delivery_benchmark import compare, content_sha256


def metric(value, unit="seconds"):
    return dict(
        value=value,
        unit=unit,
        availability="measured" if value is not None else "unavailable",
        reason=None if value is not None else "not_observed",
        provenance="fixture_clock",
    )


def retain(root, name, payload, status="PASS"):
    path = root / name
    path.write_text(json.dumps(payload), encoding="utf-8")
    return dict(path=name, sha256=content_sha256(path.read_bytes()), status=status)


def run_fixture(root, *, commit="a" * 40, seconds=10, execution="hermetic"):
    root.mkdir(parents=True, exist_ok=True)
    envelope = dict(
        schema_version=1,
        kind="native-delivery-run",
        producer=dict(name="hermetic-test", version="1", commit="c" * 40, dirty=False),
        subject=dict(commit=commit, dirty=False),
        route=dict(source="clickhouse", sink="mssql", strategy="full_refresh", mode="bounded_native"),
        workload=dict(id="narrow", seed=1, rows=3, columns=2, sha256="d" * 64),
        configuration=dict(sha256="e" * 64, limits=asdict(NativeChunkLimits(1000000, 2000000))),
        environment=dict(
            sha256="f" * 64, versions={"python": "3.12"}, target_layout_sha256="1" * 64, resource_profile={"cpus": 2}
        ),
        samples=[],
        status="PASS",
        limitations=[],
    )

    def receipt(sample_id, scope):
        ids = (
            ["empty_input", "rollback", "receipt_first_recovery", "source_free_resume"]
            if scope == "failure_recovery"
            else [
                "typed_content",
                "duplicate_multiplicity",
                "metadata_parity",
                "commit_receipt_binding",
                "outside_window_unchanged",
            ]
        )
        checks = [
            dict(
                id=name,
                status="N/A" if name == "outside_window_unchanged" else "PASS",
                method="not_applicable"
                if name == "outside_window_unchanged"
                else "live_observation"
                if name == "commit_receipt_binding"
                else "transaction_fixture"
                if scope == "failure_recovery"
                else "exact_typed_multiset",
                reason="full_refresh" if name == "outside_window_unchanged" else None,
                expected=3,
                observed=3,
                evidence=None,
            )
            for name in ids
        ]
        data = dict(
            schema_version=1,
            kind="native-delivery-correctness",
            subject_commit=commit,
            workload_sha256=envelope["workload"]["sha256"],
            configuration_sha256=envelope["configuration"]["sha256"],
            environment_sha256=envelope["environment"]["sha256"],
            sample_id=sample_id,
            route=envelope["route"],
            scope=scope,
            execution=execution,
            fixture=dict(id=sample_id, rows=3, sha256="2" * 64),
            checks=checks,
            status="PASS",
        )
        proof = retain(
            root,
            sample_id + "-proof.json",
            {
                **{
                    k: data[k]
                    for k in (
                        "schema_version",
                        "subject_commit",
                        "workload_sha256",
                        "configuration_sha256",
                        "environment_sha256",
                        "sample_id",
                        "route",
                        "execution",
                        "scope",
                        "fixture",
                        "checks",
                        "status",
                    )
                },
                "kind": "native-delivery-live-observation",
            },
        )
        data["checks"] = [{**check, "evidence": proof} for check in checks]
        return retain(root, sample_id + ".json", data)

    envelope["fidelity_receipt"] = receipt("fidelity", "type_fidelity")
    envelope["recovery_receipt"] = receipt("recovery", "failure_recovery")
    for index in range(4):
        sample_id = f"sample-{index}"
        envelope["samples"].append(
            dict(
                id=sample_id,
                is_warmup=index == 0,
                status="PASS",
                reason=None,
                visibility_seconds=metric(seconds),
                pipeline_seconds=metric(seconds + 1),
                correctness=receipt(sample_id, "sample"),
                observations=None,
                metrics={},
            )
        )
    path = root / "run.json"
    path.write_text(json.dumps(envelope), encoding="utf-8")
    return path


def test_hermetic_samples_produce_unverified_report_with_raw_references(tmp_path):
    baseline = run_fixture(tmp_path / "baseline")
    candidate = run_fixture(tmp_path / "candidate", commit="b" * 40, seconds=8)
    report = compare(baseline, candidate)
    assert report["status"] == "UNVERIFIED"
    assert report["workloads"][0]["baseline"]["successful_samples"] == 3
    assert report["workloads"][0]["ratio"] is None
    assert report["baseline"]["sha256"] == content_sha256(baseline.read_bytes())
    assert "single_workload_only" in report["limitations"]


def mutate(path, change):
    payload = json.loads(path.read_text())
    change(payload)
    path.write_text(json.dumps(payload))


def mutate_receipt(path, field, change):
    envelope = json.loads(path.read_text())
    ref = envelope[field] if isinstance(field, str) else envelope["samples"][field]["correctness"]
    receipt_path = path.parent / ref["path"]
    mutate(receipt_path, change)
    ref["sha256"] = content_sha256(receipt_path.read_bytes())
    path.write_text(json.dumps(envelope))


@pytest.mark.parametrize("seconds,expected", [(8.5, "PASS"), (8.6, "FAIL"), (10.6, "FAIL")])
def test_live_shape_gate_boundaries_are_hermetic_contract_tests(tmp_path, seconds, expected):
    # execution=live here tests consumer branching only, not actual live evidence.
    baseline = run_fixture(tmp_path / "b", execution="live")
    candidate = run_fixture(tmp_path / "c", seconds=seconds, execution="live")
    result = compare(baseline, candidate)
    assert result["status"] == expected
    assert result["workloads"][0]["ratio"] == seconds / 10
    assert "p95" not in result["workloads"][0]


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r.pop("subject"),
        lambda r: r.update(schema_version=2),
        lambda r: r["subject"].update(commit="short"),
        lambda r: r["workload"].update(seed=2),
        lambda r: r["environment"].update(resource_profile={"cpus": 4}),
        lambda r: r["configuration"]["limits"].update(parallelism=4),
        lambda r: r["configuration"].update(sha256="9" * 64),
        lambda r: r["samples"].append(r["samples"][0]),
        lambda r: r["samples"][1]["visibility_seconds"].update(value=float("nan")),
    ],
)
def test_schema_identity_and_drift_rejected(tmp_path, change):
    from dpone.runtime.native_delivery_benchmark import BenchmarkInputError

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    mutate(candidate, change)
    with pytest.raises(BenchmarkInputError):
        compare(baseline, candidate)


def test_tampered_receipt_bytes_rejected_even_when_supplied_pass(tmp_path):
    from dpone.runtime.native_delivery_benchmark import BenchmarkInputError

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    (candidate.parent / "sample-1.json").write_text("{}")
    with pytest.raises(BenchmarkInputError, match="artifact_hash_mismatch"):
        compare(baseline, candidate)


@pytest.mark.parametrize(
    "change,expected",
    [
        (lambda r: r["samples"].pop(), "UNVERIFIED"),
        (lambda r: r["samples"].pop(0), "UNVERIFIED"),
        (lambda r: r["subject"].update(dirty=True), "UNVERIFIED"),
        (lambda r: r["samples"][1].update(visibility_seconds=metric(None)), "UNVERIFIED"),
    ],
)
def test_ineligible_trials_never_produce_a_ratio(tmp_path, change, expected):
    baseline = run_fixture(tmp_path / "b", execution="live")
    candidate = run_fixture(tmp_path / "c", seconds=8, execution="live")
    mutate(candidate, change)
    result = compare(baseline, candidate)
    assert result["status"] == expected
    assert result["workloads"][0]["ratio"] is None


def test_failed_fidelity_blocks_success_despite_fast_samples(tmp_path):
    baseline = run_fixture(tmp_path / "b", execution="live")
    candidate = run_fixture(tmp_path / "c", seconds=8, execution="live")

    def fail_fidelity(receipt):
        receipt["checks"][0].update(status="FAIL", reason="wrong_type")
        proof = retain(
            candidate.parent,
            "failed-fidelity-proof.json",
            {
                **{
                    k: receipt[k]
                    for k in (
                        "schema_version",
                        "subject_commit",
                        "workload_sha256",
                        "configuration_sha256",
                        "environment_sha256",
                        "sample_id",
                        "route",
                        "execution",
                        "scope",
                        "fixture",
                        "status",
                    )
                },
                "kind": "native-delivery-live-observation",
                "checks": [{**c, "evidence": None} for c in receipt["checks"]],
            },
        )
        for check in receipt["checks"]:
            check["evidence"] = proof

    mutate_receipt(candidate, "fidelity_receipt", fail_fidelity)
    assert compare(baseline, candidate)["status"] == "FAIL"


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r.update(subject_commit="9" * 40),
        lambda r: r.update(workload_sha256="9" * 64),
        lambda r: r.update(sample_id="wrong"),
        lambda r: r.update(scope="type_fidelity"),
    ],
)
def test_receipt_identity_checked_after_valid_hash(tmp_path, change):
    from dpone.runtime.native_delivery_benchmark import BenchmarkInputError

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    mutate_receipt(candidate, 1, change)
    with pytest.raises(BenchmarkInputError, match="receipt_identity_mismatch"):
        compare(baseline, candidate)


@pytest.mark.parametrize("kind", ["traversal", "absolute", "symlink"])
def test_reference_escape_is_rejected(tmp_path, kind):
    from dpone.runtime.native_delivery_benchmark import BenchmarkInputError

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    outside = tmp_path / "outside.json"
    outside.write_bytes((candidate.parent / "fidelity.json").read_bytes())
    link = candidate.parent / "escaped.json"
    link.symlink_to(outside)
    name = {"traversal": "../outside.json", "absolute": str(outside), "symlink": "escaped.json"}[kind]
    mutate(candidate, lambda r: r["fidelity_receipt"].update(path=name))
    with pytest.raises(BenchmarkInputError, match="artifact_path_escape"):
        compare(baseline, candidate)


def campaign(root, runs, ids):
    refs = [
        dict(path=str(path.relative_to(root)), sha256=content_sha256(path.read_bytes()), status="PASS") for path in runs
    ]
    retain(
        root,
        "campaign.json",
        dict(schema_version=1, kind="native-delivery-campaign", workloads=ids, runs=refs, limitations=[]),
    )
    return root / "campaign.json"


def test_campaign_requires_all_predeclared_cases_and_every_ratio_limit(tmp_path):
    b1 = run_fixture(tmp_path / "b" / "one", execution="live")
    c1 = run_fixture(tmp_path / "c" / "one", seconds=8, execution="live")
    baseline = campaign(tmp_path / "b", [b1], ["narrow", "wide"])
    candidate = campaign(tmp_path / "c", [c1], ["narrow", "wide"])
    assert compare(baseline, candidate)["status"] == "UNVERIFIED"
    b2 = run_fixture(tmp_path / "b" / "two", execution="live")
    c2 = run_fixture(tmp_path / "c" / "two", seconds=10.5, execution="live")
    for path in (b2, c2):
        mutate(path, lambda r: r["workload"].update(id="wide"))
    baseline = campaign(tmp_path / "b", [b1, b2], ["narrow", "wide"])
    candidate = campaign(tmp_path / "c", [c1, c2], ["narrow", "wide"])
    assert compare(baseline, candidate)["status"] == "PASS"
    mutate(c2, lambda r: [s.update(visibility_seconds=metric(10.6)) for s in r["samples"]])
    candidate = campaign(tmp_path / "c", [c1, c2], ["narrow", "wide"])
    assert compare(baseline, candidate)["status"] == "FAIL"


def test_atomic_output_hash_overwrite_and_alias_protection(tmp_path):
    from dpone.runtime.native_delivery_benchmark import BenchmarkInputError, canonical_json

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    output = tmp_path / "comparison.json"
    report = compare(baseline, candidate, output=output)
    assert json.loads(output.read_text()) == report
    digest = report.pop("sha256")
    assert content_sha256(canonical_json(report)) == digest
    with pytest.raises(BenchmarkInputError, match="output_exists"):
        compare(baseline, candidate, output=output)
    assert compare(baseline, candidate, output=output, overwrite=True)["status"] == "UNVERIFIED"
    for path in (baseline, candidate.parent / "fidelity.json"):
        with pytest.raises(BenchmarkInputError, match="output_aliases"):
            compare(baseline, candidate, output=path, overwrite=True)


def test_atomic_write_failure_preserves_previous_report_and_cleans_temp(tmp_path, monkeypatch):
    import dpone.runtime.native_delivery_benchmark_artifacts as module

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    output = tmp_path / "comparison.json"
    output.write_text("previous")

    def fail(*args):
        raise OSError("cannot replace")

    monkeypatch.setattr(module.os, "replace", fail)
    with pytest.raises(OSError):
        compare(baseline, candidate, output=output, overwrite=True)
    assert output.read_text() == "previous"
    assert not list(tmp_path.glob(".native-delivery-*"))


def test_atomic_no_clobber_concurrent_publication(tmp_path, monkeypatch):
    import dpone.runtime.native_delivery_benchmark_artifacts as module

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    output = tmp_path / "comparison.json"
    original = module.os.link

    def concurrent_link(source, destination):
        output.write_text("concurrent writer")
        return original(source, destination)

    monkeypatch.setattr(module.os, "link", concurrent_link)
    with pytest.raises(FileExistsError):
        compare(baseline, candidate, output=output)
    assert output.read_text() == "concurrent writer"
    assert not list(tmp_path.glob(".native-delivery-*"))


def test_cli_status_streams_help_and_no_output_on_invalid_input(tmp_path, capsys):
    from tools.native_delivery_benchmark import main

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    output = tmp_path / "comparison.json"
    args = ["compare", "--baseline", str(baseline), "--candidate", str(candidate), "--output", str(output)]
    assert main(args) == 0
    assert capsys.readouterr().out == f"{output} UNVERIFIED\n"
    assert main(args) == 2
    captured = capsys.readouterr()
    assert captured.out == "" and "overwrite" in captured.err
    output.unlink()
    candidate.write_text('{"password":"SECRET","schema_version":9}')
    assert main(args) == 2
    assert "SECRET" not in str(capsys.readouterr())
    assert not output.exists()
    with pytest.raises(SystemExit) as caught:
        main(["compare", "--help"])
    assert caught.value.code == 0


def test_documented_python_and_cli_examples_execute_or_parse():
    import re
    import shlex

    from tools.native_delivery_benchmark import build_parser

    guide = Path("docs/delivery-acceleration/observations.md").read_text()
    for block in re.findall(r"```python\n(.*?)\n```", guide, re.S):
        exec(compile(block, "observations.md", "exec"), {})
    for block in re.findall(r"```bash\n(.*?)\n```", guide, re.S):
        for command in block.splitlines():
            tokens = shlex.split(command)
            if "tools/native_delivery_benchmark.py" in tokens:
                args = tokens[tokens.index("tools/native_delivery_benchmark.py") + 1 :]
                if "--help" in args:
                    with pytest.raises(SystemExit) as caught:
                        build_parser().parse_args(args)
                    assert caught.value.code == 0
                else:
                    parsed = build_parser().parse_args(args)
                    assert parsed.command == "compare"
                    assert parsed.output == Path("comparison.json")


def test_output_outside_input_tree_retains_relative_verified_evidence(tmp_path):
    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    destination = tmp_path / "reports"
    destination.mkdir()
    output = destination / "comparison.json"
    result = compare(baseline, candidate, output=output)
    for key in ("baseline", "candidate"):
        ref = result[key]
        path = destination / ref["path"]
        assert not Path(ref["path"]).is_absolute() and ".." not in Path(ref["path"]).parts
        assert content_sha256(path.read_bytes()) == ref["sha256"]
        assert (path.parent / "sample-1.json").exists()
    assert compare(baseline, candidate, output=output, overwrite=True) == result


def test_bundled_internal_symlink_reference_keeps_retained_bytes(tmp_path):
    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    (candidate.parent / "alias.json").symlink_to("fidelity.json")
    mutate(candidate, lambda r: r["fidelity_receipt"].update(path="alias.json"))
    destination = tmp_path / "reports"
    destination.mkdir()
    result = compare(baseline, candidate, output=destination / "comparison.json")
    retained = destination / result["candidate"]["path"]
    assert (retained.parent / "alias.json").read_bytes() == (candidate.parent / "fidelity.json").read_bytes()
    assert compare(destination / result["baseline"]["path"], retained)["status"] == "UNVERIFIED"


def test_evidence_assertions_preserve_json_scalar_types(tmp_path):
    from dpone.runtime.native_delivery_benchmark import BenchmarkInputError

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")

    def substitute(receipt):
        proof = json.loads((candidate.parent / receipt["checks"][0]["evidence"]["path"]).read_text())
        receipt["checks"][0].update(expected={"rows": 1}, observed={"rows": 1})
        proof["checks"][0].update(expected={"rows": True}, observed={"rows": True})
        ref = retain(candidate.parent, "scalar-substitution.json", proof)
        for check in receipt["checks"]:
            check["evidence"] = ref

    mutate_receipt(candidate, 1, substitute)
    with pytest.raises(BenchmarkInputError, match="live_observation_check_mismatch"):
        compare(baseline, candidate)


def test_cli_argument_errors_do_not_echo_sensitive_unrecognized_values(capsys):
    from tools.native_delivery_benchmark import main

    with pytest.raises(SystemExit) as caught:
        main(["compare", "--baseline", "b.json", "--candidate", "c.json", "--output", "out.json", "--password=SECRET"])
    assert caught.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == "" and "SECRET" not in captured.err


def test_raw_schema_version_boolean_is_not_integer_identity(tmp_path):
    from dpone.runtime.native_delivery_benchmark import BenchmarkInputError

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")

    def substitute(receipt):
        proof = json.loads((candidate.parent / receipt["checks"][0]["evidence"]["path"]).read_text())
        proof["schema_version"] = True
        ref = retain(candidate.parent, "bool-schema.json", proof)
        for check in receipt["checks"]:
            check["evidence"] = ref

    mutate_receipt(candidate, 1, substitute)
    with pytest.raises(BenchmarkInputError, match="live_observation_identity_mismatch"):
        compare(baseline, candidate)


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r.update(schema_version=1.0),
        lambda r: r["workload"].update(seed=1.0),
        lambda r: r["environment"].update(resource_profile={"cpus": 2.0}),
    ],
)
def test_identity_records_do_not_coerce_integer_or_resource_types(tmp_path, change):
    from dpone.runtime.native_delivery_benchmark import BenchmarkInputError

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    mutate(candidate, change)
    with pytest.raises(BenchmarkInputError):
        compare(baseline, candidate)


@pytest.mark.parametrize("version", [True, 1.0])
def test_observation_sidecar_version_requires_integer(tmp_path, version):
    from dpone.runtime.native_delivery_benchmark import BenchmarkInputError

    baseline, candidate = run_fixture(tmp_path / "b"), run_fixture(tmp_path / "c")
    ref = retain(
        candidate.parent,
        "observations.json",
        dict(schema_version=version, kind="native-delivery-observations", status="PASS"),
    )
    mutate(candidate, lambda r: r["samples"][1].update(observations=ref))
    with pytest.raises(BenchmarkInputError, match="invalid_observations"):
        compare(baseline, candidate)
