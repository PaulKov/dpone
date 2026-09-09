from __future__ import annotations

import copy
import importlib
from collections import Counter
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml
from tests.ci_shadow_pr3b_report_contract_support import policy
from tests.ci_shadow_pr3b_report_examples import profile_report, rebind_route, report_with_one_codeql_route

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"
CODEQL, CI, MERGE = ".github/workflows/codeql.yml", ".github/workflows/ci.yml", ".github/workflows/agent-pr-receipt.yml"


def _profile_api() -> Any:
    try:
        module = importlib.import_module("tools.agent_policy.workflow_privilege_profiles")
    except ModuleNotFoundError as exc:
        pytest.fail(f"RED: semantic privilege profile module is not implemented: {exc}", pytrace=False)
    missing = [name for name in ("validate_mandatory_profiles", "match_profile") if not hasattr(module, name)]
    if missing:
        pytest.fail(f"RED: semantic privilege profile API is incomplete: {', '.join(missing)}", pytrace=False)
    return module


def _marked_yaml(marker: str) -> dict[str, Any]:
    text = SPEC.read_text(encoding="utf-8")
    begin = f"<!-- {marker}:begin -->"
    end = f"<!-- {marker}:end -->"
    assert text.count(begin) == text.count(end) == 1
    fenced = text.split(begin, 1)[1].split(end, 1)[0]
    parsed = yaml.safe_load(fenced.split("```yaml\n", 1)[1].split("\n```", 1)[0])
    assert isinstance(parsed, dict)
    return parsed


def _codeql_workflow() -> dict[str, Any]:
    profile = _marked_yaml("pr3b-codeql-profile")
    return {
        "path": profile["workflow"],
        "name": "CodeQL",
        "on": profile["events"],
        "permissions": profile["permissions"],
        "env": {},
        "defaults": {},
        "jobs": profile["jobs"],
    }


def _governance_workflow() -> dict[str, Any]:
    profile = _marked_yaml("pr3b-adr0037-attestor-profile")
    producer, artifact, finalizer = profile["producer"], profile["producer"]["artifact"], profile["finalizer"]
    producer_job = {
        "needs": producer["needs"],
        "runs-on": producer["runs-on"],
        "permissions": producer["permissions"],
        "outputs": artifact["outputs"],
        "steps": [
            {
                "name": "Upload agent governance evidence",
                "id": artifact["step_id"],
                "uses": artifact["uses"],
                "with": artifact["with"],
            }
        ],
    }
    return {
        "path": profile["workflow"],
        "name": "CI",
        "on": profile["events"],
        "permissions": finalizer["workflow_permissions"],
        "env": finalizer["workflow_env"],
        "defaults": finalizer["workflow_defaults"],
        "jobs": {
            "quality": {"runs-on": "ubuntu-latest", "permissions": {"contents": "read"}, "steps": []},
            producer["job"]: producer_job,
            "governance-attestation": finalizer["job"],
        },
    }


def _merged_workflow() -> dict[str, Any]:
    parsed = yaml.safe_load((ROOT / MERGE).read_text(encoding="utf-8"))
    assert isinstance(parsed, dict)
    events = parsed.pop(True)
    parsed["on"] = events | {"pull_request": events["pull_request"] | {"types": ["edited", "closed"]}}
    return {"path": MERGE, **parsed}


def _workflows() -> dict[str, dict[str, Any]]:
    return {CODEQL: _codeql_workflow(), CI: _governance_workflow(), MERGE: _merged_workflow()}


def _profile_report(name: str) -> dict[str, Any]:
    return report_with_one_codeql_route() if name == "codeql" else profile_report(name)


def _evidence() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    routes: list[dict[str, Any]] = []
    authorities: list[dict[str, Any]] = []
    policy_value = policy()
    reports = zip(
        ("codeql", "governance_source", "merged_closure"),
        map(_profile_report, ("codeql", "governance", "merged_closure")),
        strict=True,
    )
    for profile_name, report in reports:
        for variant in policy_value["profiles"][profile_name]["event_variants"]:
            candidate = copy.deepcopy(report)
            candidate["routes"][0]["event_variant"] = variant
            rebind_route(candidate)
            routes.append(candidate["routes"][0])
            authorities.append(candidate["route_authority"][0])
    paired = sorted(zip(routes, authorities, strict=True), key=lambda pair: pair[0]["route_id"])
    return [pair[0] for pair in paired], [pair[1] for pair in paired]


def _as_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    converter = getattr(value, "to_mapping", None)
    if callable(converter):
        converted = converter()
        assert isinstance(converted, Mapping)
        return dict(converted)
    fields = ("id", "route_id", "workflow", "job_id", "fingerprint", "classification")
    if all(hasattr(value, field) for field in fields):
        return {field: getattr(value, field) for field in fields}
    pytest.fail(f"profile value is not a deterministic mapping/model: {value!r}")


def _codes(result: object) -> set[str]:
    codes: set[str] = set()
    for finding in getattr(result, "findings", ()):
        raw = finding.get("code") if isinstance(finding, Mapping) else getattr(finding, "code", None)
        code = getattr(raw, "value", raw)
        assert isinstance(code, str)
        codes.add(code)
    return codes


def _matches(result: object) -> list[dict[str, Any]]:
    return [_as_mapping(match) for match in getattr(result, "matches", ())]


def _validate(
    workflows: Mapping[str, Mapping[str, object]],
    routes: list[dict[str, Any]] | None = None,
    authorities: list[dict[str, Any]] | None = None,
) -> object:
    exact_routes, exact_authorities = _evidence()
    return _profile_api().validate_mandatory_profiles(
        workflows,
        routes if routes is not None else exact_routes,
        authorities if authorities is not None else exact_authorities,
        policy(),
    )


def _match(profile_name: str, workflows: Mapping[str, Mapping[str, object]]) -> Any:
    report = _profile_report(profile_name)
    return _profile_api().match_profile(
        report["routes"][0],
        report["route_authority"][0],
        workflows,
        policy(),
    )


def test_exact_three_mandatory_profiles_match_all_seven_correlated_coordinates() -> None:
    result = _validate(_workflows())
    matches = _matches(result)

    assert _codes(result) == set()
    assert Counter(match["id"] for match in matches) == {
        "CODEQL_PR_UPLOAD": 3,
        "ADR0037_GOVERNANCE_SOURCE_ATTESTOR": 3,
        "ADR0037_MERGED_CLOSURE_CHECK_PUBLISHER": 1,
    }
    expected_fingerprints = {
        value["id"]: value.get("semantic_sha256", value.get("envelope_sha256"))
        for value in policy()["profiles"].values()
    }
    assert all(match["fingerprint"] == expected_fingerprints[match["id"]] for match in matches)


@pytest.mark.parametrize("profile_name", ["codeql", "governance", "merged_closure"])
def test_each_exact_profile_matches_only_its_frozen_identity(profile_name: str) -> None:
    result = _match(profile_name, _workflows())
    match = _as_mapping(result.match)

    assert _codes(result) == set()
    assert match["route_id"] == _profile_report(profile_name)["routes"][0]["route_id"]
    assert match["id"] in {
        "CODEQL_PR_UPLOAD",
        "ADR0037_GOVERNANCE_SOURCE_ATTESTOR",
        "ADR0037_MERGED_CLOSURE_CHECK_PUBLISHER",
    }


def _drift_codeql_trigger(workflows: dict[str, dict[str, Any]]) -> None:
    workflows[CODEQL]["on"]["pull_request"]["paths"] = ["src/**"]


def _drift_governance_trigger(workflows: dict[str, dict[str, Any]]) -> None:
    workflows[CI]["on"].pop("workflow_dispatch")


def _drift_merge_trigger(workflows: dict[str, dict[str, Any]]) -> None:
    workflows[MERGE]["on"]["pull_request"]["types"] = ["closed"]


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        (_drift_codeql_trigger, "PRIVILEGE_CODEQL_PROFILE_DRIFT"),
        (_drift_governance_trigger, "PRIVILEGE_ADR0037_PROFILE_DRIFT"),
        (_drift_merge_trigger, "PRIVILEGE_ADR0037_PROFILE_DRIFT"),
    ],
)
def test_complete_trigger_mapping_drift_is_detected_before_route_matching(
    mutation: Callable[[dict[str, dict[str, Any]]], None],
    expected_code: str,
) -> None:
    workflows = _workflows()
    mutation(workflows)

    result = _validate(workflows)

    assert expected_code in _codes(result)


def _drift_codeql_envelope(workflows: dict[str, dict[str, Any]]) -> None:
    workflows[CODEQL]["jobs"]["analyze"]["steps"][0]["with"]["persist-credentials"] = True


def _drift_codeql_top_level(workflows: dict[str, dict[str, Any]]) -> None:
    workflows[CODEQL]["concurrency"] = {"group": "copied-profile"}


def _drift_governance_envelope(workflows: dict[str, dict[str, Any]]) -> None:
    workflows[CI]["jobs"]["governance-attestation"]["steps"].append({"run": "echo unsafe"})


def _duplicate_governance_upload(workflows: dict[str, dict[str, Any]]) -> None:
    producer = workflows[CI]["jobs"]["governance-source"]
    producer["steps"].append(copy.deepcopy(producer["steps"][-1]))


def _drift_merge_envelope(workflows: dict[str, dict[str, Any]]) -> None:
    workflows[MERGE]["jobs"]["merge-closure"]["permissions"]["checks"] = "read"


@pytest.mark.parametrize(
    ("profile_name", "mutation", "expected_code"),
    [
        ("codeql", _drift_codeql_envelope, "PRIVILEGE_CODEQL_PROFILE_DRIFT"),
        ("codeql", _drift_codeql_top_level, "PRIVILEGE_CODEQL_PROFILE_DRIFT"),
        ("governance", _drift_governance_envelope, "PRIVILEGE_ADR0037_PROFILE_DRIFT"),
        ("governance", _duplicate_governance_upload, "PRIVILEGE_ADR0037_PROFILE_DRIFT"),
        ("merged_closure", _drift_merge_envelope, "PRIVILEGE_ADR0037_PROFILE_DRIFT"),
    ],
)
def test_action_input_and_resolved_envelope_drift_cannot_match(
    profile_name: str,
    mutation: Callable[[dict[str, dict[str, Any]]], None],
    expected_code: str,
) -> None:
    workflows = _workflows()
    mutation(workflows)

    result = _match(profile_name, workflows)

    assert result.match is None
    assert expected_code in _codes(result)


@pytest.mark.parametrize(
    ("target", "field", "value"),
    [
        ("producer", "if", "always()"),
        ("upload", "if", False),
        ("upload", "continue-on-error", True),
    ],
)
def test_governance_producer_control_flow_drift_cannot_match(target: str, field: str, value: object) -> None:
    workflows = _workflows()
    producer = workflows[CI]["jobs"]["governance-source"]
    subject = (
        producer
        if target == "producer"
        else next(step for step in producer["steps"] if step["id"] == "governance-upload")
    )
    subject[field] = value

    result = _match("governance", workflows)

    assert result.match is None
    assert _codes(result) == {"PRIVILEGE_ADR0037_PROFILE_DRIFT"}


def test_missing_renamed_and_copied_profile_subjects_violate_exact_occurrence() -> None:
    missing = _workflows()
    missing.pop(CODEQL)
    renamed = _workflows()
    renamed[CI]["jobs"]["renamed-attestation"] = renamed[CI]["jobs"].pop("governance-attestation")
    copied = _workflows()
    copied[".github/workflows/copied.yml"] = copy.deepcopy(copied[CI])
    copied_job = copied[".github/workflows/copied.yml"]["jobs"]["governance-attestation"]
    copied[".github/workflows/copied.yml"].update(
        path=".github/workflows/copied.yml", name="Renamed", jobs={"renamed-attestation": copied_job}
    )

    assert "PRIVILEGE_CODEQL_PROFILE_DRIFT" in _codes(_validate(missing))
    assert "PRIVILEGE_ADR0037_PROFILE_DRIFT" in _codes(_validate(renamed))
    assert "PRIVILEGE_ADR0037_PROFILE_DRIFT" in _codes(_validate(copied))


def test_missing_duplicate_and_wrong_variant_profile_routes_violate_coordinate_counters() -> None:
    routes, authorities = _evidence()
    codeql_route = next(route for route in routes if route["workflow"] == CODEQL)
    missing_routes = [route for route in routes if route is not codeql_route]
    missing_authorities = [item for item in authorities if item["route_id"] != codeql_route["route_id"]]

    duplicate_routes = copy.deepcopy(routes)
    duplicate_authorities = copy.deepcopy(authorities)
    duplicate_routes.append(copy.deepcopy(duplicate_routes[0]))
    duplicate_authorities.append(copy.deepcopy(duplicate_authorities[0]))

    wrong_routes = copy.deepcopy(routes)
    wrong_authorities = copy.deepcopy(authorities)
    merge_index = next(index for index, route in enumerate(wrong_routes) if route["workflow"] == MERGE)
    wrong_routes[merge_index]["event_variant"] = "CLOSED_UNMERGED"
    report = {
        "routes": [wrong_routes[merge_index]],
        "route_authority": [wrong_authorities[merge_index]],
        "privileged_profiles": [],
    }
    rebind_route(report)

    assert "PRIVILEGE_CODEQL_PROFILE_DRIFT" in _codes(_validate(_workflows(), missing_routes, missing_authorities))
    assert _codes(_validate(_workflows(), duplicate_routes, duplicate_authorities)) & {
        "PRIVILEGE_CODEQL_PROFILE_DRIFT",
        "PRIVILEGE_ADR0037_PROFILE_DRIFT",
    }
    assert "PRIVILEGE_ADR0037_PROFILE_DRIFT" in _codes(_validate(_workflows(), wrong_routes, wrong_authorities))


def test_governance_route_must_keep_both_needs_edges_in_exact_order() -> None:
    routes, authorities = _evidence()
    index = next(index for index, route in enumerate(routes) if route["workflow"] == CI)
    routes[index]["edge_chain"] = list(reversed(routes[index]["edge_chain"]))
    report = {
        "routes": [routes[index]],
        "route_authority": [authorities[index]],
        "privileged_profiles": [],
    }
    rebind_route(report)

    result = _validate(_workflows(), routes, authorities)

    assert "PRIVILEGE_ADR0037_PROFILE_DRIFT" in _codes(result)


@pytest.mark.parametrize(
    ("profile_name", "field", "replacement", "expected_code"),
    [
        ("codeql", "environment", "production", "PRIVILEGE_CODEQL_PROFILE_DRIFT"),
        ("governance", "secrets", "EXPLICIT", "PRIVILEGE_ADR0037_PROFILE_DRIFT"),
        (
            "merged_closure",
            "runner",
            {"classification": "SELF_HOSTED", "labels": ["self-hosted"]},
            "PRIVILEGE_ADR0037_PROFILE_DRIFT",
        ),
    ],
)
def test_profile_authority_runner_secret_and_environment_must_be_exact(
    profile_name: str,
    field: str,
    replacement: object,
    expected_code: str,
) -> None:
    report = _profile_report(profile_name)
    report["route_authority"][0][field] = replacement

    result = _profile_api().match_profile(
        report["routes"][0],
        report["route_authority"][0],
        _workflows(),
        policy(),
    )

    assert result.match is None
    assert expected_code in _codes(result)
