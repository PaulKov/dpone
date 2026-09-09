from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("expression", "expected"),
    (
        pytest.param("${{ secrets }}", True, id="bare-secrets-object"),
        pytest.param("${{ toJSON(secrets) }}", True, id="secrets-object-argument"),
        pytest.param("${{ secrets.DEPLOY_KEY }}", True, id="repository-secret-dot-access"),
        pytest.param("${{ secrets['DEPLOY_KEY'] }}", True, id="repository-secret-bracket-access"),
        pytest.param("${{ SeCrEtS.DEPLOY_KEY }}", True, id="case-insensitive-repository-secret"),
        pytest.param("${{ 'safe\\' || secrets.DEPLOY_KEY }}", True, id="backslash-does-not-escape-quote"),
        pytest.param("${{ format('}} {0}', secrets.DEPLOY_KEY) }}", True, id="quoted-closing-braces"),
        pytest.param("${{ format('it''s }} {0}', secrets.DEPLOY_KEY) }}", True, id="doubled-quote-and-closing-braces"),
        pytest.param("${{ github.actor }}:${{ secrets.DEPLOY_KEY }}", True, id="later-expression"),
        pytest.param(
            "${{ secrets.GITHUB_TOKEN || secrets.DEPLOY_KEY }}",
            True,
            id="builtin-token-does-not-hide-repository-secret",
        ),
        pytest.param("${{ secrets.GITHUB_TOKEN_SUFFIX }}", True, id="builtin-token-prefix-is-not-exempt"),
        pytest.param("${{ secrets.GITHUB_TOKEN }}", False, id="builtin-token-dot-access"),
        pytest.param("${{ secrets['GITHUB_TOKEN'] }}", False, id="builtin-token-bracket-access"),
        pytest.param('${{ secrets["GITHUB_TOKEN"] }}', False, id="builtin-token-double-quoted-bracket"),
        pytest.param("${{ github.event.secrets }}", False, id="ordinary-member"),
        pytest.param("${{ github.event.items.*.secrets }}", False, id="object-filter-member"),
        pytest.param("${{ github.event.items.*. secrets }}", False, id="spaced-object-filter-member"),
        pytest.param("${{ github.event.items.*.secrets.DEPLOY_KEY }}", False, id="nested-object-filter-member"),
        pytest.param("${{ github.event.items.*['secrets'] }}", False, id="bracket-object-filter-member"),
        pytest.param("${{ format('secrets.DEPLOY_KEY', github.actor) }}", False, id="quoted-secret-text"),
        pytest.param(
            "${{ format('it''s secrets.DEPLOY_KEY }}', github.actor) }}", False, id="doubled-quoted-secret-text"
        ),
    ),
)
def test_secret_reference_lexer_distinguishes_repository_authority(expression: str, expected: bool) -> None:
    from tools.agent_policy.workflow_privilege_expressions import contains_secret_reference

    assert contains_secret_reference(expression) is expected


@pytest.mark.parametrize("tail", ("", "'"), ids=("unclosed-expression", "unclosed-quoted-expression"))
def test_secret_reference_lexer_is_bounded_at_workflow_byte_limit(tail: str) -> None:
    from tests.agent_policy.workflow_privilege_fixtures import LIMITS

    script = (
        "import time\nfrom tools.agent_policy.workflow_privilege_expressions import contains_secret_reference\n"
        f"limit={LIMITS['workflow_bytes']}\n"
        "value='${{'*(limit//4)+"
        f"{tail!r}\n"
        "value+='x'*(limit-len(value))\n"
        "started=time.monotonic()\n"
        "assert len(value)==limit and not contains_secret_reference(value)\n"
        "assert time.monotonic()-started < 5\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script], cwd=ROOT, capture_output=True, text=True, timeout=30, check=False
    )
    assert completed.returncode == 0, completed.stderr


def test_expression_overflow_preserves_graph_and_prior_route_fail_evidence(tmp_path: Path) -> None:
    from tests.agent_policy.workflow_privilege_fixtures import copy_repository_fixture
    from tools.agent_policy.workflow_privilege_service import scan_repository

    workflows = copy_repository_fixture(tmp_path, "target") / ".github/workflows"
    (workflows / "forbidden-target.yml").write_text(
        "name: Forbidden target\non: pull_request_target\npermissions: {}\njobs: {scan: {runs-on: ubuntu-latest}}\n",
        encoding="utf-8",
    )
    (workflows / "000-unsafe.yml").write_text(
        "name: Unsafe route\non:\n  pull_request:\n    types: [opened]\npermissions: {}\njobs:\n"
        "  unsafe:\n    runs-on: ubuntu-latest\n    permissions: write-all\n"
        "    steps: [{run: echo unsafe}]\n",
        encoding="utf-8",
    )
    oversized = ("true||" * 1_639) + "true"
    (workflows / "zzz-expression-overflow.yml").write_text(
        "name: Expression overflow\non:\n  pull_request:\n    types: [opened]\npermissions: {}\njobs:\n"
        f"  overflow:\n    if: ${{{{ {oversized} }}}}\n    runs-on: ubuntu-latest\n"
        "    steps: [{run: echo inspect}]\n",
        encoding="utf-8",
    )
    report = scan_repository(workflows.parents[1])
    assert (report["status"], report["ok"], report["inventory"]["complete"]) == ("FAIL", False, False)
    assert report["inventory"]["overflow_dimensions"] == ["expression_bytes"]
    assert [item["code"] for item in report["findings"]] == [
        "PRIVILEGE_PULL_REQUEST_TARGET",
        "PRIVILEGE_UNAPPROVED_PR_WRITE",
        "PRIVILEGE_WRITE_ALL",
        "PRIVILEGE_RESOURCE_LIMIT",
    ]
    assert all(item["route_id"] is not None for item in report["findings"][1:3])


def test_three_valued_boolean_transfer_table_is_complete() -> None:
    from tools.agent_policy.workflow_privilege_contracts import TruthValue
    from tools.agent_policy.workflow_privilege_expressions import tri_and, tri_not, tri_or

    true = TruthValue.TRUE
    false = TruthValue.FALSE
    unknown = TruthValue.UNKNOWN
    expected = {
        (true, true): (true, true),
        (true, false): (false, true),
        (true, unknown): (unknown, true),
        (false, true): (false, true),
        (false, false): (false, false),
        (false, unknown): (false, unknown),
        (unknown, true): (unknown, true),
        (unknown, false): (false, unknown),
        (unknown, unknown): (unknown, unknown),
    }

    assert {(left, right): (tri_and(left, right), tri_or(left, right)) for left, right in expected} == expected
    assert {value: tri_not(value) for value in TruthValue} == {
        true: false,
        false: true,
        unknown: unknown,
    }


def test_expression_subset_proves_only_closed_boolean_results() -> None:
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, TruthValue
    from tools.agent_policy.workflow_privilege_expressions import (
        direct_pr_context,
        evaluate_condition,
    )

    context = direct_pr_context("ACTIVITY:opened", target_branch_ref="refs/heads/master")

    assert evaluate_condition("github.event_name == 'PULL_REQUEST'", context, limits=V1_LIMITS).value is TruthValue.TRUE
    assert (
        evaluate_condition("startsWith(github.ref, 'REFS/PULL/')", context, limits=V1_LIMITS).value is TruthValue.TRUE
    )
    assert (
        evaluate_condition("github.ref != 'refs/pull/1/merge'", context, limits=V1_LIMITS).value is TruthValue.UNKNOWN
    )
    assert (
        evaluate_condition("startsWith(github.ref, 'refs/pull/1')", context, limits=V1_LIMITS).value
        is TruthValue.UNKNOWN
    )
    assert evaluate_condition("github.ref == 'refs/heads/master'", context, limits=V1_LIMITS).value is TruthValue.FALSE
    assert evaluate_condition("1 == '1'", context, limits=V1_LIMITS).value is TruthValue.UNKNOWN
    assert evaluate_condition("github.event_name", context, limits=V1_LIMITS).value is TruthValue.UNKNOWN
    assert evaluate_condition("false && unsupported(github.ref)", context, limits=V1_LIMITS).value is TruthValue.FALSE
    assert evaluate_condition("true || false && false", context, limits=V1_LIMITS).value is TruthValue.TRUE


def test_expression_integer_boundary_is_closed_at_640_digits() -> None:
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, TruthValue
    from tools.agent_policy.workflow_privilege_expressions import direct_pr_context, evaluate_condition

    context = direct_pr_context("ACTIVITY:opened", target_branch_ref="refs/heads/master")
    assert evaluate_condition(f"{'9' * 640} == {'9' * 640}", context, limits=V1_LIMITS).value is TruthValue.TRUE
    assert evaluate_condition(f"{'9' * 641} == 1", context, limits=V1_LIMITS).value is TruthValue.UNKNOWN


def test_expression_byte_and_token_limits_accept_n_and_reject_n_plus_one() -> None:
    from tests.agent_policy.workflow_privilege_fixtures import LIMITS
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, TruthValue
    from tools.agent_policy.workflow_privilege_expressions import (
        ExpressionLimitError,
        direct_pr_context,
        evaluate_condition,
    )

    context = direct_pr_context("ACTIVITY:opened", target_branch_ref="refs/heads/master")
    byte_maximum = LIMITS["expression_bytes"]
    base = "github.event_name == 'é'"
    exact_bytes = base + (" " * (byte_maximum - len(base.encode())))
    assert len(exact_bytes.encode()) == byte_maximum
    assert evaluate_condition(exact_bytes, context, limits=V1_LIMITS).value is TruthValue.FALSE
    with pytest.raises(ExpressionLimitError) as byte_error:
        evaluate_condition(f"{exact_bytes} ", context, limits=V1_LIMITS)
    assert byte_error.value.dimension == "expression_bytes"
    exact_tokens = "!" + ("false||" * 255) + "false"
    assert evaluate_condition(exact_tokens, context, limits=V1_LIMITS).value is TruthValue.TRUE
    with pytest.raises(ExpressionLimitError) as token_error:
        evaluate_condition(f"!{exact_tokens}", context, limits=V1_LIMITS)
    assert (token_error.value.dimension, LIMITS["expression_tokens"]) == ("expression_tokens", 512)


def test_safe_ast_bridge_preserves_the_closed_expression_grammar() -> None:
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, TruthValue
    from tools.agent_policy.workflow_privilege_expressions import direct_pr_context, evaluate_condition

    context = direct_pr_context("ACTIVITY:opened", target_branch_ref="refs/heads/master")

    assert evaluate_condition("!true == false", context, limits=V1_LIMITS).value is TruthValue.UNKNOWN
    assert evaluate_condition(r"'\\n' == 'n'", context, limits=V1_LIMITS).value is TruthValue.FALSE
    assert evaluate_condition("true == true == true", context, limits=V1_LIMITS).value is TruthValue.UNKNOWN
    assert evaluate_condition("(true, false)", context, limits=V1_LIMITS).value is TruthValue.UNKNOWN


def test_closed_merged_ref_uses_the_exact_filtered_target_or_unknown() -> None:
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, TruthValue
    from tools.agent_policy.workflow_privilege_expressions import direct_pr_context, evaluate_condition

    exact = direct_pr_context("CLOSED_MERGED", target_branch_ref="refs/heads/develop")
    unknown = direct_pr_context("CLOSED_MERGED", target_branch_ref=None)

    assert evaluate_condition("github.ref != 'refs/heads/master'", exact, limits=V1_LIMITS).value is TruthValue.TRUE
    assert (
        evaluate_condition("github.ref != 'refs/heads/master'", unknown, limits=V1_LIMITS).value is TruthValue.UNKNOWN
    )


def test_closed_variants_stay_correlated_and_workflow_run_resets_context() -> None:
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, EdgeKind, TruthValue
    from tools.agent_policy.workflow_privilege_expressions import (
        direct_pr_context,
        evaluate_condition,
        transition_context,
    )

    unmerged = direct_pr_context("CLOSED_UNMERGED", target_branch_ref="refs/heads/master")
    merged = direct_pr_context("CLOSED_MERGED", target_branch_ref="refs/heads/master")
    guard = "github.event.pull_request.merged == true && github.ref == 'refs/heads/master'"

    assert evaluate_condition(guard, unmerged, limits=V1_LIMITS).value is TruthValue.FALSE
    assert evaluate_condition(guard, merged, limits=V1_LIMITS).value is TruthValue.TRUE

    downstream = transition_context(
        merged,
        EdgeKind.WORKFLOW_RUN,
        workflow_run_activity="completed",
    )
    assert (
        evaluate_condition("github.event_name != 'pull_request'", downstream, limits=V1_LIMITS).value is TruthValue.TRUE
    )
    assert (
        evaluate_condition("github.event.action == 'completed'", downstream, limits=V1_LIMITS).value is TruthValue.TRUE
    )
    assert (
        evaluate_condition("github.ref == 'refs/heads/master'", downstream, limits=V1_LIMITS).value
        is TruthValue.UNKNOWN
    )
    assert (
        evaluate_condition("github.event.pull_request.merged == true", downstream, limits=V1_LIMITS).value
        is TruthValue.UNKNOWN
    )


def test_status_functions_are_explicit_needs_overrides() -> None:
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, JobResult, TruthValue
    from tools.agent_policy.workflow_privilege_expressions import (
        direct_pr_context,
        evaluate_condition,
        with_needs,
    )

    failed = with_needs(
        direct_pr_context("ACTIVITY:synchronize", target_branch_ref="refs/heads/master"),
        {"build": JobResult.FAILURE},
    )
    successful = with_needs(failed, {"build": JobResult.SUCCESS})
    cancelled = with_needs(failed, {"build": JobResult.CANCELLED})
    unknown = with_needs(failed, {"build": JobResult.UNKNOWN})
    mixed = with_needs(failed, {"build": JobResult.SUCCESS, "lint": JobResult.FAILURE})
    skipped = with_needs(failed, {"Build-Docs": JobResult.SKIPPED})

    failure = evaluate_condition("failure()", failed, limits=V1_LIMITS)
    always = evaluate_condition("always()", failed, limits=V1_LIMITS)
    assert (failure.value, failure.status_override) == (TruthValue.TRUE, True)
    assert (always.value, always.status_override) == (TruthValue.TRUE, True)
    assert evaluate_condition("success()", successful, limits=V1_LIMITS).value is TruthValue.TRUE
    assert evaluate_condition("success()", failed, limits=V1_LIMITS).value is TruthValue.FALSE
    cancelled_result = evaluate_condition("cancelled()", cancelled, limits=V1_LIMITS)
    negated_failure = evaluate_condition("!failure()", failed, limits=V1_LIMITS)
    assert (cancelled_result.value, cancelled_result.status_override) == (TruthValue.TRUE, True)
    assert (negated_failure.value, negated_failure.status_override) == (TruthValue.FALSE, True)
    assert evaluate_condition("success()", unknown, limits=V1_LIMITS).value is TruthValue.UNKNOWN
    assert evaluate_condition("failure()", mixed, limits=V1_LIMITS).value is TruthValue.TRUE
    assert evaluate_condition("success()", skipped, limits=V1_LIMITS).value is TruthValue.FALSE
    assert evaluate_condition("failure()", skipped, limits=V1_LIMITS).value is TruthValue.UNKNOWN
    assert evaluate_condition("cancelled()", skipped, limits=V1_LIMITS).value is TruthValue.UNKNOWN
    assert evaluate_condition("success()", mixed, limits=V1_LIMITS).value is TruthValue.FALSE
    assert (
        evaluate_condition("needs.build-docs.result == 'skipped'", skipped, limits=V1_LIMITS).value is TruthValue.TRUE
    )
    assert (
        evaluate_condition("needs.missing.result == 'success'", skipped, limits=V1_LIMITS).value is TruthValue.UNKNOWN
    )


def test_expression_overflow_and_terminal_mutation_remain_closed_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.agent_policy.workflow_privilege_fixtures import copy_repository_fixture
    from tools.agent_policy.workflow_privilege_service import scan_repository
    from tools.agent_policy.workflow_privilege_snapshot import SnapshotLease

    root = copy_repository_fixture(tmp_path, "target")
    oversized = ("true||" * 1_639) + "true"
    (root / ".github/workflows/expression-overflow.yml").write_text(
        "name: Expression overflow\non: pull_request\npermissions: {}\njobs:\n  inspect:\n"
        f"    if: ${{{{ {oversized} }}}}\n    runs-on: ubuntu-latest\n    steps: [{{run: echo inspect}}]\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(SnapshotLease, "_revalidates", lambda _self: False)

    report = scan_repository(root)

    assert (report["status"], report["inventory"]["complete"], report["inventory"]["manifest_sha256"]) == (
        "UNVERIFIED",
        False,
        None,
    )
    assert report["inventory"]["overflow_dimensions"] == ["expression_bytes"]
    assert [item["code"] for item in report["findings"]] == [
        "PRIVILEGE_CONCURRENT_MUTATION",
        "PRIVILEGE_RESOURCE_LIMIT",
    ]


def test_float_literal_is_unknown_and_cannot_hide_pr_write(tmp_path: Path) -> None:
    from tests.agent_policy.workflow_privilege_fixtures import copy_repository_fixture
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, TruthValue
    from tools.agent_policy.workflow_privilege_expressions import direct_pr_context, evaluate_condition
    from tools.agent_policy.workflow_privilege_service import scan_repository

    context = direct_pr_context("ACTIVITY:opened", target_branch_ref="refs/heads/master")
    assert evaluate_condition("1.0 == 2.0", context, limits=V1_LIMITS).value is TruthValue.UNKNOWN

    root = copy_repository_fixture(tmp_path, "target")
    (root / ".github/workflows/float-guard.yml").write_text(
        "name: Float guard\non: pull_request\npermissions: {}\njobs:\n  publish:\n"
        "    if: 1.0 == 2.0\n    runs-on: ubuntu-latest\n    permissions: {contents: write}\n    steps: []\n",
        encoding="utf-8",
    )
    report = scan_repository(root)

    assert report["status"] == "UNVERIFIED"
    assert "PRIVILEGE_UNKNOWN_EXPRESSION" in {item["code"] for item in report["findings"]}


@pytest.mark.parametrize("branches", ("master", "[master]"))
def test_scalar_and_list_branch_filters_have_identical_closed_ref_proof(branches: str, tmp_path: Path) -> None:
    from tests.agent_policy.workflow_privilege_fixtures import copy_repository_fixture
    from tools.agent_policy.workflow_privilege_service import scan_repository

    root = copy_repository_fixture(tmp_path, "target")
    (root / ".github/workflows/scalar-branch.yml").write_text(
        "name: Scalar branch\non:\n  pull_request:\n    types: [closed]\n"
        f"    branches: {branches}\npermissions: {{}}\njobs:\n  publish:\n"
        "    if: github.event.pull_request.merged == true && github.ref == 'refs/heads/master'\n"
        "    runs-on: ubuntu-latest\n    permissions: {contents: write}\n    steps: []\n",
        encoding="utf-8",
    )
    report = scan_repository(root)

    assert report["status"] == "FAIL"
    assert "PRIVILEGE_UNAPPROVED_PR_WRITE" in {item["code"] for item in report["findings"]}


def test_real_workflow_run_edge_resets_event_context_before_guard_proof(tmp_path: Path) -> None:
    from tests.agent_policy.workflow_privilege_fixtures import copy_repository_fixture
    from tools.agent_policy.workflow_privilege_service import scan_repository

    root = copy_repository_fixture(tmp_path, "target")
    workflows = root / ".github/workflows"
    (workflows / "context-producer.yml").write_text(
        "name: Context producer\non: pull_request\npermissions: {}\n"
        "jobs: {build: {runs-on: ubuntu-latest, steps: []}}\n",
        encoding="utf-8",
    )
    (workflows / "context-consumer.yml").write_text(
        "name: Context consumer\non:\n  workflow_run:\n    workflows: [Context producer]\n"
        "    types: [completed]\npermissions: {}\njobs:\n  deploy:\n"
        "    if: github.event_name != 'pull_request'\n    runs-on: ubuntu-latest\n"
        "    permissions: {contents: write}\n    steps: []\n",
        encoding="utf-8",
    )

    report = scan_repository(root)

    assert report["status"] == "FAIL"
    assert "PRIVILEGE_UNAPPROVED_PR_WRITE" in {item["code"] for item in report["findings"]}
    consumer = [route for route in report["routes"] if route["workflow"].endswith("context-consumer.yml")]
    assert consumer and {route["classification"] for route in consumer} == {"PR_HEAD"}
