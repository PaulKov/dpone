# Route Run Supervisor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `RouteRunSupervisorService` and `RouteRunEvidenceBundle` as a generic route lifecycle receipt for manifest/run/evidence/recovery decisions.

**Architecture:** Add focused route supervisor model, policy, and service modules under `dpone.ops.routes`, plus a tiny public facade and thin CLI wiring. The service reads existing artifacts, normalizes their status, evaluates conservative retry/manual-approval policy, and writes stable JSON/Markdown without executing heavy tests or mutating sinks.

**Tech Stack:** Python dataclasses, existing `RouteKey`/`RouteProfileCatalog`, existing `sha256_file`, argparse CLI, pytest, ruff, mypy, MkDocs.

---

### Task 1: Public Contract Tests

**Files:**
- Create: `tests/test_route_run_supervisor.py`
- Create: `tests/test_cli_route_run_supervisor_command.py`
- Create: `tests/test_route_run_supervisor_docs_contract.py`

- [ ] **Step 1: Write failing service tests**

```python
from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.route_run_supervisor import RouteRunSupervisorService


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _route(source: str = "mssql", sink: str = "clickhouse", strategy: str = "incremental_merge") -> dict[str, str]:
    return {"source": source, "sink": sink, "strategy": strategy}


def _artifact(path: Path, *, passed: bool = True, route: dict[str, str] | None = None, **extra: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "dpone.test.v1",
        "route": route or _route(),
        "passed": passed,
        "blockers": [] if passed else ["runtime.transient_connection"],
        "summary": "test artifact",
    }
    payload.update(extra)
    return _write_json(path, payload)


def test_route_run_supervisor_builds_ready_receipt(tmp_path: Path) -> None:
    artifacts = {
        "route_readiness": _artifact(tmp_path / "readiness.json"),
        "route_execution_ledger": _artifact(tmp_path / "ledger.json"),
        "state_promotion": _artifact(tmp_path / "state.json"),
    }

    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest="manifests/orders.yml",
        artifacts=artifacts,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert payload["schema_version"] == "dpone.route_run_supervisor.v1"
    assert payload["decision"]["status"] == "ready"
    assert payload["passed"] is True
    assert payload["run"]["run_id"] == "orders-run-001"
    assert payload["run"]["dataset"] == "analytics.orders"
    assert payload["run"]["manifest"] == "manifests/orders.yml"
    assert payload["phases"]["preflight"]["passed"] is True
    assert payload["phases"]["execution"]["passed"] is True
    assert payload["phases"]["state"]["passed"] is True
    assert "Route run receipt" in markdown


def test_route_run_supervisor_blocks_missing_required_evidence(tmp_path: Path) -> None:
    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={"route_readiness": _artifact(tmp_path / "readiness.json")},
    )

    assert report.passed is False
    assert report.decision.status == "blocked"
    assert "route_execution_ledger.missing" in report.blockers
    assert "state_promotion.missing" in report.blockers


def test_route_run_supervisor_classifies_retry_and_unsafe_retry(tmp_path: Path) -> None:
    retryable = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "retryable",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={
            "route_readiness": _artifact(tmp_path / "readiness.json"),
            "route_execution_ledger": _artifact(
                tmp_path / "ledger.json",
                passed=False,
                blockers=["runtime.transient_connection"],
                safe_to_retry=True,
            ),
            "state_promotion": _artifact(tmp_path / "state.json"),
        },
    )
    unsafe = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "unsafe",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={
            "route_readiness": _artifact(tmp_path / "readiness2.json"),
            "route_execution_ledger": _artifact(tmp_path / "ledger2.json"),
            "state_promotion": _artifact(
                tmp_path / "state2.json",
                passed=False,
                blockers=["state_promotion.commit_token_mismatch"],
                safe_to_retry=False,
            ),
        },
    )

    assert retryable.decision.status == "retryable"
    assert unsafe.decision.status == "unsafe_to_retry"


def test_route_run_supervisor_requires_manual_approval_for_schema_gate(tmp_path: Path) -> None:
    report = RouteRunSupervisorService().evaluate(
        output_dir=tmp_path / "route-run",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        run_id="orders-run-001",
        dataset="analytics.orders",
        manifest=None,
        artifacts={
            "route_readiness": _artifact(tmp_path / "readiness.json"),
            "route_execution_ledger": _artifact(tmp_path / "ledger.json"),
            "state_promotion": _artifact(tmp_path / "state.json"),
            "route_schema_evolution": _artifact(
                tmp_path / "schema.json",
                apply_decision={"mode": "manual_approval", "requires_approval": True},
            ),
        },
    )

    assert report.passed is False
    assert report.decision.status == "manual_approval_required"
    assert "route_schema_evolution.manual_approval_required" in report.blockers
```

- [ ] **Step 2: Write failing CLI tests**

```python
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_ops_route_run_supervisor_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    route = {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"}
    artifacts = {
        name: _write_json(tmp_path / f"{name}.json", {"route": route, "passed": True, "blockers": []})
        for name in ("route_readiness", "route_execution_ledger", "state_promotion")
    }

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-run-supervisor",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--run-id",
                "orders-run-001",
                "--dataset",
                "analytics.orders",
                "--manifest",
                "manifests/orders.yml",
                "--artifact",
                f"route_readiness={artifacts['route_readiness']}",
                "--artifact",
                f"route_execution_ledger={artifacts['route_execution_ledger']}",
                "--artifact",
                f"state_promotion={artifacts['state_promotion']}",
                "--output-dir",
                str(tmp_path / "route-run"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["decision"]["status"] == "ready"
    assert Path(payload["json_path"]).exists()
```

- [ ] **Step 3: Write failing docs contract test**

```python
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_route_run_supervisor_docs_are_self_service() -> None:
    required_docs = [
        "docs/route-run-supervisor.md",
        "docs/developer-route-run-supervisor.md",
    ]
    for relative_path in required_docs:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "Runbook" in text, relative_path
        assert "source -> sink" in text, relative_path
        assert "dpone ops route-run-supervisor" in text, relative_path

    ops_cli = (ROOT / "docs/ops-cli.md").read_text(encoding="utf-8")
    assert "dpone ops route-run-supervisor" in ops_cli

    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    assert "Route run supervisor: route-run-supervisor.md" in mkdocs
```

- [ ] **Step 4: Run tests to verify RED**

Run:

```bash
uv run pytest tests/test_route_run_supervisor.py tests/test_cli_route_run_supervisor_command.py tests/test_route_run_supervisor_docs_contract.py -q
```

Expected: FAIL because `dpone.ops.route_run_supervisor` and docs do not exist.

### Task 2: Models, Policy, and Service

**Files:**
- Create: `src/dpone/ops/routes/run_supervisor_models.py`
- Create: `src/dpone/ops/routes/run_supervisor_policy.py`
- Create: `src/dpone/ops/routes/run_supervisor.py`
- Create: `src/dpone/ops/route_run_supervisor.py`
- Modify: `src/dpone/ops/routes/__init__.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`

- [ ] **Step 1: Add report dataclasses**

Create `RouteRunIdentity`, `RouteRunEvidence`, `RouteRunPhase`, `RouteRunDecision`, and `RouteRunEvidenceBundle` with `to_dict()`, `to_json()`, `to_markdown()`, and `write()`.

- [ ] **Step 2: Add pure policy**

Create `RouteRunSupervisorPolicy.evaluate()` that accepts route profile state,
required domains, and normalized evidence, then returns `RouteRunDecision`.

- [ ] **Step 3: Add service orchestration**

Create `RouteRunSupervisorService.evaluate()` that builds `RouteKey`, looks up
`RouteProfileCatalog`, reads evidence artifacts, calls the policy, builds
phases, writes `route_run_receipt.json` and `route_run_receipt.md`, and returns
`RouteRunEvidenceBundle`.

- [ ] **Step 4: Wire facade and catalogs**

Expose `RouteRunSupervisorService` through `dpone.ops.route_run_supervisor`,
`ReadinessOpsCatalog.route_run_supervisor()`, `ReleaseReadinessOps`, and lazy
exports in `dpone.ops.routes`.

- [ ] **Step 5: Run service tests to verify GREEN**

Run:

```bash
uv run pytest tests/test_route_run_supervisor.py -q
```

Expected: PASS.

### Task 3: CLI Wiring

**Files:**
- Modify: `src/dpone/commands/ops_parsers_routes.py`
- Modify: `src/dpone/services/ops/command_handlers_routes.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`

- [ ] **Step 1: Add parser**

Add `register_route_run_supervisor_parser()` with `--source`, `--sink`,
`--strategy`, `--run-id`, `--dataset`, optional `--manifest`, repeatable
`--artifact name=path`, repeatable `--require`, `--output-dir`, and `--format`.

- [ ] **Step 2: Add handler**

Add `cmd_route_run_supervisor()` that parses artifacts with existing
`_parse_artifacts()`, invokes `_ops(ctx).route_run_supervisor().evaluate()`,
emits JSON/Markdown, and returns non-zero when `report.passed` is false.

- [ ] **Step 3: Register command**

Register `route-run-supervisor` in ops command exports and registry.

- [ ] **Step 4: Run CLI tests to verify GREEN**

Run:

```bash
uv run pytest tests/test_cli_route_run_supervisor_command.py -q
uv run dpone ops route-run-supervisor --help
```

Expected: PASS and help exits `0`.

### Task 4: Documentation and Architecture

**Files:**
- Create: `docs/route-run-supervisor.md`
- Create: `docs/developer-route-run-supervisor.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/operational-control-plane.md`
- Modify: `docs/route-release-gate.md`
- Modify: `docs/ci-cd.md`
- Modify: `mkdocs.yml`
- Update generated: `docs/cli-reference.md`
- Update generated: `docs/quality-metrics.md`

- [ ] **Step 1: Add user docs**

Document CLI usage, output files, decision statuses, evidence domains, and
operator Runbook in `docs/route-run-supervisor.md`.

- [ ] **Step 2: Add developer docs**

Document module boundaries, extension rules, dependency direction, tests, and
Runbook in `docs/developer-route-run-supervisor.md`.

- [ ] **Step 3: Update architecture and navigation docs**

Update operations, release gate, CI/CD, ops CLI, and MkDocs navigation so the
new command is discoverable.

- [ ] **Step 4: Regenerate docs**

Run:

```bash
uv run dpone docs update-cli-reference
uv run dpone docs update-dev-metrics
```

- [ ] **Step 5: Run docs tests to verify GREEN**

Run:

```bash
uv run pytest tests/test_route_run_supervisor_docs_contract.py -q
uv run dpone docs check-docs
uv run mkdocs build --strict
```

Expected: PASS.

### Task 5: Final Verification and PR

**Files:**
- All files changed by Tasks 1-4.

- [ ] **Step 1: Run focused route supervisor suite**

```bash
uv run pytest tests/test_route_run_supervisor.py tests/test_cli_route_run_supervisor_command.py tests/test_route_run_supervisor_docs_contract.py -q
```

- [ ] **Step 2: Run neighboring route regression tests**

```bash
uv run pytest tests/test_route_readiness.py tests/test_cli_route_readiness_command.py tests/test_route_release_gate.py tests/test_cli_route_release_gate_command.py tests/test_route_schema_evolution.py tests/test_route_reconciliation_repair.py -q
```

- [ ] **Step 3: Run quality gates**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run mkdocs build --strict
```

- [ ] **Step 4: Commit and open PR**

```bash
git add src/dpone/ops/routes/run_supervisor_models.py src/dpone/ops/routes/run_supervisor_policy.py src/dpone/ops/routes/run_supervisor.py src/dpone/ops/route_run_supervisor.py src/dpone/ops/routes/__init__.py src/dpone/ops/catalog_readiness.py src/dpone/ops/catalog_release_sections.py src/dpone/commands/ops_parsers_routes.py src/dpone/services/ops/command_handlers_routes.py src/dpone/services/ops/__init__.py src/dpone/commands/ops_cmd.py src/dpone/commands/registry_ops.py tests/test_route_run_supervisor.py tests/test_cli_route_run_supervisor_command.py tests/test_route_run_supervisor_docs_contract.py docs/route-run-supervisor.md docs/developer-route-run-supervisor.md docs/ops-cli.md docs/operational-control-plane.md docs/route-release-gate.md docs/ci-cd.md docs/cli-reference.md docs/quality-metrics.md mkdocs.yml docs/superpowers/specs/2026-06-14-route-run-supervisor-design.md docs/superpowers/plans/2026-06-14-route-run-supervisor.md
git commit -m "Add route run supervisor evidence bundle"
git push origin codex/route-run-supervisor
gh pr create --draft --base codex/route-schema-evolution-repair --head codex/route-run-supervisor --title "[codex] Add route run supervisor evidence bundle" --body "<clear PR description>"
```

## Self-review

- Spec coverage: route lifecycle receipt, CLI/API parity, status taxonomy,
  generic route policy, docs, architecture, and tests are covered.
- Placeholder scan: no `TBD` or deferred requirements remain.
- Type consistency: service returns `RouteRunEvidenceBundle`; decision status
  values match the design document.
