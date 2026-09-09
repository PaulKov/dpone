# Agent Governance Artifact Provenance Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Bind every enforced agent-control PR receipt to the exact downloaded governance artifact archive bytes by recording and validating local SHA-256 and size evidence.

**Architecture:** Add a pure archive-evidence helper that computes a local archive fingerprint from bytes and delegates JSON parsing to the existing content parser. Keep GitHub REST in `pr_receipt_github.py`, keep identity validation in `pr_receipt_github_metadata.py`, and keep compact copying in `pr_evidence_chain.py`/`audit_manifest.py`.

**Tech Stack:** Python stdlib `hashlib`, existing `urllib` GitHub helper, pytest, JSON Schema 2020-12, dpone agent-policy tooling.

---

### Task 1: Failing Archive Evidence Tests

**Files:**
- Create: `tests/agent_policy/test_governance_artifact_archive.py`
- Modify: `tests/agent_policy/test_pr_receipt_evidence_chain.py`

- [x] **Step 1: Write the failing pure archive test**

Create `tests/agent_policy/test_governance_artifact_archive.py` with:

```python
from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
import zipfile
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


governance_artifact_archive = _load(
    "dpone_agent_governance_artifact_archive_test",
    "tools/agent_policy/governance_artifact_archive.py",
)


def _archive() -> bytes:
    payload = {
        "schema_version": 1,
        "status": "PASS",
        "control_surface_changed": True,
        "changed_paths": ["tools/agent_policy/governance_gate.py"],
        "head_commit": "merge123",
        "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("agent_governance_gate.json", json.dumps(payload))
    return buffer.getvalue()


def test_archive_evidence_records_downloaded_bytes_fingerprint() -> None:
    raw = _archive()

    evidence = governance_artifact_archive.evidence_from_archive_bytes(raw)

    assert evidence.sha256 == "sha256:" + hashlib.sha256(raw).hexdigest()
    assert evidence.size_bytes == len(raw)
    assert evidence.content.status == "PASS"
    assert evidence.content.changed_paths == ["tools/agent_policy/governance_gate.py"]
```

- [x] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/agent_policy/test_governance_artifact_archive.py -q
```

Expected: FAIL because `tools/agent_policy/governance_artifact_archive.py` does not exist.

- [x] **Step 3: Add failing receipt mismatch tests**

In `tests/agent_policy/test_pr_receipt_evidence_chain.py`, add tests that construct `GitHubArtifactEvidence` with `archive_sha256` and `archive_size_bytes`:

```python
def test_pr_receipt_fails_when_downloaded_archive_digest_differs_from_metadata() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=pr_receipt.GitHubEvidence(
            head_sha="abc1234",
            required_checks=["Quality checks (3.11)"],
            check_runs=[
                pr_receipt.GitHubCheckEvidence(
                    name="Quality checks (3.11)",
                    source="check_run",
                    status="completed",
                    conclusion="success",
                    url="https://github.example/check",
                )
            ],
            statuses=[],
            artifacts=[
                pr_receipt.GitHubArtifactEvidence(
                    name="agent-governance-gate",
                    artifact_id=42,
                    workflow_run_head_sha="abc1234",
                    workflow_run_id=10,
                    digest="sha256:" + "a" * 64,
                    size_in_bytes=200,
                    archive_sha256="sha256:" + "b" * 64,
                    archive_size_bytes=200,
                    expired=False,
                    url="https://github.example/artifact",
                    content=governance_artifact_content.content_from_payload(
                        {
                            "schema_version": 1,
                            "status": "PASS",
                            "control_surface_changed": True,
                            "changed_paths": ["tools/agent_policy/governance_gate.py"],
                            "head_commit": "merge123",
                            "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
                        }
                    ),
                )
            ],
        ),
        require_github_evidence=True,
    )

    assert result.status == "FAIL"
    assert any("does not match downloaded archive SHA-256" in error for error in result.errors)


def test_pr_receipt_fails_when_downloaded_archive_size_differs_from_metadata() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=pr_receipt.GitHubEvidence(
            head_sha="abc1234",
            required_checks=["Quality checks (3.11)"],
            check_runs=[
                pr_receipt.GitHubCheckEvidence(
                    name="Quality checks (3.11)",
                    source="check_run",
                    status="completed",
                    conclusion="success",
                    url="https://github.example/check",
                )
            ],
            statuses=[],
            artifacts=[
                pr_receipt.GitHubArtifactEvidence(
                    name="agent-governance-gate",
                    artifact_id=42,
                    workflow_run_head_sha="abc1234",
                    workflow_run_id=10,
                    digest="sha256:" + "a" * 64,
                    size_in_bytes=200,
                    archive_sha256="sha256:" + "a" * 64,
                    archive_size_bytes=201,
                    expired=False,
                    url="https://github.example/artifact",
                    content=governance_artifact_content.content_from_payload(
                        {
                            "schema_version": 1,
                            "status": "PASS",
                            "control_surface_changed": True,
                            "changed_paths": ["tools/agent_policy/governance_gate.py"],
                            "head_commit": "merge123",
                            "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
                        }
                    ),
                )
            ],
        ),
        require_github_evidence=True,
    )

    assert result.status == "FAIL"
    assert any("size does not match downloaded archive size" in error for error in result.errors)
```

- [x] **Step 4: Run receipt test to verify it fails**

Run:

```bash
uv run pytest tests/agent_policy/test_pr_receipt_evidence_chain.py -q
```

Expected: FAIL because `GitHubArtifactEvidence` does not accept `archive_sha256` or `archive_size_bytes` yet, or because metadata validation does not check them.

### Task 2: Minimal Archive Fingerprint Implementation

**Files:**
- Create: `tools/agent_policy/governance_artifact_archive.py`
- Modify: `tools/agent_policy/pr_receipt_github.py`
- Modify: `tools/agent_policy/pr_receipt_github_metadata.py`
- Modify: `tools/agent_policy/setup_inventory.py`
- Modify: `tests/agent_policy/test_agent_policy.py`

- [x] **Step 1: Add the focused archive helper**

Create `tools/agent_policy/governance_artifact_archive.py` with a frozen dataclass, sibling content-parser loading, `evidence_from_archive_bytes()`, and `error_evidence()`.

- [x] **Step 2: Attach archive evidence in GitHub fetch**

Modify `GitHubArtifactEvidence` to include `archive_sha256` and `archive_size_bytes`. Replace `_fetch_artifact_content()` with an archive helper call that downloads bytes once, computes fingerprint evidence, and parses content from the same bytes.

- [x] **Step 3: Validate digest and size agreement**

Extend `governance_artifact_identity_errors()` to require local SHA-256, positive metadata/local size, digest equality, and size equality.

- [x] **Step 4: Register the new policy module**

Add `tools/agent_policy/governance_artifact_archive.py` to setup inventory and agent-policy inventory tests.

- [x] **Step 5: Run focused implementation tests**

Run:

```bash
uv run pytest tests/agent_policy/test_governance_artifact_archive.py tests/agent_policy/test_pr_receipt_evidence_chain.py -q
```

Expected: PASS.

### Task 3: Schema, Audit, Docs, and Metrics

**Files:**
- Modify: `evals/agent/pr-receipt.schema.json`
- Modify: `evals/agent/audit-manifest.schema.json`
- Modify: `tools/agent_policy/pr_evidence_chain.py`
- Modify: `tools/agent_policy/audit_manifest.py`
- Modify: `tests/agent_policy/test_audit_manifest.py`
- Modify: `docs/agent-governance.md`
- Modify: `docs/github-branch-protection.md`
- Modify: `docs/feature-design-agent-governance-artifact-content-verification.md`
- Modify: `docs/quality-metrics.md` if generated metrics change

- [x] **Step 1: Copy archive fields into compact evidence**

Add `archive_sha256` and `archive_size_bytes` to `pr_evidence_chain._artifact_payload()` and `audit_manifest._optional_chain_artifact()`.

- [x] **Step 2: Expand JSON schemas**

Add the two archive fields to raw artifact evidence and compact evidence-chain artifact definitions in both schemas.

- [x] **Step 3: Update schema-backed tests**

Update expected payloads in `test_pr_receipt_evidence_chain.py` and `test_audit_manifest.py` so schema validation proves the new contract.

- [x] **Step 4: Update docs**

Document that the receipt verifies metadata digest/size against the downloaded archive before trusting parsed governance content.

- [x] **Step 5: Refresh generated quality metrics if needed**

Run:

```bash
uv run dpone docs update-dev-metrics
```

Expected: `docs/quality-metrics.md` changes only if tracked quality metrics changed.

### Task 4: Validation and PR

**Files:**
- No new files.

- [x] **Step 1: Generate change-aware validation plan**

Run:

```bash
uv run python tools/agent_policy/select_checks.py --base-ref origin/master
```

Expected: plan includes agent-policy tests, docs checks, and broad Python gates.

- [x] **Step 2: Run focused tests**

Run:

```bash
uv run pytest tests/agent_policy/test_governance_artifact_archive.py tests/agent_policy/test_governance_artifact_content.py tests/agent_policy/test_pr_receipt.py tests/agent_policy/test_pr_receipt_evidence_chain.py tests/agent_policy/test_audit_manifest.py tests/agent_policy/test_agent_policy.py -q
```

Expected: PASS.

- [x] **Step 3: Run required quality/documentation checks**

Run:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
uv run dpone docs check-docs
uv run pytest tests/test_docs_language_contracts.py -q
```

Expected: PASS or explicit fix before PR.

- [x] **Step 4: Commit, push, and open PR**

Commit with:

```bash
git add tools/agent_policy tests/agent_policy evals/agent docs
git commit -m "Harden agent governance artifact provenance"
git push -u origin codex/agent-artifact-provenance
```

Open a PR against `master` with a body that references the approved spec, lists validation evidence, and includes owner-attestation checklist items for the final maintainer edit.
