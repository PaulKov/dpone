# Benchmark Source Citation Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add source registry, source verification, and claim-to-source traceability to the OSS code quality benchmark.

**Architecture:** Keep benchmark business logic split into small modules. `source_verification.py` owns source discovery, verification interfaces, stale-safe merge, and summary scoring; markdown/SVG/PR renderers only format already-merged evidence; `core.py` only orchestrates payload enrichment and artifact writes.

**Tech Stack:** Python stdlib, pytest, existing `tools/oss_benchmark` package, MkDocs, GitHub Actions.

---

### Task 1: Source Verification Domain

**Files:**
- Create: `tools/oss_benchmark/source_verification.py`
- Test: `tests/test_oss_code_quality_benchmark.py`

- [ ] **Step 1: Write failing source-registry tests**

```python
def test_source_verification_registry_links_claims_and_sources() -> None:
    payload = {
        "generated_at": "2026-06-14T00:00:00+00:00",
        "closed_core_notes": [{"name": "Fivetran", "url": "https://github.com/fivetran"}],
        "feature_parity": {
            "entries": [
                {
                    "tool": "airbyte",
                    "dimension": "cdc",
                    "sources": ["https://docs.airbyte.com/platform/understanding-airbyte/cdc"],
                },
                {"tool": "dpone", "dimension": "cdc", "sources": ["docs/cdc.md"]},
            ]
        },
        "public_evidence_integrity": {
            "claim_evidence": [
                {
                    "claim_id": "feature_parity_public_sources",
                    "source": "feature_parity.entries[].sources",
                    "status": "covered",
                }
            ]
        },
    }

    registry = tool.build_source_registry(payload)

    source_values = {source["value"]: source for source in registry}
    assert "https://docs.airbyte.com/platform/understanding-airbyte/cdc" in source_values
    assert "docs/cdc.md" in source_values
    assert "https://github.com/fivetran" in source_values
    assert "feature_parity_public_sources" in source_values[
        "https://docs.airbyte.com/platform/understanding-airbyte/cdc"
    ]["linked_claim_ids"]
```

- [ ] **Step 2: Write failing stale-preservation tests**

```python
def test_source_verification_preserves_previous_source_when_refresh_fails() -> None:
    class FakeVerifier:
        def verify(self, source, *, checked_at, verify_urls):
            if source["value"].endswith("/ok"):
                return tool.SourceCheckResult(
                    status="verified",
                    last_checked_at=checked_at,
                    content_hash="sha256:ok",
                    verification_mode="live-url",
                )
            return tool.SourceCheckResult(
                status="unavailable",
                last_checked_at=checked_at,
                verification_mode="live-url",
                last_error="404",
            )

    payload = {
        "generated_at": "2026-06-14T00:00:00+00:00",
        "feature_parity": {
            "entries": [
                {"tool": "dpone", "dimension": "ok", "sources": ["https://example.com/ok"]},
                {"tool": "dpone", "dimension": "bad", "sources": ["https://example.com/bad"]},
            ]
        },
    }
    previous_id = tool.source_id_for("https://example.com/bad")
    previous = {
        "source_verification": {
            "sources": [
                {
                    "source_id": previous_id,
                    "value": "https://example.com/bad",
                    "status": "verified",
                    "last_updated_at": "2026-06-01T00:00:00+00:00",
                    "last_checked_at": "2026-06-01T00:00:00+00:00",
                    "content_hash": "sha256:old",
                    "linked_claim_ids": ["feature:dpone:bad"],
                }
            ]
        }
    }

    verification = tool.build_source_verification(
        payload,
        previous_payload=previous,
        verifier=FakeVerifier(),
        verify_urls=True,
    )

    bad = {item["value"]: item for item in verification["sources"]}["https://example.com/bad"]
    assert bad["status"] == "stale"
    assert bad["last_updated_at"] == "2026-06-01T00:00:00+00:00"
    assert bad["content_hash"] == "sha256:old"
    assert bad["refresh_attempted_at"] == "2026-06-14T00:00:00+00:00"
    assert bad["last_error"] == "404"
```

- [ ] **Step 3: Implement minimal source verification**

Implement:
- `SourceCheckResult`
- `SourceVerifier` protocol
- `DefaultSourceVerifier`
- `source_id_for(value)`
- `build_source_registry(payload)`
- `build_source_verification(payload, previous_payload, verifier, verify_urls)`

Rules:
- URL sources use deterministic IDs and optional live checks.
- Local docs paths verify by repository-relative file existence and content hash.
- Failed checks with previous evidence become `stale`.
- Failed checks without previous evidence become `unavailable`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_oss_code_quality_benchmark.py -q`

Expected: source verification tests pass.

### Task 2: Renderers and PR Summary

**Files:**
- Create: `tools/oss_benchmark/renderers/source_verification.py`
- Create: `tools/oss_benchmark/renderers/source_verification_svg.py`
- Create: `tools/oss_benchmark/pr_source_verification.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Modify: `tools/oss_benchmark/pr_summary.py`
- Test: `tests/test_oss_code_quality_benchmark.py`

- [ ] **Step 1: Write failing renderer tests**

```python
def test_source_verification_markdown_svg_and_pr_summary() -> None:
    payload = {
        "source_verification": {
            "status": "verified",
            "summary": {
                "source_count": 2,
                "verified_count": 2,
                "stale_count": 0,
                "unavailable_count": 0,
                "claim_traceability_percent": 100,
                "source_health_score": 100,
            },
            "sources": [
                {
                    "source_id": "src_airbyte",
                    "value": "https://docs.airbyte.com/platform/understanding-airbyte/cdc",
                    "status": "verified",
                    "linked_claim_ids": ["feature_parity_public_sources"],
                    "last_checked_at": "2026-06-14T00:00:00+00:00",
                }
            ],
        }
    }

    markdown = tool.render_source_verification_section(payload)
    svg = tool.render_source_verification_svg(payload)
    pr = tool.render_pr_summary(payload)

    assert "Source Citation Verification" in markdown
    assert "claim-to-source matrix" in markdown
    assert "<svg" in svg
    assert "Source citation verification" in svg
    assert "Source Citation Verification" in pr
```

- [ ] **Step 2: Implement renderers**

Render:
- summary KPI table
- source health breakdown
- top source registry table
- claim-to-source matrix preview
- compact SVG scorecard
- PR summary block

- [ ] **Step 3: Wire renderers**

Add section to table of contents, markdown body, PR summary, and visual asset list.

### Task 3: Orchestration, CLI, Workflow, Docs

**Files:**
- Modify: `tools/oss_benchmark/core.py`
- Modify: `tools/oss_code_quality_benchmark.py`
- Modify: `tools/oss_benchmark/evidence_trust.py`
- Modify: `.github/workflows/oss-code-quality-benchmark.yml`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cicd/workflows.md`
- Test: `tests/test_oss_code_quality_benchmark.py`
- Test: `tests/test_cicd_docs_contracts.py`

- [ ] **Step 1: Write failing integration/contract tests**

Add tests for:
- `--verify-source-urls` CLI parsing.
- raw evidence includes `source_verification`.
- benchmark markdown links `assets/oss-source-verification.svg`.
- workflow exposes and forwards `verify_source_urls`.
- CI/CD docs mention source verification and stale-safe behavior.

- [ ] **Step 2: Implement wiring**

Pass `verify_source_urls` from CLI to `write_benchmark_outputs`, build `source_verification` after public evidence integrity, write `oss-source-verification.svg`, include it in provenance artifact checksums, and add provenance schema metadata.

- [ ] **Step 3: Run focused checks**

Run:

```bash
uv run ruff check tools/oss_code_quality_benchmark.py tools/oss_benchmark tests/test_oss_code_quality_benchmark.py tests/test_cicd_docs_contracts.py
uv run pytest tests/test_oss_code_quality_benchmark.py -q
uv run pytest tests/test_cicd_docs_contracts.py -q
```

### Task 4: Regenerate and Verify

**Files:**
- Modify generated benchmark markdown, raw JSON, SVG assets and PR summary.

- [ ] **Step 1: Regenerate benchmark artifacts**

Run:

```bash
uv run python tools/oss_code_quality_benchmark.py --workspace . --project dpone --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --baseline-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --allow-stale --runner-name codex-local --workflow-name local-source-citation-verification --max-stale-days 30 --external-analyzer-timeout 60 --pr-summary test_artifacts/oss-code-quality-benchmark/pr-comment.md --enforce-quality-gates
uv run dpone docs update-dev-metrics
```

- [ ] **Step 2: Full verification**

Run:

```bash
uv run ruff check tools/oss_code_quality_benchmark.py tools/oss_benchmark tests/test_oss_code_quality_benchmark.py tests/test_cicd_docs_contracts.py
uv run pytest tests/test_oss_code_quality_benchmark.py -q
uv run pytest tests/test_docs_language_contracts.py tests/test_cicd_docs_contracts.py -q
uv run dpone docs check-architecture-fitness --format json --top 30
uv run dpone docs update-dev-metrics --check
uv run --with mkdocs --with mkdocs-material --with pymdown-extensions mkdocs build --strict
```

- [ ] **Step 3: Public safety check**

Run:

```bash
rg -n "/Users/|/private/tmp|<repo-name>" docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json docs/benchmarks/oss-code-quality-benchmark-2026-06-12.md docs/benchmarks/assets
```

Expected: no matches.
