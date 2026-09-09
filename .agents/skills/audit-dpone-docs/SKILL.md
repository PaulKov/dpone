---
name: audit-dpone-docs
description: Audit or update dpone documentation, examples, navigation, architecture diagrams, runbooks, and first-time-user CJM. Use for feature completion and release gate R6.
---

# Audit dpone documentation

1. Read `docs/documentation-standard.md` and the feature specification.
2. Walk the complete first-time-user journey: discover, prepare, configure, run,
   observe, diagnose, recover, operate, and upgrade.
3. Verify every public feature has the appropriate tutorial, how-to, reference,
   architecture/explanation, runbook, examples, limitations, and cross-links.
4. Validate YAML indentation, schemas, CLI/API examples, expected outputs, and
   artifact paths. Prefer generated/tested references over copied lists.
5. Decompose a monolith only when audience, task, or update cadence differs; keep
   a stable overview and navigation path.
6. Run:

   ```bash
   uv run dpone docs check-docs
   uv run pytest tests/test_docs_language_contracts.py -q
   uv run mkdocs build --strict
   ```

7. Inspect rendered navigation and report stale, contradictory, orphaned, or
   hard-to-find content separately from build failures.
