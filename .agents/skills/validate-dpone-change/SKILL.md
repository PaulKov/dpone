---
name: validate-dpone-change
description: Select and execute the correct focused and broad checks for a dpone diff. Use after code, docs, schema, connector, workflow, packaging, or release changes; never treat skipped live checks as passed.
---

# Validate a dpone change

1. Generate a plan:

   ```bash
   uv run python tools/agent_policy/select_checks.py --base-ref origin/master
   ```

2. Inspect the diff and add any contract-specific checks the router cannot infer.
3. Run focused tests first and fix root causes rather than weakening assertions.
4. Run the required broad checks from `AGENTS.md` and `docs/quality-tooling.md`.
5. For docs, run `dpone docs check-docs`, docs contract tests, and strict MkDocs.
6. For packaging, build all affected packages and run fresh-environment smoke
   where required.
7. Run live integration only in an explicitly approved environment. Record a
   missing environment as SKIP/UNVERIFIED, not PASS.
8. Return a table with command, status, observed result, duration if known,
   artifact path, and remaining risk.
