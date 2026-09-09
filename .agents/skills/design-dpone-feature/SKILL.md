---
name: design-dpone-feature
description: Design or materially change a dpone feature, public contract, connector capability, load strategy, state/evidence behavior, user journey, or Airflow/dbt integration. Use before implementation; do not use for a trivial typo or isolated behavior-neutral refactor.
---

# Design a dpone feature

1. Read active `AGENTS.md`, `docs/feature-design-standard.md`, and the relevant
   architecture/contract docs.
2. Delegate independent read-only work to `dpone_explorer`, `dpone_architect`,
   `dpone_test_certifier`, and `dpone_docs_ux_reviewer` when the scope warrants it.
3. Reconcile findings; do not paste four disconnected reports.
4. Create a specification from `docs/agent-templates/feature-design-spec.md`.
5. Explain the step-by-step algorithm, state/evidence/transaction boundaries,
   retries, replay, failure semantics, compatibility, and user journey.
6. Research only relevant comparator systems using current official primary
   sources. Record version/date, facts, adopted/rejected patterns, and N/A reasons.
7. Define any superiority claim by scenario, metric, target, reproducible
   procedure, artifact, and limitation.
8. Partition future writes with explicit owned/read-only/forbidden paths and one
   integrator for shared files.
9. Finish at `RESEARCHED`. Do not change production code until the maintainer
   marks the specification `APPROVED`.
