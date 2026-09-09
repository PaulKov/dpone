# Documentation rules

These rules extend the repository-level `AGENTS.md` for `docs/**`.

- Public documentation is English-only and must build with MkDocs strict mode.
- Apply `docs/documentation-standard.md`. Separate tutorials, how-to guides,
  reference, explanation/architecture, and runbooks instead of growing a single
  monolith.
- Write for a first-time user before writing for the maintainer. Explain purpose,
  prerequisites, happy path, observable result, common failures, recovery, and
  next steps.
- Every public feature needs a user journey, runnable example, contract or
  reference, architecture/data-flow explanation, operational runbook, test or
  certification guidance, and cross-links to related features.
- Validate YAML indentation and examples. Do not publish placeholders, broken
  links, stale CLI output, or examples that cannot match current schemas.
- Update diagrams and architecture pages when component boundaries, state
  transitions, evidence flow, or public schemas change.
- Prefer generated reference material when the code or schema can be the source
  of truth; test generated output for drift.
