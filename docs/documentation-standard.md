# Documentation and user-journey standard

Status: normative

Documentation is part of the product contract. A feature that works only after
reading source code or asking its author is not complete.

## Audiences

Every feature identifies its primary audience and writes the shortest safe path
for that audience:

- first-time business or analytics user;
- data engineer;
- platform/DevOps/DataOps engineer;
- connector author;
- operator/on-call engineer;
- security or audit reviewer;
- maintainer.

Start with the least experienced intended user. Advanced detail must not obscure
the first successful path.

## Information architecture

Use purposeful document types:

- **tutorial**: guided learning and first success;
- **how-to**: a concrete task with prerequisites and outcome;
- **reference**: exact CLI/API/schema/options/status contract;
- **explanation/architecture**: why and how the system works;
- **runbook**: detection, diagnosis, recovery, escalation, and verification.

Do not turn one page into all five. Decompose when audiences, tasks, navigation,
or update cadence differ. Keep a stable overview page that links the pieces.

## Required feature documentation set

A material public feature has, as applicable:

1. purpose, personas, prerequisites, and non-goals;
2. first-success tutorial;
3. CLI and Python API examples;
4. manifest/configuration examples with valid YAML indentation;
5. exact reference for options, defaults, outputs, exit codes, statuses, and
   compatibility;
6. architecture and data-flow/state diagrams;
7. identity, state, retry, recovery, and artifact semantics;
8. operations runbook and observability signals;
9. test/certification instructions and limitations;
10. migration/deprecation notes;
11. cross-links to connectors, strategies, schemas, Airflow/dbt, and related
    concepts;
12. CJM described below.

## Customer journey map

Document the complete path:

| Stage | User question | Required documentation |
|---|---|---|
| Discover | Is this feature for me? | purpose, scope, constraints |
| Prepare | What do I need? | prerequisites, permissions, credentials |
| Configure | What is the smallest valid setup? | copyable example and schema links |
| Execute | What command or API call do I run? | CLI/Python parity examples |
| Observe | How do I know it worked? | expected console/file/artifact output |
| Diagnose | What does this error mean? | actionable errors and troubleshooting |
| Recover | Can I retry safely? | state, idempotency, cleanup, rollback |
| Operate | How do I run this in production? | runbook, SLOs, alerts, capacity |
| Upgrade | What changes between versions? | compatibility and migration |

## Example quality

Examples MUST be runnable or validated by a parser/test whenever feasible.

- Never use malformed indentation or ellipses where exact structure matters.
- State required environment variables without embedding secrets.
- Show expected output and artifact location.
- Use stable identifiers and clearly mark placeholders.
- Keep CLI help and generated references synchronized with code.
- For file output, document overwrite, atomicity, encoding, and partial-failure
  behavior.

## Navigation and auditability

- Each page begins with purpose and intended audience.
- Use descriptive titles and headings; avoid several pages with nearly identical
  names and no overview.
- Every leaf page links back to an overview and to the next likely task.
- Architecture and schema changes update diagrams and cross-links in the same PR.
- Generated sections identify their producer and must not be hand-edited.
- Stale or superseded pages are redirected, archived with a banner, or removed;
  they are not left silently contradictory.

## Validation

Run:

```bash
uv run dpone docs check-docs
uv run dpone docs check-generated-references
uv run pytest tests/test_docs_language_contracts.py -q
uv run mkdocs build --strict
```

Also inspect rendered navigation and first-time-user flows. A strict build alone
does not establish understandable UX.
