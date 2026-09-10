# Independent review follow-up

Date: 2026-09-10. Scope: DDA-04 draft PR
[29](https://github.com/PaulKov/dpone/pull/29). The maintainer requested a new
independent subagent review after each revision. Both reviews below used fresh
contexts and made no workspace changes.

## Review of the submitted component

Reviewer: `review_current_pr` (`dpone_architect`). Exact subject:
`039fe201c9c24c9f95f1b2af55794c384b2299c5`.

The reviewer found no new confirmed production-code P1/P2 defect, reran the 107
focused/producer cases successfully, and verified that retained source/producer
fingerprints still matched that subject. One P2 documentation defect remained:
both the component guide and DDA-05 fixture requirements omitted SELECT on
`sys.sql_expression_dependencies`, although TABLE_SQL reads that catalog view.
A principal with only the listed permissions could fail during catalog reading,
before any mutation. Live reproduction was not performed.

## Correction and independent review

The two prerequisite lists now explicitly require
`SELECT ON OBJECT::sys.sql_expression_dependencies` in the target database.
They assign provisioning to the fixture owner and do not require `db_owner`.
The guide links to the
[official Microsoft permission requirements](https://learn.microsoft.com/en-us/sql/relational-databases/tables/view-the-dependencies-of-a-table?view=sql-server-ver17#permissions).
No runtime permission grant or production-code change was introduced.

Reviewer: `review_permissions_fix` (`dpone_docs_ux_reviewer`). Subject: the
two-page working diff against `039fe201c9c24c9f95f1b2af55794c384b2299c5`,
13 insertions and 5 deletions. The reviewer independently checked the SQL query,
official permission requirements, approved specification section 6, task contract,
setup journey, failure semantics and certification limits. Result: **no actionable
P1/P2 findings**; the correction is ready for integration after current docs
checks pass. Reviewer `git diff --check` passed. No live or broad tests were run
by this second reviewer.

## Current documentation checks

The parent ran the existing `../run_checks.py` producer with OUTPUT redirected
to this directory, preserving historical records. These records assess the
unchanged working candidate based on HEAD `039fe201c9c24c9f95f1b2af55794c384b2299c5`;
they do not claim execution on a later commit. All before/after source and producer
identities match. The checked source content SHA-256 is
`9aa7ab446560064dd950e812d6117af5a98ae42aaab5f3c9a308c1fe63085997`.

| Check | Status | Evidence |
| --- | --- | --- |
| Change-aware check selection | PASS | [selection.json](selection.json) |
| Documentation contracts | PASS | [docs.json](docs.json) |
| Generated references | PASS | [references.json](references.json) |
| Documentation language contracts | PASS | [docs-language.json](docs-language.json) |
| Strict MkDocs build | PASS | [mkdocs.json](mkdocs.json) |

The parent also inspected the built HTML text and links with the standard-library
HTML parser: the permission, official reference and related guide/specification
links are present. The first inspection helper could not import optional `bs4`;
the standard-library inspection succeeded without installing dependencies.
Shared navigation registration remains DDA-06's responsibility.

## Impact and remaining work

The correction completes the fixture author's preparation journey. Public API,
manifest, runtime admission, transaction ownership and compatibility are unchanged;
no migration is needed. Public native SWITCH rejection remains enabled.

The earlier [completion report](../completion.md) remains historical evidence for
its named subject. Its reported layer failure (217 > 214), clustering failure
(0.1823846551601551 > 0.182), and full-suite result (28 failures, 2 collection
errors) remain unresolved. The broad Python suite was not repeated for these
documentation-only edits. They are ready for review and scoped integration;
the overall PR is not merge-ready until required gates pass. Live SQL and
performance remain **SKIP / UNVERIFIED**. Release/publication is **N/A**.
