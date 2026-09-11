# PostgreSQL implementation closure

This report closes the PostgreSQL strategy-preservation implementation and its
requested independent reviews, large-partition measurements and documentation
deployment. It distinguishes the measured source, approved source and integrated
source; no historical receipt is reassigned to another commit.

## Approval and integration

The maintainer approved the final diff with “APPROVE, продолжай” and subsequently
confirmed approval of the superseded MR #31. [MR #32](https://github.com/PaulKov/dpone/pull/32)
contains that work and was squash-merged on 2026-09-10 at 23:48:10 UTC. MR #31
remains closed as superseded. Normal repository protections were preserved.

| Identity | Commit |
| --- | --- |
| Final measured source | `bef37a7752db43dcae42298da7bd62509a186535` |
| Independently reviewed and owner-approved head | `f643a7e7de97578c5ac86e5ed97e29d3499185e1` |
| Integrated source on master | `fa20f554bab3024f240b92aadf176527ee967925` |
| Integration parent | `e15ad32b850708207c2f8f1b6faf597ef0f5b0b1` |

The reviewed and integrated commits have identical Git tree
`edcca7195ab61da8d6942df501e2556cf4a422b7`. Execution inputs and benchmark producers
are unchanged from the final measured source. The later commits contain only
documentation and evidence additions.

The fresh-context reviewer `/root/postgres_final_independent_review` issued
**APPROVE** on the exact final head. All three P2 findings are closed: legacy
collaborators are honored, native partition metrics distinguish replacement
from deletion, and snapshot deletions cannot inflate loaded rows. See
[independent-review.md](independent-review.md) and
[performance-review.md](performance-review.md) for findings and regressions.

## Completed validation

| Check | Status | Exact evidence and scope |
| --- | --- | --- |
| Focused regressions | PASS | 89 integrator cases; reviewer independently ran 50 cases on the identical resolved source tree |
| Real PostgreSQL | PASS | [42 direct cases](verification-direct-bef37a7.json) on measured source; changed snapshots, replay, empty input, constraint preservation and failure recovery |
| Large partitions | PASS | [Nine final cases](verification-performance-bef37a7.json), 100k/1m/5m rows with three attempts each; 27 attempts across the three retained campaigns |
| Full approved-head CI | PASS | [CI 34531053345](https://github.com/PaulKov/dpone/actions/runs/34531053345): all 16 Python 3.11/3.12 shards and both aggregate gates passed on `f643a7e` |
| Integrated-source CI | PASS | [CI 34543755375](https://github.com/PaulKov/dpone/actions/runs/34543755375) completed successfully on `fa20f55` after merge |
| Required contexts | PASS | All 21 required checks passed before merge, including the full Airflow matrix, wheel smokes, documentation, security and PostgreSQL XMin checks |
| Approved PR receipt | PASS | [Run 34543612318](https://github.com/PaulKov/dpone/actions/runs/34543612318), including checked owner approval and provider-verified governance evidence |
| Integration receipt | PASS | [Run 34543755469](https://github.com/PaulKov/dpone/actions/runs/34543755469) binds reviewed head to `fa20f55`; exact integration check, Git trees and retained source archive verified |
| Published documentation | PASS | [Recovery 34544261615](https://github.com/PaulKov/dpone/actions/runs/34544261615), attempt 1 on `fa20f55`; exact successful deploy step and public page content verified |
| Additional Kubernetes/live route reruns | N/A | No new route behavior beyond the reviewed PostgreSQL correction; earlier live campaigns retain their original identities |
| Release or PyPI publication | N/A | No named release or publication request; implementation closure does not certify a release |

The independent CI audit reran the canonical shard receipt validator and checked
membership against the preflight population. Each interpreter selected exactly
21,586 nodeids with no gaps or overlaps; population SHA256:
`850c9c2aae106aeda451cd9f97f571f9879bf91ee6defe576fcd8f62f9c46a55`.
This is a population count, not a passed-test count. Summed job reports contain
21,052 passed / 782 skipped for Python 3.11 and 21,051 passed / 783 skipped for
Python 3.12. Those skip totals include collection skips and are not unique-node
counts. Skipped cases are not represented as passes.

The successful pre-merge receipt SHA256 is
`e7bb3265f29219e18663c078ec271c6275b109f210d071f781569ba795d24d90`.
The closure preserves source archive SHA256
`d44a260ed0a0261621b602ed02ecd5b0df29fc33325a8fa4a545b3ee32472f9f`
and binding ID
`sha256:2e7bf77bf9b71209f62164cc813bf761c5185727276d822db82a4fbdf3c7dfdb`.
The source archive, body digest, receipt digest, schema, exact GitHub Actions App
and check-to-producer identity were checked against the retained/provider data.

## Documentation deployment recovery

The initial master [Pages run 34543755365](https://github.com/PaulKov/dpone/actions/runs/34543755365)
built and uploaded the site successfully and verified current master. Configure
Pages then failed with HTTP 404; Deploy Pages was skipped. Authenticated provider
observations confirmed `has_pages: false`. This is retained as a failed deploy.
The independent read-only investigation found no source defect.

The already documented repository setting was enabled with build source GitHub
Actions. The failed run and its artifact were preserved. The recovery used the
[documented new-run procedure](../../docs/cicd/runbooks.md#docs-and-github-pages-failures),
not a rerun. It dispatched new run `34544261615`, attempt `1`, against exact
current master `fa20f554bab3024f240b92aadf176527ee967925`. No workflow permission
or protection was changed.

[The deployment observation](pages-deployment-fa20.json) binds the authenticated
workflow ID/path/blob, run ID/attempt/event/branch/SHA, complete attempt-specific
Jobs API page, unique deploy job and pinned deploy action. Job `103093852281`,
step 3 `Deploy Pages`, completed successfully. Observation SHA256:
`41c9039ced94ada64add7185d3763b547874a15a1f4a86aab112664d3adea438`.
The read-only observation was produced from live provider responses; it does not
infer deployment from workflow conclusion alone.

The [documentation home](https://paulkov.github.io/dpone/) and
[PostgreSQL guide](https://paulkov.github.io/dpone/postgres/) returned HTTP 200.
The published guide contains the final measurement table and the earlier slow
series, including its operational lock warning.

## Compatibility, user journey and limits

The implementation preserves existing target structure and configured strategy
semantics, and fixes PostgreSQL row accounting. No new manifest fields or data
migration are required. Tables damaged by earlier CTAS refreshes still require
inspection and restoration from approved DDL; upgrading cannot reconstruct lost
constraints automatically. The route and compatibility guides cover that path.

The final 5m-row series has a 12.059-second load median and an 8.139-second
exclusive-lock median. The earlier 55–71-second loads and 38–48-second locks are
retained. Cache state and host contention were uncontrolled. These observations
are neither a production SLA nor evidence of a code speedup; long reader waits
remain an operational constraint.

Implementation, independent review, source CI, protected integration and
published documentation are complete. This closure update corrects obsolete
status text in campaign reports; it adds no runtime behavior. Historical failure
and intermediate evidence remain available under their original source IDs.
