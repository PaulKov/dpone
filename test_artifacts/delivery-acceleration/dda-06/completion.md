# Delivery acceleration integration

Status: **reviewable draft; architecture HOLD**. Required final local checks are
complete. The full non-live suite recorded 21,240 passed, 570 skipped and two
architecture failures. This is not a merge or release approval.
Integration PR: [#25](https://github.com/PaulKov/dpone/pull/25).

## Change and compatibility

The integration carries the native frame's admitted encoded-byte reservation
through the scheduler, inserts business values and framework metadata together,
and computes the two initial prepared digests from one iterator. Independent
verification still performs four raw reads and two prepared reads. The finalizer
retains its target-clock metadata UPDATE; direct BCP retains its existing metadata
projection behavior.

Optional bounded observations cover actual coordinator and worker boundaries.
Observer/clock failures stay diagnostic and cannot authorize success. Existing
Mapping/tuple inputs, default resource limits, wire files, journal/recovery
records, source lifetime, owner/fencing checks and publication authorities remain
compatible. SQL Server SWITCH remains an isolated, unregistered component;
public native SWITCH admission still fails before I/O. No migration is required.

The producer/consumer contract rejects invalid retained proofs and repeated timed
operations, preserves atomic target/receipt recovery semantics, and keeps exported
bundles usable after their original inputs are removed. Frozen v1 schema fields
and check IDs are unchanged. Hermetic or absent execution produces no performance
certification or speed ratio.

## Final source and executable checks

Evaluated source/docs commit:
`be7655ac2e36cd1e9601217c6b0a1fbd10e2eb3f`.
All 21 final receipts bind to this clean, unchanged source. Their exact
commands, results and raw logs are linked in [final-checks.md](final-checks.md).
Receipt integrity is audited separately in
[final-receipt-audit.json](final-receipt-audit.json).

- **PASS:** 490 native/producer integration tests and 296 lease/schema/authority
  regressions, with no failures or skips in those two focused runs.
- **PASS:** lint, formatting, type checking, import rules, exact-commit module
  budgets, change-aware check selection and ownership/provenance audit.
- **PASS:** docs links, generated references, language contracts, strict MkDocs,
  generated metrics freshness and Airflow public contracts.
- **PASS:** both requested Airflow package builds and Twine checks of their four
  fresh distributions; [artifact hashes](final-package-artifacts.json).
- **FAIL:** runtime-to-contracts flow is 215 versus permitted 214;
  [actual receipt](final-layers.json).
- **FAIL:** average clustering is 0.18322547520065463 versus checker limit 0.182;
  [actual receipt](final-architecture.json).
- **FAIL:** complete non-live suite: 21,240 passed, two failed, zero errors and
  570 skipped; exit 1, pytest 1,257.00 seconds. Both failures assert the same
  measured clustering above 0.182; no other failures were recorded. The two
  workers used the verified effective-GID temporary parent. See the
  [receipt](final-full-nonlive.json), [JUnit](final-full-nonlive-junit.xml), and
  [failure and skip analysis](full-suite-analysis.json). The 570 skips retain
  their 30 actual reason groups; none is counted as a pass.
- **SKIP / UNVERIFIED:** live SQL/BCP fidelity and measured acceleration; there is
  no approved disposable live environment.

The approved annotation cleanups reduced the earlier measured flow from 221 to
215. No threshold, baseline, graph exclusion, gate implementation or unrelated
PostgreSQL authority code was changed. Architecture HOLD remains open regardless
of passing focused, type, packaging and documentation checks. GitHub Quality
preflight independently reports the same flow failure on the same source; the
[provider observation](remote-ci-be7655a/observation.json) retains job identity,
API commands and the exact layer-step excerpt. Its full run remains distinct
from local checks and any later PR head.

## Provenance and independent review

- Reviewed source/import lineage: [dependencies.md](dependencies.md).
- Approved scope and validation procedure: [integration-plan.md](integration-plan.md).
- Shared observation and worker review: [observation-review.md](observation-review.md).
- Regression and evidence-recorder review:
  [review-regressions-and-evidence.md](review-regressions-and-evidence.md).
- Corrected producer/consumer integration: **PASS**, 229 cases in 105.469 seconds,
  on clean unchanged `003d7d5073306f3cb445d9179acc420cd3dcc952`;
  [receipt](corrected-producer-consumer.json). The final 490-test run includes
  those files on the final source. Independent
  [interoperability review](interoperability-review.md): **APPROVE**, no findings.
- Final [documentation and user-journey review](final-documentation-review.md):
  **APPROVE**, no remaining blocking documentation findings. The reviewer
  inspected actual built HTML and 4,074 local links/anchors.
- Independent [final evidence review](final-evidence-review.md) verifies
  executable receipt integrity separately from acceptance. Final full-suite and
  artifact follow-up: **APPROVE**, no blocking integrity findings. All 21
  receipts, three JUnit files, full failure/skip analysis and 69 report links
  were independently verified.
- Final canonical combined dashboard generation: **PASS** for all 5,856 tracked
  Python inputs; [producer receipt](integrated-metrics-final/refresh.json)
  retains the prior stale result, successful generation/freshness, preserved
  corrected prose and byte-identical repeated generation. Dashboard freshness
  does not establish architecture acceptance.

The separate DDA-02 component dashboard operation and reviewed docs-only handoff
are documented in [component-metrics/README.md](component-metrics/README.md).
The recipient imported it as `d3ac05c7d3f9d6b15bfdf6fec58f5dda5ae08d44` and passed
freshness/doc checks. Its final
[immutable completion](https://github.com/PaulKov/dpone/blob/b9b47121788a67463f829bb1589c763d71728efc/test_artifacts/delivery-acceleration/dda-02/followup-01/README.md)
records **20,848 passed / 574 skipped**, exit 0, on component source
`61bcebc0c96b4db535bd4494de6f30cb15d11d18`. This later artifact handoff and its
component dashboard are not imported into the frozen combined source. Historical
component failures remain retained alongside subsequent replays; component PASS
does not replace combined validation.

DDA-05's independently approved
[immutable completion](https://github.com/PaulKov/dpone/blob/c4e3df4321badd682ba6fa670b43f894185344df/test_artifacts/delivery-acceleration/dda-05/review-followup/completion.md)
records **20,918 passed / 570 skipped**, exit 0, on clean unchanged component
source `33b7ad30cdb12a0f1804bb1b72570504133f4342`. Its artifact-only retention
commit is referenced separately; both corrective source commits are already
integrated. The owner released the exclusive full-test slot immediately after
that process exited. Its passing component architecture gates do not establish
combined architecture acceptance.

## Documentation, limits and follow-up

The overview, runbook and seven delivery guides cover discovery, first success,
configuration, observation, diagnosis, recovery and upgrade compatibility. ADR
0062 records the isolated SWITCH activation boundary. Collector/session lifetime
is explicitly one invocation; snapshots are cumulative. The quality dashboard's
obsolete manual PASS summary was replaced by the exact approved RED/HOLD prose;
[byte-level correction proof](quality-summary-correction.json) preserves the
original generated bytes. The final canonical refresh preserves the correction.
The existing architecture document's size is a future documentation task outside
this scope.

This handoff retains the completed checks and independent review in an
evidence-only commit. Source, product documentation, Python inputs and
dependencies are byte-identical to the evaluated commit above; the retention
commit is distinct from that tested commit. Merge readiness requires
resolution of the actual architecture failures through separately owned work.
Live speed and route certification require an approved disposable environment
and current evidence. No merge, tag, provider change, upload or publication is
performed or authorized by this integration.
