# DDA-05 independent review follow-up

The user requested a fresh independent subagent review and the same review after
every further refinement. A fresh-context read-only architect reviewed
`38f66039eb7424be077682763eb98fbaeaca08fa` against planning dependency
`f3682940f8864563cde0e6b6ecee60f746b49020`, without relying on prior approvals.
The verdict was **REQUEST CHANGES**. The earlier component approval is withdrawn.

## Findings and correction

| Finding | Severity | Corrective behavior |
| --- | --- | --- |
| Unknown commit accepted partially published business rows | P1 | Initial state must be the exact prior state with zero publications or the complete replacement with one publication. Recovery preserves that state and its bindings. |
| Rollback accepted a completed pipeline | P1 | Rollback, post-EOF and unknown-outcome boundaries require an incomplete pipeline. |
| Source-free recovery could hide initial outside-window corruption | P1 | Check both row multisets and metadata before recovery, with no publication or receipt advancement. |
| Ordinary trials accepted repeated source queries/publications | P2 | Fresh invocation counters start at zero; successful fidelity/warmup/trials require exactly one query and publication. |

The same boundary review identified a related lost-ACK gap: content/metadata/
receipt must already match at the confirmed commit boundary, and recovery must
preserve them. Pending evidence/checkpoint completion is allowed after known
commit. Unknown-outcome recovery may intentionally lack a receipt, but only to
prove replay is blocked; metadata authority remains required. Missing metadata
produces UNVERIFIED. Single-use driver iterators are captured before repeated
boundary assertions.

No frozen receipt IDs, report fields, schema, route admission, production runtime,
wire/state format or opt-in requirements change. No new ADR is required for this
correction. Live authority remains application/environment supplied.

## Ownership and evidence

DDA-06 explicitly assigned the new cohesive regression module
`tests/test_native_delivery_live_recovery_boundaries.py`, avoiding growth beyond
the existing test module's 400-SLOC budget. The exact successor contract is
retained as [task-contract.yml](task-contract.yml), copied from the integrator's
`test_artifacts/delivery-acceleration/dda-06/supplemental-contracts/dda-05-recovery-boundaries.yml`.
Other owned and forbidden paths are unchanged.

The first regression run on the old implementation failed 27 of 32 new cases;
[red.log](red.log) retains the original failures. It also exposed rejection of a
valid single-use row iterator. Added coverage includes metadata/receipt corruption,
missing authority, pre-existing invocation progress, legitimate old/new unknown
states and pending known-commit pipeline work.

| Check | Status | Evidence |
| --- | --- | --- |
| Corrected focused tests | PASS: 114 cases, including 44 boundary cases | [focused.log](focused.log), [collection.log](collection.log) |
| Ruff and format | PASS | [ruff.log](ruff.log), [format.log](format.log) |
| Mypy and architecture checks | PASS | [mypy.log](mypy.log), [import-rules.log](import-rules.log), [layer-metrics.log](layer-metrics.log) |
| Support module-size budget | PASS; no issues | [tool-module-size.log](tool-module-size.log) |
| Documentation links, generated references and language | PASS | [docs-check.log](docs-check.log), [generated-references.log](generated-references.log), [docs-language.log](docs-language.log) |
| Strict documentation build | PASS | [mkdocs.log](mkdocs.log) |
| Supplemental task contract | PASS | [task-contract.log](task-contract.log) |

This record is authored before the successor independent review and before the
new frozen broad run. It does not claim either has completed. Final review/check
results will be linked in the PR; the historical full FAIL and controlled
permission recheck in the parent directory retain their original identities.

Focused command:

```bash
uv run --locked --no-sync pytest \
  tests/test_native_delivery_live_benchmark.py \
  tests/test_native_delivery_live_recovery_boundaries.py -q
```

Live SQL/BCP, containers, credentials and performance claims: **SKIP/UNVERIFIED**.
Package/release changes: **N/A**. The correction awaits fresh review; it is not a
merge or release approval.
