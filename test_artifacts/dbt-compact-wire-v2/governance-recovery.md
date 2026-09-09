# Public snapshot governance recovery

Date: 2026-09-09. Final runtime code commit: `355e9df967f1db6ea0171f9ea5aa4c37a272d0f9`.
This immutable checkpoint records remediation and completed local checks. The
[PR validation table](https://github.com/PaulKov/dpone/pull/2) records subsequent
full Docker and required GitHub outcomes for their exact commits.
Approved scope: [recovery design](../../docs/feature-design-public-snapshot-ci-recovery.md).
This follow-up supersedes the merge-blocker assessment in the earlier
[dbt validation](validation.md), while retaining its historical observations.

## Changes and authority

- The active protection policy now binds live ruleset 22617738, version 49106165,
  updated 2026-09-09T06:52:26.800000Z. Readback of the history-version state and
  live ruleset proved every protection decision unchanged. No GitHub rule was
  written and no administrator bypass was used. The derived authority artifact
  was regenerated, not hand-edited.
- The 51 module-debt caps remain exact and unchanged. The producer verified the
  immutable public-root ledger and every source measurement, then changed only
  `baseline_commit`. The [adoption report](public-module-debt-adoption.json)
  retains original unavailable identities. The ordinary ancestry check passes
  against the real public root; foreign history still fails. The exact gate
  independently recomputes adoption eligibility and source bounds.
- Current tests verify public-root/tree and intervening-history continuity of
  frozen imported contracts, plus existing active/synthetic behavior. All seven
  original retrospective assertion groups remain explicitly callable. The
  [historical report](public-historical-proof.json) remains UNVERIFIED, exit 2;
  it is not a historical certification. Optimized Python fails closed.
- Workflow snapshots retain byte/stat/path checks and add sticky Linux inotify /
  macOS kqueue observations. Restored namespace/attribute/content mutations,
  queue overflow, watch loss and observation failures cannot yield a complete
  verified snapshot. Partial policy-only scope and resource cleanup are retained.
  Observation ends at final polling; arbitrary remote/mmap or later mutations
  are not claimed to be covered.

## Verification

| Check | Status | Result |
| --- | --- | --- |
| Exact module-size gate | PASS | 51 debt entries, no violations; base f8c6a4 / head 355e9df |
| Exact governance gate | PASS | All executable governance controls passed |
| Producer idempotence | PASS | Committed candidate returns NO_CHANGE; still not a historical proof |
| Adoption/policy/authority tests | PASS | 52 cases; interruption/retry pair publication adds focused coverage |
| macOS workflow snapshot family | PASS | 392 cases; unchanged ABA scenarios included |
| Linux focused governance matrix | PASS | 101 cases in 12.85 s |
| Complete Linux offline suite on 5910c68 | FAIL | 19275 passed, 567 skipped, 4 failed in 1047.06 s; remediation below |
| Final complete Linux suite | UNVERIFIED | Started on 355e9df with two workers; subsequent result belongs in the PR validation table |
| Confined-file regression | PASS | 24 cases: frozen-stat mutation during either pass, both APIs, byte boundaries, growth/truncation, seek/read/stat failures |
| Doctor import full-file Docker rerun | PASS | 31 cases without changing the original timeout |
| Ruff lint/format | PASS | Complete repository |
| Mypy | PASS | 1143 source files |
| Import/layer/architecture checks | PASS | No violations; 74 architecture regression cases |
| Documentation and package checks | PASS | 795 Markdown files / 3129 links, 32 language cases, strict MkDocs, four builds and Twine |
| Fresh-context review | PASS | Adoption bounds, original historical assertions, observer semantics and producer recovery reviewed; optimized-Python guard added |
| Original historical CI-shadow proof | UNVERIFIED | Seven checks lack pre-public-root objects; dedicated command exits 2 |
| Live warehouse SQL / release | N/A | No certification or publication requested |

## Docker environment

The offline container uses the previously built dependency image
`sha256:1ab663f7a0c1f6b786cf241965ebf98b2e48a2d00c0bcde59839c152c5e1dc7f`
with the exact new source supplied through a public-only Git bundle. Dependency
manifests are unchanged. Linux ARM64, Python 3.12.14, uv 0.12.10,
dbt-core 1.12.3/dbt-sqlserver 1.11.1, jq 1.6, uid 10001; `--init`,
`--network none`, four CPU quota. No host worktree/credentials are mounted.

Initial full command: `uv run pytest -m "not integration_live" -n 4 --dist loadfile`.
Final full command: `uv run pytest -m "not integration_live" -n 2 --dist loadfile`.
The targeted run covers snapshot/observer, public-history continuity, module
adoption and authority-rebinding tests. Earlier 278 native-dbt Docker tests
remain recorded in the prior validation; the full run exercises them again.

## Full-run remediation

The two strict module-budget failures were corrected by separating pure snapshot
state/projection from acquisition and moving observation-specific tests into their
own module. Both exact strict gates and 392 snapshot regressions pass.

One unchanged doctor test exceeded its 20-second outer timeout while tracing
allocations under the full four-worker workload. Its entire 31-test file passed
in the same Docker container independently. The final full run uses two workers;
no assertion, product timeout or test timeout was relaxed.

A confined-file test exposed a real same-size write with indistinguishable
filesystem timestamps. The reader now compares bounded hashes through the same
held descriptor, preserving first-pass output, size limits and failure behavior.
Four deterministic frozen-stat cases failed before the fix; all 24 tests pass
afterward. The original concurrency assertion is retained. Content consistency
across two reads does not certify the absence of every transient mutation.

Required GitHub checks and normal protected merge remain the final acceptance
boundary. A passing public continuity test is never substituted for original
historical provider evidence. Reports contain no host paths or credentials.
