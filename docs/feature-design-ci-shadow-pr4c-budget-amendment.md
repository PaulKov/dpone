# Feature design: CI shadow PR4C measured request-budget amendment

- Status: APPROVED
- Owner: repository owner (explicit `APPROVED` and complete-all authorization in this task)
- Issue: [#512](https://github.com/PaulKov/dpone/issues/512)
- Target release: TBD
Last verified: 2026-08-27

## Executive summary

PR4C's first authenticated calibration, run
[33105043667](https://github.com/PaulKov/dpone/actions/runs/33105043667),
reached the 800-request hard cap without retries.  This is a feasibility result,
not a software failure: the approved stateless, two-observation algorithm needs
at least 899 dispatches for the observed 14-day population.  Keeping the old
400-request approval threshold would therefore make the reconciler permanently
unapprovable at ordinary observed volume.

This parent amendment adds a successor HTTP request budget of `3000/1500`
(hard/approval). It preserves the two-times safety factor and every exact-input,
stateless, independent-observation, byte, wall-time, source-binding, and
fail-closed invariant. It does not make calibration or reconciliation
authoritative, and it does not change legacy merge checks.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| CI/security operator | Measure a real 14-day observer safely | The old hard cap stops before a complete double observation | Fresh immutable receipt, still `UNVERIFIED`, with complete/equal snapshots and bounded counters |
| Reconciler implementer | Obtain a truthful PR4C prerequisite | A threshold impossible for the actual input set looks like a product defect | Parent-approved policy digest and fresh qualifying evidence |
| Contributor | Retain current merge safety | A new observer could be mistaken for a gate | Existing nineteen required checks remain unchanged |

The operator dispatches the trusted default-branch workflow, reads the
intentionally-red result as an artifact publication signal, downloads the
capacity artifact, and verifies its exact source/policy/bundle binding.  A
complete receipt at or below 1,500 requests is a narrow prerequisite for the
later reconciler child only; it is never a daily reconciliation `PASS`.

## Scope

### In scope

- Replace only the closed request hard maximum and derived approval threshold.
- Keep 14-day whole-window coverage and two independently acquired snapshots.
- Update ADR, parent specification, capacity specification, contracts, tests,
  public reference, runbook, navigation, and generated documentation.
- Obtain fresh exact-master capacity evidence after merge.

### Non-goals

- No caching between observations, sampling, cursor/ledger, reduced history,
  skipped attempts/Jobs/artifacts, GraphQL substitution, or mutable-list
  authority.
- No increase to the response-byte or wall-time budgets.
- No change to Actions permissions, PR workflow execution, branch protection,
  release authority, or artifact retention.
- No reconciler, PR5--PR7, or acceptance-canary implementation in this child.

### Assumptions and constraints

The authenticated run observed `retry_requests=0` and stopped at 800 requests.
For 73 producer and 36 auditor runs in one observed pass, the required two-pass
lower estimate is `29 + 6P + 12A = 899` dispatches.  The amendment selects
1,500 as a measured approval threshold: it is above that lower estimate and
retains a 2x hard ceiling of 3,000.  GitHub-hosted execution remains bounded by
the existing 900-second wall-time hard maximum and read-only token permissions.

## Public contract

### CLI and Python API

The CLI flags, outputs, exits (`1`, `2`, `70`), public imports, and injection
boundaries are unchanged. `ReconciliationPolicyV1.fixed()` remains the
historical `800/400` policy with byte-for-byte stable canonical bytes and digest.
`ReconciliationPolicyV2.fixed()` is the sole policy used by the capacity
workflow and returns a distinct canonical digest with
`hard_max_http_requests=3000` and a derived threshold of `1500`. There is no
user-supplied policy or limit option.

### Schema, artifacts, and compatibility

`dpone.ci-shadow-reconciliation-capacity.v1` remains accepted only as immutable
historical `UNVERIFIED` evidence under `ReconciliationPolicyV1`; its schema and
limits are not changed. New workflow receipts use the additive
`dpone.ci-shadow-reconciliation-capacity.v2` schema and V2 policy digest.
Consumers retain V1 validation for historical receipts and select V2 only for
new capacity evidence; a V1 receipt never qualifies V2 because its schema and
`policy_sha256` differ. Both schemas retain the closed request classes,
saturation semantics, source/bundle identity, and 24-hour expiry.

## Detailed algorithm

1. Authenticate the default-branch workflow source and verify its static bundle.
2. Construct `ReconciliationPolicyV2` with `hard_max_http_requests=3000`; derive
   the 1,500 request threshold by integer halving.
3. Acquire the complete producer and auditor inputs for a closed 14-day window;
   meter every dispatch before it occurs, including retries and redirects.
4. Independently reacquire every required provider input for the second
   observation. No cache, cursor, reuse, or post-second-observation read is
   permitted.
5. Compare canonical topology, records, archive bytes, payload bytes, and
   identity. Counter values are never snapshot identity.
6. Persist a create-only `UNVERIFIED` receipt. It has
   `RECONCILIATION_CAPACITY_CALIBRATION_ONLY` only when both observations are
   complete/equal and requests are at most 1,500, bytes at most 67,108,864, and
   wall time at most 450 seconds; otherwise it is blocked.

```text
policy = ReconciliationPolicyV2.fixed()  # 3000 hard / 1500 approval requests
first = acquire_complete_observation(policy, fresh_provider_reads=True)
second = acquire_complete_observation(policy, fresh_provider_reads=True)
decision = unverified_capacity_decision(first, second, policy.approval_thresholds)
write_create_only_receipt(decision)
return 1
```

Any API inconsistency, missing input, resource overflow, stale source, changed
head, archive ambiguity, process crash, or output conflict remains fail-closed.
An existing receipt is never overwritten or relabelled.

## Architecture and alternatives

| Alternative | Result | Decision |
|---|---|---|
| Keep 800/400 and refactor locally | Impossible: required minimum is about 899 requests | Reject |
| Cache/reuse first observation | Violates independent stateless observation | Reject |
| Omit exact Jobs/attempt/artifact reads | Weakens exact provider evidence | Reject |
| Reduce the 14-day interval or sample history | Changes reconciliation scope | Reject |
| Raise HTTP cap and retain two-times approval margin | Meets measured feasibility while preserving evidence semantics | Adopt |

No new abstraction, port, adapter, or migration is required. The existing
contract remains the source of fixed policy; only its closed constants and
evidence validation boundaries change.  This requires an ADR 0048 amendment
because parent-owned capacity policy is normative architecture.

## Market comparison

| System/version | Relevant capability | Observed design | Adopt/reject | Source/date |
|---|---|---|---|---|
| GitHub Actions REST API | Per-run Actions history, Jobs, and artifact reads | Repository API exposes bounded per-resource REST reads; it does not make a multi-run historical audit authoritative by itself | Adopt bounded read accounting and retain dpone's fail-closed evidence model | GitHub Docs, 2026-08-27 |
| dlt, Airbyte, Fivetran, Informatica, Pentaho, SSIS, gusty, Astronomer Cosmos, Apache Beam | CI-shadow exact-attempt evidence | N/A: data-pipeline/orchestration products, not a GitHub Actions audit evidence transport | N/A | N/A |

```yaml
axis: sustained safe capacity for a stateless 14-day observer
scenario: two independent complete observations of the actual GitHub Actions history
baseline: 800/400 request budget that stopped at its hard cap
metric: complete/equal exact-source receipt with total_http_requests <= 1500
target: one authenticated master receipt before reconciler approval
procedure: dispatch trusted capacity workflow, authenticate ZIP and JSON receipt, verify policy/bundle/source identity
artifact: pr-gate-shadow-reconciliation-capacity-<run>-<attempt>.json
limitations: measurement does not make the later reconciler authoritative
```

## Security, testing, documentation, rollout

Permissions remain `contents/actions/pull-requests: read`; no secret, cache,
OIDC, PR checkout, or write transport is introduced.  Tests must prove policy
bytes/digest, 1,499/1,500/1,501 and 2,999/3,000 boundary behavior, request-class
conservation, independent two-pass acquisition, legacy 800/400 receipts being
non-qualifying under the new digest, and unchanged byte/wall limits.  The live
gate is a new authenticated `master` receipt with all source/bundle/configuration
bindings, not a mocked pass.

Documentation adds a dedicated capacity runbook: dispatch, expected red workflow,
inspection-only download, source/policy/expiry checks, blocked-result matrix,
and recovery.  It cross-links the capacity overview, shadow overview, workflow
inventory, and CI/CD index.

Rollout is one additive policy commit, then one fresh manual default-branch
dispatch.  Rollback is a Git revert; neither old nor new receipt gains authority.
If a receipt exceeds the amended limit, it remains blocked and triggers a new
parent decision rather than a silent increase.

## Agent execution plan

| Role | Owned paths | Dependency |
|---|---|---|
| Integrator | ADR/spec, contract, services, tests, docs, manifest, workflow | This approved amendment |
| Fresh reviewer | Read-only final diff and evidence | Focused and broad validation |

## Approval checklist

- [x] Live feasibility evidence is linked and independently reviewed.
- [x] Algorithm preserves exact-input, stateless, independent-observation semantics.
- [x] Public artifact compatibility and non-authority boundaries are explicit.
- [x] Measurable rollout, test, recovery, and rollback criteria are defined.
- [x] Repository owner authorized complete implementation in this task.
