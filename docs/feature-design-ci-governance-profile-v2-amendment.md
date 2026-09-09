# Feature design: CI governance-profile V2 amendment

- Status: APPROVED
- Owner: PaulKov / Codex
- Issues: #607, #618
- Target release: next patch
Last verified: 2026-08-27

## Executive summary

The exact-head receipt fix (#607) must observe `pull_request` `opened`,
`reopened`, and `synchronize` events; the CI-speed change (#618) later inserts
read-only quality stages before the existing governance producer.  The current
ADR-0037 policy is deliberately a closed V1 value, so directly editing it is
not a valid implementation path.  Exact-head CI has demonstrated that the
existing parser and profile tests fail closed when such a direct edit is made.

This amendment defines the approval and migration boundary for a versioned V2
policy.  It permits #607 only after the maintainer approves an exact reviewed
V2 profile.  #618 is expressly deferred to a separately approved V3 profile;
it cannot widen V2 or reuse V2 approval.

## Personas and journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Contributor | Receive an `Agent PR receipt` for the current pushed head. | A receipt workflow that does not subscribe to `synchronize` cannot observe normal head replacement. | One receipt run binds its event head and fails closed if the head changes. |
| Maintainer | Evolve CI safely. | A closed governance profile makes an unaudited YAML edit look tempting. | A prior, versioned policy approval is visible before the workflow change. |
| Auditor | Reproduce authority at a historical commit. | Replacing V1 destroys the prior contract. | V1 remains byte-identical and V2 selection plus evidence are deterministic. |

Journey: the maintainer reviews this researched contract, changes only its
status to `APPROVED` on the reviewed SHA, and merges it.  A later #607
implementation starts from that merge, adds the exact V2 policy plus workflow
subscription atomically, and publishes producer-generated security evidence.
An operator sees `STALE_HEAD`, timeout, or terminal failure in the receipt and
pushes/reruns only as the receipt runbook permits.  #618 follows the same
journey with a distinct V3 amendment after its topology is fully reviewed.

## Scope

### In scope

- a versioned, prior-approved way to retire a live V1 enforcement profile while
  preserving V1 as a historical parser and evidence fixture;
- the exact V2 semantic delta for #607: the existing `merged_closure` profile
  changes its pull-request trigger from `[edited, closed]` to
  `[opened, reopened, synchronize, edited, closed]`, with unchanged job ID,
  permissions, envelope hash, and closed-event merge-closure semantics;
- deterministic V1/V2 selection, evidence identity, rollback, and tests;
- a separate V3 approval requirement for #618 quality topology.

### Non-goals

- changing branch protection, required contexts, GitHub App permissions,
  release workflows, data-plane behavior, or credentials;
- allowing a policy to accept a wildcard trigger, alternate profile, or a
  partial migration;
- implementing #607 or #618 in this design-only increment;
- treating a queued, cancelled, or stale hosted run as a pass.

### Constraints

V1 is immutable: its source YAML, schema, fixtures, and historical evidence
continue to parse and validate byte-for-byte.  GitHub documents that the
relevant `pull_request` activity types include `opened`, `synchronize`, and
`reopened`, and that a concurrency group with `cancel-in-progress: true`
cancels obsolete work.  The implementation therefore keeps per-PR
cancellation but makes the profile projection explicit.  Sources checked
2026-08-27: [triggering workflows](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow) and [concurrency](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency).

## Public contract and compatibility

The public repository contracts are the exact workflow subscription, `Agent PR
receipt` semantics, privileged-policy artifact, canonical security report and
operator runbook.  No dpone CLI or data API changes.

The implementation adds a new full policy/schema pair at versioned paths and
records its digest and selected version in the producer-owned governance
artifact.  It does not modify V1.  The scanner selects exactly one active
version by an explicit immutable amendment binding:

1. no valid V2 binding: V1 remains the sole active policy;
2. valid V2 binding and exact V2 files: V2 is sole active policy;
3. missing, duplicate, malformed, non-ancestor, or mismatched binding: emit
   `UNVERIFIED` and block governance; never silently fall back after a V2
   workflow is present.

Rollback is a reviewed revert to a commit where V1 remains sole active policy.
It never restores a V1-shaped workflow after a V2 binding exists, and never
reintroduces write/OIDC authority into PR-controlled execution.

## Detailed algorithm

1. The amendment merge records the reviewed specification SHA and its one
   approved V2 profile projection; it changes no live policy or workflow.
2. The #607 implementation creates the V2 policy, V2 schema, binding and
   scanner-selection adapter in one atomic change with the workflow trigger.
3. The scanner reads V1 as historical evidence, parses the binding with closed
   JSON/YAML limits, verifies parent/base/approved-spec identity, and selects
   V2 only when every identity is exact.
4. It parses the selected full policy, checks the full trigger projection,
   permission/envelope fingerprints, mandatory occurrence cardinality and all
   routes.  V2 permits exactly the #607 trigger list and no alternative.
5. #607 receipt polling captures `H`; each observation and final pass re-read
   the PR head.  A different head is `STALE_HEAD`, which is non-PASS; workflow
   concurrency cancels the obsolete run.  Timeout/backoff remain bounded.
6. A subsequent #618 change first obtains a V3 amendment containing its full
   four-edge chain.  It cannot edit V2 or rely on a generic topology allowance.

```text
if valid_v2_binding():
    selected = parse_exact_v2()
elif v2_files_or_v2_workflow_present():
    UNVERIFIED("GOVERNANCE_PROFILE_SELECTION_INVALID")
else:
    selected = parse_exact_v1()
scan(selected, current_workflows)
```

State transitions are `V1_ACTIVE -> V2_APPROVED_DESIGN -> V2_ACTIVE`; only the
maintainer-approved design merge enables implementation.  `V2_ACTIVE ->
V3_APPROVED_DESIGN -> V3_ACTIVE` is separate.  Any invalid intermediate state
is blocking `UNVERIFIED`, not an implicit V1 or V2 pass.

## Architecture

| Component | Responsibility | Dependency direction |
|---|---|---|
| V1 parser/schema | Historical closed contract. | No dependency on V2. |
| Amendment binding model | Validate version, base, spec digest and policy digest. | `dpone.contracts` only. |
| Policy selector | Return one exact active policy or a typed blocking outcome. | service depends on contracts/port. |
| Workflow scanner adapter | Read files and report selected policy evidence. | adapter into selector. |
| CI workflow | Thin composition root; no policy branching. | invokes scanner/receipt. |

The implementation uses canonical packages for new domain contracts and
services; compatibility shims may re-export but cannot contain selection
policy.  No generic plugin registry is justified: V1, V2 and later V3 are a
small, closed, auditable vocabulary.  New modules must remain within the
repository module-size and layer budgets.

Alternatives rejected: (1) mutate V1 and update tests, because it rewrites
historical authority; (2) allow both trigger projections in V1, because it
weakens exact identity; (3) combine #607 and #618 in one profile, because it
makes unrelated security/topology changes inseparable; (4) use
`pull_request_target`, because it expands the trust boundary.

An ADR-0037 amendment is required because the change alters long-lived
workflow authority and the exact privileged-profile selection contract.

## Market comparison

This is repository-specific CI governance.  dlt, Informatica, Airbyte,
Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos and Apache Beam are N/A:
none owns GitHub pull-request event authority or dpone's ADR-0037 policy.
GitHub Actions is the relevant primary source.  We adopt its documented
event/concurrency semantics but reject relying on provider cancellation as
evidence; dpone independently verifies the exact head and policy identity.

## Measurable differentiation

```yaml
axis: exactness of receipt and privilege-policy evolution
scenario: push a new head while a prior receipt is waiting for CI
baseline: direct V1 modification is rejected by exact closed-contract checks
metric: false PASS receipts and unauthorized profile selections
target: 0 across the #607 state/negative matrix
procedure: unit state matrix plus exact-head hosted run and producer evidence
artifact: test_artifacts/agent-policy/governance-profile-v2-certification.json
limitations: does not measure CI wall-clock speed; #618 has its own p95 target
```

## Security, tests, operations and documentation

Required negative cases: V1 byte mutation; missing/duplicate V2 binding;
binding/spec/base/policy digest mismatch; V2 policy without binding; V2
workflow with V1 selection; unknown profile version; trigger add/remove/order
drift; stale head before/during/final receipt check; timeout; terminal failed
check; cancellation; malformed/expired evidence.  Every case is `FAIL` or
`UNVERIFIED`, never PASS.

Hosted proof must be exact-head, contain all protected checks and the
producer-generated governance artifact, and have no bypass.  Documentation
adds a version-selection explanation and recovery instructions: operators
restore the last approved commit or create a new amendment; they do not edit
policy YAML to repair a red check.

## Rollout and rollback

Rollout order: merge approved design → implementation exact-head PASS → merge
#607 → observe receipt evidence → research/approve V3 → implement #618.  The
receipt wait has no feature flag because accepting a stale receipt is unsafe.
Rollback is a reviewed Git revert of the whole V2 activation and evidence
binding, followed by exact-head policy verification.

## Agent execution plan

| Role | Owned paths | Dependency |
|---|---|---|
| Integrator | policy/schema/workflow, selector, shared tests, docs, evidence | approved V2 amendment |
| Test certifier | focused state/profile matrix and hosted evidence review | implementation head |
| Docs reviewer | operator runbook/CJM | selected policy contract |

## Approval checklist

- [x] The direct-V1-mutation failure is reproduced on the exact #607/#618 heads.
- [x] V1 preservation, exact V2 selection, failure semantics and #607/V3 split are explicit.
- [x] Relevant provider behavior is based on current primary documentation.
- [x] Maintainer reviewed this exact document and changed status to `APPROVED`
  on 2026-08-27.
