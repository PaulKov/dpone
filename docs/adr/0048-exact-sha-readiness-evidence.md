# ADR 0048: Exact-SHA evidence is data-only, attempt-bound, and default-deny

## Status

Accepted.

Acceptance is evidenced by the exact-head owner attestation and successful
`Agent PR receipt` for the PR that lands this ADR, plus the merge commit recorded
in [issue #512](https://github.com/PaulKov/dpone/issues/512). The status line does
not by itself prove approval, implementation, producer authenticity, or live
certification.

## Context

A producer-controlled JSON field, artifact digest, or self-described `PASS`
does not prove that bytes belong to the intended repository, workflow, run
attempt, subject commit, and candidate. A trusted `workflow_run` may receive
privilege, so it must not execute subject content. The prior proposal also
mixed diagnostic CI evidence with release authority and grew reconciliation
into a cursor/ledger/DAG system whose failure surface exceeded this shadow goal.

Fork-approval behavior creates another boundary. GitHub documents approval and
event types, but does not guarantee the complete action-required-to-completed
delivery sequence or reuse of one run ID and attempt. Repository retention
settings require a separate Administration-read capability, which is not a
prerequisite for an App-free diagnostic observer.

## Decision

### Shadow evidence

1. Every PR-head product, control, and claims job is untrusted and read-only.
   It receives no secret, write scope, OIDC, environment authority, or trusted
   cache. Product commands fail their jobs natively; no same-runner
   `continue-on-error` enforcement step may manufacture provider success.
2. GitHub REST workflow-run `head_sha` binds reviewed head `H`, not synthetic
   merge `M`. Current PR association membership and SHA fields are mutable
   diagnostics, never attempt authority.
3. The checkout-free collector claims repository/PR identity, `B`, `H`, and
   event `GITHUB_SHA=M`. The default-branch auditor completely enumerates
   eligible open PRs, requires the exact GitHub-owned
   `refs/pull/<N>/merge` to resolve uniquely to claimed `M`, loads that commit,
   and requires exactly two ordered parents `(B, H)` plus `run.head_sha == H`.
   It re-enumerates the complete eligible set and re-resolves the same ref immediately
   before receipt persistence; that final read is the linearization point.
4. Missing, moved, ambiguous, conflicted, or superseded merge identity is
   `UNVERIFIED` and requires a fresh eligible event. The workflow path/mode/blob
   must be identical in `B/H/M`. Product/control work checks out exact
   `H` with credentials and submodules disabled and proves clean HEAD/tree
   identity before executing repository bytes.
5. The producer uploads one attempt-specific closed UTF-8 JSON file with
   `archive: false`, `overwrite: false`, and `retention-days: 90`. Claims never
   self-authenticate their containing artifact.
6. The auditor has only `contents: read`, `actions: read`, and
   `pull-requests: read`, no secret/cache/PR checkout, and never executes,
   imports, sources, deserializes executable formats, templates, or interpolates
   downloaded content.
7. Bounded direct-JSON parsing rejects duplicate keys, unknown fields, invalid
   UTF-8, trailing bytes, over-limit structures, and identity mismatches.
8. The auditor independently recomputes the plan, exact-attempt Jobs/cases, and
   trusted implementation closure. Producer claims never substitute for
   provider conclusions or trusted policy.
9. Neither producer nor auditor creates or requires an attestation. ADR
   0037's exact legacy governance-source attestation remains outside this shadow
   graph and cannot be copied or extended without amending ADR 0037.

### Transitive trust and workflow depth

10. A closed manifest separates routing/evidence `TRUST_CORE` from product
    `SUBJECT_INPUT`. Dynamic imports, `exec`, `eval`, undeclared helpers, local
    actions/reusable workflows, or unknown dependencies are `UNVERIFIED`.
11. Shadow and candidate chains each contain one downstream `workflow_run` hop
    and terminate at their data-only auditor/verifier. A new finalizer or deeper
    chain requires a separately approved decision.

### Stateless reconciliation and retries

12. The YAML under
    `test_artifacts/agent-policy/dpone-ci-shadow-reconciliation-mvp.yml` is a
    closed, schema-validated design fixture. It is not a runtime policy file and
    a production command cannot load it. PR 4C owns the canonical
    `dpone.contracts.ci_shadow_reconciliation.ReconciliationPolicyV1` object;
    the composition root injects that object and exposes no mutable `--policy`
    argument.
13. PR 4C is stateless. An injected trusted UTC clock is floored to a whole
    second; every daily run observes the exact closed interval
    `[observation_started_at - 30 minutes - 14 days,
    observation_started_at - 30 minutes]`. There is no activation clamp or
    operator-selected lower bound. The scope is the `provider-observable exact
    interval`, never a claim that hidden or deleted runs did not exist.
14. Producer and auditor endpoints are queried with closed UTC-second bounds
    and 100-result pages. Two consecutive complete whole-window observations
    independently construct and page the deterministic partition tree and
    acquire every run/attempt record, exact-attempt Jobs page, artifact
    inventory, and bounded selected artifact byte sequence. They require
    identical topology, canonical records, downloaded bytes, and all response/
    content digests. A capped one-second slice is irreducible; adjacent seconds
    split into two singleton children; wider slices use overlapping midpoint
    children. Changed records, incomplete pagination, or API inconsistency are
    `UNVERIFIED`. Completion of the second matching observation is the report's
    `evidence_observed_through` boundary; evaluation uses only its captured
    bytes and performs no later provider read.
    Auditor discovery extends through `observation_started_at`. Each full
    observation performs an exact producer-run lookup for every auditor record,
    and provider-authenticated producer `workflow_run.created_at` alone decides
    interval membership; an auditor rerun never changes it. Producers before
    `scan_from` are diagnostic `OUT_OF_SCOPE_OLD_PRODUCER_RERUN`, exact-bound
    producers evaluate normally, producers in the grace interval defer to the
    next report, and future, query-omitted, missing, or foreign producers are
    `UNVERIFIED`. Query omission uses
    `RECONCILIATION_PRODUCER_QUERY_OMISSION/RECONCILIATION_RETRY`; missing or
    foreign identity uses
    `RECONCILIATION_PRODUCER_IDENTITY_UNRESOLVABLE/VERIFY_PRODUCER_IDENTITY_OR_CREATE_NEW_RUN`.
    Every auditor record is classified in report bytes.
15. Producer identity is `(repository_id, workflow_id, run_id, run_attempt)`.
    Every producer attempt and exact-attempt Jobs result is observed. Within one
    auditor workflow run, the highest observed rerun attempt must be terminal;
    a higher requested, queued, or in-progress attempt blocks an earlier
    completed result. Two independent auditor run IDs or conflicting decisions
    for one producer attempt are `UNVERIFIED`.
16. The report separates coverage from product outcome. Root decision folds as
    `UNVERIFIED > FAIL > PASS` for the exact daily interval. Named rollout
    canaries are not additional out-of-window `PASS` prerequisites, but every
    canary attempt visible inside the producer interval folds normally: expected
    FAIL remains daily `FAIL` and expected cancellation remains `UNVERIFIED`.
    The separate acceptance oracle verifies the expected outcome without
    excluding or repainting it.
17. There is no cursor, predecessor state, unresolved ledger, genesis state,
    DAG, ACI lattice, tombstone, external sort, checkpoint promotion, or
    manifest/state/finalization transaction. A prior reconciliation artifact is
    never an input and there is no latest-artifact-wins rule.
18. Each run creates one immutable direct-JSON report named
    `pr-gate-shadow-reconciliation-<observer_run_id>-<observer_run_attempt>.json`
    with schema `dpone.pr-gate-shadow-reconciliation.v1` and a 1-MiB limit. The
    payload never contains its own byte length or digest; provider artifact
    identity/digest/size are authenticated after upload. Before workflow
    integration, PR 4C introduces
    `dpone.ports.evidence.CreateOnlyEvidenceWriterV1` and
    `dpone.adapters.filesystem_evidence.DescriptorPinnedCreateOnlyEvidenceWriter`.
    The current route-attestation helper is not direct authority; extraction or
    extension must preserve its compatibility tests. An identical existing
    target is idempotent only after the current retry revalidates root, parent,
    leaf, inode, and bytes and successfully fsyncs file and parent; ambiguous
    durability or foreign targets are `UNVERIFIED`.
19. Every producer, auditor, and reconciler source workflow explicitly requests
    `retention-days: 90`, `archive: false`, and `overwrite: false`. The separate
    PR 4C capacity diagnostic is the narrowly scoped exception: it requests
    `archive: true` because its authenticated transport contract validates the
    provider ZIP bytes and then its sole JSON member; it is never consumed as
    reconciliation authority. Producer settings are bound by the
    exact workflow path/mode/blob in authenticated `B/H/M`; auditor and
    reconciler settings are separately bound by their allowlisted default-branch
    workflow ID/path/revision/ref/event/run/attempt. PR 4C reads each required
    object's `created_at`, `expires_at`, `expired`, identity, digest, size, and
    bytes through ordinary Actions-read metadata.
    `expires_at` is an availability deadline, not proof of an exact 90-day
    timestamp subtraction. Missing, expired, early-deleted, inconsistent, or
    unreadable required evidence is immutable
    `UNVERIFIED/RECONCILIATION_RETENTION_HISTORY_LOST` for every overlapping
    report.
20. Repository-level retention settings are optional PR 7 telemetry and never
    an authority input for PR 4C. The reconciler receives no
    `Administration: read`, write credential, OIDC, secret, cache, arbitrary
    request, or mutation capability.
21. API requests, raw run/artifact records, aggregate API-response plus
    artifact-download bytes across both observations, wall time, attempts,
    receipts, partition depth, JSON shape, and output size have exact hard
    budgets. Crossing one stops enumeration and writes only a compact incomplete
    `UNVERIFIED` lower-bound summary; it never invents a full count, range, or
    digest for unseen records. A request is counted before every outbound HTTP
    dispatch, including Git/PR reads, polls, artifact endpoints, every followed
    redirect, and every retry; response bytes are application-delivered body
    octets before decompression. The parent-owned hard maxima are 800 requests,
    134,217,728 response-body bytes, and 900 seconds; a child cannot raise them.
    A separately approved source-free read-only calibration-probe child runs the
    complete 14-day double observation under those maxima and emits closed,
    exact-source-bound `dpone.ci-shadow-reconciliation-capacity.v1` evidence.
    It is always `UNVERIFIED`; qualifying observations use
    `RECONCILIATION_CAPACITY_CALIBRATION_ONLY` and incomplete or over-threshold
    observations use `RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED`. It expires
    for approval after 24 hours and has no daily-root authority. The artifact
    binds the policy and domain-separated shared acquisition-bundle digest. The
    calibration and reconciliation workflows/CLI roots are authenticated
    separately; only their canonical acquisition policy/service/ports/adapters/
    imports/scripts/locks must be byte-identical. Controllable runner label,
    architecture, and Python version also match; hosted image version remains
    diagnostic under the two-times margin and runtime hard caps. Before parsing,
    the approval adapter verifies the selected artifact's exact repository/run/
    attempt/ID/name, rejects provider archive size above 8,388,608 bytes,
    bounded-reads the ZIP archive at most one byte beyond that cap, and compares
    both provider size and digest with those same archive bytes. It then accepts
    exactly one expected, confined regular JSON member (no duplicate, directory,
    symlink, encrypted, traversal, or unsupported-compression entry), separately
    bounds and hashes its direct payload bytes, and only then applies the
    duplicate-free/non-finite-rejecting strict parser capped at depth 32 and
    50,000 nodes. Request classes canonically sum to
    `total_http_requests`; retry count remains an intersecting subset no larger
    than that total; API plus artifact body bytes equal the total response
    bytes. The approval consumer uses injected trusted UTC time, verifies exact
    14-day/grace/observation ordering, requires the calibration stamp within the
    900-second observation budget, requires approval strictly before the
    24-hour expiry, and matches expected source/policy/bundle/configuration
    identity. Every crossed counter is serialized at its hard maximum in
    canonical hard-maximum order. PR 4C
    reconciler-child approval requires complete matching observations at or
    below 400 requests, 67,108,864 response-body bytes, and 450 seconds.
    Incomplete, stale, or over-threshold evidence is
    `RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED`; raising a maximum requires an
    approved parent amendment. Counters are exact below limits and saturating
    lower bounds after bounded `remaining+1` byte detection. Wall time uses an
    injected monotonic clock and every dispatch timeout is capped by remaining
    budget; a crossed request/byte/wall limit can never qualify evidence.
22. Fork approval is fail-closed telemetry. An immutable incoming event whose
    own conclusion is `action_required` may no-op as receipt-free
    `PENDING_OBSERVER`; this is not a promise that the event exists. A current
    API action-required run is `UNVERIFIED/PROVENANCE_APPROVAL_PENDING`. Only an
    independently authenticated post-start completed attempt enters audit. The
    design does not require the same run ID or run attempt.
23. PR 4B first lands the safe fallback on the default branch. An authorized
    public-fork lifecycle canary then records the actual event/API lifecycle
    before the PR 4C reconciler child contract and implementation. Route, deliberate
    failure/cancellation, burst, and distinct-SHA campaigns depend on PR 4C and
    run after its implementation, before goal acceptance. Unknown, moved,
    deleted, closed/reopened, or ambiguous fork forms remain `UNVERIFIED`;
    absence of a canary cannot weaken the fallback.
24. A retry repeats the complete interval and writes a new immutable report. A
    fresh auditable attempt may establish new exact-head evidence but cannot
    repair an overlapping daily interval containing lost evidence. Daily-root
    recovery waits until a fully observed clean window excludes the lost
    attempt. A deterministic resource cap likewise waits for a clean window or
    an approved contract amendment; only transient API/wall-time failure uses
    immediate retry. A later `PASS` never relabels an older
    `FAIL|UNVERIFIED` report, and no recovery repairs state or re-genesises.

### Immutable candidate and readiness

25. Candidate artifacts come only from completed allowlisted default-branch
    producers. Manual, tag, PR, same-run, foreign, cancelled, timed-out, stale,
    ambiguous, malformed, or incomplete provenance is `UNVERIFIED`.
26. The verifier uses trusted data-only preflight and evaluation around a
    separately unprivileged, cacheless executor. Downloaded candidate content
    is never executed by the trusted verifier process.
27. The default provenance verifier denies authority to fixture, static-list,
    shadow, stale, and future-dated evidence. Candidate verification is first
    required for the exact subject and then published, so no public file is
    written before authenticated success.
28. Readiness is default deny. Durable evidence is written before its Markdown
    projection. The pair uses a confined journal with actual authenticated
    bytes for prior-state recovery; digests are never treated as reconstructable
    content. While a
    journal exists, consumers return `UNVERIFIED`.
29. PR 6A may land dormant pure decision and pair-transaction internals after
    PR 2, with no public CLI or PASS authority. PR 6B public adapters, migration,
    merge, and activation require PR 5B authenticated verifier evidence.

### Status taxonomy

30. `PASS` means authenticated and complete evidence. `FAIL` means an
    authenticated terminal product or contract failure with no uncertainty.
    `UNVERIFIED` means absent, ambiguous, unauthenticated, pending, stale,
    cancelled, timed-out, unavailable, or resource-limited proof. `N/A` is only
    a trusted-plan non-applicable job.
31. Every evidence matrix disables fail-fast. Missing/nonterminal/skipped/
    cancelled/timed-out/action-required/stale required evidence is
    `UNVERIFIED`; only a complete uncertainty-free unexpected/failed case is
    `FAIL`.
32. The required contexts resolved from the canonical active policy remain the
    only merge authority. The diagnostic
    producer result, audit receipt, reconciliation report, candidate evidence,
    and readiness pair cannot mutate protection or release state in this goal.

### GitHub Pages deployment recovery

33. A Pages pull-request build is read-only and may rerun because it never
    uploads or deploys. Every non-PR build/upload, current-master verification,
    and deploy job is eligible only for provider run attempt `1`. Rerunning all
    jobs, only verification, or only deploy is unsupported and cannot deploy.
    Recovery always dispatches a new `pages.yml` run on the current
    `refs/heads/master`; the new run receives a new run ID, attempt `1`, and
    artifact namespace. The versioned workflow-dispatch REST response must
    return and bind that exact positive run ID plus repository URLs; a prior,
    missing, malformed, or ambiguous run response is `UNVERIFIED`. No recovery
    selects, deletes, overwrites, or reuses a prior run's immutable
    `github-pages` artifact.
34. The operator validates the positive prior run ID, local `gh`/`jq`
    prerequisites, and active GitHub authentication before the mutating
    dispatch. Every API request explicitly binds `--hostname github.com`, so an
    ambient `GH_HOST` cannot redirect the write. The recovery and deployment
    API identity is exact repository, workflow path and ID, event,
    provider-visible `head_branch`, run ID, run attempt, `head_sha`, job ID, and
    pinned action revision. The Workflow Runs API does not expose `ref`; ref
    authority is instead proven by the exact
    `github.ref == 'refs/heads/master'` conditions in the authenticated workflow
    blob and a successful admitted job. Foreign, stale, malformed, ambiguous,
    prior-attempt, or skipped identity is `UNVERIFIED` and cannot receive
    Pages/OIDC authority. The
    read-only current-master verifier emits SHA and attempt outputs, and the
    deploy job accepts them only from its direct dependency and exact attempt
    `1`.
35. A job skipped by a false job-level condition may appear as provider check
    success, but its deployment outcome is `NOT_RUN` and its evidence is
    `UNVERIFIED`. PR 3A does not select a Pages REST record by SHA because
    repeated runs can share that SHA. Deployment PASS requires the exact
    attempt-1 deploy job and its provider-visible deploy step to conclude
    success. The authenticated workflow blob maps YAML job key `deploy` and the
    SHA-pinned `actions/deploy-pages` invocation to the exact provider display
    name and ordered step name/number; the Jobs API never supplies the job key
    or action `uses`. The pinned action creates and polls the Pages deployment.
    A workflow conclusion, environment state, or older/same-SHA deployment
    record is not deployment evidence.

### PR5B transport and subject-identity amendment (2026-08-30)

36. `actions/upload-artifact@v7` direct mode accepts one file. The three PR5A
    wheels and closed manifest therefore travel as one bounded deterministic,
    uncompressed raw USTAR file uploaded with `archive:false`; trusted
    preflight/evaluation never opens it, and only the unprivileged cacheless
    executor performs hardened extraction.
37. Candidate subject authority is provider-authenticated producer
    `workflow_run.head_sha`, the allowlisted workflow/revision, exact checkout,
    immutable artifact ID/digest/size and PR5A manifest hashes. A SHA copied into
    candidate-controlled manifest or wheel bytes would remain self-description,
    so PR5B preserves manifest v1 and does not use an embedded SHA as authority.
38. Executor case receipts are untrusted closed data. The evaluator must bind
    the exact current verifier run attempt through provider Jobs/artifact APIs,
    require all ten fresh cases, and reject prior or partial reruns. Its final
    receipt becomes authenticated only when a later consumer verifies the
    completed verifier run and post-upload artifact ID/digest/size.
39. The preflight handoff is a direct-JSON evidence coordinate, not an implicit
    workspace file. Producer outputs are published only after post-upload
    current-attempt readback authenticates provider ID/name/digest/size and
    payload digest. Every executor and evaluator re-acquires that exact receipt;
    missing, partial, stale or invalid coordinates are `UNVERIFIED` and never
    fall back to a prior attempt.
40. Public dependency resolution is a separate tokenless trusted capability.
    It receives the closed case plan plus an explicit descriptor-confined sealed
    candidate extraction, may read wheel metadata but never execute candidate
    code. Exact source/derived constraints and installer wheel first form their
    own descriptor-confined sealed resolver-input capability. A capped tokenless
    resolver receives that capability plus candidate/runtime/materialized-plan
    descriptors through a closed sequenced leaf/ACK protocol. Its authenticated
    root-launcher source is provisioned create-only under the root-owned staging
    protocol before those descriptors cross the Unix rendezvous; no launcher FD
    is preserved through sudo. The resolver then seals the offline bundle before
    the networkless sandbox is constructed.
    Physical transient bounds are distinct from logical admission limits; the
    design does not claim to count opaque pip HTTPS bodies. Case evidence binds
    plan, resolver-input, dependency and runtime inventory digests.
41. Candidate execution receives sealed candidate, dependency, verifier-tree,
    setup-python runtime and materialized-plan directory capabilities explicitly.
    Ubuntu sudo does not provide the previously assumed `--preserve-fds`
    option. Each privileged launcher is therefore provisioned before candidate
    execution as an authenticated create-only root-owned file, establishes a
    separately authenticated Unix rendezvous, and uses a sealed memfd only for
    its namespace re-exec. Neither launcher reacquires a workspace path or
    exposes a control descriptor to candidate children. The minimal launcher
    completes infrastructure bootstrap before READY, then executes one
    classified materialized operation per command after irreversible drop to
    the dedicated UID/GID. No mutable path reacquisition, hidden global,
    degraded sandbox or candidate-written receipt is allowed.
42. A verifier cannot authenticate its own just-uploaded final artifact. A
    separate read-only, manually dispatched certification workflow performs
    exact-attempt post-upload Jobs/artifact readback from explicit immutable
    coordinates and emits a closed certification receipt. It is not a second
    `workflow_run` hop: clause 11 remains unchanged and the candidate chain
    terminates at the verifier. A campaign manifest is usable for the diagnostic
    claim only after the collector re-authenticates every one of eight explicit
    role-bound certifier coordinates and its later consumer re-authenticates the
    uploaded campaign artifact; neither layer grants merge, readiness, release
    or publication authority.
43. Privileged root cleanup is authorized only by a terminal process
    observation bound to one root-owned, descriptor-confined, hash-chained
    cleanup journal. The sole root steward durably records process or mount
    intent before releasing the blocked child or performing the mount effect;
    the exact trusted parent reaper records raw wait status. Cleanup binds the
    journal, process-inventory and mount-inventory digests, proves recorded
    identities absent without claiming `waitpid` over nonchildren,
    and preserves the stage on a missing, torn, stale or substituted journal.
    Never-started identities remain null, started-unreaped identities retain
    PID/start/group with null wait, and no wait status is synthesized. Root
    peer, namespace and privileged descendants retain the runner-recorded outer
    process group; only the runner claims that group absent after a closed
    pidfd-zombie and descriptor-relative procfs proof followed by exact direct-
    child reap. The common stage/outer prover returns a closed success or
    failure outcome and releases its pidfd on every path without synthesizing a
    wait or absence claim. Stage/outer/helper/journal outcomes fold into the
    single public process diagnostic through a fixed global priority, so
    waitpid, nonzero and group failures remain representable independent of
    execution order. The journal uses a closed eight-byte framed/hash-chain format
    whose 262,144-byte physical cap includes framing, reserves exactly 8,208
    bytes for two retirement records, freezes every payload key and requires
    independent golden byte vectors. It has operation-ID mount transitions and
    authenticated descendant terminal counts. After the steward terminates, the sole cleanup writer durably
    records retirement intent, no-replace renames the complete stage with its
    authority journal, records retirement and retains the sealed directory as
    the crash-recovery tombstone. Retry has five exact states from active stage
    before durable intent through the already sealed state; the latter appends
    no additional record. Every REMOVE_AND_PROVE helper attempt is
    retained in order behind a separate exact helper-response DTO/digest that
    excludes runner-owned PID/wait/history. No successor starts before the prior
    helper is reaped and its sole-writer authority ends. A successor is legal
    only after a raw-zero MISSING response; nonzero, signalled, unreaped,
    invalid or UNVERIFIED attempts terminate retry and therefore cannot be
    followed by physical recovery in the same case. A helper response becomes
    accepted only with raw-zero child exit plus a complete EOF-terminated body
    before deadline; partial-at-deadline is MISSING and established invalidity
    is INVALID. Accepted UNVERIFIED physical fields
    and diagnostic are copied unchanged, while every no-response branch uses a
    conservative zero-progress projection. Pre-READY identities may remain explicit
    UNKNOWN until authenticated journal discovery; NEVER_STARTED is never used
    as a substitute. Resolver-input acquisition likewise returns a close-once
    outcome with exact take/close/cancellation transfer; only the case service
    constructs INPUT_INVALID, and downstream dependency preparation receives
    only a verified snapshot. Resolver and sandbox start outcomes apply the
    same rule: dependency preparer alone owns resolver starts, the case service
    alone owns sandbox starts, and closing DISPATCHED_FAILURE or an untaken
    READY capability returns its lifecycle evidence without a second consumer.
    Both
    CLEANED and UNVERIFIED cleanup evidence carry the exact preceding process
    digest. Pre-READY root-peer and namespace states remain public lifecycle
    evidence through explicit start/terminal outcome bundles; a fully verified
    session requires raw wait status zero for every started stage, outer,
    helper, root-peer, namespace and recorded privileged descendant process.

## Consequences

- Exact identities and trusted recomputation prevent producer self-description
  from becoming authority.
- Stateless reconciliation has a deliberately bounded observation claim. It
  cannot prove global historical absence, but it avoids a second distributed
  ledger, permanent cursor deadlock, and an App-only repository prerequisite.
- Cross-window auditor records cannot extend or repaint the daily producer
  interval, and the PR 4C reconciler child cannot begin until a separate
  read-only calibration probe proves the two-times request, byte, and wall-time
  safety margin within parent hard maxima.
- Fork approval remains visibly `UNVERIFIED` until live evidence supports a
  stronger child contract; uncertain provider behavior cannot become false
  success.
- Trusted upload parameters plus per-object availability validation detect
  missing/expired evidence without requiring repository-admin scope. Optional
  PR 7 settings telemetry remains separate from PR 4C decisions.
- The design reuses canonical contracts/services/ports/adapters and existing
  bounded governance/evidence primitives behind thin composition roots.
- Pages recovery is intentionally less flexible than GitHub's generic rerun
  controls. A new current-master run avoids cross-attempt artifact selection and
  makes blocked/skipped executions visible instead of manufacturing success.
- No live workflow, ruleset, classic protection, release path, tag, variable,
  or publication authority changes in the specification PR.

## Links

### Amendment: PR4C capacity policy V2 (2026-08-27)

The original V1 calibration policy and
`dpone.ci-shadow-reconciliation-capacity.v1` schema remain immutable
historical contracts at 800 hard / 400 approval requests. Their receipts stay
valid only as `UNVERIFIED` historical evidence. The authenticated capacity
workflow uses the additive `ReconciliationPolicyV2` and
`dpone.ci-shadow-reconciliation-capacity.v2` schema at 3,000 hard / 1,500
approval requests. For the amended PR4C prerequisite, consumers must require a
fresh V2 schema and V2 policy digest; V1 can never qualify V2. This amendment
supersedes the V1 numeric calibration prerequisite above, while preserving all
other exact-input, two-observation, byte, wall-time, and fail-closed rules.

- [Feature design](../feature-design-ci-pr-gate-exact-sha-evidence.md)
- [PR 3A CI hygiene design](../feature-design-ci-shadow-pr3a-ci-hygiene.md)
- [ADR 0046](0046-component-aware-pr-gate-authority.md)
- [ADR 0037](0037-immutable-agent-pr-merge-closure.md)
- [ADR 0028](0028-frozen-release-policy-and-publication-boundary.md)
- [Issue #512](https://github.com/PaulKov/dpone/issues/512)
