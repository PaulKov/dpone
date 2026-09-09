# Shape B GO readiness brief (2026-07-20)

Authority: `docs/feature-design-release-trust-boundary-v0732.md`, ADR 0028.
Live inventory: `shape-b-live-inventory-2026-07-19.md`.
Operator runbook (gates A–D): `shape-b-go-operator-runbook-2026-07-20.md`.

## Verdict

**NO-GO for ADR 0028 / policy v2 activation.**

Bootstrap controller path is provisioned and observed end-to-end through
AUTHORIZED + observe inventories + LEASE_RELEASED. That is **not** production
publication authority and must not be treated as Trusted Publisher cutover.

## Ready (evidence exists)

| Capability | Evidence |
|---|---|
| Tag ruleset `protect-release-tags` | ruleset `19179508` |
| Controller App + install | app `4341356`, install `147673155` |
| B2 Object Lock evidence store | bucket `dpone-release-evidence-v1`, smoke PASS |
| Lease acquire / release | runs `29704954934`, `29722846127` |
| Live draft staging (unpublished) | run `29706714213` / later bootstrap drafts |
| Snapshot A/B + AUTHORIZED | runs `29721569043`, `29722846127` |
| PyPI file inventory (no upload) | run `29726607809` |
| Immutable-release setting observe | run `29726607809` → `UNVERIFIED` (`HTTP_404`) |
| Trusted Publisher / Integrity observe | run `29728990019` → `PROVENANCE_MISSING`, no rebind |
| Draft inventory observe (no publish) | run `29730823249` → `STILL_DRAFT`, `publish_attempted=false` |

## Blocked / UNVERIFIED (do not ignore at GO)

| Item | Status | Why it matters |
|---|---|---|
| Env required-reviewers (`release-attest` / `pypi` / `github-release`) | BLOCKED | GitHub free plan `422`; deployment protection incomplete |
| Immutable releases enablement | UNVERIFIED | User-owned account; org settings API 404; not enabled |
| Four-project TP → controller workflow | UNVERIFIED config | No public TP management API; latest `0.73.2` files lack Integrity provenance |
| Candidate `release.yml` still live publisher | ACTIVE | Must remain until explicit cutover |
| Policy v1 production bytes | ACTIVE | Atomic v1→v2 not started |
| B2 application key rotation | Recommended | Key ID exposed in chat history |

## Explicit GO gates (require operator GO)

1. Rebind four PyPI Trusted Publishers to
   `PaulKov/dpone-release-controller` /
   `.github/workflows/release-controller.yml` / environment `pypi`.
2. Enable immutable releases by a supported GitHub path for this account type.
3. Publish a real candidate (not bootstrap draft) only after TP + immutable
   preconditions and exact inventory closure.
4. Atomic policy v1→v2 consumer cutover + fresh GO before claiming ADR 0028 active.

## Recommended before any GO

1. Rotate `B2_APPLICATION_KEY` and update controller Actions secret.
2. Confirm billing/plan path for required-reviewers, or accept documented residual
   risk with compensating controls.
3. Decide whether first GO is a **dry production rehearsal** (TP rebind + observe
   only) or a **full publication** attempt.

## Non-claims

- Controller scaffold ≠ ADR 0028 active.
- `AUTHORIZED` ≠ `PASS` / `GO`.
- Observe inventories ≠ publisher rebind, upload, draft publish, or policy cutover.
