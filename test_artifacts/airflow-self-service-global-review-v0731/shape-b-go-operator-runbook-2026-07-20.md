# Shape B operator GO runbook (2026-07-20)

Companion to `shape-b-go-readiness-2026-07-20.md`.
This runbook is **instructional only**. Do not execute Gate A–D without an
explicit operator GO for that gate. ADR 0028 / policy v2 remain inactive until
Gate D closes with fresh evidence.

Controller: `PaulKov/dpone-release-controller`
Workflow: `.github/workflows/release-controller.yml` (id `316322127`)
Target: `PaulKov/dpone` (id `1255975556`)

## Preflight (before any gate)

- [ ] Read readiness brief; confirm verdict is still NO-GO until gates close.
- [ ] Rotate B2 application key (recommended) and update Actions secrets
  `B2_APPLICATION_KEY_ID` / `B2_APPLICATION_KEY` on the controller.
- [ ] Optionally narrow B2 key capabilities (drop `deleteFiles` /
  `bypassGovernance` once Object Lock workflow is settled).
- [ ] Decide first GO mode:
  - **Rehearsal:** Gate A only (+ observe), no publish;
  - **Publication path:** Gates A→B→C in order, then Gate D separately.

Compensating note for env required-reviewers (`422` on free plan): document
accepted residual risk, or upgrade billing before treating environments as
production-hardened. Do not silently skip this row.

## Gate A — Trusted Publisher rebind (manual PyPI UI)

**GO phrase required:** `GO Gate A: Trusted Publisher rebind`

Exact binding to configure for **each** of:

- `dpone`
- `dpone-native-accel`
- `dpone-airflow-pack`
- `apache-airflow-providers-dpone`

| Field | Value |
|---|---|
| Owner | `PaulKov` |
| Repository | `dpone-release-controller` |
| Workflow filename | `release-controller.yml` |
| Environment | `pypi` |

Procedure:

1. For each project: PyPI → Manage → Publishing → GitHub → add/replace publisher
   with the table above.
2. Remove or disable the legacy candidate binding to `PaulKov/dpone` /
   `release.yml` only after the controller binding is confirmed present
   (order matters for resume safety).
3. Do **not** upload. Re-run controller `mode=live` observe path and confirm
   `trusted-publisher-inventory-observe` still has `rebind_attempted=false`
   in tooling (rebind is UI-side; receipts remain observe-only until upload).
4. Record evidence: screenshots or operator notes for all four projects +
   controller run URL.

Exit criteria:

- Four projects show controller publisher in PyPI UI;
- Candidate `release.yml` is no longer the sole/authoritative TP for v2 path;
- No PyPI upload performed in this gate.

## Gate B — Immutable releases enablement

**GO phrase required:** `GO Gate B: immutable releases enablement`

Checklist: `shape-b-gate-b-immutable-checklist-2026-07-20.md`

Notes:

- Org API `GET /orgs/PaulKov/settings/immutable-releases` returns `404` for this
  user-owned account. Use **repo** API instead:
  `GET|PUT /repos/{owner}/{repo}/immutable-releases`.
- Live probe (2026-07-20): both `dpone` and `dpone-release-controller` report
  `enabled=false` via repo GET.
- Observe tooling must remain `mutated=false` until this gate is explicitly
  executed.

Procedure:

1. Enable immutable releases on `PaulKov/dpone` and
   `PaulKov/dpone-release-controller` (repo API `PUT` or Settings → Releases).
2. Re-run `immutable-inventory-observe` (controller `mode=live`).
3. Accept only `ENABLED` from the **repo** source. `UNVERIFIED` is not enablement.

Exit criteria:

- Inventory observes `ENABLED` (or provider-documented enabled state);
- No draft publish performed in this gate.

## Gate C — Real publication attempt (not bootstrap)

**GO phrase required:** `GO Gate C: publish candidate <version>`

Preconditions: Gate A closed; Gate B closed or consciously waived with written
risk acceptance; sealed candidate inventory for `<version>` exists.

Procedure (high level; follow feature design §6–§7):

1. Run controller publication path for the **real** version (not
   `v0.0.0-shape-b-bootstrap-*`).
2. Upload only `PENDING_UPLOAD` subset under AUTHORIZED + fencing token.
3. Publish the exact staged draft by release ID; never “latest”.
4. Verify Integrity provenance for each file and GitHub immutable release
   attestations.
5. Do **not** write policy v2 activation or ADR 0028 “active” in this gate.

Exit criteria:

- Eight exact files verified (or documented `PYPI_PARTIAL_EXACT` recovery);
- Published GitHub release `draft=false` with required immutable checks;
- Evidence chain closed for the attempt without claiming policy cutover.

## Gate D — Atomic policy v1→v2 cutover

**GO phrase required:** `GO Gate D: policy v2 cutover`
**Second phrase required before claim:** `GO ADR 0028 active`

Procedure:

1. Freeze policy digest and live producer/ruleset inventory.
2. Atomically cut consumers to v2 per feature design matrix.
3. Retain rollback path until post-cutover observation window closes.
4. Only after fresh GO evidence, claim ADR 0028 active in docs/release notes.

Exit criteria:

- Production `.agents/policy/github-branch-protection.yml` (or successor) is v2
  with verified live identities;
- No false `PASS`/`GO` from bootstrap receipts;
- Fresh GO review artifact attached.

## Abort / hold

Stop and append an incident hold (no further mutation) if any of:

- TP / Integrity / digest conflict;
- Draft ID or asset mismatch;
- Immutable setting regresses to disabled/unverified after Gate B;
- Lease fencing mismatch;
- Unexpected publisher identity on upload.

## Evidence pointers

| Artifact | Path / URL |
|---|---|
| Readiness brief | `shape-b-go-readiness-2026-07-20.md` |
| Live inventory | `shape-b-live-inventory-2026-07-19.md` |
| Latest draft observe | controller run `29730823249` |
| Latest TP observe | controller run `29728990019` |
