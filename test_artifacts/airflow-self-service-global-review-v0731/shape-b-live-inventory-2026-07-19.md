# Shape B live GitHub inventory (2026-07-19)

Status: **PARTIAL** — branch + tag rulesets, controller scaffold, GitHub App
**installed** (`4341356` / installation `147673155` on `dpone` + controller),
B2 WORM bucket live with **smoke append PASS**. Gate A Trusted Publisher UI
rebind **done** (2026-07-20). Remaining blockers: Gate B–D, policy v2 cutover,
key rotation after chat exposure. Does **not** activate ADR 0028.

Captures against `PaulKov/dpone` (and controller scaffold) on 2026-07-19.
No credentials or secret values are recorded.

## Activation gate summary

| Blocker (from release-trust design) | Status | Notes |
|---|---|---|
| 1. Live provider inventory | PARTIAL→improved | Repo + `protect-master` + **`protect-release-tags` live**; producer map below; immutable-release parity still `UNVERIFIED` |
| 2. External controller inventory | PARTIAL | App id `4341356`, installation `147673155` on `PaulKov/dpone` + `PaulKov/dpone-release-controller`; PEM in Actions secrets; mutation jobs still absent |
| 3. Evidence store inventory | PARTIAL→verified smoke | B2 bucket + Object Lock + keyId `0057b4c6b10ab460000000001`; smoke object with compliance retention **PASS** |
| 4. Isolated ruleset-history reader | `UNVERIFIED` | History readable with admin session; dedicated pinned reader App not provisioned |
| 5. Atomic v1→v2 policy cutover | OPEN | Production `.agents/policy/github-branch-protection.yml` remains schema v1 |

Verdict: **shape B NO-GO** for ADR 0028 activation. Gate A TP UI rebind is
complete; Gates B–D + policy v2 cutover still require fresh GO phrases.

## 1. Repository

| Field | Live value | Evidence |
|---|---|---|
| `name_with_owner` | `PaulKov/dpone` | `GET /repos/PaulKov/dpone` |
| `repository_id` (numeric) | `1255975556` | same |
| `node_id` | `R_kgDOStyqhA` | same / GraphQL `id` |
| `canonical_remote_url` | `https://github.com/PaulKov/dpone.git` | `clone_url` |
| `protected_base_ref` | `refs/heads/master` | ruleset conditions |
| Owner login / id | `PaulKov` / `74862786` | `GET /users/PaulKov` |
| Viewer permission (capture session) | `ADMIN` | GraphQL `viewerPermission` |

## 2. Branch ruleset `protect-master`

Live `GET /repos/PaulKov/dpone/rulesets/18806829` (+ history head):

| Field | Live value |
|---|---|
| `id` | `18806829` |
| history-head `version_id` | `43592502` (as of 2026-07-19T21:37:47.780+03:00) |
| `name` | `protect-master` |
| `target` | `branch` |
| `source_type` / `source` | `Repository` / `PaulKov/dpone` |
| `enforcement` | `active` |
| conditions.include | `refs/heads/master` |
| conditions.exclude | `[]` |
| bypass_actors | one row: `actor_id=5`, `actor_type=RepositoryRole`, `bypass_mode=always` |
| pull_request | required; merge methods `merge`,`squash`; dismiss stale on push; required approving review count `0`; require code owner `false`; require last push approval `false`; review thread resolution `true` |
| required_status_checks.strict | `true` |
| protected_actions | deletion + non_fast_forward present (maps to block deletion / force-push) |

Checked-in v1 policy id `18806829` matches live. History-head `version_id`
`43592502` is **not** yet frozen into production policy (v1 has no version
field).

### Required checks (context → integration_id)

Every required context on the live ruleset carries `integration_id: 15368`
(GitHub Actions app). Context list matches the v1 policy check names exactly
(19 contexts).

### Observed producer workflow map (context → workflow)

Producer `workflow_path` / `workflow_id` are **not** on the ruleset API; they
were observed from successful check-runs around tip `f6cae1e0` / PR #353 CI.
Treat as inventory candidates for v2 `producer` blocks — re-verify at cutover
time against the exact release commit.

| Context | `integration_id` | `workflow_path` | `workflow_id` | Workflow name |
|---|---|---|---|---|
| Agent PR receipt | 15368 | `.github/workflows/agent-pr-receipt.yml` | 311743814 | Agent PR receipt |
| Build Airflow distribution artifacts | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Airflow 2.10.5 / py3.11 | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Airflow 2.10.5 / py3.12 | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Airflow 2.11.0 / py3.11 | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Airflow 2.11.0 / py3.12 | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Airflow 3.2.0 / py3.11 | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Airflow 3.2.0 / py3.12 | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Airflow 3.3.0 / py3.11 | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Airflow 3.3.0 / py3.12 | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Runtime wheel smoke / py3.11 | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Runtime wheel smoke / py3.12 | 15368 | `.github/workflows/airflow-pack-compat.yml` | 309393676 | Airflow pack compatibility |
| Analyze Python | 15368 | `.github/workflows/codeql.yml` | 286856013 | CodeQL |
| Build GitHub Pages documentation | 15368 | `.github/workflows/pages.yml` | 288538896 | docs |
| Dependency Review | 15368 | `.github/workflows/dependency-review.yml` | 311302094 | Dependency Review |
| PostgreSQL XMin integration | 15368 | `.github/workflows/ci.yml` | 286829746 | CI |
| Quality checks (3.11) | 15368 | `.github/workflows/ci.yml` | 286829746 | CI |
| Quality checks (3.12) | 15368 | `.github/workflows/ci.yml` | 286829746 | CI |
| TruffleHog verified secrets | 15368 | `.github/workflows/secret-scan.yml` | 286856016 | Secret Scan |

## 3. Tag ruleset `protect-release-tags` (created 2026-07-19)

Live `POST/GET /repos/PaulKov/dpone/rulesets` → id `19179508`:

| Field | Live value |
|---|---|
| `id` | `19179508` |
| history-head `version_id` | `43597427` |
| `name` | `protect-release-tags` |
| `target` | `tag` |
| `source_type` / `source` | `Repository` / `PaulKov/dpone` |
| `enforcement` | `active` |
| conditions.include | `refs/tags/v*.*.*` |
| conditions.exclude | `[]` |
| bypass_actors | `[]` |
| rules | `update`, `deletion` (block tag rewrite + delete) |
| `current_user_can_bypass` | `never` |
| HTML | https://github.com/PaulKov/dpone/rules/19179508 |

Matches ADR 0028 / release-trust design intent for tag protection. Not yet
frozen into production policy v2 bytes.

## 4. Environments (target repo `PaulKov/dpone`)

| Environment | id | protection_rules count | Shape B note |
|---|---|---|---|
| `github-pages` | 16181378654 | 1 | Pages deploy; not the release controller |
| `pypi` | 16069802387 | 0 | Candidate-repo env still present; PyPI Trusted Publishers now point at **controller** `pypi` env (Gate A) |
| `release-attest` | MISSING on target | — | Lives on controller scaffold instead |
| `github-release` | MISSING on target | — | Lives on controller scaffold instead |

## 5. External controller scaffold (`PaulKov/dpone-release-controller`)

Created 2026-07-19 as a **non-activated** scaffold. Public visibility required
for Actions indexing on the free plan; mutation authority is intentionally
absent.

| Field | Live value |
|---|---|
| `name_with_owner` | `PaulKov/dpone-release-controller` |
| `repository_id` | `1305993853` |
| `node_id` | `R_kgDOTdfifQ` |
| default branch / head SHA | `master` / `97885b97e197721b62d7b83e212a1639c331f090` |
| workflow path | `.github/workflows/release-controller.yml` |
| workflow id | `316322127` |
| workflow name | `Release controller scaffold` |
| workflow state | `active` |
| dry-run evidence | Actions run `29702106003` success (push) |
| env `release-attest` | id `18405660744`, protection_rules `0` |
| env `pypi` | id `18405660890`, protection_rules `0` |
| env `github-release` | id `18405661023`, protection_rules `0` |

| Still missing for SS-47 closure | Status |
|---|---|
| Controller GitHub App id | **Done** — `4341356` |
| Controller App installation id | **Done** — `147673155` (repos: `dpone`, `dpone-release-controller`) |
| Protected-environment required reviewers / deployment policies | BLOCKED on free billing plan for required-reviewers rules (`422`) |
| Mutation jobs (attest / draft / PyPI / publish) | PARTIAL — live bootstrap **draft staging** PASS; no PyPI / no draft publish |
| Evidence lease tooling | **Done** — Actions run `29704954934` PASS |
| Attest/draft dry-run tooling | **Done** — Actions run `29705802633` PASS (`admit-and-lease` → `attest-and-draft`) |
| Live attest + draft staging | **Done** — controller run `29706714213` PASS; draft id `356449311` (`v0.0.0-shape-b-bootstrap-29706714213`, unpublished) |
| Snapshot B / AUTHORIZED bootstrap | **Done** — controller run `29721569043` PASS; draft `356531042`; auth `sha256:6e9f30f4…61882e140` (no PASS/GO) |
| Snapshot A + lease release | **Done** — controller run `29722846127` PASS (A → draft → AUTHORIZED → LEASE_RELEASED) |
| PyPI inventory observe (no upload) | **Done** — controller run `29726607809` PASS; four projects reachable at `0.73.2`; `upload_attempted=false` |
| Immutable-release setting inventory | **Done (UNVERIFIED)** — run `29726607809`; org API `HTTP_404` for user-owned `PaulKov`; `mutated=false` (no enablement) |
| Four-project PyPI Trusted Publisher → this controller workflow | **Gate A UI rebind DONE** (2026-07-20) — all four projects bound to `PaulKov/dpone-release-controller` / `release-controller.yml` / `pypi`; legacy `PaulKov/dpone`+`release.yml` removed where present. Checklist: `shape-b-gate-a-tp-rebind-checklist-2026-07-20.md`. Post-observe run `29735049575`. Prior observe `29728990019` (`PROVENANCE_MISSING`, `rebind_attempted=false`) remains historical. |
| Draft inventory observe (no publish) | **Done** — controller run `29730823249` PASS (`STILL_DRAFT`, `publish_attempted=false`) |
| GO readiness brief | **Done** — `shape-b-go-readiness-2026-07-20.md` (NO-GO) |
| Append-only evidence bucket + Object Lock | **Done** — smoke `bootstrap/smoke/20260719T212938Z-store-smoke.json` compliance retention PASS |
| B2 applicationKeyId | **Done** — `0057b4c6b10ab460000000001` (rotate application key secret after chat exposure) |

### Evidence store live values (2026-07-19)

| Field | Value |
|---|---|
| provider | Backblaze B2 Object Lock |
| bucket | `dpone-release-evidence-v1` |
| bucket_id | `87db248c461b71c09afb0416` |
| endpoint / region | `s3.us-east-005.backblazeb2.com` / `us-east-005` |
| object_lock | enabled (screenshot + console) |
| store_id | `b2://dpone-release-evidence-v1?endpoint=s3.us-east-005.backblazeb2.com&object_lock=enabled&bucket_id=87db248c461b71c09afb0416&retention_days_pre_mutation=365&retention_days_closed=2557` |
| applicationKeyId | `0057b4c6b10ab460000000001` |
| account_id | `7b4c6b10ab46` |
| smoke | `bootstrap/smoke/20260719T212938Z-store-smoke.json` fileId `4_z87db248c461b71c09afb0416_f1083b98f31dbed94_d20260719_m212938_c005_v0501039_t0039_u01784496578985` retention compliance PASS |
| secrets location | `PaulKov/dpone-release-controller` Actions secrets (`B2_*`, `DPONE_RELEASE_APP_*`) — never committed |

## 6. Explicit non-claims

- Do **not** treat this file or the controller scaffold as policy v2 activation.
- Do **not** paste these IDs into production
  `.agents/policy/github-branch-protection.yml` without the atomic consumer
  cutover and fresh GO.
- Gate A TP rebind is complete; do **not** upload/publish or cut over policy
  without Gate B/C/D GO phrases.
- Live K8s / Vault / GCS certification remains out of scope here (`UNVERIFIED`).
- Repository immutable-release setting parity was not certified (`UNVERIFIED`).

## 7. Next operator actions (shape B)

1. ~~Create active tag ruleset `protect-release-tags`.~~ **Done** (`19179508` /
   `43597427`).
2. ~~Create external controller repository scaffold + envs + dry-run workflow.~~
   **Done** (`PaulKov/dpone-release-controller`, workflow `316322127`).
3. ~~Lock free WORM provider + App manifest.~~ **Done**.
4. ~~Register GitHub App.~~ **Done** — app_id `4341356`.
5. ~~Create B2 Object Lock bucket.~~ **Done** — smoke PASS.
6. ~~Install App + B2 keyId.~~ **Done** — installation `147673155`, keyId
   `0057b4c6b10ab460000000001`.
7. **Recommended:** rotate `B2_APPLICATION_KEY` (chat exposure) and update the
   controller Actions secret; optionally narrow key capabilities (drop
   `deleteFiles` / `bypassGovernance` once Object Lock workflow is settled).
8. ~~Implement lease/receipt adapters + admit-and-lease.~~ **Done** (unit tests
   + controller run `29704954934`).
9. ~~Attest/bundle/draft dry-run receipts.~~ **Done** (unit tests + controller
   run `29705802633`).
10. ~~Live attest + draft staging (App contents write).~~ **Done** (controller
    run `29706714213`, draft `356449311`, still unpublished; no TP/PyPI).
11. ~~Snapshot B + `AUTHORIZED` bootstrap.~~ **Done** (controller run
    `29721569043`; no PyPI / no publish / no PASS-GO).
12. ~~Snapshot A + A→B binding + `LEASE_RELEASED`.~~ **Done** (controller run
    `29722846127`).
13. ~~Read-only PyPI + immutable-release inventory.~~ **Done** (controller run
    `29726607809`; immutable setting remains `UNVERIFIED` / not enabled).
14. ~~Read-only Trusted Publisher / Integrity claim inventory.~~ **Done**
    (controller run `29728990019`; latest files lack Integrity provenance;
    TP config still `UNVERIFIED` / not rebound).
15. ~~Draft inventory observe + GO readiness brief.~~ **Done** (controller run
    `29730823249`; `STILL_DRAFT`; brief remains NO-GO).
16. ~~Operator GO runbook (gates A–D, no execution).~~ **Done** —
    `shape-b-go-operator-runbook-2026-07-20.md`.
17. ~~Under explicit GO: Trusted Publisher rebind (A).~~ **Done** UI 2026-07-20
    + observe `29735049575` (see Gate A checklist).
18. Under explicit GO phrase per gate: immutable enablement (B), real publish
    (C), policy v1→v2 cutover (D).
19. Fresh GO review before claiming ADR 0028 active.
