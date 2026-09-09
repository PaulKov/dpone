# Gate B checklist — Immutable releases enablement (2026-07-20)

**GO phrase required:** `GO Gate B: immutable releases enablement`
**Status:** prepared — **not executed** (awaiting exact GO phrase).
**Mode:** enable setting only — no draft publish, no PyPI upload, no policy cutover.

## Why org API was UNVERIFIED

`GET /orgs/PaulKov/settings/immutable-releases` returns `404` for this
user-owned account. Repo-level API is available and authoritative here:

```text
GET /repos/{owner}/{repo}/immutable-releases
→ {"enabled": false|true, "enforced_by_owner": false|true}
PUT /repos/{owner}/{repo}/immutable-releases   # enable (admin)
```

Live probe 2026-07-20 (observe-only GET):

| Repository | enabled | enforced_by_owner |
|---|---|---|
| `PaulKov/dpone` | `false` | `false` |
| `PaulKov/dpone-release-controller` | `false` | `false` |

Docs: [Preventing changes to your releases](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/establish-provenance-and-integrity/preventing-changes-to-your-releases)
(Changelog GA: 2025-10-28).

## Targets to enable under GO

1. `PaulKov/dpone` (publication target / tags)
2. `PaulKov/dpone-release-controller` (controller drafts / attest path)

Optional UI path per repo: Settings → General → Releases →
**Enable release immutability**.

## Procedure (only after GO phrase)

1. `PUT` repo immutable-releases (or UI checkbox) on both targets.
2. Confirm `GET` returns `enabled=true` for each.
3. Dispatch controller `mode=live` observe; accept only `observed=ENABLED`
   from repo-source inventory (not `UNVERIFIED`).
4. Do **not** publish drafts or upload to PyPI in this gate.

## Exit criteria

- [ ] `PaulKov/dpone` immutable = enabled
- [ ] `PaulKov/dpone-release-controller` immutable = enabled
- [ ] Post-enable controller observe records `ENABLED` with `mutated=false`
- [ ] No draft publish / PyPI upload performed

Post-observe controller run: `_pending_`
