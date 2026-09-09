# ADR 0049: Release publication requires newest eligible exact-SHA provider evidence

- Status: Accepted
- Date: 2026-08-13

## Context

The release and runtime-image workflows independently react to the same tag and
can mutate PyPI and GHCR. Required real-live evidence was documented but not
consumed by either workflow. Existing behavioral JSON assemblers are useful
diagnostics, but accept caller-selected inputs and cannot authenticate their
workflow run or containing artifact.

## Decision

One manual `Release candidate evidence` workflow runs on exact `master`. Its
fixed native terminal job publishes one attempt-specific closed artifact. A
read-only publication verifier first authenticates the paired tag runs and
freezes the eligible evidence set to exact-SHA dispatches whose provider
`created_at` is no later than the shared publication cutoff. It selects the
unique maximum provider `created_at` in that set before inspecting the run's
current attempt or status. Equal maximum values are ambiguous and fail closed.
The verifier then reads that run's current `run_attempt`, requires success,
binds its terminal job through the GitHub check suite to the same attempt, and
validates the unique unexpired artifact under bounded resource and strict JSON
rules. Rerunning an older dispatch cannot change dispatch order, while a new
post-cutoff dispatch is ignored for the frozen tag.

Both tag-triggered publication workflows invoke this verifier in preflight and
again inside every mutation-capable job immediately before its first external
write: release attestation, PyPI publication, GitHub Release creation,
digest-only GHCR publication with attestations, and GHCR alias promotion. A
multi-write block does not run a separate gate before each attestation; its
later attest writes remain in the same gated block, with no intervening
checked-out repository code able to change evidence authority. A newer
eligible failed, cancelled, pending, missing, malformed, or expired
dispatch for the same exact commit blocks; an older eligible PASS is never a
fallback. A dispatch created after the provider cutoff cannot supersede or
deny the already-frozen tag. A different tagged commit requires its own
evidence. Later `master` advancement does not invalidate evidence for frozen
commit C.

PyPI is an additional non-transactional boundary inside the package publisher.
Its upload API accepts files individually, while the pinned publishing action's
`skip-existing` behavior continues after an existing filename. Therefore the
fresh provider gate alone is insufficient for a safe same-version resume. The
ordinary publisher performs a second fail-closed check immediately before the
action: it rehashes the exact eight local candidates, reads each of the four
exact-version PyPI JSON endpoints, and requires every visible file to be an
exact non-yanked candidate by filename, SHA-256, and size. The only accepted
states are a fully absent release or an exact candidate subset; unexpected,
duplicate, conflicting, malformed, oversized, or unavailable state blocks
before upload.

The second gate writes a deterministic closed receipt before the publishing
action. It binds repository, tagged commit, release tag, fixed workflow path,
positive run ID/attempt asserted against GitHub environment values, raw
candidate-inventory digest, all endpoint observations, and all candidate
classifications. `skip-existing: true` is retained only as mechanics for that
proved exact subset, never as authority to ignore a conflict. Post-publication
byte verification remains defense in depth. A concurrent conflicting upload
between the final observation and the action is an unavoidable external race;
it blocks downstream steps when detected but may require a new patch version
because accepted PyPI files cannot be rolled back.

The verifier remains stdlib-only because the OIDC publisher job deliberately
does not check out or install repository/application code. Shared closed JSON,
error, receipt, and candidate contracts are factored once; local inventory and
PyPI transport are separate adapters; the CLI composes policy. This small
source-free bundle avoids both dependency installation at the privileged
boundary and duplicated validation logic while keeping each module below the
repository size thresholds.

The time boundary is provider-owned, not tagger-authored. For both
`.github/workflows/release.yml` and
`.github/workflows/runtime-image.yml`, the verifier requires exactly one
tag-push run with the exact repository, workflow path, tag, commit, and `push`
event. The calling run must also match its exact positive run ID and attempt and
still be `in_progress`. The earlier of the two provider `created_at` values is
the immutable publication cutoff. Only evidence dispatches created no later
than that instant are eligible; the selected execution must also have
completed, its terminal check must have completed, and its artifact must have
been created by the cutoff. Later evidence dispatches are ignored. Artifact
retention is availability, not permanent durability.

Every successful verification receipt retains the complete publication pair,
not only the calling run. The pair is exactly the `release.yml` and
`runtime-image.yml` identities sorted by workflow path; every closed entry
contains only workflow path, positive run ID, positive run attempt, and
provider creation time. The caller must match its corresponding entry, the
serialized cutoff must equal the exact minimum creation time, and a tagged
SHA-256 binds the canonical JSON bytes of the ordered pair. The retained
receipt can therefore reproduce its cutoff decision without later GitHub API
history.

Paired-run discovery absorbs only provider indexing latency. By default each
gate performs an initial observation plus at most 30 ten-second waits (31
observations and about 300 seconds of sleep budget, excluding request time).
Every observation re-fetches and re-authenticates the caller's exact current
run detail, then lists both fixed workflow paths. Runs for another tag are
ignored. Only zero exact-tag matches
for a path and mutable current-run `run_attempt`/`updated_at` detail/list
projection drift are retryable. A malformed exact match, more than one exact
match, or immutable current-run `id`/`created_at` drift fails immediately.
Persistent mutable projection drift fails after the same bounded poll.

The authoritative `native_transfer` campaign is a strict `real_local` superset
and derives claims only from observed inputs. Disabled placeholder assemblers
remain disabled. Its 25,000-row stress evidence fixes every required phase at a
minimum 500 rows/s and rejects rates that are inconsistent with the producer's
three-decimal duration and two-decimal rate rounding. No custom check,
`checks: write`, publication token, OIDC, or protected environment is granted
to the producer.

## Consequences

- A tag without current exact-SHA evidence fails before PyPI and GHCR work.
- Package release has no manual dispatch; only the paired tag-push workflows
  can enter publication, and each authenticates the other provider run.
- Both irreversible DAGs independently verify the same bytes and identity.
- A partial PyPI retry is machine-admitted only for an exact immutable public
  candidate subset and leaves an attempt-bound PASS/FAIL receipt.
- Reruns are fail-closed. Before tagging, artifact deletion or expiry requires
  a new campaign on the then-current `master` candidate. After the tag-run
  cutoff is frozen, post-cutoff evidence cannot replace lost authority;
  recovery requires a newly reviewed version and tag.
- Raw live certification and legacy evidence-pack APIs remain compatible but
  diagnostic.
- Tag creation remains a maintainer action; workflow code blocks publication,
  while repository tag rules are the separate preventive control.

## Related material

- [Exact-SHA release candidate evidence design](../feature-design-release-candidate-evidence-gate.md)
- [Release evidence](../release-evidence.md)
- [ADR 0028](0028-frozen-release-policy-and-publication-boundary.md)
- [ADR 0037](0037-immutable-agent-pr-merge-closure.md)
- [ADR 0048](0048-exact-sha-readiness-evidence.md)
- [PyPI upload API](https://docs.pypi.org/api/upload/)
- [Pinned publisher action upload command](https://github.com/pypa/gh-action-pypi-publish/blob/cef221092ed1bacb1cc03d23a2d87d1d172e277b/twine-upload.sh)
