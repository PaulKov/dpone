# Release RC finalizer

`dpone ops release-rc-finalize` is the release-level integration gate before a
minor or major tag. It validates the stacked PR train, version context, and
release evidence that came from route, CDC, docs, package, and quality gates.

This command is credential-free. It does not call the GitHub API, merge pull
requests, create tags, start Docker, or run tests. CI or an operator exports a
`merge_train.json` file, passes already-produced evidence artifacts, and the
finalizer writes one stable go/no-go receipt.

## When to use it

Use Release RC finalizer after route-level release evidence exists and before
creating `v0.10.0` or another release tag.

For the current route-heavy release, the expected order is:

1. Run route evidence gates such as `route-rc-orchestrator`,
   `route-release-finalize`, and `release-evidence-pack`.
2. Run [Release RC collector](release-rc-collector.md) to export the stacked
   PR train into `merge_train.json`.
3. Run `dpone ops release-rc-finalize` in `pre_merge` mode.
4. Merge the train in the recorded order.
5. Run `dpone ops release-rc-finalize --mode post_merge`.
6. Tag only while `release_rc_finalizer.json` is `rc_ready`.

## Merge train JSON

`merge_train.json` is ordered from base to head:

```json
{
  "schema_version": "dpone.release_rc_merge_train.v1",
  "base_branch": "codex/route-certify-release-automation",
  "head_branch": "codex/route-conformance-lab",
  "pull_requests": [
    {
      "number": 75,
      "title": "Add generic route bootstrap doctor",
      "base_ref": "codex/route-certify-release-automation",
      "head_ref": "codex/route-bootstrap-doctor",
      "state": "OPEN",
      "merge_state": "CLEAN",
      "is_draft": false,
      "checks": [
        {
          "name": "Build GitHub Pages documentation",
          "status": "COMPLETED",
          "conclusion": "SUCCESS",
          "details_url": "https://github.com/PaulKov/dpone/actions/runs/..."
        }
      ],
      "url": "https://github.com/PaulKov/dpone/pull/75"
    }
  ]
}
```

The finalizer also accepts GitHub CLI JSON field variants such as
`baseRefName`, `headRefName`, `mergeStateStatus`, `isDraft`, `detailsUrl`, and
`statusCheckRollup`. Prefer `dpone ops release-rc-collect` when generating this
file from GitHub CLI exports.

## CLI

```bash
uv run dpone ops release-rc-finalize \
  --release v0.10.0 \
  --previous-release v0.9.0 \
  --package-version 0.10.0 \
  --merge-train-json test_artifacts/release/v0.10.0/merge_train.json \
  --artifact route_release_finalizer=test_artifacts/route_release_finalize/route_release_finalizer.json \
  --artifact release_evidence_pack=test_artifacts/release/v0.10.0/release_evidence_pack.json \
  --artifact pre_release_checklist=test_artifacts/release/v0.10.0/pre_release_checklist.json \
  --require-artifact route_release_finalizer \
  --require-artifact release_evidence_pack \
  --require-artifact pre_release_checklist \
  --output-dir test_artifacts/release/v0.10.0/release-rc-finalizer \
  --format json
```

Use `--mode post_merge` after merging the train. In post-merge mode every PR in
`merge_train.json` must have state `MERGED`.

## Output

```text
test_artifacts/release/v0.10.0/release-rc-finalizer/
  release_rc_finalizer.json
  release_rc_finalizer.md
```

`release_rc_finalizer.json` has schema `dpone.release_rc_finalizer.v1`.
It contains:

- `release`, `previous_release`, `package_version`, and `mode`;
- the normalized merge train;
- evidence artifact status, checksums, and blockers;
- policy checks for version increment, package version, PR chain, PR state,
  check rollup, and release artifacts;
- operator next actions.

## Runbook

Failure: `release.version_not_incremented`.

1. Compare with the latest published release.
2. Pick a higher semantic version.
3. Rerun the finalizer.

Failure: `release.package_version_mismatch`.

1. Update `pyproject.toml` to the release version, for example `0.10.0`.
2. Regenerate package and docs evidence.
3. Rerun the finalizer.

Failure: `merge_train.chain_broken`.

1. Regenerate `merge_train.json` in base-to-head order.
2. Confirm each PR head is the next PR base.
3. Rerun the finalizer.

Failure: `pull_request.<number>.not_clean` or
`pull_request.<number>.check_failed:<name>`.

1. Open the PR and the named check.
2. Fix or rerun the failing check.
3. Export a fresh `merge_train.json`.

Failure: `route_release_finalizer.missing` or `release_evidence_pack.missing`.

1. Generate the required upstream evidence.
2. Pass it with `--artifact name=/path/to/file`.
3. Keep generated evidence immutable for the release candidate.

## Related docs

- [Route release finalize](route-release-finalize.md)
- [Release RC collector](release-rc-collector.md)
- [Route release candidate orchestrator](route-rc-orchestrator.md)
- [Release evidence](release-evidence.md)
- [Release](release.md)
