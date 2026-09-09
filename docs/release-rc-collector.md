# Release RC collector

`dpone ops release-rc-collect` prepares release-candidate inputs for
`dpone ops release-rc-finalize`. It converts ordered GitHub CLI PR JSON exports
and evidence references into stable, reviewable files:

- `merge_train.json`
- `release_rc_inputs.json`
- `release_rc_collect.json`
- `release_rc_collect.md`

The collector is credential-free. It does not call GitHub APIs, run `gh`,
merge pull requests, create tags, or evaluate final release policy. It consumes
files that an operator or CI job already exported with GitHub CLI.

## When to use it

Use Release RC collector before the Release RC finalizer whenever a release is
made from a stacked PR train. The collector removes hand-written
`merge_train.json` drift and gives reviewers a stable command line for the
final gate.

Typical flow:

1. Export each PR with `gh pr view`.
2. Pass the PR JSON files to `dpone ops release-rc-collect` in base-to-head
   order.
3. Review `merge_train.json` and `release_rc_inputs.json`.
4. Run the generated `release-rc-finalize` command.

## GitHub CLI export

Export each PR with the fields the collector can normalize:

```bash
mkdir -p test_artifacts/release/v0.10.0/prs

gh pr view 75 \
  --json number,title,baseRefName,headRefName,state,mergeStateStatus,isDraft,url,statusCheckRollup \
  > test_artifacts/release/v0.10.0/prs/pr-75.json

gh pr view 76 \
  --json number,title,baseRefName,headRefName,state,mergeStateStatus,isDraft,url,statusCheckRollup \
  > test_artifacts/release/v0.10.0/prs/pr-76.json
```

`statusCheckRollup` is normalized into the same `checks` contract consumed by
the finalizer.

## CLI

```bash
uv run dpone ops release-rc-collect \
  --release v0.10.0 \
  --previous-release v0.9.0 \
  --package-version 0.10.0 \
  --base-branch codex/route-certify-release-automation \
  --head-branch codex/rc-integration-finalizer \
  --pull-request-json test_artifacts/release/v0.10.0/prs/pr-75.json \
  --pull-request-json test_artifacts/release/v0.10.0/prs/pr-76.json \
  --artifact route_release_finalizer=test_artifacts/release/v0.10.0/route_release_finalizer.json \
  --artifact release_evidence_pack=test_artifacts/release/v0.10.0/release_evidence_pack.json \
  --require-artifact route_release_finalizer \
  --require-artifact release_evidence_pack \
  --output-dir test_artifacts/release/v0.10.0/release-rc-collect \
  --finalizer-output-dir test_artifacts/release/v0.10.0/release-rc-finalizer \
  --format json
```

`--pr-json` is a short alias for `--pull-request-json`.

## Outputs

```text
test_artifacts/release/v0.10.0/release-rc-collect/
  merge_train.json
  release_rc_inputs.json
  release_rc_collect.json
  release_rc_collect.md
```

`merge_train.json` uses schema `dpone.release_rc_merge_train.v1`.

`release_rc_inputs.json` uses schema `dpone.release_rc_inputs.v1` and contains
the generated `release-rc-finalize` command, evidence artifact references, and
required artifact names.

`release_rc_collect.json` uses schema `dpone.release_rc_collect.v1` and records
collection blockers such as `release_rc_collect.pull_requests_missing`,
`merge_train.base_mismatch`, `merge_train.head_mismatch`, and
`merge_train.chain_broken`.

## Runbook

Failure: `release_rc_collect.pull_requests_missing`.

1. Export every stacked PR with `gh pr view`.
2. Pass the files with repeated `--pull-request-json` in base-to-head order.
3. Rerun the collector.

Failure: `merge_train.chain_broken`.

1. Confirm each PR head branch is the next PR base branch.
2. Reorder the `--pull-request-json` arguments if the files are correct.
3. Re-export stale PR JSON if the GitHub branch chain changed.

Failure after collection during `release-rc-finalize`.

1. Keep the collector outputs immutable.
2. Fix the failing PR check, version, or evidence artifact.
3. Re-export PR JSON and rerun collector plus finalizer.

## Related docs

- [Release RC finalizer](release-rc-finalizer.md)
- [Route release finalize](route-release-finalize.md)
- [Release](release.md)
