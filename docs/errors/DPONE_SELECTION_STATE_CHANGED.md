# DPONE_SELECTION_STATE_CHANGED

**Audience:** pipeline authors and CI maintainers.

A catalog, authoring source, named-selector file, or state baseline changed
between planning and materialization. Retry the command against a stable
working tree so one report never mixes two project states.

Stop the concurrent editor or CI job, inspect the reported project-relative
path, and rerun the original command. For workload-index baselines, use the
guarded temporary-file flow:

```bash
mkdir -p .dpone-ci/candidate
tmp="$(mktemp .dpone-ci/candidate/workload-index.XXXXXX)"
dpone workload index > "$tmp" && mv "$tmp" .dpone-ci/candidate/workload-index.json
```

The command succeeds only when every consumed source has the same digest at
commit time. Never edit generated fingerprints or cache pointers to bypass the
check. Keep the accepted base/release baseline separate from this candidate.

[Domain-first error overview](index.md) ·
[Domain-first operations](../domain-first-operations.md)
