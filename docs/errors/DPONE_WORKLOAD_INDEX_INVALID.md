# DPONE_WORKLOAD_INDEX_INVALID

**Audience:** CI maintainers and platform engineers.

The workload-index baseline, candidate, or projection does not satisfy the
closed `dpone.workload-index.v1` contract. The structured error includes a
manual fix id: repair current authoring for `workload index`, or restore and
revalidate the protected accepted baseline for `workload impact`.

Typical causes are an unsafe or out-of-root baseline path, duplicate workload
ids, missing closed fields, uppercase or malformed digests, or a workload or
project fingerprint that does not match the declared public material.

Inspect the current project without replacing the last valid baseline:

```bash
candidate="$(mktemp .workload-index.candidate.XXXXXX)"
if ! dpone workload index > "$candidate"; then
  cat "$candidate"
  rm -f "$candidate"
  exit 1
fi

if ! dpone workload impact \
  --baseline workload-index.json \
  --current "$candidate" \
  > workload-impact.json; then
  cat workload-impact.json
  rm -f "$candidate"
  exit 1
fi
if test -z "${DPONE_WORKLOAD_IMPACT_APPROVER:-}" ||
  ! "$DPONE_WORKLOAD_IMPACT_APPROVER" \
    change \
    workload-impact.json \
    "$candidate" \
    workload-index.json; then
  rm -f "$candidate"
  exit 1
fi
rm -f "$candidate"
```

Index exit `0` creates a verified candidate without replacing the baseline.
Exit `1` prints the structured error envelope; exit `2` is invalid CLI usage
on stderr. Impact validates and compares the exact candidate against the
previously accepted baseline before promotion. The platform-owned approver
must implement the protected four-argument contract and invoke
`dpone workload promote` as documented in
[Domain-first discovery and CI](../domain-first-discovery-ci.md), enforce its
change/removal policy, and fail closed. Repair the first reported project error
and rerun the guarded sequence.

If the accepted baseline itself is malformed, preserve it under a distinct
name and use the repository's reviewed baseline-reset procedure. Do not
silently replace it and claim an empty impact.

Do not edit fingerprints. Keep the baseline inside the project root. If a
freshly generated index still fails validation, preserve it under a different
name and escalate with the dpone version and error code.

Never repair individual fingerprint fields by hand. Validate the configured
project root, remove traversal or symlink indirection, and atomically regenerate
a candidate from current editable authority. Promote it only through the
reviewed base/release procedure.

[Domain-first error overview](index.md) ·
[Return to the domain-first tutorial](../getting-started/domain-first-airflow.md)
