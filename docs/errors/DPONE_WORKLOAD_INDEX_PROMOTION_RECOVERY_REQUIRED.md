# DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED

**Audience:** CI maintainers and platform engineers.

The baseline reached or approached its atomic replacement point, but durable
transaction cleanup could not be proven.

Stop concurrent promotions immediately. Preserve:

- the structured failed promotion result;
- the approved candidate and impact evidence;
- the accepted baseline path;
- every project-relative `recovery_artifacts[]` entry;
- the CI job, source revision, and approver audit identifiers.

Do not delete a hidden recovery leaf, rewrite the baseline, rerun stale guards,
or claim that publication succeeded.

## Classify the durable state

Run from the same project root with the original confined paths:

```bash
baseline=".dpone-ci/workload-index/accepted.json"
candidate=".dpone-ci/workload-index/candidate.json"
approval_evidence=".dpone-ci/workload-index/impact.json"
recovery_impact=".dpone-ci/workload-index/recovery-impact.json"

sha256_file() {
  python -c \
    'import hashlib, pathlib, sys; print("sha256:" + hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' \
    "$1"
}
json_field() {
  python -c \
    'import json, pathlib, sys; value=json.loads(pathlib.Path(sys.argv[1]).read_text())[sys.argv[2]]; print(value)' \
    "$1" "$2"
}

candidate_sha256="$(sha256_file "$candidate")"
evidence_schema="$(json_field "$approval_evidence" schema)"
if test "$evidence_schema" = "dpone.workload-change-impact.v1"; then
  approved_before_sha256="$(json_field "$approval_evidence" baseline_content_sha256)"
  approved_candidate_sha256="$(json_field "$approval_evidence" current_content_sha256)"
else
  approved_before_sha256="absent"
  approved_candidate_sha256="$candidate_sha256"
fi

if test "$candidate_sha256" != "$approved_candidate_sha256"; then
  echo "Candidate changed after approval; quarantine the job." >&2
  exit 1
fi

if ! test -e "$baseline" && test "$approved_before_sha256" = "absent"; then
  recovery_state="bootstrap_not_committed"
elif ! test -f "$baseline"; then
  echo "Expected baseline is missing or is not a regular file; quarantine the project." >&2
  exit 1
elif test "$(sha256_file "$baseline")" = "$approved_candidate_sha256"; then
  recovery_state="candidate_committed"
elif test "$(sha256_file "$baseline")" = "$approved_before_sha256"; then
  recovery_state="baseline_preserved"
else
  echo "Baseline matches neither approved identity; quarantine the project." >&2
  exit 1
fi

if test "$recovery_state" != "bootstrap_not_committed"; then
  dpone workload impact \
    --baseline "$baseline" \
    --current "$candidate" \
    > "$recovery_impact"
fi
printf 'Recovery state: %s\n' "$recovery_state"
```

For `candidate_committed`, the mutation reached its linearization point. For
`baseline_preserved`, the previous accepted bytes remain authoritative.
Submit `recovery-impact.json` to the protected approval adapter and perform a
fresh change promotion using identities from that new impact. A byte-identical
candidate produces a no-op CAS receipt; it does not rewrite the file. For
`bootstrap_not_committed`, repeat protected bootstrap approval against the
still-absent baseline. Any new checkout or different identity requires normal
review.

After a successful fresh receipt and durable protected publication:

1. archive every named recovery artifact by content digest with the failed and
   successful receipts;
2. verify the protected current baseline digest equals `promoted_sha256`;
3. let a platform operator remove only the exact archived confined recovery
   leaves while promotions remain locked;
4. rerun `dpone workload impact --baseline "$baseline" --current "$baseline"`
   and require empty `added`, `modified`, and `removed`;
5. reopen promotions and retain the audit bundle according to policy.

If an artifact is a symlink, escapes the project root, has an unexpected name,
or cannot be archived and removed safely, keep the project quarantined and
escalate. There is no force flag and no shell overwrite recovery path.

[Domain-first discovery and CI](../domain-first-discovery-ci.md) ·
[Domain-first operations](../domain-first-operations.md) ·
[Domain-first error overview](index.md)
