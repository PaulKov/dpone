# Airflow self-service evidence

`dpone certify self-service` turns the two external acceptance gates from the
Industrial Airflow roadmap into one bounded JSON and Markdown report:

1. at least five real first-time users complete the beginner journey;
2. at least two independent production deployments prove the same dpone
   contracts with different deployment and signer identities.

This is a release-auditor and platform-engineer command. It is not a beginner
command. The normal offline user journey is the five-command checked, previewed,
and hermetically tested path in
[First Airflow DAG](getting-started/first-airflow-dag.md).

The `dpone.self-service-usability-study.v1` protocol below remains frozen as its
original four-command preview plus fifth platform-gated sample. It is not
reinterpreted to certify the newer five-offline-command journey plus a sixth
sample. That combined study requires a future versioned evidence contract.

## Start with an honest readiness report

Run the collector without external evidence to see the outstanding gates:

```bash
dpone certify self-service \
  --commit-sha "$(git rev-parse HEAD)" \
  --output-dir test_artifacts/airflow-self-service-certification \
  --format json
```

The report is valid with `overall_status: UNVERIFIED` when evidence is missing.
The process exits non-zero in that case so shell/`&&` release gates cannot treat
incomplete certification as success. Use `--allow-unverified` only for
publish-only workflows that intentionally accept an incomplete report.

Production promotion additionally requires Sigstore re-verification artifacts
beside each reference deployment proof (`route-attestation.sigstore.json` and
`route-attestation-policy.json`). Consistency-only verification receipts are not
enough for `production-certified`.

Those files certify a source-to-target route. Production scheduler and runtime
bytes additionally require a deployment-scoped artifact attestation. The two
controls are intentionally independent; see
[Airflow artifact trust and attestation](airflow-artifact-trust.md).

The collector is local-only for ordinary I/O: it does not contact Airflow,
Kubernetes, Vault, databases, or participants. Cryptographic re-verification of
explicitly supplied Sigstore artifacts may invoke the local `cosign` verifier.

## Five-user study protocol

Use the frozen v1 four-command preview path plus the platform-gated sample for
every end-to-end participant:

```bash
dpone init project --airflow
dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental --airflow
dpone check pipelines/orders_daily
dpone airflow preview orders_daily
dpone run pipelines/orders_daily --sample 1000 --target temporary
```

### Participant eligibility

- participant has not used dpone before;
- participant gives consent under the research process;
- facilitator does not perform a command for the participant;
- machine, installation, and approved test connections may be prepared before
  the timed session;
- the sample runtime, signed authorization overlay, and certified copy executor
  are prepared so the fifth command can return exit code `0`;
- a maintainer, repeat user, CI job, recording playback, or synthetic fixture
  does not count as a first-time participant.

### What to record

Create one `dpone.self-service-usability-study.v1` JSON file. Use pseudonymous
SHA-256 references, not names or email addresses:

```json
{
  "schema": "dpone.self-service-usability-study.v1",
  "protocol": "airflow_first_dag_and_safe_sample_v1",
  "target_commit": "0123456789abcdef0123456789abcdef01234567",
  "facilitator_ref": "research-team-1",
  "started_at": "2026-07-16T09:00:00Z",
  "completed_at": "2026-07-16T12:00:00Z",
  "sessions": [
    {
      "session_id": "session-01",
      "participant_ref": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "first_time_dpone_user": true,
      "consent_recorded": true,
      "started_at": "2026-07-16T09:00:00Z",
      "dag_preview_at": "2026-07-16T09:07:30Z",
      "safe_sample_at": "2026-07-16T09:12:10Z",
      "finished_at": "2026-07-16T09:12:10Z",
      "outcome": "passed",
      "commands_used": 5,
      "assistance_events": 0,
      "authored_airflow_python": false,
      "transcript_sha256": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "blocker_codes": []
    }
  ]
}
```

Store raw recordings or transcripts only in research-controlled storage. dpone
does not read them. The transcript digest proves which external record was used
without copying its contents into repository evidence.

### Acceptance

A session succeeds only when the participant:

- reaches DAG preview in at most 600 seconds;
- reaches the safe sample in at most 900 seconds;
- receives exit code `0` from the safe-sample command; an exit `3` local handoff
  is recorded as a blocker and does not count as a completed sample;
- uses exactly the five commands in the frozen v1 protocol;
- receives zero assistance events;
- authors no Airflow Python;
- records `outcome=passed`.

The study passes with at least five eligible sessions, at least 80% complete
success, at least 80% meeting each time target, and 100% authoring no Airflow
Python. Fewer than five valid sessions are `UNVERIFIED`; a complete study that
misses a target is `FAIL`.

Validate the input before publication:

```bash
dpone gitops schema validate \
  --kind dpone.self-service-usability-study.v1 \
  --payload evidence/usability-study.json
```

## Production reference checklist

Prepare one non-recursive directory per approved production environment:

```text
reference-prod-a/
  release-set.json
  deployment-set.json
  route_certification_bundle.json
  route-attestation.json
  route-attestation-verification.json
  airflow-evidence-bundle.json
```

The files must be original outputs from existing dpone producers. Never edit a
digest, identity, timestamp, signer, or status to make this collector pass.

Each directory must prove:

- a content-valid release for the exact CLI commit;
- a content-valid runnable production deployment;
- `init_fetch`, checksum verification, production attestation policy, workload
  identity, and pinned runtime image;
- a fresh production-certified route attestation;
- a release-policy Airflow evidence bundle with all required artifacts passed;
- complete run identity, correlation, pod UID, namespace, service account,
  image digest, dpone run ID, and runtime evidence digest;
- identical release and deployment identities across all files.

Two references pass only when both are valid and their deployment IDs and
verified signer identities are distinct. Different directories containing the
same deployment or signer are not independent evidence.

## Publish the combined report

```bash
dpone certify self-service \
  --commit-sha "$(git rev-parse HEAD)" \
  --usability-study evidence/usability-study.json \
  --reference-deployment evidence/reference-prod-a \
  --reference-deployment evidence/reference-prod-b \
  --output-dir test_artifacts/airflow-self-service-certification \
  --max-age-hours 168 \
  --format md
```

The output directory contains:

```text
self-service-certification.json
self-service-certification.md
```

The report includes only aggregates, content digests, release/deployment IDs,
signer identities, correlation IDs, and stable blocker codes. It excludes
participant identity, raw transcript, absolute input paths, Vault paths,
registry bodies, credentials, source rows, and signed URLs.

## Status and exit codes

| Situation | Report | Exit |
|---|---|---:|
| No external evidence | `UNVERIFIED` | `1` |
| Four valid user sessions | usability `UNVERIFIED` | `1` |
| `UNVERIFIED` with `--allow-unverified` | `UNVERIFIED` | `0` |
| Five-user study misses target | `FAIL` | `1` |
| Malformed supplied deployment proof | `FAIL` | `1` |
| Both external axes pass | `PASS` | `0` |
| Unsafe path, bad SHA, or immutable output conflict | structured error | `2` |

`FAIL` means supplied evidence contradicts or misses the contract.
`UNVERIFIED` means required external evidence is absent or insufficient.
`--allow-unverified` changes only the process exit code for an intentionally
publish-only workflow; it never changes the report status or makes missing
evidence pass.

## Troubleshooting

### Output already exists

Exact republication is a no-op. Different bytes at the same destination are
blocked. Use a new commit-bound directory; do not delete or overwrite release
evidence during an audit.

### Reference proof fails

Read `reference_deployments.proofs[].blockers`. Repair the upstream release,
deployment, route attestation, or Airflow evidence producer, then collect a new
proof directory. This command intentionally has no autofix for production
evidence.

### Study input fails

Validate it against the public schema. Unknown fields are rejected to prevent
names, emails, notes, or secrets from entering the artifact. Confirm timestamp
ordering, canonical SHA-256 references, exact target commit, consent, and
first-time status.

## Related contracts

- [Frozen Airflow architecture](airflow-self-service-architecture.md)
- [Route certification matrix](route-certification-matrix.md)
- [Production route attestation](airflow-route-attestation.md)
- [Airflow runtime observability](observability.md)
- [Release protocol](agent-release-protocol.md)
- [GitOps schema catalog](reference/gitops-schema-catalog.md)
