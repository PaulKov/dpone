# ClickHouse external-publication mocked evidence

This directory is reserved for generated, machine-readable evidence from the
external-replication publication test profile. Tests must produce the receipt;
maintainers must not hand-edit one to claim a pass.

Generate the offline in-process mocked receipt from a clean checkout with:

```bash
uv run python tools/clickhouse_external_publication_evidence.py
```

The red-first API contract is
`ClickHouseExternalReplicationRuntime(service=service).run(request)`, with
`ExternalArtifactReceipt`, `ExternalPublicationRequest`, and
`ExternalPublicationError` supplied by
`dpone.contracts.clickhouse_external_replication`. The injected service owns
only these effects: complete inventory, authority read/CAS, candidate
observation, one-shot member stage, exact owned-candidate drop, one-shot
publication, publication observation, one-shot cleanup, and cleanup
observation. The test fake gives the exact method signatures and return shapes.

The test uses two opaque synthetic members and synthetic rows. It verifies
all-member staging and publication, duplicate-free lost-ack recovery, partial
stage replay, divergent-generation failure, completed-operation idempotency,
cleanup recovery, and redacted evidence.

The generated local receipt path is:

```text
test_artifacts/clickhouse-external-publication/mocked-receipt.json
```

The opt-in two-member Docker Desktop profile writes a separate producer-owned
receipt to:

```text
test_artifacts/clickhouse-external-publication/docker-receipt.json
```

Its scope is `local_synthetic`; scenario details remain nested per scenario,
each completed Docker scenario uses `PASS`, and production certification
remains `UNVERIFIED`. The matrix exercises fresh cleanup, lost member-load,
publication and cleanup responses, plus recovery from `STAGING` through a
freshly composed service in the same process.

The exact-commit performance producer writes:

```text
test_artifacts/clickhouse-external-publication/benchmark-receipt.json
```

Schema `dpone.clickhouse.external-publication-benchmark.v1` records the tracked
budget digest, source commit/tree, fixture digest, environment, measured trial
durations, exact member counts, content-digest equality, publication phase,
cleanup proof, observation ID, producer command, test node, and start/finish
times. Its verdict is `PASS` only when both the canonical-digest
and two-member fan-out sections pass their tracked budgets and correctness
checks. The receipt deliberately omits endpoints, database names, SQL,
credentials, and row values. It is generated after the reviewed commit and
stored byte-identically with its SHA-256 in release evidence; it is not added
back to the commit whose identity it records. Creation is atomic and exclusive:
the producer refuses to replace an existing receipt, and fixture setup does not
delete it. A second observation therefore requires a fresh checkout or a new
artifact identity after the first receipt has been preserved; never delete a
failed or slow observation to obtain a passing sample.

The receipt must bind the exact commit and fixture-configuration digests, label
its scope `mocked_in_process`, list each scenario independently, and use only
`PASS`, `FAIL`, `SKIP`, or `UNVERIFIED`. It must contain opaque member IDs and
digests only—never endpoints, credentials, source values, SQL text, or local
filesystem paths.

The offline profile is a mocked in-process integration contract. The pinned
two-member Docker profile is local synthetic evidence, not production
certification. Production certification remains `UNVERIFIED` until an
explicitly approved external environment runs. Receipts are generated after
the reviewed commit and are not checked in with a stale parent identity.
