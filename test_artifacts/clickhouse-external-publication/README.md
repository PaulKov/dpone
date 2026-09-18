# ClickHouse external-publication synthetic evidence

This directory is reserved for generated, machine-readable evidence from the
external-replication publication test profile. Tests must produce the receipt;
maintainers must not hand-edit one to claim a pass.

The offline in-process contract is:

```bash
uv run pytest tests/test_clickhouse_external_replication_runtime.py -q
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

When a producer is implemented, its generated receipt path is:

```text
test_artifacts/clickhouse-external-publication/synthetic-receipt.json
```

The receipt must bind the exact commit and fixture-configuration digests, label
its scope `local_synthetic`, list each scenario independently, and use only
`PASS`, `FAIL`, `SKIP`, or `UNVERIFIED`. It must contain opaque member IDs and
digests only—never endpoints, credentials, source values, SQL text, or local
filesystem paths.

This profile is a mocked integration contract. It is not a live integration or
production certification. Until an explicitly approved external environment is
run, live certification remains `UNVERIFIED`.
