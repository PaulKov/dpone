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

Its scope is `local_synthetic`; scenario details remain nested per scenario and
production certification remains `UNVERIFIED`.

The receipt must bind the exact commit and fixture-configuration digests, label
its scope `mocked_in_process`, list each scenario independently, and use only
`PASS`, `FAIL`, `SKIP`, or `UNVERIFIED`. It must contain opaque member IDs and
digests only—never endpoints, credentials, source values, SQL text, or local
filesystem paths.

This profile is a mocked in-process integration contract, not the pinned
two-member Docker synthetic profile and not a live integration or production
certification. Both local synthetic and live certification remain `UNVERIFIED`
until their separately approved profiles run. The receipt is generated after
the reviewed commit and is not checked in with a stale parent identity.
