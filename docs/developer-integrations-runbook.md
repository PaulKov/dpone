# Developer integrations runbook

This runbook describes how to add or extend a source, sink, or provider integration.

## Integration checklist

1. Define the public manifest contract.
2. Add or update credentials parsing with secret redaction.
3. Implement connector capabilities behind lazy imports.
4. Add source or sink strategy classes through runtime factories.
5. Support staging-first writes for database targets.
6. Add schema introspection and schema evolution hooks where applicable.
7. Add state handling for incremental, XMin, CDC, or Kafka offset workflows.
8. Add unit, contract, mock integration, and optional live tests.
9. Add connector docs, source/sink docs, and runbooks.
10. Add certification metadata and artifact generation.

## Source implementation guidance

A source should produce one of the canonical artifacts:

- In-memory rows for small fixtures.
- Streaming rows for API and Kafka batches.
- File exports for native database extraction.
- Partitioned file exports for parallel or large-volume loads.
- Object storage staging manifests for cloud-native load jobs and replay evidence.

The source must also emit an `ExtractResult.schema` when schema evolution or typed sinks need it.

## Sink implementation guidance

A database sink must use staging or shadow tables for heavy writes. Do not perform row-by-row writes directly against the final target.

Required behavior:

- Create staging objects.
- Load into staging through the native fast path.
- Apply set-based finalization.
- Apply reconciliation deletes only through staging-first plans.
- Clean up or retain staging according to manifest policy.

When object storage is used, sinks should consume
`ObjectStorageStagingManifest` rather than provider-specific dictionaries. This
keeps S3, GCS, Azure Blob, and local CI staging on one checksum/evidence
contract.

Kafka sinks are append/event-log targets and do not mutate the topic. Strategies map to event semantics.

## State implementation guidance

State backends should support typed state records rather than raw dictionaries. Current state families include:

- Run state.
- Postgres XMin state.
- CDC offsets.
- Kafka offsets.

State must advance only after the sink load succeeds.

## Schema evolution guidance

Use the shared schema evolution planner. Connector-specific code should provide dialect introspection and DDL rendering rather than reimplementing comparison logic.

Breaking changes must not be applied automatically. Incompatible type changes may use `on_type_change: new_column` when explicitly configured.

## Testing guidance

Add tests in layers:

- Unit tests for SQL builders, codecs, credential parsing, and redaction.
- Contract tests for manifest parsing and factory wiring.
- Mock integration tests for source/sink behavior without credentials.
- Local service integration tests for real database or Kafka behavior.
- Vendor live tests only for manual or scheduled certification.

## Documentation guidance

Every integration should have:

- Connector page.
- Source/sink guide entries for supported targets.
- Type mapping notes.
- Load strategy compatibility notes.
- Troubleshooting section.
- Example manifests.
- Object storage staging notes when the integration can use S3/GCS/Azure fast paths.
