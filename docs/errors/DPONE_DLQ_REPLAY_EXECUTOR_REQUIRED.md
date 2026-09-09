# DPONE_DLQ_REPLAY_EXECUTOR_REQUIRED

## Meaning

The generic `dpone ops quarantine-replay` command can inspect DLQ metadata and
build a replay preview, but it has no source-record resolver or target sink. It
therefore cannot truthfully apply records.

## Safe fix

1. Fix the source mapping, schema contract, or transform that caused the stable
   DLQ reason code.
2. Configure a route-specific `DlqRecordResolver` that resolves `record_ref`
   without reading row values from the DLQ artifact.
3. Configure a `DlqReplaySink` that honors the supplied idempotency key.
4. Execute the immutable replay plan through that integration.
5. Verify that every successful sink call has a matching acknowledgement.

Do not pass credentials, source rows, or secret URLs to the generic command.
The deprecated `--yes` flag intentionally returns exit code `4` until a real
executor is used.

## Related

- [Runtime data contracts and DLQ](../data-contract-runtime.md)
- [`dpone ops` DLQ runbook](../ops-cli.md#dlq-inspection-and-replay-planning)
