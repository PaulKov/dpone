# Install and maintain the MSSQL SqlClient writer

## Install

Install matching core and companion versions in the Linux x86-64 execution
image. The companion requires `Microsoft.NETCore.App 10.x`.

```bash
pip install "dpone[clickhouse,mssql-sqlclient]==0.88.0"
dpone runtime mssql-sqlclient doctor --format json
```

The doctor is read-only. It verifies the package tree, descriptor, .NET runtime,
writer identity, and an executable Python/C# session-identity parity vector. It
does not open a database connection. Exit `0` means `ready: true`; exit `2`
returns a closed readiness document with stable `blocker_codes`.

```json
{"backend":"mssql_sqlclient","blocker_codes":[],"ready":true,"runtime_major":10,"schema_version":"dpone.mssql-sqlclient.doctor.v1"}
```

## Upgrade

1. Reconcile every retained invocation with `dpone ops mssql-native-recovery`.
2. Build a new immutable execution image with core and companion from the same
   minor line. The certified release set uses the same exact version for both
   distributions so operators can pin one immutable version.
3. Run the doctor in that image.
4. Run the synthetic campaign for the exact candidate commit.
5. Start new invocations on the new image. Do not resume an old invocation with
   a different writer or layout identity.

Candidate certification v1 receipts are not migration inputs. Version 0.88.0
requires v2 image, runner, cell, and campaign receipts regenerated from the
exact clean commit.

## Remove or roll back

Reconcile all `UNKNOWN`, `PREPARED`, and custody-retaining invocations first.
Change new manifests back to `import_backend: bcp`, build a matching image, and
only then remove the optional package:

```bash
pip uninstall dpone-mssql-sqlclient
```

Keep journals, receipts, recovery plans, and the generic transaction catalog
until retention policy proves that no invocation references them. Uninstalling
the companion does not authorize deleting a stage or replaying an uncertain
grant.

## Readiness blockers

| Code | Operator action |
|---|---|
| `mssql_sqlclient.optional_package_required` | Install the version-matched extra. |
| `mssql_sqlclient.unsupported_platform` | Move execution to Linux x86-64. |
| `mssql_sqlclient.runtime_unavailable` | Install `Microsoft.NETCore.App 10.x`. |
| `mssql_sqlclient.invalid_descriptor` | Reinstall the exact companion package. |
| `mssql_sqlclient.artifact_unavailable` | Rebuild or reinstall the wheel. |
| `mssql_sqlclient.artifact_identity_mismatch` | Replace the changed package tree; never bypass the digest. |
| `mssql_sqlclient.application_identity_unverified` | Replace the image; Python and C# session identity do not agree. |
| `mssql_sqlclient.companion_unavailable` | Inspect the image and reinstall the exact package. |
