# dbt runtime correctness fixture

This project is the source for an exact `dbt-core 1.12.3` /
`dbt-sqlserver 1.11.1` runtime artifact. It contains an ephemeral parent, one
contracted model, one data test, and one unit test.

Set the `DPONE_DBT_FIXTURE_MSSQL_*` variables through an approved local secret
mechanism, then generate artifacts against that local MSSQL environment:

```bash
uv run python tools/dbt_self_service/generate_runtime_fixture.py \
  --profiles-dir tests/fixtures/dbt-runtime-correctness-v1/profiles \
  --output-dir tests/fixtures/dbt-runtime-correctness-v1/generated \
  --environment-id local-mssql-certification \
  --database-version 2022-CU-current
```

The generator never creates credentials or starts infrastructure. The supplied
profile must use environment-owned secrets. Provenance binds the exact source
tree digest, source commit, generator command, toolchain, approved environment
identifier, reported database version, generation timestamp, and artifact
checksums. Until `generated/provenance.json` exists from an approved run, the
real dbt-build fixture is `UNVERIFIED`; unit and contract tests must not
reinterpret source-only coverage as a live pass.
