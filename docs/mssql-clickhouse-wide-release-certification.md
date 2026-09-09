# Certify the wide MSSQL/dbt → ClickHouse release route locally

This operator how-to proves one exact clean commit in disposable Docker
services. It is required when a release changes MSSQL type mapping, BCP Native,
the optional native accelerator, the Python reference transcoder, dbt SQL
Server materialization, or Parquet/S3 pull. Its receipts always say
`production_certification: UNVERIFIED`; local success never certifies a real
deployment.

## 1. Prepare the host

Run from a clean repository checkout. Install all route extras and verify the
two system tools used by SQL Server:

```bash
uv sync \
  --extra mssql --extra clickhouse --extra accel \
  --extra dbt-mssql --extra columnar --extra s3
command -v bcp
odbcinst -q -d | grep -F "ODBC Driver 18 for SQL Server"
```

Supply secrets through your shell or secret manager. The following exports map
the host process to the published ports in
`docker/docker-compose.integration.yml`; they do not print or persist secret
values:

```bash
: "${DPONE_IT_MSSQL_PASSWORD:?set DPONE_IT_MSSQL_PASSWORD}"
: "${DPONE_IT_CH_PASSWORD:?set DPONE_IT_CH_PASSWORD}"
: "${DPONE_IT_MINIO_ACCESS_KEY:?set DPONE_IT_MINIO_ACCESS_KEY}"
: "${DPONE_IT_MINIO_SECRET_KEY:?set DPONE_IT_MINIO_SECRET_KEY}"

export DPONE_IT_MSSQL_HOST=127.0.0.1
export DPONE_IT_MSSQL_PORT="${DPONE_IT_MSSQL_PORT_FORWARD:-51433}"
export DPONE_IT_MSSQL_DATABASE="${DPONE_IT_MSSQL_DATABASE:-dpone_it}"
export DPONE_IT_MSSQL_USER="${DPONE_IT_MSSQL_USER:-sa}"
export DPONE_IT_CH_HOST=127.0.0.1
export DPONE_IT_CH_PORT="${DPONE_IT_CH_PORT_FORWARD:-59000}"
export DPONE_IT_CH_HTTP_PORT="${DPONE_IT_CH_HTTP_PORT_FORWARD:-58123}"
export DPONE_IT_CH_DATABASE="${DPONE_IT_CH_DATABASE:-dpone_it}"
export DPONE_IT_CH_USER="${DPONE_IT_CH_USER:-default}"
export DPONE_IT_S3_ENDPOINT="http://127.0.0.1:${DPONE_IT_MINIO_PORT_FORWARD:-59090}"
export DPONE_IT_S3_REGION="${DPONE_IT_S3_REGION:-us-east-1}"
export DPONE_IT_S3_ACCESS_KEY="${DPONE_IT_MINIO_ACCESS_KEY}"
export DPONE_IT_S3_SECRET_KEY="${DPONE_IT_MINIO_SECRET_KEY}"
```

Start every dependency before the first live command. `minio-init` creates
`dpone-stage`; ClickHouse mounts the matching `dpone_stage` named collection.

```bash
docker compose -f docker/docker-compose.integration.yml up -d --wait mssql clickhouse minio
docker compose -f docker/docker-compose.integration.yml run --rm mssql-init
docker compose -f docker/docker-compose.integration.yml run --rm minio-init
docker compose -f docker/docker-compose.integration.yml ps
```

Use a new directory outside the checkout. Every campaign and authority plan is
create-once; do not reuse or delete a directory to hide a failed attempt.

```bash
export DPONE_RELEASE_ID=0.74.0  # replace with the synchronized candidate
export DPONE_WIDE_CERT_ROOT="$(mktemp -d "/tmp/dpone-wide-v${DPONE_RELEASE_ID}.XXXXXX")"
```

## 2. Create 201 columns and materialize 202 through dbt

The first command creates `wide_release_source.orders` with the canonical 201
source columns and also exercises the required native backend once. Its output
is kept outside Git so source-snapshot verification stays clean.

```bash
uv run python tools/mssql_clickhouse_bcp_native_type_certification.py \
  --release-id "${DPONE_RELEASE_ID}" \
  --source-schema wide_release_source --source-table orders \
  --rows 10000 --column-count 201 --typed-hash-rows 10000 \
  --binary-format native --acceleration-mode required \
  --target-table wide_source_native_required \
  --target-rows-per-partition 2500 --export-workers 1 --load-workers 1 \
  --output-dir "${DPONE_WIDE_CERT_ROOT}/source"
```

The checked-in dbt project must preserve all 201 fields and add nullable
`decimal(38,8)` `dbt_calculated_amount` as field 202. The producer verifies the
canonical source inventory, every passthrough type/nullability, all-row hashes,
the calculation, manifest, run results, connection identity, and exact commit.

```bash
uv run python tools/mssql_dbt_wide_materialization.py \
  --release-id "${DPONE_RELEASE_ID}" \
  --source-schema wide_release_source --source-table orders \
  --target-schema wide_release_dbt --target-table wide_dbt_result \
  --rows 10000 --source-column-count 201 \
  --target-path "${DPONE_WIDE_CERT_ROOT}/dbt-target" \
  --output-dir "${DPONE_WIDE_CERT_ROOT}/dbt-evidence"
```

Freeze an independent, secret-free authority plan before the downstream loads.
It observes the 202-column MSSQL schema and binds MSSQL, ClickHouse, and S3
coordinates plus policy-derived target schema digests. An acknowledgement-loss
retry must reuse this exact file; the producer refuses to overwrite it.

```bash
uv run python tools/mssql_clickhouse_wide_release_authority.py \
  --release-id "${DPONE_RELEASE_ID}" \
  --source-schema wide_release_dbt --source-table wide_dbt_result \
  --output "${DPONE_WIDE_CERT_ROOT}/authority.json"
```

## 3. Prove native, Python, and Parquet/S3

Both BCP commands reuse the exact dbt relation and hash all 10,000 rows. The
`required` run must observe `native_accelerated`; the `off` run must observe
`python_reference` as the deterministic reference oracle. Both compare the
actual ClickHouse type/nullability schema with the independently planned schema
before reporting PASS.

```bash
for MODE in required off; do
  uv run python tools/mssql_clickhouse_bcp_native_type_certification.py \
    --release-id "${DPONE_RELEASE_ID}" --skip-source-prepare \
    --rows 10000 --column-count 202 --typed-hash-rows 10000 \
    --source-schema wide_release_dbt --source-table wide_dbt_result \
    --target-table "wide_dbt_${MODE}" \
    --binary-format native --acceleration-mode "${MODE}" \
    --upstream-evidence "${DPONE_WIDE_CERT_ROOT}/dbt-evidence/mssql_dbt_wide_materialization.json" \
    --output-dir "${DPONE_WIDE_CERT_ROOT}/${MODE}"
done
```

Run the separate temporal target matrix to prove real `auto` fallback when the
accelerator is unavailable and compare the decoded ClickHouse rows
value-for-value with the required native backend. It covers `date -> Date|Date32`,
`datetime2 -> DateTime|DateTime64`, legacy `datetime`, `smalldatetime`,
`datetimeoffset`, `time`, target-range boundaries, and nulls. The strict JUnit
gate rejects a skipped live test.

```bash
DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1 uv run pytest \
  tests/integration/mssql/test_mssql_clickhouse_native_temporal_matrix_live.py \
  -m integration_live -q \
  --junitxml "${DPONE_WIDE_CERT_ROOT}/native-temporal-matrix.xml"
uv run python tools/ci/assert_junit_executed.py \
  --junit "${DPONE_WIDE_CERT_ROOT}/native-temporal-matrix.xml" \
  --min-passed 1 --max-skipped 0 \
  --profile mssql_clickhouse_native_temporal_matrix \
  --commit-sha "$(git rev-parse HEAD)" \
  --evidence-json "${DPONE_WIDE_CERT_ROOT}/native-temporal-matrix.json"
```

The Parquet command uses the same upstream evidence, writes chunks to the
run-specific MinIO prefix, pulls them through ClickHouse `s3()`, verifies the
same all-row typed hash and exact sink schema, and proves prefix cleanup.

```bash
uv run python tools/mssql_clickhouse_parquet_s3_type_certification.py \
  --release-id "${DPONE_RELEASE_ID}" \
  --rows 10000 --column-count 202 --typed-hash-rows 10000 \
  --source-schema wide_release_dbt --source-table wide_dbt_result \
  --target-table wide_dbt_parquet --run-id "wide-${DPONE_RELEASE_ID}" \
  --upstream-evidence "${DPONE_WIDE_CERT_ROOT}/dbt-evidence/mssql_dbt_wide_materialization.json" \
  --output-dir "${DPONE_WIDE_CERT_ROOT}/parquet"
```

Each producer exits `0` only for a self-verified PASS receipt. Exit `1` means
the data may have transferred but exact evidence is not acceptable. Failure
JSON contains a stable phase code, not raw driver text or credentials. Fix the
cause and start a new `mktemp` campaign; never overwrite the old evidence.

## 4. Build and verify the immutable campaign

The campaign accepts exactly one required-native receipt, one Python-reference
receipt, and one Parquet receipt. It deep-verifies copied dbt/result bytes,
source generation, 10,000/201/202 shape, endpoint authorities, target schemas,
cleanup, and the fixed slot order before returning `0`.

```bash
uv run python tools/mssql_clickhouse_wide_release_campaign.py \
  --release-id "${DPONE_RELEASE_ID}" \
  --source-relation wide_release_dbt.wide_dbt_result \
  --dbt-evidence "${DPONE_WIDE_CERT_ROOT}/dbt-evidence/mssql_dbt_wide_materialization.json" \
  --authority "${DPONE_WIDE_CERT_ROOT}/authority.json" \
  --native-required-receipt "${DPONE_WIDE_CERT_ROOT}/required/local_route_certification_receipt.json" \
  --native-required-target dpone_it.wide_dbt_required \
  --native-off-receipt "${DPONE_WIDE_CERT_ROOT}/off/local_route_certification_receipt.json" \
  --native-off-target dpone_it.wide_dbt_off \
  --parquet-receipt "${DPONE_WIDE_CERT_ROOT}/parquet/local_route_certification_receipt.json" \
  --parquet-target dpone_it.wide_dbt_parquet \
  --output-dir "${DPONE_WIDE_CERT_ROOT}/campaign"
```

Verify an existing campaign after an output/acknowledgement loss without
rewriting any bytes:

```bash
uv run python tools/mssql_clickhouse_wide_release_campaign.py \
  --release-id "${DPONE_RELEASE_ID}" \
  --source-relation wide_release_dbt.wide_dbt_result \
  --dbt-evidence "${DPONE_WIDE_CERT_ROOT}/dbt-evidence/mssql_dbt_wide_materialization.json" \
  --authority "${DPONE_WIDE_CERT_ROOT}/authority.json" \
  --native-required-target dpone_it.wide_dbt_required \
  --native-off-target dpone_it.wide_dbt_off \
  --parquet-target dpone_it.wide_dbt_parquet \
  --verify-existing "${DPONE_WIDE_CERT_ROOT}/campaign/mssql_clickhouse_wide_release_campaign.json"
```

Machine contracts:

- [wide dbt materialization v1](schemas/dpone.dbt-sqlserver-wide-materialization.v1.schema.json)
- [wide release authority v1](schemas/dpone.mssql-clickhouse-wide-release-authority.v1.schema.json)
- [local route receipt v1](schemas/dpone.local-route-certification-receipt.v1.schema.json)
- [wide release campaign v1](schemas/dpone.mssql-clickhouse-wide-release-campaign.v1.schema.json)

Attach the campaign as supplemental local evidence, not as a required
production route slot. Add this argument to the complete R1–R9 command in
[Release evidence](release-evidence.md):

```bash
--artifact local_mssql_clickhouse_wide_campaign="${DPONE_WIDE_CERT_ROOT}/campaign/mssql_clickhouse_wide_release_campaign.json"
--artifact local_mssql_clickhouse_temporal_matrix="${DPONE_WIDE_CERT_ROOT}/native-temporal-matrix.json"
```

The normal R1–R9 artifacts remain required separately. Do not tag or publish
only because this local campaign passed.
