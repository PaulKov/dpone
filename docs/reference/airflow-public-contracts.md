# Airflow self-service public contracts

This page is the readable projection of the reviewed v1 baseline in
[`airflow-self-service-public-contracts-v1.yaml`](../airflow-self-service-public-contracts-v1.yaml).
The reference generator never rewrites the reviewed baseline from current code;
an implementation cannot approve its own breaking change.

Check the implementation against the promise with:

```bash
dpone docs check-airflow-public-contracts
```

<!-- DPONE_AIRFLOW_PUBLIC_CONTRACTS_START -->
_This section is generated from the reviewed Airflow v1 public-contract baseline._

## Beginner command contract

### Successful flat offline preview path

- `dpone init project --airflow`
- `dpone init pipeline <name> --recipe mssql-to-clickhouse-incremental --airflow`
- `dpone check <target>`
- `dpone airflow preview <pipeline>`

### Successful domain-first offline preview path

- `dpone init project --airflow --layout domain-first`
- `dpone init domain <name> --owner-team data-crm --owner-contact crm@example.com --approver-team data-platform`
- `dpone init pipeline <name> --domain crm --route mssql:clickhouse:incremental_merge --from mssql_dev:dbo.orders --to clickhouse_dev:analytics.orders --key order_id`
- `dpone check <target>`
- `dpone airflow preview <pipeline>`

### Optional platform-gated safe sample

- `dpone run <path> --sample 1000 --target temporary` — returns success only when the approved sample runtime is configured.

## Canonical provider

- Distribution: `apache-airflow-providers-dpone`
- Namespace: `airflow.providers.dpone`
- Lightweight reader: `dpone-airflow-pack`
- Required exports: **16**

## Public schema majors

- Frozen v1 schema kinds: **75**

## Compatibility policy

- SemVer: `2.0.0`
- Provider legacy namespace: `dpone_airflow_pack`
- Legacy minimum: 2 minor releases and 365 days, whichever is later.

<!-- DPONE_AIRFLOW_PUBLIC_CONTRACTS_END -->
The baseline covers the stable Airflow self-service surface, not every internal
dpone module or historical command. Additive optional capabilities remain
possible under the compatibility policy.
