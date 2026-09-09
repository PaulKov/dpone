# DPONE_DISCOVERY_LIMIT_EXCEEDED

**Audience:** platform engineers and CI maintainers.

Domain-first discovery exceeded a published file, byte, domain, or pipeline
budget.

Inspect the bounded project report:

```bash
dpone check . --format json
```

Remove only unintended visible entries from `layout.root`, or split the
repository at an intentional platform boundary. Do not bypass the budget with
recursive discovery. Verify the repair without overwriting a baseline:

```bash
dpone workload index
```

Success is exit `0` and schema `dpone.workload-index.v1`. If the intended
project legitimately exceeds a published budget, escalate for a measured
budget change rather than deleting authority.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
