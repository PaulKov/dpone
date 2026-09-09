# Airflow pack rules

These rules extend the repository-level `AGENTS.md` for
`packages/dpone-airflow-pack/**`.

- Keep the provider thin. Core planning, contracts, identity, state, evidence,
  and execution semantics belong in canonical dpone packages.
- Preserve supported Airflow/provider version compatibility and lazy optional
  imports. Importing dpone core must not require Airflow.
- Treat DAG parse time, serialization, task mapping, templating, connection
  resolution, Kubernetes pod materialization, retries, logs, and XCom/artifact
  behavior as public integration contracts.
- Never log connection URIs, passwords, tokens, service-account material, or
  rendered secrets.
- New behavior requires provider-focused unit/contract tests, documented
  examples for a first-time Airflow user, upgrade notes, and real-environment
  certification when it claims production support.
- Compare orchestration UX with relevant current official guidance from Airflow,
  Astronomer Cosmos, gusty, and dbt integrations; record versions and avoid
  unsupported superiority claims.
