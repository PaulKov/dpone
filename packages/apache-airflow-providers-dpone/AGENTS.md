# Formal Airflow provider rules

These rules extend the repository-level `AGENTS.md` for
`packages/apache-airflow-providers-dpone/**`.

- This distribution owns only the canonical `airflow.providers.dpone`
  namespace, provider discovery metadata, typing markers, and compatibility
  declarations.
- Runtime pack parsing and DAG construction implementations stay in the
  dependency-light `dpone-airflow-pack` reader package.
- Keep imports parse-safe. Importing the namespace must not perform network,
  filesystem discovery, metadata DB, Variable, Connection, Vault, Kubernetes,
  or cache-refresh I/O.
- Public signatures and return types are stable integration contracts across
  the tested Airflow/Python matrix.
- Never serialize or log credentials, connection URIs, Vault paths, signed
  URLs, service-account material, or secret-derived hashes.
