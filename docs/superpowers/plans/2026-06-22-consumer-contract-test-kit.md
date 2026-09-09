# Consumer Contract Test Kit & Compatibility Certification

## Scope

Add an offline, provider-neutral layer above schema contract consumer matrix and compatibility views:

- build deterministic consumer test kits from matrix/view evidence;
- render self-service pytest/Markdown instructions for consumer owners;
- certify consumer test execution through a machine-readable artifact;
- make certification usable by schema migration bundles, bundle policy gates and evidence registry.

## Architecture

- `SchemaConsumerTestKitBuilder`: pure builder from matrix and optional compatibility view plan.
- `SchemaConsumerCertificationEvaluator`: pure evaluator from kit plus result evidence.
- `SchemaConsumerTestKitRenderer`: deterministic JSON/text/Markdown/table/pytest rendering.
- `SchemaConsumerTestKitFacade`: thin file-IO service for CLI.

No target DB, SCM, ClickHouse, Airflow or catalog SDK imports in generic modules.

## CLI

- `dpone schema contract consumers test-kit plan --manifest ... --matrix ...`
- `dpone schema contract consumers test-kit render --kit ... --format pytest|md|json|text|table`
- `dpone schema contract consumers test-kit certify --kit ... --result passed|failed`

## Acceptance

- production modules stay below 400 SLOC;
- bundle policy can require `consumer_certification`;
- registry can record stage `consumer_certified`;
- docs and public JSON schemas describe the artifacts.
