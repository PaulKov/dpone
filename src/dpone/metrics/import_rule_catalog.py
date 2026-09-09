from __future__ import annotations

from .import_rule_models import ImportRule

LEGACY_SHIM_PREFIXES: tuple[str, ...] = (
    "dpone.source",
    "dpone.sink",
    "dpone.etl",
    "dpone.etl_logging",
    "dpone.credentials",
    "dpone.state",
    "dpone.reconciliation",
    "dpone.sql_helpers",
    "dpone.xmin",
    "dpone.lib.connectors",
    "dpone.yaml_config_handler",
)

LEGACY_SHIM_MODULE_PREFIXES: tuple[str, ...] = (
    "dpone.source",
    "dpone.sink",
    "dpone.etl",
    "dpone.etl_logging",
    "dpone.credentials",
    "dpone.state",
    "dpone.reconciliation",
    "dpone.sql_helpers",
    "dpone.xmin",
    "dpone.lib.connectors",
    "dpone.yaml_config_handler",
)

RUNTIME_PREFIXES: tuple[str, ...] = ("dpone.runtime",) + LEGACY_SHIM_PREFIXES
CLI_PREFIXES: tuple[str, ...] = ("dpone.cli",)
CLI_RENDER_PREFIXES: tuple[str, ...] = ("dpone.cli_render",)
COMMANDS_PREFIXES: tuple[str, ...] = ("dpone.commands",)
SERVICES_PREFIXES: tuple[str, ...] = ("dpone.services",)
MANIFEST_PREFIXES: tuple[str, ...] = ("dpone.manifest",)
DAG_PREFIXES: tuple[str, ...] = ("dpone.dag", "dpone.yaml_config_handler")
ADAPTERS_PREFIXES: tuple[str, ...] = ("dpone.adapters",)
APP_PREFIXES: tuple[str, ...] = ("dpone.app",)
PORTS_PREFIXES: tuple[str, ...] = ("dpone.ports",)


def default_import_rules() -> tuple[ImportRule, ...]:
    return (
        ImportRule(
            id="no-legacy-shims-outside-shims",
            source_prefixes=("dpone",),
            allowed_source_prefixes=LEGACY_SHIM_MODULE_PREFIXES,
            forbidden_prefixes=LEGACY_SHIM_PREFIXES,
            description=(
                "Deprecated shim packages must not be imported from canonical code. "
                "Use dpone.runtime.* / dpone.dag.* instead."
            ),
        ),
        ImportRule(
            id="commands-no-runtime-or-legacy-cli",
            source_prefixes=COMMANDS_PREFIXES,
            forbidden_prefixes=RUNTIME_PREFIXES + ("dpone.cli.legacy",),
            description=(
                "Commands are orchestration-only and must not depend on runtime modules or the legacy CLI shim."
            ),
        ),
        ImportRule(
            id="commands-no-ops-internals",
            source_prefixes=COMMANDS_PREFIXES,
            forbidden_prefixes=("dpone.ops",),
            description=(
                "Commands must call ops use cases through dpone.services.ops facades instead of importing ops internals."
            ),
        ),
        ImportRule(
            id="services-no-cli-render-runtime",
            source_prefixes=SERVICES_PREFIXES,
            forbidden_prefixes=CLI_PREFIXES + CLI_RENDER_PREFIXES + RUNTIME_PREFIXES + COMMANDS_PREFIXES,
            description=("Services must stay independent from CLI presentation and runtime execution modules."),
            allowed_target_prefixes=("dpone.cli.legacy",),
        ),
        ImportRule(
            id="renderers-no-runtime-or-commands",
            source_prefixes=CLI_RENDER_PREFIXES,
            forbidden_prefixes=RUNTIME_PREFIXES
            + COMMANDS_PREFIXES
            + ("dpone.cli.legacy", "dpone.cli.main", "dpone.cli.parser"),
            description=(
                "CLI renderers are presentation-only and must not depend on runtime modules, command wiring or legacy CLI."
            ),
        ),
        ImportRule(
            id="manifest-no-runtime-or-cli",
            source_prefixes=MANIFEST_PREFIXES,
            forbidden_prefixes=RUNTIME_PREFIXES + COMMANDS_PREFIXES + CLI_RENDER_PREFIXES + ("dpone.cli.legacy",),
            description=("Manifest compilation/validation/explain code must remain runtime-light and CLI-independent."),
        ),
        ImportRule(
            id="dag-no-runtime-or-cli",
            source_prefixes=DAG_PREFIXES,
            allowed_source_prefixes=("dpone.yaml_config_handler",),
            forbidden_prefixes=RUNTIME_PREFIXES + COMMANDS_PREFIXES + CLI_RENDER_PREFIXES + ("dpone.cli.legacy",),
            description=(
                "DAG analysis/build/explain code must remain runtime-light and must not depend on CLI layers."
            ),
        ),
        ImportRule(
            id="runtime-no-cli-or-services",
            source_prefixes=("dpone.runtime",),
            forbidden_prefixes=COMMANDS_PREFIXES
            + CLI_PREFIXES
            + CLI_RENDER_PREFIXES
            + SERVICES_PREFIXES
            + APP_PREFIXES,
            description=(
                "Runtime execution code must not depend on command, presentation or application-service layers."
            ),
        ),
        ImportRule(
            id="runtime-no-core-or-lib",
            source_prefixes=("dpone.runtime",),
            forbidden_prefixes=("dpone.core", "dpone.lib"),
            description=(
                "Runtime code must use canonical contracts/runtime support modules instead of legacy dpone.core / dpone.lib helpers."
            ),
        ),
        ImportRule(
            id="artifact-core-no-vendor-adapters",
            source_prefixes=(
                "dpone.runtime.artifact_models",
                "dpone.runtime.artifact_protocols",
                "dpone.runtime.row_artifacts",
                "dpone.runtime.file_artifacts",
                "dpone.runtime.cloud_artifacts",
                "dpone.runtime.staging",
                "dpone.runtime.artifacts",
            ),
            forbidden_prefixes=(
                "dpone.runtime.connectors",
                "dpone.runtime.sources",
                "dpone.runtime.sinks",
                "dpone.runtime.strategies",
            ),
            description=(
                "Artifact core must stay vendor-neutral; source/sink-specific staging behavior belongs in adapters."
            ),
        ),
        ImportRule(
            id="connectors-no-command-service-layers",
            source_prefixes=("dpone.runtime.connectors",),
            forbidden_prefixes=COMMANDS_PREFIXES
            + SERVICES_PREFIXES
            + CLI_PREFIXES
            + CLI_RENDER_PREFIXES
            + APP_PREFIXES,
            description=(
                "Connector adapters must stay infrastructure-focused and must not depend on command/service/presentation layers."
            ),
        ),
        ImportRule(
            id="sink-strategies-no-unrelated-sinks",
            source_prefixes=("dpone.runtime.sinks.strategies",),
            forbidden_prefixes=(
                "dpone.runtime.sinks.bigquery",
                "dpone.runtime.sinks.clickhouse",
                "dpone.runtime.sinks.kafka",
                "dpone.runtime.sinks.mssql",
                "dpone.runtime.sinks.postgres",
            ),
            allowed_target_prefixes=(
                "dpone.runtime.sinks.strategies",
                "dpone.runtime.sinks.strategy",
                "dpone.runtime.sinks.strategy_support",
            ),
            description=("Sink strategy packages must not couple to unrelated concrete sink implementations."),
        ),
        ImportRule(
            id="reconciliation-core-no-vendor-adapters",
            source_prefixes=("dpone.runtime.reconciliation",),
            allowed_source_prefixes=(
                "dpone.runtime.reconciliation.bigquery",
                "dpone.runtime.reconciliation.soft_delete",
            ),
            forbidden_prefixes=(
                "google",
                "google.cloud",
                "dpone.runtime.connectors.bigquery",
                "dpone.runtime.reconciliation.soft_delete.bigquery",
                "dpone.runtime.reconciliation.soft_delete.clickhouse",
                "dpone.runtime.reconciliation.soft_delete.mssql",
                "dpone.runtime.reconciliation.soft_delete.postgres",
            ),
            description=(
                "Generic reconciliation core must depend on protocols/facades; vendor-specific implementations live in adapters."
            ),
        ),
        ImportRule(
            id="api-core-no-provider-adapters",
            source_prefixes=(
                "dpone.runtime.api_registry",
                "dpone.runtime.connectors.api.base",
                "dpone.runtime.connectors.api.connector",
                "dpone.runtime.connectors.api.config",
                "dpone.runtime.connectors.api.credentials",
                "dpone.runtime.connectors.api.parallel",
                "dpone.runtime.connectors.api.rate_limit",
            ),
            forbidden_prefixes=(
                "dpone.runtime.connectors.api.appsflyer",
                "dpone.runtime.connectors.api.cbr",
                "dpone.runtime.connectors.api.fasttrack",
                "dpone.runtime.connectors.api.google_ads",
                "dpone.runtime.connectors.api.google_sheets",
                "dpone.runtime.connectors.api.mindbox",
                "dpone.runtime.connectors.api.omnidesk",
                "dpone.runtime.connectors.api.openexchangerates",
                "dpone.runtime.connectors.api.similarweb",
                "dpone.runtime.connectors.api.yandex_webmaster",
            ),
            description=(
                "Generic API runtime registry/base infrastructure must not import concrete provider adapters directly."
            ),
        ),
        ImportRule(
            id="ports-no-runtime-or-presentation",
            source_prefixes=PORTS_PREFIXES,
            forbidden_prefixes=("dpone.runtime",)
            + COMMANDS_PREFIXES
            + CLI_PREFIXES
            + CLI_RENDER_PREFIXES
            + SERVICES_PREFIXES
            + ADAPTERS_PREFIXES,
            description=(
                "Ports define abstract boundaries and must not depend on runtime, adapters, commands or renderers."
            ),
        ),
        ImportRule(
            id="adapters-no-runtime-or-services",
            source_prefixes=ADAPTERS_PREFIXES,
            forbidden_prefixes=("dpone.runtime",)
            + COMMANDS_PREFIXES
            + CLI_PREFIXES
            + CLI_RENDER_PREFIXES
            + SERVICES_PREFIXES
            + MANIFEST_PREFIXES
            + DAG_PREFIXES
            + APP_PREFIXES,
            description=(
                "Adapters should stay small and infrastructure-only; they must not depend on runtime or higher-level layers."
            ),
        ),
    )
