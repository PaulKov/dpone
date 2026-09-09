"""Feature parity ratings for feature_legacy_ratings."""

from __future__ import annotations

from tools.oss_benchmark.feature_catalog import FeatureRating
from tools.oss_benchmark.feature_catalog import rating as _r
from tools.oss_benchmark.feature_sling import SLING_FEATURE_RATINGS


def legacy_feature_ratings() -> tuple[FeatureRating, ...]:
    return (
        _r(
            "pentaho-kettle",
            "connectors",
            "strong",
            "Pentaho exposes a large transformation step catalog.",
            "https://docs.pentaho.com/pdia-data-integration/pdi-transformation-steps-reference-overview",
        ),
        _r(
            "pentaho-kettle",
            "cdc_incremental",
            "partial",
            "Real-time/CDC patterns are possible but are not a modern managed CDC core in OSS Kettle.",
            "https://docs.pentaho.com/pdia-data-integration/pipeline-designer/working-with-transformations",
        ),
        _r(
            "pentaho-kettle",
            "schema_evolution",
            "partial",
            "Schema changes are usually handled through transformation design and metadata.",
            "https://docs.pentaho.com/pdia-data-integration/pipeline-designer/working-with-transformations",
        ),
        _r(
            "pentaho-kettle",
            "orchestration",
            "native",
            "Jobs, transformations, schedules, repositories, and Carte execution are documented.",
            "https://docs.pentaho.com/pdia-data-integration/10.2-data-integration/schedule-perspective-in-the-pdi-client/schedule-a-transformation-or-job",
            "https://docs.pentaho.com/pdia-data-integration/10.2-data-integration/advanced-topics-pentaho-data-integration-overview/use-carte-clusters/run-transformations-and-jobs-from-the-repository-on-the-carte-server",
        ),
        _r(
            "pentaho-kettle",
            "retry_resume",
            "partial",
            "Operational retry/recovery is job design and repository/server dependent.",
            "https://docs.pentaho.com/pdia-data-integration/pipeline-designer/working-with-transformations",
        ),
        _r(
            "pentaho-kettle",
            "observability",
            "supported",
            "Step metrics and logs are part of transformation execution.",
            "https://docs.pentaho.com/pdia-data-integration/pipeline-designer/working-with-transformations",
        ),
        _r(
            "pentaho-kettle",
            "lineage_catalog",
            "partial",
            "Automatic documentation and repository metadata exist, but catalog lineage is not the OSS core focus.",
            "https://docs.pentaho.com/pdia-data-integration/pdi-transformation-steps-reference-overview",
        ),
        _r(
            "pentaho-kettle",
            "governance_security",
            "supported",
            "Repository-backed enterprise deployments are documented.",
            "https://docs.pentaho.com/pdia-data-integration/9.3-data-integration/use-a-pentaho-repository-in-pdi",
        ),
        _r(
            "pentaho-kettle",
            "deployment_modes",
            "supported",
            "Desktop, repository, server, and Carte execution patterns are documented.",
            "https://docs.pentaho.com/pdia-data-integration/10.2-data-integration/advanced-topics-pentaho-data-integration-overview/use-carte-clusters/run-transformations-and-jobs-from-the-repository-on-the-carte-server",
        ),
        _r(
            "pentaho-kettle",
            "certification_evidence",
            "not_detected",
            "No first-class release evidence chain is visible in public docs.",
            "https://docs.pentaho.com/pdia-data-integration/pipeline-designer/working-with-transformations",
        ),
        _r(
            "apache-hop",
            "connectors",
            "strong",
            "Hop is metadata-driven and plugin-oriented, with database plugins and SDK surfaces.",
            "https://hop.apache.org/",
            "https://hop.apache.org/manual/latest/database/databases.html",
        ),
        _r(
            "apache-hop",
            "cdc_incremental",
            "partial",
            "CDC-style workloads depend on plugins and pipeline design rather than a managed CDC core.",
            "https://hop.apache.org/manual/latest/pipeline/transforms/metainject.html",
        ),
        _r(
            "apache-hop",
            "schema_evolution",
            "partial",
            "Metadata injection supports dynamic runtime configuration, but schema evolution is design-driven.",
            "https://hop.apache.org/manual/latest/pipeline/transforms/metainject.html",
        ),
        _r(
            "apache-hop",
            "orchestration",
            "native",
            "Hop SDK documents workflow and pipeline metadata loading and execution.",
            "https://hop.apache.org/dev-manual/latest/sdk/hop-sdk.html",
        ),
        _r(
            "apache-hop",
            "retry_resume",
            "partial",
            "Workflow execution can model recovery, but evidence-grade recovery gates are external.",
            "https://hop.apache.org/dev-manual/latest/sdk/hop-sdk.html",
        ),
        _r(
            "apache-hop",
            "observability",
            "native",
            "Execution Information Location supports storing execution metadata.",
            "https://hop.apache.org/manual/latest/metadata-types/execution-information-location.html",
            "https://hop.apache.org/manual/latest/metadata-types/index.html",
        ),
        _r(
            "apache-hop",
            "lineage_catalog",
            "partial",
            "Metadata and execution information help inspection, but full catalog lineage is integration-dependent.",
            "https://hop.apache.org/manual/latest/metadata-types/index.html",
        ),
        _r(
            "apache-hop",
            "governance_security",
            "partial",
            "Project metadata supports governance patterns, but enterprise controls are operator-owned.",
            "https://hop.apache.org/",
        ),
        _r(
            "apache-hop",
            "deployment_modes",
            "strong",
            "Hop metadata can run through GUI, CLI/server, embedded Java, and multiple engines.",
            "https://hop.apache.org/dev-manual/latest/sdk/hop-sdk.html",
        ),
        _r(
            "apache-hop",
            "certification_evidence",
            "not_detected",
            "No first-class release evidence chain is visible in public docs.",
            "https://hop.apache.org/manual/latest/metadata-types/index.html",
        ),
        *(
            _r("sling", dimension, level, evidence, *sources)
            for dimension, level, evidence, sources in SLING_FEATURE_RATINGS
        ),
    )
