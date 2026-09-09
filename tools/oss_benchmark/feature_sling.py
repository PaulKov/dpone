"""Static Sling feature-parity evidence used by the benchmark."""

from __future__ import annotations

SLING_FEATURE_RATINGS = (
    (
        "connectors",
        "strong",
        "Sling positions database and file connections as a CLI-first data movement surface.",
        ("https://github.com/slingdata-io/sling-cli", "https://docs.slingdata.io/"),
    ),
    (
        "cdc_incremental",
        "partial",
        "Replication-oriented workflows are visible, but managed log-based CDC is not evidenced as the OSS core.",
        ("https://github.com/slingdata-io/sling-cli",),
    ),
    (
        "schema_evolution",
        "partial",
        "Schema handling is part of data movement workflows, while governed compatibility policy is external.",
        ("https://github.com/slingdata-io/sling-cli",),
    ),
    (
        "orchestration",
        "external",
        "The CLI is scheduler-friendly, with orchestration typically delegated to CI, cron, Airflow, or operators.",
        ("https://github.com/slingdata-io/sling-cli",),
    ),
    (
        "retry_resume",
        "partial",
        "CLI execution can be retried operationally, but auditable resume guarantees are not a first-class surface.",
        ("https://github.com/slingdata-io/sling-cli",),
    ),
    (
        "observability",
        "partial",
        "CLI output and run logs support diagnosis, while evidence-grade observability remains operator-owned.",
        ("https://github.com/slingdata-io/sling-cli",),
    ),
    (
        "lineage_catalog",
        "external",
        "Lineage and catalog integration are not visible as the core OSS product surface.",
        ("https://github.com/slingdata-io/sling-cli",),
    ),
    (
        "governance_security",
        "external",
        "Secrets, policy gates, and enterprise governance are primarily deployment-environment responsibilities.",
        ("https://github.com/slingdata-io/sling-cli",),
    ),
    (
        "deployment_modes",
        "strong",
        "A CLI-first binary/runtime can be embedded in local, CI/CD, scheduler, and container workflows.",
        ("https://github.com/slingdata-io/sling-cli",),
    ),
    (
        "certification_evidence",
        "not_detected",
        "No first-class release evidence chain is visible in public product docs.",
        ("https://github.com/slingdata-io/sling-cli",),
    ),
)
