"""Feature parity ratings for feature_closed_core_ratings."""

from __future__ import annotations

from tools.oss_benchmark.feature_catalog import FeatureRating
from tools.oss_benchmark.feature_catalog import rating as _r


def closed_core_feature_ratings() -> tuple[FeatureRating, ...]:
    return (
        _r(
            "fivetran",
            "connectors",
            "managed",
            "Fivetran positions connectors as fully managed data integration.",
            "https://www.fivetran.com/connectors/drift",
        ),
        _r(
            "fivetran",
            "cdc_incremental",
            "managed",
            "Database connector docs describe initial sync and continuous replication patterns.",
            "https://fivetran.com/docs/connectors/databases/sql-server",
        ),
        _r(
            "fivetran",
            "schema_evolution",
            "managed",
            "Fivetran docs cover schema configuration, re-sync, schema drift, and hashing/blocking columns.",
            "https://fivetran.com/docs/core-concepts/features",
            "https://fivetran.com/docs/getting-started/fivetran-dashboard/connectors/schema",
        ),
        _r(
            "fivetran",
            "orchestration",
            "managed",
            "Managed sync scheduling and connector operation are product features.",
            "https://fivetran.com/docs/core-concepts/features",
        ),
        _r(
            "fivetran",
            "retry_resume",
            "managed",
            "Retry/recovery is handled as part of the managed service operating model.",
            "https://fivetran.com/docs/core-concepts/features",
        ),
        _r(
            "fivetran",
            "observability",
            "managed",
            "Managed connector operation exposes dashboard-driven sync state and operational controls.",
            "https://fivetran.com/docs/core-concepts/features",
        ),
        _r(
            "fivetran",
            "lineage_catalog",
            "partial",
            "Lineage and catalog posture usually depends on destination and partner integrations.",
            "https://fivetran.com/docs/getting-started/fivetran-dashboard/connectors/schema",
        ),
        _r(
            "fivetran",
            "governance_security",
            "managed",
            "Schema controls include excluding tables, blocking columns, and hashing columns.",
            "https://fivetran.com/docs/getting-started/fivetran-dashboard/connectors/schema",
        ),
        _r(
            "fivetran",
            "deployment_modes",
            "strong",
            "SaaS and Hybrid deployment models are documented for connectors.",
            "https://fivetran.com/docs/connectors/applications/drift",
        ),
        _r(
            "fivetran",
            "certification_evidence",
            "partial",
            "Managed service evidence is operational, not a repository-local auditable release evidence chain.",
            "https://fivetran.com/docs/core-concepts/features",
        ),
        _r(
            "informatica",
            "connectors",
            "managed",
            "IDMC positions end-to-end cloud data management and integration services.",
            "https://www.informatica.com/download.html",
        ),
        _r(
            "informatica",
            "cdc_incremental",
            "managed",
            "Enterprise integration products include database and data replication patterns.",
            "https://www.informatica.com/download.html",
        ),
        _r(
            "informatica",
            "schema_evolution",
            "managed",
            "Enterprise data integration and quality services cover data structure management.",
            "https://www.informatica.com/download.html",
        ),
        _r(
            "informatica",
            "orchestration",
            "managed",
            "IDMC provides enterprise integration orchestration services.",
            "https://www.informatica.com/download.html",
        ),
        _r(
            "informatica",
            "retry_resume",
            "managed",
            "Operational recovery is part of the managed enterprise integration platform.",
            "https://www.informatica.com/download.html",
        ),
        _r(
            "informatica",
            "observability",
            "managed",
            "Data Quality & Observability is part of Informatica's governance/catalog positioning.",
            "https://www.informatica.com/products/data-governance/cloud-data-governance-and-catalog.html",
        ),
        _r(
            "informatica",
            "lineage_catalog",
            "managed",
            "Informatica positions automated, end-to-end data lineage and Cloud Data Governance and Catalog.",
            "https://www.informatica.com/products/data-catalog/data-lineage.html",
            "https://www.informatica.com/products/data-governance/cloud-data-governance-and-catalog.html",
        ),
        _r(
            "informatica",
            "governance_security",
            "managed",
            "Governance, catalog, data quality, access management, and marketplace are part of IDMC positioning.",
            "https://www.informatica.com/products/data-governance/cloud-data-governance-and-catalog.html",
        ),
        _r(
            "informatica",
            "deployment_modes",
            "strong",
            "IDMC is positioned as cloud-native enterprise data management.",
            "https://www.informatica.com/download.html",
        ),
        _r(
            "informatica",
            "certification_evidence",
            "partial",
            "Enterprise governance evidence exists, but no repository-local auditable release evidence chain is public.",
            "https://www.informatica.com/products/data-governance/cloud-data-governance-and-catalog.html",
        ),
    )
