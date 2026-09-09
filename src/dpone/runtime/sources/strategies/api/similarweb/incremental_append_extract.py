from __future__ import annotations

from typing import Any

from dpone.runtime.connectors.api.similarweb_resources import SIMILARWEB_KEYWORDS_SCHEMA
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.similarweb.base import SimilarwebBaseStrategy
from dpone.runtime.sources.strategies.api.similarweb.transformer import SimilarwebRowTransformer
from dpone.runtime.sources.strategies.api.similarweb.validator import SimilarwebDQValidator


class SimilarwebKeywordsIncrementalAppendStrategy(SimilarwebBaseStrategy):
    """Monthly snapshot append strategy for SimilarWeb website keywords."""

    def __init__(self, connector, sink_connector, logger):
        super().__init__(connector, sink_connector, logger)
        self._transformer = SimilarwebRowTransformer()
        self._validator = SimilarwebDQValidator(etl_logger=logger)

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        del last_state
        options = self._get_options(load_config)
        resource = str(options.get("resource", getattr(load_config, "source_table", "keywords"))).strip() or "keywords"
        if resource != "keywords":
            raise ValueError(f"Unsupported SimilarWeb resource '{resource}'. Only 'keywords' is implemented.")

        snapshot_month, start_date, end_date = self._resolve_period(options)
        domains = self._resolve_domains(options)
        self._validator.check_period(snapshot_month)
        api_params = self._read_api_params(options)
        force_reload = bool(options.get("force_reload", False))
        min_keywords_count = int(options.get("min_keywords_count", 1000))

        active_domains = self._resolve_active_domains(
            domains=domains,
            snapshot_month=snapshot_month,
            load_config=load_config,
            force_reload=force_reload,
        )
        if not active_domains:
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=SIMILARWEB_KEYWORDS_SCHEMA,
                state=None,
                force_full_refresh=False,
            )

        transformed_records: list[dict[str, Any]] = []
        for domain in active_domains:
            raw_rows = self.connector.fetch_resource_rows(
                resource_name=resource,
                url=domain,
                start_date=start_date,
                end_date=end_date,
                **api_params,
            )
            transformed_records.extend(
                self._transformer.transform(raw_rows, snapshot_month=snapshot_month, domain=domain)
            )

        self._validator.validate(
            transformed_records,
            snapshot_month=snapshot_month,
            min_keywords_count=min_keywords_count,
        )
        return ExtractResult(
            artifact=InMemoryRowsArtifact(transformed_records),
            schema=SIMILARWEB_KEYWORDS_SCHEMA,
            state=None,
            force_full_refresh=False,
        )

    def _resolve_active_domains(
        self,
        *,
        domains: list[str],
        snapshot_month: str,
        load_config: Any,
        force_reload: bool,
    ) -> list[str]:
        active: list[str] = []
        for domain in domains:
            if self._is_domain_month_loaded(load_config, snapshot_month, domain):
                if force_reload:
                    self._delete_domain_month_data(load_config, snapshot_month, domain)
                    active.append(domain)
                else:
                    self.logger.info(
                        "Skipping SimilarWeb snapshot for %s / %s because rows already exist",
                        snapshot_month,
                        domain,
                    )
            else:
                active.append(domain)
        return active

    @staticmethod
    def _read_api_params(options: dict[str, Any]) -> dict[str, Any]:
        total_limit = int(options.get("limit", 2000))
        page_size = int(options.get("page_size", total_limit))
        return {
            "limit": total_limit,
            "page_size": page_size,
            "traffic_source": str(options.get("traffic_source", "Organic")),
            "web_source": str(options.get("web_source", "Total")),
            "branded_type": str(options.get("branded_type", "All")),
            "country": str(options.get("country", "world")),
            "sort": options.get("sort"),
            "asc": options.get("asc"),
        }
