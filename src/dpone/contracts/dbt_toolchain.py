"""Single authority for the certified dbt SQL Server toolchain."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.dbt_contract_validation import canonical_fingerprint


@dataclass(frozen=True, slots=True)
class DbtToolchainContract:
    """Exact build/runtime versions participating in release identity."""

    contract_id: str
    dbt_core_version: str
    adapter_name: str
    adapter_distribution: str
    adapter_version: str
    manifest_schema_version: str
    run_results_schema_version: str

    @property
    def sha256(self) -> str:
        return canonical_fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, str]:
        return {
            "contract_id": self.contract_id,
            "dbt_core_version": self.dbt_core_version,
            "adapter_name": self.adapter_name,
            "adapter_distribution": self.adapter_distribution,
            "adapter_version": self.adapter_version,
            "manifest_schema_version": self.manifest_schema_version,
            "run_results_schema_version": self.run_results_schema_version,
        }


DBT_SQLSERVER_1_12_CERTIFIED = DbtToolchainContract(
    contract_id="dbt-sqlserver-1.11-core-1.12-certified",
    dbt_core_version="1.12.3",
    adapter_name="sqlserver",
    adapter_distribution="dbt-sqlserver",
    adapter_version="1.11.1",
    manifest_schema_version="v12",
    run_results_schema_version="v6",
)


def certified_adapter_distribution(adapter_name: str) -> str:
    """Return the adapter distribution owned by the certified contract."""

    if adapter_name != DBT_SQLSERVER_1_12_CERTIFIED.adapter_name:
        raise ValueError("dbt adapter is not supported by the certified toolchain")
    return DBT_SQLSERVER_1_12_CERTIFIED.adapter_distribution


__all__ = [
    "DBT_SQLSERVER_1_12_CERTIFIED",
    "DbtToolchainContract",
    "certified_adapter_distribution",
]
