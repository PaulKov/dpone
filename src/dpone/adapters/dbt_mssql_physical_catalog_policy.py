"""Authenticate selected catalog allocation policy through real native originals.

This read-only adapter establishes policy provenance, not SQL registration
existence, effective database permissions, empirical capacity or model membership.
The subsequent binding installer must independently resolve the registration and
observe the database/schema IDs before mutating its protected local binding.
"""

from dpone.adapters.native_delivery_originals import NativeOriginalVerifier
from dpone.adapters.native_project_documents import NativeProjectDocumentReader
from dpone.contracts.dbt_mssql_physical_catalog_binding import CatalogPolicyProjection, project_catalog_policy
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.native_delivery import NativeOriginalsRefV1


class MssqlPhysicalCatalogPolicyReader:
    """Consume full original verification before projecting catalog authority.

    Compose the concrete verifier and document reader in the trusted application;
    keep the invocation-confined verifier alive for the duration of ``read``.
    Neither a caller-created resolved DTO nor a proof callback is accepted.
    """

    def __init__(self, *, verifier: NativeOriginalVerifier, documents: NativeProjectDocumentReader) -> None:
        if type(verifier) is not NativeOriginalVerifier or type(documents) is not NativeProjectDocumentReader:
            raise ValueError("catalog policy requires concrete native original and member readers")
        self._verifier = verifier
        self._documents = documents

    def read(
        self, refs: NativeOriginalsRefV1, *, registration: MssqlPhysicalRuntimeRegistration
    ) -> CatalogPolicyProjection:
        """Authenticate archive members, then require exact selected-policy binding.

        The registration argument is a claim checked against policy here. Its
        complete digest is retained for later independent protected SQL readback.
        Missing opt-in configuration rejects without database side effects.
        """
        if type(registration) is not MssqlPhysicalRuntimeRegistration:
            raise ValueError("catalog policy requires an exact physical registration")
        registration.__post_init__()
        original = self._verifier.resolve(refs)
        policy_bytes, intent = self._documents.read(original.project_directory, original.project_bundle)
        if policy_bytes != original.policy_document:
            raise ValueError("catalog policy member changed after original verification")
        return project_catalog_policy(
            original=original, policy_bytes=policy_bytes, intent=intent, registration=registration
        )
