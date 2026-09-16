"""Authenticate selected catalog allocation policy through real native originals.

This read-only adapter establishes policy provenance, not SQL registration
existence, effective database permissions, empirical capacity or model membership.
The subsequent binding installer must independently resolve the registration and
observe the database/schema IDs before mutating its protected local binding.
"""

from dataclasses import dataclass
from typing import Any, cast

from dpone.adapters.native_delivery_originals import NativeOriginalVerifier
from dpone.adapters.native_project_documents import NativeProjectDocumentReader
from dpone.contracts.dbt_execution_pack import SUPPORTED_DBT_ADAPTER
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from dpone.contracts.dbt_mssql_physical_registration_values import PlatformSelection, RegisteredLimits
from dpone.contracts.dbt_mssql_physical_validation import require_physical_identifier
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_delivery import NativeOriginalsRefV1
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativePlatformOriginalSubject, decode_native_original_subject
from dpone.contracts.native_project_documents import NATIVE_POLICY_MEMBER


@dataclass(frozen=True, slots=True)
class CatalogPolicyProjection:
    """Detached policy selection, never an independently authenticating token.

    ``resource_bounds`` identifies the selected opaque profile projection. It is
    not capacity authority and no profile payload is decoded. Bounds are explicit
    platform-authorized policy choices, not an empirical qualification result.
    """

    registration_id: str
    registration_sha256: str
    platform_subject: NativePlatformOriginalSubject
    trusted_profile: PlatformSelection
    profile_name: str
    workflow_id: str
    policy_member: OriginalRef
    project_archive_sha256: str
    model_database_name: str
    model_schema: str
    resource_bounds: OriginalRef


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
        policy = cast(dict[str, Any], decode_native_delivery_json(policy_bytes))
        # The concrete member reader has already validated the full v4 schema
        # and exact selected intent. Decode solely to project authenticated fields.
        profile = policy["profiles"][intent["profile"]]
        native = profile["native_execution"]
        catalog = native.get("physical_catalog_limits")
        if catalog is None:
            raise ValueError("catalog provisioning requires native_execution.physical_catalog_limits")
        target = profile.get("authoring_template", {}).get("invocation_target")
        if target is None:
            raise ValueError("catalog provisioning requires authoring_template.invocation_target")
        subject = NativePlatformOriginalSubject(original.authority, original.policy_sha256)
        selected = _selection(native["trusted_execution"]["profile"], subject)
        limits = RegisteredLimits(
            max_metadata_bytes=native["limits"]["max_metadata_bytes"],
            max_generation_bytes=native["generation"]["max_generation_bytes"],
            **catalog,
        )
        if registration.platform_subject != subject:
            raise ValueError("catalog registration PLATFORM subject differs from selected policy")
        if registration.trusted_profile != selected:
            raise ValueError("catalog registration profile reference or subject differs from selected policy")
        if registration.limits != limits:
            raise ValueError("catalog registration limits differ from selected policy")
        control = native["control"]
        if (
            registration.control_authority != OriginalRef(**control["authority"])
            or registration.control_connection_ref != control["connection_ref"]
            or registration.control_schema != control["schema"]
        ):
            raise ValueError("catalog registration control selection differs from selected policy")
        if registration.capacity_authority != OriginalRef(**native["generation"]["capacity_authority"]):
            raise ValueError("catalog registration capacity authority differs from selected policy")
        if registration.trusted_toolchain != _selection(native["trusted_execution"]["toolchain"], subject):
            raise ValueError("catalog registration toolchain selection differs from selected policy")
        if registration.qualification_policy_id != native["trusted_execution"]["qualification_policy_id"]:
            raise ValueError("catalog registration qualification policy differs from selected policy")
        database = require_physical_identifier(target["database"], "model_database")
        schema = native_control_schema(target["schema"])
        if schema.lower() in {
            "dbo",
            "sys",
            "information_schema",
            registration.local_schema.lower(),
            registration.control_schema.lower(),
        }:
            raise ValueError("catalog model schema must be dedicated and distinct from control schemas")
        if registration.model_database.database_name != database:
            raise ValueError("catalog registration model database differs from selected invocation target")
        owners = tuple(
            item
            for item in original.sources.workflows
            if item.project.project_bundle_sha256 == original.project_bundle.archive_sha256
            and item.source.workflow_id == intent["workflow"]
        )
        if len(owners) != 1:
            raise ValueError("catalog policy requires one authenticated execution owner")
        effective = owners[0].execution.invocation_profile()
        if effective.adapter_type != SUPPORTED_DBT_ADAPTER:
            raise ValueError("catalog execution requires the supported SQL Server adapter")
        if registration.model_connection_ref != effective.connection_ref:
            raise ValueError("catalog registration model connection differs from authenticated execution profile")
        if (effective.database, effective.schema) != (database, schema):
            raise ValueError("catalog policy differs from the effective execution target")
        return CatalogPolicyProjection(
            registration.registration_id,
            physical_runtime_registration_digest(registration),
            subject,
            registration.trusted_profile,
            intent["profile"],
            intent["workflow"],
            OriginalRef(NATIVE_POLICY_MEMBER, original.policy_sha256),
            original.project_bundle.archive_sha256,
            database,
            schema,
            selected.reference,
        )


def _selection(value: dict[str, Any], subject: NativePlatformOriginalSubject) -> PlatformSelection:
    retained = value["subject"]
    selected_subject = (
        subject if retained is None else decode_native_original_subject(encode_native_delivery_json(retained))
    )
    if type(selected_subject) is not NativePlatformOriginalSubject:
        raise ValueError("catalog selection requires a PLATFORM subject")
    return PlatformSelection(OriginalRef(**value["reference"]), selected_subject)
