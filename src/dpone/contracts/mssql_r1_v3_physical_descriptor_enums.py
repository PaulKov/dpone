"""Closed discriminators for the R1 physical descriptor."""

from dpone._compat import StrEnum


class MssqlR1DefinitionKindV1(StrEnum):
    TABLE_DDL = "table_ddl"
    MODULE_TEXT = "module_text"
    MODULE_TEMPLATE = "module_template"


class MssqlR1TableLifecycleV1(StrEnum):
    IMMUTABLE = "immutable"
    APPEND_ONLY = "append_only"
    CAS_HEAD = "cas_head"
    STATE_MACHINE = "state_machine"
    GUARDED_MUTABLE = "guarded_mutable"


class MssqlR1MutationPolicyV1(StrEnum):
    INSTALLER_ONLY = "installer_only"
    GUARDED_PROCEDURE_ONLY = "guarded_procedure_only"
    BINDING_MODULE_ONLY = "binding_module_only"


class MssqlR1ResourceInstanceSelectorKindV1(StrEnum):
    DECLARED_SINGLETON = "declared_singleton"
    SINGLE_VALUE = "single_value"
    ORDERED_VALUES = "ordered_values"


class MssqlR1TransitionKindV1(StrEnum):
    CREATE = "create"
    CAS = "cas"
    APPEND = "append"


class MssqlR1TransitionAuthorityV1(StrEnum):
    SELF_CONTAINED = "self_contained"
    CALLER_UOW = "caller_uow"
    READ_ONLY = "read_only"


class MssqlR1TransitionCardinalityV1(StrEnum):
    ONE = "one"
    ZERO_OR_ONE = "zero_or_one"
    ONE_OR_MORE = "one_or_more"
    EXACT_REQUEST_SET = "exact_request_set"


class MssqlR1RevisionRuleKindV1(StrEnum):
    CREATE_ONE = "create_one"
    EQUAL_REQUEST = "equal_request"
    ADJACENT = "adjacent"
    UNCHANGED = "unchanged"
    NULLABLE_INITIAL = "nullable_initial"
    SET_CANDIDATE = "set_candidate"


class MssqlR1ReplayComparatorV1(StrEnum):
    EXACT_REQUEST = "exact_request"
    EXACT_PROJECTION = "exact_projection"
    FRESH_COHERENT_PROOF = "fresh_coherent_proof"


class MssqlR1ReplayBooleanOperatorV1(StrEnum):
    ALL = "all"
    ANY = "any"


class MssqlR1ComparisonOperatorV1(StrEnum):
    EQUAL = "equal"
    NOT_EQUAL = "not_equal"
    ADJACENT = "adjacent"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    ROW_EXISTS = "row_exists"
    ROW_ABSENT = "row_absent"


class MssqlR1ComparisonSourceV1(StrEnum):
    REQUEST_FIELD = "request_field"
    RESULT_COLUMN = "result_column"
    RESOURCE_FIELD = "resource_field"
    PREDECESSOR_RESOURCE_FIELD = "predecessor_resource_field"
    CANDIDATE_RESOURCE_FIELD = "candidate_resource_field"
    DESCENDANT_RECEIPT = "descendant_receipt"
    PROCEDURE_PARAMETER = "procedure_parameter"
    SESSION_BINDING = "session_binding"


class MssqlR1PrincipalKindV1(StrEnum):
    PROVISIONER = "provisioner"
    RUNTIME = "runtime"
    LOADER = "loader"
    OBSERVER = "observer"


class MssqlR1ProjectionRoleV1(StrEnum):
    REQUEST_JSON = "request_json"


class MssqlR1RequestBindingKindV1(StrEnum):
    PAYLOAD_PROJECTION = "payload_projection"
    SEALED_REQUEST_DIGEST = "sealed_request_digest"


class MssqlR1ExecutionPathV1(StrEnum):
    FRESH_MUTATION = "fresh_mutation"
    IDEMPOTENT_REPLAY = "idempotent_replay"
    READ_ONLY_PROBE = "read_only_probe"


class MssqlR1OutcomeClassV1(StrEnum):
    KNOWN_NOT_COMMITTED = "known_not_committed"
    COMMITTED = "committed"
    UNKNOWN = "unknown"


class MssqlR1RetryClassV1(StrEnum):
    IMMEDIATE = "immediate"
    BOUNDED_BACKOFF = "bounded_backoff"
    SEALED_TAKEOVER = "sealed_takeover"
    BLOCKED = "blocked"


class MssqlR1FreshProbeKindV1(StrEnum):
    NONE = "none"
    CONTROL = "control"
    STAGE_OPEN = "stage_open"
    STAGE_CHUNK = "stage_chunk"
    STAGE_SEAL = "stage_seal"
    OPEN_RECOVERY = "open_recovery"
    EFFECT = "effect"


class MssqlR1RedactionClassV1(StrEnum):
    PUBLIC = "public"
    INTERNAL_REDACTED = "internal_redacted"


class MssqlR1MigrationObservationKindV1(StrEnum):
    INVENTORY_ABSENT = "inventory_absent"
    EXACT_SCHEMA2 = "exact_schema2"
    EMPTY_LEGACY = "empty_legacy"
    LEGACY_WRITER_GRANT = "legacy_writer_grant"
    DURABLE_LEGACY_STATE = "durable_legacy_state"
    INCOMPATIBLE_V3 = "incompatible_v3"
    PARTIAL_INVENTORY = "partial_inventory"
    UNREADABLE_INVENTORY = "unreadable_inventory"
    UNKNOWN_CODEC = "unknown_codec"
    NONTERMINAL_STATE = "nonterminal_state"
    RETAINED_STAGE = "retained_stage"
    SEALED_INTENT = "sealed_intent"
    AMBIGUOUS_STATE = "ambiguous_state"


class MssqlR1MigrationDispositionV1(StrEnum):
    INSTALL = "install"
    REPLAY = "replay"
    COEXIST = "coexist"
    BLOCK = "block"


class MssqlR1BindingModuleKindV1(StrEnum):
    BATCH_MUTATE = "batch_mutate"
    BATCH_ROW_HASH = "batch_row_hash"
    BATCH_QUALITY = "batch_quality"
    XMIN_MUTATE = "xmin_mutate"
    XMIN_ROW_HASH = "xmin_row_hash"
    XMIN_QUALITY = "xmin_quality"


class MssqlR1BindingSignerKindV1(StrEnum):
    BINDING_SCOPED = "binding_scoped"


__all__ = (
    "MssqlR1DefinitionKindV1",
    "MssqlR1TableLifecycleV1",
    "MssqlR1MutationPolicyV1",
    "MssqlR1ResourceInstanceSelectorKindV1",
    "MssqlR1TransitionKindV1",
    "MssqlR1TransitionAuthorityV1",
    "MssqlR1TransitionCardinalityV1",
    "MssqlR1RevisionRuleKindV1",
    "MssqlR1ReplayComparatorV1",
    "MssqlR1ReplayBooleanOperatorV1",
    "MssqlR1ComparisonOperatorV1",
    "MssqlR1ComparisonSourceV1",
    "MssqlR1PrincipalKindV1",
    "MssqlR1ProjectionRoleV1",
    "MssqlR1RequestBindingKindV1",
    "MssqlR1ExecutionPathV1",
    "MssqlR1OutcomeClassV1",
    "MssqlR1RetryClassV1",
    "MssqlR1FreshProbeKindV1",
    "MssqlR1RedactionClassV1",
    "MssqlR1MigrationObservationKindV1",
    "MssqlR1MigrationDispositionV1",
    "MssqlR1BindingModuleKindV1",
    "MssqlR1BindingSignerKindV1",
)
