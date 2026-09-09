"""Pure readiness projection for self-service connector, route, and recipe discovery."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

from dpone.contracts.capability_discovery import (
    EVIDENCE_STATUSES,
    ROUTE_CERTIFICATION_LEVELS,
    BeginnerRouteSupport,
    CapabilityDiscoverySnapshot,
    CapabilityIssue,
    ConnectorDeclaration,
    RecipeCapability,
    RecipeDiscoveryEntry,
    RouteCapability,
    RouteCertification,
    RouteCertificationVariant,
    RouteSupport,
)
from dpone.contracts.connector_declarations import canonical_connector_id
from dpone.readiness.capability_discovery_protocols import (
    CapabilitySnapshotProtocol,
    RouteProfileProtocol,
)

_LEVEL_RANK = {level: index for index, level in enumerate(ROUTE_CERTIFICATION_LEVELS)}
_SUPPORT_BY_GATE = {
    "contract_gate": "supported",
    "manual_live_gate": "supported",
    "not_supported": "not_supported",
}
_AUTHORING_BLOCKING_ENTITY_KINDS = frozenset({"recipe_catalog"})


class CapabilityDiscoveryError(ValueError):
    """Stable safe failure raised while projecting or resolving capabilities."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class CapabilityDiscoveryService:
    """Project already-loaded authorities without filesystem or vendor I/O."""

    def __init__(
        self,
        *,
        connectors: Sequence[ConnectorDeclaration],
        route_profiles: Sequence[RouteProfileProtocol],
        recipes: Sequence[RecipeDiscoveryEntry],
        certification_variants: Sequence[RouteCertificationVariant] = (),
        issues: Sequence[CapabilityIssue] = (),
    ) -> None:
        self._connectors = tuple(connectors)
        self._route_profiles = tuple(route_profiles)
        self._recipes = tuple(recipes)
        self._certification_variants = tuple(certification_variants)
        self._issues = tuple(issues)
        self._validate_identities()

    def snapshot(self) -> CapabilityDiscoverySnapshot:
        variants_by_route: dict[str, list[RouteCertificationVariant]] = defaultdict(list)
        known_route_ids = {profile.key.colon_id for profile in self._route_profiles}
        projection_issues = list(self._issues)
        for variant in self._certification_variants:
            route_id = variant.route_id
            if route_id not in known_route_ids:
                projection_issues.append(
                    CapabilityIssue(
                        code="DPONE_CAPABILITY_EVIDENCE_ROUTE_UNKNOWN",
                        entity_kind="route_certification_variant",
                        entity_id=variant.id,
                        message="Certification evidence does not match a declared runtime route.",
                    )
                )
                continue
            variants_by_route[route_id].append(variant)

        recipe_refs_by_route: dict[str, list[str]] = defaultdict(list)
        default_recipe_by_route: dict[str, str] = {}
        for recipe in self._recipes:
            if recipe.route_id is not None and recipe.scaffoldable:
                recipe_refs_by_route[recipe.route_id].append(recipe.ref)
                if recipe.default_for_route:
                    if recipe.route_id in default_recipe_by_route:
                        raise CapabilityDiscoveryError(
                            "DPONE_ROUTE_RECIPE_AMBIGUOUS",
                            "A route cannot declare more than one default beginner recipe.",
                        )
                    default_recipe_by_route[recipe.route_id] = recipe.ref

        routes = tuple(
            self._project_route(
                profile,
                recipe_refs=tuple(sorted(recipe_refs_by_route.get(profile.key.colon_id, ()))),
                default_recipe_ref=default_recipe_by_route.get(profile.key.colon_id),
                variants=tuple(
                    sorted(
                        variants_by_route.get(profile.key.colon_id, ()),
                        key=lambda item: item.id,
                    )
                ),
            )
            for profile in sorted(self._route_profiles, key=lambda item: item.key.colon_id)
        )
        route_by_id = {item.id: item for item in routes}
        recipes = tuple(
            self._project_recipe(
                recipe,
                route=(route_by_id.get(recipe.route_id) if recipe.route_id is not None else None),
            )
            for recipe in sorted(self._recipes, key=lambda item: item.ref)
        )
        return CapabilityDiscoverySnapshot(
            connectors=tuple(sorted(self._connectors, key=lambda item: item.id)),
            routes=routes,
            recipes=recipes,
            issues=tuple(
                sorted(
                    projection_issues,
                    key=lambda item: (item.code, item.entity_kind, item.entity_id),
                )
            ),
        )

    def resolve_beginner_recipe(self, route_ref: str) -> str:
        route_id = normalize_route_ref(route_ref)
        snapshot = self.snapshot()
        require_authoring_authority(snapshot)
        route = next((item for item in snapshot.routes if item.id == route_id), None)
        if route is None or route.support.status == "not_supported":
            raise CapabilityDiscoveryError(
                "DPONE_ROUTE_NOT_SUPPORTED",
                "The requested source, sink, and strategy route is not supported.",
            )
        if not route.beginner.recipe_refs:
            raise CapabilityDiscoveryError(
                "DPONE_ROUTE_NOT_SCAFFOLDABLE",
                "The route is supported by runtime contracts but has no beginner recipe.",
            )
        if route.beginner.default_recipe_ref is not None:
            return route.beginner.default_recipe_ref
        if len(route.beginner.recipe_refs) == 1:
            return route.beginner.recipe_refs[0]
        raise CapabilityDiscoveryError(
            "DPONE_ROUTE_RECIPE_AMBIGUOUS",
            "The route has multiple beginner recipes but no explicit default.",
        )

    def resolve_scaffold_recipe(self, recipe_ref: str) -> str:
        """Resolve one explicit recipe through the same route capability authority."""

        snapshot = self.snapshot()
        require_authoring_authority(snapshot)
        recipe = next((item for item in snapshot.recipes if item.ref == recipe_ref), None)
        if recipe is None:
            raise CapabilityDiscoveryError(
                "DPONE_RECIPE_NOT_FOUND",
                "The requested recipe is not present in the current capability snapshot.",
            )
        if not recipe.scaffoldable:
            code = "DPONE_ROUTE_NOT_SUPPORTED" if recipe.route_id is not None else "DPONE_ROUTE_NOT_SCAFFOLDABLE"
            raise CapabilityDiscoveryError(
                code,
                "The requested recipe is not scaffoldable by the current route capability snapshot.",
            )
        return recipe.ref

    def _validate_identities(self) -> None:
        _require_unique(
            tuple(item.id for item in self._connectors),
            code="DPONE_CAPABILITY_CONNECTOR_DUPLICATE",
            label="connector ids",
        )
        _require_unique(
            tuple(item.key.colon_id for item in self._route_profiles),
            code="DPONE_CAPABILITY_ROUTE_DUPLICATE",
            label="route ids",
        )
        _require_unique(
            tuple(item.ref for item in self._recipes),
            code="DPONE_CAPABILITY_RECIPE_DUPLICATE",
            label="recipe refs",
        )
        _require_unique(
            tuple(item.id for item in self._certification_variants),
            code="DPONE_CAPABILITY_EVIDENCE_DUPLICATE",
            label="certification variant ids",
        )
        dimensions = tuple(
            (
                item.route_id,
                item.transport,
                item.schema_evolution,
                item.airflow_runtime_mode,
            )
            for item in self._certification_variants
        )
        _require_unique(
            dimensions,
            code="DPONE_CAPABILITY_EVIDENCE_AMBIGUOUS",
            label="certification variant dimensions",
        )
        for item in self._certification_variants:
            if item.evidence_status not in EVIDENCE_STATUSES:
                raise CapabilityDiscoveryError(
                    "DPONE_CAPABILITY_EVIDENCE_STATUS_INVALID",
                    "Certification evidence status is unsupported.",
                )
            if item.level not in _LEVEL_RANK:
                raise CapabilityDiscoveryError(
                    "DPONE_CAPABILITY_CERTIFICATION_LEVEL_INVALID",
                    "Route certification level is unsupported.",
                )
            if item.evidence_status != "PASS" and item.level != "experimental":
                raise CapabilityDiscoveryError(
                    "DPONE_CAPABILITY_EVIDENCE_UNPROVEN_LEVEL",
                    "Only PASS evidence can promote a route above experimental.",
                )
            if item.evidence_status == "PASS" and item.level != "experimental" and not item.evidence_refs:
                raise CapabilityDiscoveryError(
                    "DPONE_CAPABILITY_EVIDENCE_REF_MISSING",
                    "Promoted route certification requires a content-digest evidence reference.",
                )
            if any(not ref.startswith("sha256:") for ref in item.evidence_refs):
                raise CapabilityDiscoveryError(
                    "DPONE_CAPABILITY_EVIDENCE_REF_INVALID",
                    "Certification evidence references must be SHA-256 content digests.",
                )

    @staticmethod
    def _project_route(
        profile: RouteProfileProtocol,
        *,
        recipe_refs: tuple[str, ...],
        default_recipe_ref: str | None,
        variants: tuple[RouteCertificationVariant, ...],
    ) -> RouteCapability:
        evidence_status = _aggregate_evidence_status(variants)
        level = (
            max(
                (item.level for item in variants),
                key=lambda value: _LEVEL_RANK[value],
                default="experimental",
            )
            if evidence_status == "PASS"
            else "experimental"
        )
        reason_codes = tuple(
            dict.fromkeys(
                (
                    *(code for item in variants for code in item.reason_codes),
                    *(() if variants else ("route_certification_evidence_missing",)),
                )
            )
        )
        return RouteCapability(
            id=profile.key.colon_id,
            source=profile.key.source,
            sink=profile.key.sink,
            strategy=profile.key.strategy,
            support=RouteSupport(
                status=_SUPPORT_BY_GATE.get(profile.certification_status, "not_supported"),
                limitations=_support_limitations(profile.certification_status),
                docs_link=profile.docs_link,
                install_extras=profile.install_extras,
            ),
            certification=RouteCertification(
                level=level,
                evidence_status=evidence_status,
                reason_codes=reason_codes,
                variants=variants,
            ),
            beginner=BeginnerRouteSupport(
                recipe_available=bool(recipe_refs),
                recipe_refs=recipe_refs,
                default_recipe_ref=default_recipe_ref,
            ),
        )

    @staticmethod
    def _project_recipe(
        recipe: RecipeDiscoveryEntry,
        *,
        route: RouteCapability | None,
    ) -> RecipeCapability:
        reason_codes = recipe.reason_codes
        if route is None and recipe.route_id is None:
            reason_codes = tuple(dict.fromkeys((*reason_codes, "route_metadata_unavailable")))
        elif route is None or route.support.status == "not_supported":
            reason_codes = tuple(dict.fromkeys((*reason_codes, "route_not_supported")))
        scaffoldable = recipe.scaffoldable and route is not None and route.support.status != "not_supported"
        return RecipeCapability(
            ref=recipe.ref,
            origin=recipe.origin,
            status=recipe.status,
            source=route.source if route is not None else None,
            sink=route.sink if route is not None else None,
            strategy=route.strategy if route is not None else None,
            route_id=recipe.route_id,
            support_status=route.support.status if route is not None else "unknown",
            certification_level=route.certification.level if route is not None else "experimental",
            evidence_status=route.certification.evidence_status if route is not None else "UNVERIFIED",
            install_extras=route.support.install_extras if route is not None else (),
            limitations=route.support.limitations if route is not None else ("route_metadata_unavailable",),
            scaffoldable=scaffoldable,
            reason_codes=reason_codes,
            scaffold_argv=(
                "dpone",
                "init",
                "pipeline",
                "<pipeline_id>",
                "--recipe",
                recipe.ref,
            )
            if scaffoldable
            else (),
        )


def normalize_route_ref(route_ref: str) -> str:
    parts = tuple(part.strip().lower().replace("-", "_") for part in route_ref.split(":"))
    if len(parts) != 3 or not all(parts):
        raise CapabilityDiscoveryError(
            "DPONE_ROUTE_NOT_SUPPORTED",
            "Route must use source:sink:strategy syntax.",
        )
    source, sink, strategy = parts
    return ":".join(
        (
            normalize_connector_ref(source),
            normalize_connector_ref(sink),
            strategy,
        )
    )


def normalize_connector_ref(value: str) -> str:
    """Normalize an endpoint family to its canonical provider identity."""

    return canonical_connector_id(value)


def _aggregate_evidence_status(variants: tuple[RouteCertificationVariant, ...]) -> str:
    statuses = {item.evidence_status for item in variants}
    if "FAIL" in statuses:
        return "FAIL"
    if "SKIP" in statuses:
        return "SKIP"
    if "UNVERIFIED" in statuses or not statuses:
        return "UNVERIFIED"
    if statuses == {"PASS"}:
        return "PASS"
    return "UNVERIFIED"


def _support_limitations(gate: str) -> tuple[str, ...]:
    if gate == "not_supported":
        return ("strategy_not_supported",)
    if gate == "manual_live_gate":
        return ("manual_live_evidence_required",)
    if gate not in _SUPPORT_BY_GATE:
        return ("certification_gate_unknown",)
    return ()


def _require_unique(values: Sequence[object], *, code: str, label: str) -> None:
    if len(values) != len(set(values)):
        raise CapabilityDiscoveryError(code, f"Capability snapshot contains duplicate {label}.")


def require_authoring_authority(snapshot: CapabilitySnapshotProtocol) -> None:
    """Fail closed when the recipe authority could not be projected safely."""

    for raw_issue in snapshot.issues:
        issue = raw_issue.to_dict()
        if issue.get("entity_kind") in _AUTHORING_BLOCKING_ENTITY_KINDS:
            raise CapabilityDiscoveryError(
                str(issue.get("code") or "DPONE_CAPABILITY_AUTHORITY_INVALID"),
                "The configured authoring authority is invalid; fix it before scaffolding or planning.",
            )


__all__ = [
    "CapabilityDiscoveryError",
    "CapabilityDiscoveryService",
    "normalize_connector_ref",
    "normalize_route_ref",
    "require_authoring_authority",
]
