"""Benchmark output renderers."""

from tools.oss_benchmark.renderers.governance import (
    render_governance_compliance_section,
    render_governance_compliance_svg,
)
from tools.oss_benchmark.renderers.markdown import render_file_table, render_markdown
from tools.oss_benchmark.renderers.operability import render_operability_tco_section, render_operability_tco_svg
from tools.oss_benchmark.renderers.reliability import (
    render_operational_reliability_section,
    render_operational_reliability_svg,
)
from tools.oss_benchmark.renderers.security import (
    render_security_supply_chain_section,
    render_security_supply_chain_svg,
)
from tools.oss_benchmark.renderers.svg import (
    render_architecture_risk_svg,
    render_feature_parity_svg,
    render_hotspots_svg,
    render_loc_sloc_svg,
    render_quadrant_svg,
    render_quality_trend_svg,
    render_scorecard_svg,
)
from tools.oss_benchmark.trust_center import render_trust_center_badge_svg, render_trust_center_markdown

__all__ = (
    "render_architecture_risk_svg",
    "render_feature_parity_svg",
    "render_file_table",
    "render_governance_compliance_section",
    "render_governance_compliance_svg",
    "render_hotspots_svg",
    "render_loc_sloc_svg",
    "render_markdown",
    "render_operability_tco_section",
    "render_operability_tco_svg",
    "render_operational_reliability_section",
    "render_operational_reliability_svg",
    "render_quality_trend_svg",
    "render_quadrant_svg",
    "render_scorecard_svg",
    "render_security_supply_chain_section",
    "render_security_supply_chain_svg",
    "render_trust_center_badge_svg",
    "render_trust_center_markdown",
)
