"""Actionable insight generation from readiness scores."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from dbt_ai_readiness.readiness_score.dimensions.documentation import DocumentationScore
from dbt_ai_readiness.readiness_score.dimensions.mcp import MCPScore
from dbt_ai_readiness.readiness_score.dimensions.semantic import SemanticScore
from dbt_ai_readiness.readiness_score.dimensions.testing import TestingScore


class InsightSeverity(str, Enum):
    CRITICAL = "critical"   # Blocks AI functionality
    HIGH = "high"           # Significantly degrades AI quality
    MEDIUM = "medium"       # Reduces AI reliability
    LOW = "low"             # Minor improvement opportunity


@dataclass
class Insight:
    severity: InsightSeverity
    dimension: str
    title: str
    detail: str
    action: str
    ai_impact: str  # What AI capability is blocked/degraded


def generate_insights(
    doc_score: DocumentationScore,
    test_score: TestingScore,
    semantic_score: SemanticScore,
    mcp_score: MCPScore,
) -> list[Insight]:
    """Generate prioritized, actionable insights from all dimension scores."""
    insights: list[Insight] = []

    # ------------------------------------------------------------------ MCP
    if not mcp_score.server_configured:
        insights.append(Insight(
            severity=InsightSeverity.CRITICAL,
            dimension="MCP Activation",
            title="dbt MCP Server is not configured",
            detail="No MCP server configuration was found for this project.",
            action=(
                "Run `dbt-ai-readiness mcp enable` to generate an MCP config, "
                "then add it to your AI tool (Claude Desktop, VS Code, Cursor)."
            ),
            ai_impact=(
                "AI agents and copilots cannot query dbt lineage, models, or metadata. "
                "All AI-assisted dbt workflows are unavailable."
            ),
        ))
    elif not mcp_score.cloud_credentials_present:
        insights.append(Insight(
            severity=InsightSeverity.CRITICAL,
            dimension="MCP Activation",
            title="dbt Cloud credentials are missing",
            detail=(
                f"Required environment variables not set: "
                f"{', '.join(mcp_score.missing_env_vars)}"
            ),
            action=(
                "Set DBT_TOKEN and DBT_HOST in your environment or MCP config. "
                "Get your token from dbt Cloud → Account Settings → API Tokens."
            ),
            ai_impact="MCP Server cannot authenticate with dbt Cloud. AI agents will fail to connect.",
        ))

    if mcp_score.missing_env_vars and mcp_score.cloud_credentials_present:
        insights.append(Insight(
            severity=InsightSeverity.LOW,
            dimension="MCP Activation",
            title="Optional MCP environment variables not configured",
            detail=f"Not set: {', '.join(mcp_score.missing_env_vars)}",
            action=(
                "Set DBT_ENVIRONMENT_ID and DBT_PROJECT_ID to scope MCP queries "
                "to a specific dbt Cloud environment."
            ),
            ai_impact="MCP may return results from multiple environments; scoping improves accuracy.",
        ))

    # ---------------------------------------------------------- Documentation
    if doc_score.model_coverage < 50:
        insights.append(Insight(
            severity=InsightSeverity.CRITICAL,
            dimension="Documentation",
            title=f"{len(doc_score.undocumented_models)} models lack descriptions",
            detail=(
                f"Only {doc_score.model_coverage:.0f}% of models have descriptions. "
                f"Undocumented: {', '.join(doc_score.undocumented_models[:5])}"
                + (" and more." if len(doc_score.undocumented_models) > 5 else ".")
            ),
            action=(
                "Run `dbt-ai-readiness autopilot --dry-run` to preview AI-generated "
                "documentation, then `dbt-ai-readiness autopilot` to apply it."
            ),
            ai_impact=(
                "AI agents cannot understand model purpose. Natural language queries, "
                "lineage explanations, and code generation will be inaccurate."
            ),
        ))
    elif doc_score.model_coverage < 80:
        insights.append(Insight(
            severity=InsightSeverity.HIGH,
            dimension="Documentation",
            title=f"{len(doc_score.undocumented_models)} models still need descriptions",
            detail=f"Model coverage: {doc_score.model_coverage:.0f}%. Target: 80%+.",
            action="Run `dbt-ai-readiness autopilot --models-only` to fill model gaps.",
            ai_impact="Incomplete context reduces AI answer accuracy for undocumented models.",
        ))

    if doc_score.column_coverage < 50:
        total_gaps = sum(
            len(cols) for cols in doc_score.undocumented_model_columns.values()
        )
        insights.append(Insight(
            severity=InsightSeverity.HIGH,
            dimension="Documentation",
            title=f"{total_gaps} columns lack descriptions across {len(doc_score.undocumented_model_columns)} models",
            detail=f"Column coverage: {doc_score.column_coverage:.0f}%. This is the largest AI readiness gap.",
            action=(
                "Run `dbt-ai-readiness autopilot --columns-only` to generate "
                "column-level docs. Column docs are critical for Semantic Layer queries."
            ),
            ai_impact=(
                "AI cannot answer questions about specific fields. "
                "Semantic Layer metric discovery and NL-to-SQL will miss column context."
            ),
        ))
    elif doc_score.column_coverage < 75:
        insights.append(Insight(
            severity=InsightSeverity.MEDIUM,
            dimension="Documentation",
            title=f"Column coverage is {doc_score.column_coverage:.0f}% — target 75%+",
            detail="Some models have well-documented columns but others have none.",
            action="Run `dbt-ai-readiness autopilot --columns-only` to fill remaining gaps.",
            ai_impact="Partial column docs lead to inconsistent AI query results.",
        ))

    # --------------------------------------------------------------- Testing
    if test_score.model_test_coverage < 50:
        insights.append(Insight(
            severity=InsightSeverity.HIGH,
            dimension="Testing",
            title=f"{len(test_score.untested_models)} models have no tests",
            detail=(
                f"Only {test_score.model_test_coverage:.0f}% of models are tested. "
                f"Untested: {', '.join(test_score.untested_models[:5])}"
                + (" and more." if len(test_score.untested_models) > 5 else ".")
            ),
            action=(
                "Run `dbt-ai-readiness autopilot --tests-only` to generate "
                "AI-recommended tests and apply them as patch YAML."
            ),
            ai_impact=(
                "AI-generated transformations and auto-builds cannot be validated. "
                "dbt Fusion Engine AI builds will lack safety assertions."
            ),
        ))
    elif test_score.untested_models:
        insights.append(Insight(
            severity=InsightSeverity.MEDIUM,
            dimension="Testing",
            title=f"{len(test_score.untested_models)} models still have no tests",
            detail=f"Model test coverage: {test_score.model_test_coverage:.0f}%.",
            action="Run `dbt-ai-readiness autopilot --tests-only` for remaining gaps.",
            ai_impact="Untested models reduce AI build confidence.",
        ))

    if test_score.quality_test_variety < 75:
        missing_types = {"not_null", "unique", "accepted_values", "relationships"} - set(
            test_score.test_type_counts.keys()
        )
        insights.append(Insight(
            severity=InsightSeverity.MEDIUM,
            dimension="Testing",
            title="Limited data quality test variety",
            detail=f"Missing test types: {', '.join(missing_types)}.",
            action=(
                "Add not_null, unique, accepted_values, and relationships tests. "
                "The AI Autopilot will recommend these based on your model SQL."
            ),
            ai_impact=(
                "Without diverse quality tests, AI-generated transforms cannot "
                "verify referential integrity or value constraints."
            ),
        ))

    # --------------------------------------------------------- Semantic Layer
    if semantic_score.semantic_model_coverage == 0 and semantic_score.metric_count == 0:
        insights.append(Insight(
            severity=InsightSeverity.HIGH,
            dimension="Semantic Layer",
            title="No Semantic Layer models or metrics defined",
            detail="The dbt Semantic Layer is not configured for this project.",
            action=(
                "Define semantic models for your mart-tier models (fct_, dim_) "
                "and create at least one MetricFlow metric. "
                "See: https://docs.getdbt.com/docs/use-dbt-semantic-layer/quickstart-sl"
            ),
            ai_impact=(
                "AI agents cannot answer business questions using natural language. "
                "MCP metric queries, NL-to-SQL, and AI dashboards are unavailable."
            ),
        ))
    elif semantic_score.semantic_model_coverage < 50:
        insights.append(Insight(
            severity=InsightSeverity.MEDIUM,
            dimension="Semantic Layer",
            title=f"{len(semantic_score.models_without_semantic)} mart-tier models not in Semantic Layer",
            detail=f"Coverage: {semantic_score.semantic_model_coverage:.0f}% of mart/fact/dim models.",
            action=(
                "Add semantic model definitions for: "
                + ", ".join(semantic_score.models_without_semantic[:5])
            ),
            ai_impact="AI metric queries are limited to a subset of your data.",
        ))

    if not semantic_score.has_entities and semantic_score.semantic_model_coverage > 0:
        insights.append(Insight(
            severity=InsightSeverity.MEDIUM,
            dimension="Semantic Layer",
            title="Semantic models have no entities defined",
            detail="Entities enable joins between semantic models for multi-dimensional queries.",
            action="Add primary_key and foreign_key entities to your semantic models.",
            ai_impact="AI cannot perform cross-model aggregations or dimensional analysis.",
        ))

    if semantic_score.metric_count == 0 and semantic_score.semantic_model_coverage > 0:
        insights.append(Insight(
            severity=InsightSeverity.HIGH,
            dimension="Semantic Layer",
            title="No metrics defined despite having semantic models",
            detail="Semantic models exist but no metrics have been created.",
            action="Define at least one MetricFlow metric to enable AI-powered business queries.",
            ai_impact="AI agents cannot answer KPI questions without metrics.",
        ))

    # Sort: CRITICAL → HIGH → MEDIUM → LOW
    order = {
        InsightSeverity.CRITICAL: 0,
        InsightSeverity.HIGH: 1,
        InsightSeverity.MEDIUM: 2,
        InsightSeverity.LOW: 3,
    }
    insights.sort(key=lambda i: order[i.severity])
    return insights
