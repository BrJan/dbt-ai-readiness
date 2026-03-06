"""Tests for actionable insight generation."""

import pytest

from dbt_ai_readiness.readiness_score.dimensions.documentation import DocumentationScore
from dbt_ai_readiness.readiness_score.dimensions.testing import TestingScore
from dbt_ai_readiness.readiness_score.dimensions.semantic import SemanticScore
from dbt_ai_readiness.readiness_score.dimensions.mcp import MCPScore
from dbt_ai_readiness.readiness_score.insights import generate_insights, InsightSeverity


def _make_scores(
    model_coverage=100.0,
    column_coverage=100.0,
    model_test_coverage=100.0,
    column_test_coverage=100.0,
    quality_test_variety=100.0,
    semantic_coverage=100.0,
    metric_count=5,
    has_entities=True,
    has_measures=True,
    mcp_configured=True,
    mcp_credentials=True,
):
    doc = DocumentationScore(
        raw=model_coverage * 0.4 + column_coverage * 0.6,
        model_coverage=model_coverage,
        column_coverage=column_coverage,
    )
    tst = TestingScore(
        raw=model_test_coverage * 0.5 + column_test_coverage * 0.3 + quality_test_variety * 0.2,
        model_test_coverage=model_test_coverage,
        column_test_coverage=column_test_coverage,
        quality_test_variety=quality_test_variety,
    )
    sem = SemanticScore(
        raw=min(semantic_coverage * 0.5 + (20 if metric_count else 0) + (15 if has_entities else 0) + (15 if has_measures else 0), 100),
        semantic_model_coverage=semantic_coverage,
        metric_count=metric_count,
        has_entities=has_entities,
        has_measures=has_measures,
    )
    mcp = MCPScore(
        raw=100.0 if mcp_configured and mcp_credentials else 0.0,
        server_configured=mcp_configured,
        cloud_credentials_present=mcp_credentials,
    )
    return doc, tst, sem, mcp


def test_no_insights_when_fully_ready():
    doc, tst, sem, mcp = _make_scores()
    insights = generate_insights(doc, tst, sem, mcp)
    assert len(insights) == 0


def test_critical_mcp_not_configured():
    doc, tst, sem, mcp = _make_scores(mcp_configured=False, mcp_credentials=False)
    insights = generate_insights(doc, tst, sem, mcp)
    critical = [i for i in insights if i.severity == InsightSeverity.CRITICAL and "MCP" in i.dimension]
    assert len(critical) >= 1


def test_critical_doc_coverage_below_50():
    doc, tst, sem, mcp = _make_scores(model_coverage=30.0)
    insights = generate_insights(doc, tst, sem, mcp)
    doc_critical = [i for i in insights if i.severity == InsightSeverity.CRITICAL and i.dimension == "Documentation"]
    assert len(doc_critical) == 1


def test_high_severity_no_semantic_layer():
    doc, tst, sem, mcp = _make_scores(semantic_coverage=0.0, metric_count=0, has_entities=False, has_measures=False)
    insights = generate_insights(doc, tst, sem, mcp)
    sem_high = [i for i in insights if i.severity == InsightSeverity.HIGH and "Semantic" in i.dimension]
    assert len(sem_high) >= 1


def test_insights_sorted_critical_first():
    doc, tst, sem, mcp = _make_scores(
        model_coverage=20.0,
        mcp_configured=False,
        mcp_credentials=False,
        semantic_coverage=0.0,
        metric_count=0,
        has_entities=False,
        has_measures=False,
    )
    insights = generate_insights(doc, tst, sem, mcp)
    assert insights[0].severity == InsightSeverity.CRITICAL


def test_insight_has_all_fields():
    doc, tst, sem, mcp = _make_scores(mcp_configured=False, mcp_credentials=False)
    insights = generate_insights(doc, tst, sem, mcp)
    for insight in insights:
        assert insight.title
        assert insight.action
        assert insight.ai_impact
        assert insight.dimension
        assert insight.severity in InsightSeverity


def test_missing_credential_insight():
    doc, tst, sem, mcp = _make_scores(mcp_configured=True, mcp_credentials=False)
    mcp.missing_env_vars = ["DBT_TOKEN", "DBT_HOST"]
    insights = generate_insights(doc, tst, sem, mcp)
    cred_insights = [i for i in insights if "credentials" in i.title.lower()]
    assert len(cred_insights) >= 1
