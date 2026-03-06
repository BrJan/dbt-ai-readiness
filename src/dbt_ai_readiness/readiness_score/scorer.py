"""Main AI Readiness Scorer — aggregates all dimension scores."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from dbt_ai_readiness.dbt_project import DbtProject
from dbt_ai_readiness.readiness_score.dimensions.documentation import (
    DocumentationScore,
    score_documentation,
)
from dbt_ai_readiness.readiness_score.dimensions.mcp import MCPScore, score_mcp
from dbt_ai_readiness.readiness_score.dimensions.semantic import (
    SemanticScore,
    score_semantic,
)
from dbt_ai_readiness.readiness_score.dimensions.testing import (
    TestingScore,
    score_testing,
)
from dbt_ai_readiness.readiness_score.insights import Insight, generate_insights

# Dimension weights — must sum to 1.0
WEIGHTS = {
    "documentation": 0.30,
    "testing": 0.25,
    "semantic": 0.25,
    "mcp": 0.20,
}


def _grade(score: float) -> str:
    if score >= 90:
        return "Excellent"
    if score >= 75:
        return "Good"
    if score >= 50:
        return "Fair"
    if score >= 25:
        return "Poor"
    return "Critical"


@dataclass
class ReadinessScore:
    overall: float
    grade: str
    documentation: DocumentationScore
    testing: TestingScore
    semantic: SemanticScore
    mcp: MCPScore
    insights: list[Insight] = field(default_factory=list)
    project_name: str = ""
    model_count: int = 0
    test_count: int = 0

    def as_dict(self) -> dict:
        return {
            "project_name": self.project_name,
            "overall_score": self.overall,
            "grade": self.grade,
            "model_count": self.model_count,
            "test_count": self.test_count,
            "dimensions": {
                "documentation": {
                    "score": self.documentation.raw,
                    "model_coverage": self.documentation.model_coverage,
                    "column_coverage": self.documentation.column_coverage,
                    "weight": WEIGHTS["documentation"],
                },
                "testing": {
                    "score": self.testing.raw,
                    "model_test_coverage": self.testing.model_test_coverage,
                    "column_test_coverage": self.testing.column_test_coverage,
                    "quality_test_variety": self.testing.quality_test_variety,
                    "weight": WEIGHTS["testing"],
                },
                "semantic": {
                    "score": self.semantic.raw,
                    "semantic_model_coverage": self.semantic.semantic_model_coverage,
                    "metric_count": self.semantic.metric_count,
                    "weight": WEIGHTS["semantic"],
                },
                "mcp": {
                    "score": self.mcp.raw,
                    "server_configured": self.mcp.server_configured,
                    "cloud_credentials_present": self.mcp.cloud_credentials_present,
                    "weight": WEIGHTS["mcp"],
                },
            },
            "insights": [
                {
                    "severity": i.severity.value,
                    "dimension": i.dimension,
                    "title": i.title,
                    "action": i.action,
                    "ai_impact": i.ai_impact,
                }
                for i in self.insights
            ],
        }


class ReadinessScorer:
    """Orchestrates all scoring dimensions and produces a ReadinessScore."""

    def __init__(self, project: DbtProject):
        self.project = project

    def score(self) -> ReadinessScore:
        project_info = self.project.get_project_info()
        models = self.project.get_models()
        tests = self.project.get_tests()

        doc_score = score_documentation(self.project)
        test_score = score_testing(self.project)
        semantic_score = score_semantic(self.project)
        mcp_score = score_mcp(project_dir=self.project.project_dir)

        overall = round(
            doc_score.raw * WEIGHTS["documentation"]
            + test_score.raw * WEIGHTS["testing"]
            + semantic_score.raw * WEIGHTS["semantic"]
            + mcp_score.raw * WEIGHTS["mcp"],
            1,
        )

        insights = generate_insights(doc_score, test_score, semantic_score, mcp_score)

        return ReadinessScore(
            overall=overall,
            grade=_grade(overall),
            documentation=doc_score,
            testing=test_score,
            semantic=semantic_score,
            mcp=mcp_score,
            insights=insights,
            project_name=project_info.name,
            model_count=len([m for m in models if m.package_name == project_info.name]),
            test_count=len(tests),
        )
