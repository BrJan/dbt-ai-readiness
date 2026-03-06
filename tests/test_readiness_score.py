"""Tests for the AI Readiness scoring dimensions and scorer."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dbt_ai_readiness.dbt_project import DbtProject
from dbt_ai_readiness.readiness_score.dimensions.documentation import score_documentation
from dbt_ai_readiness.readiness_score.dimensions.testing import score_testing
from dbt_ai_readiness.readiness_score.dimensions.semantic import score_semantic
from dbt_ai_readiness.readiness_score.dimensions.mcp import score_mcp
from dbt_ai_readiness.readiness_score.scorer import ReadinessScorer


FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def project(tmp_path):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    manifest = json.loads((FIXTURES / "manifest_full.json").read_text())
    (target_dir / "manifest.json").write_text(json.dumps(manifest))
    import shutil
    shutil.copy(FIXTURES / "dbt_project.yml", tmp_path / "dbt_project.yml")
    return DbtProject(tmp_path)


# ---------------------------------------------------------------- Documentation
class TestDocumentationScore:
    def test_partial_documentation(self, project):
        score = score_documentation(project)
        # stg_orders: documented, 75% column coverage
        # stg_customers: undocumented, 0% column coverage
        # fct_orders: documented, 100% column coverage
        assert score.model_coverage == pytest.approx(66.7, abs=0.5)
        assert len(score.undocumented_models) == 1
        assert "stg_customers" in score.undocumented_models

    def test_column_coverage_calculation(self, project):
        score = score_documentation(project)
        # stg_orders: 3/4 documented
        # stg_customers: 0/4 documented
        # fct_orders: 5/5 documented
        # Total: 8/13 = ~61.5%
        assert score.column_coverage == pytest.approx(61.5, abs=1.0)

    def test_raw_score_is_weighted_average(self, project):
        score = score_documentation(project)
        expected = score.model_coverage * 0.40 + score.column_coverage * 0.60
        assert score.raw == pytest.approx(expected, abs=0.1)

    def test_no_models_returns_zero(self, tmp_path):
        empty_manifest = {
            "metadata": {"project_name": "empty"},
            "nodes": {}, "sources": {}, "semantic_models": {}, "metrics": {}
        }
        target = tmp_path / "target"
        target.mkdir()
        (target / "manifest.json").write_text(json.dumps(empty_manifest))
        (tmp_path / "dbt_project.yml").write_text("name: empty\nversion: '1.0.0'\n")
        p = DbtProject(tmp_path)
        score = score_documentation(p)
        assert score.raw == 0.0


# ---------------------------------------------------------------- Testing
class TestTestingScore:
    def test_coverage_for_fixture_project(self, project):
        score = score_testing(project)
        # stg_orders: 2 tests -> tested
        # stg_customers: 0 tests -> untested
        # fct_orders: 1 test -> tested
        assert score.model_test_coverage == pytest.approx(66.7, abs=0.5)
        assert "stg_customers" in score.untested_models

    def test_quality_test_variety(self, project):
        score = score_testing(project)
        # not_null, unique, accepted_values — missing: relationships
        assert score.quality_test_variety == pytest.approx(75.0, abs=0.1)

    def test_test_type_counts(self, project):
        score = score_testing(project)
        assert score.test_type_counts.get("not_null", 0) == 1
        assert score.test_type_counts.get("unique", 0) == 1
        assert score.test_type_counts.get("accepted_values", 0) == 1


# ---------------------------------------------------------------- Semantic
class TestSemanticScore:
    def test_semantic_model_coverage(self, project):
        score = score_semantic(project)
        # fct_orders is a mart-tier model covered by "orders" semantic model
        # stg_* models are not mart-tier
        assert score.semantic_model_coverage == pytest.approx(100.0, abs=1.0)

    def test_metric_count(self, project):
        score = score_semantic(project)
        assert score.metric_count == 2

    def test_entities_and_measures(self, project):
        score = score_semantic(project)
        assert score.has_entities is True
        assert score.has_measures is True

    def test_full_score_near_max(self, project):
        score = score_semantic(project)
        # Full coverage + 2 metrics + entities + measures = 100
        assert score.raw == pytest.approx(100.0, abs=1.0)


# ---------------------------------------------------------------- MCP
class TestMCPScore:
    def test_no_config_no_env(self, tmp_path):
        with (
            patch.dict("os.environ", {}, clear=True),
            patch(
                "dbt_ai_readiness.readiness_score.dimensions.mcp._MCP_CONFIG_CANDIDATES",
                [],
            ),
        ):
            score = score_mcp(project_dir=tmp_path)
        assert score.server_configured is False
        assert score.cloud_credentials_present is False
        assert score.raw < 50

    def test_credentials_present_no_config(self, tmp_path):
        env = {"DBT_HOST": "https://myaccount.us1.dbt.com", "DBT_TOKEN": "secret"}
        with patch.dict("os.environ", env, clear=True):
            score = score_mcp(project_dir=tmp_path)
        assert score.cloud_credentials_present is True
        assert score.raw >= 35

    def test_config_file_found(self, tmp_path):
        config = {
            "mcpServers": {
                "dbt": {
                    "command": "uvx",
                    "args": ["dbt-mcp"],
                    "env": {"DBT_HOST": "https://x.dbt.com", "DBT_TOKEN": "tok"}
                }
            }
        }
        (tmp_path / "mcp.json").write_text(json.dumps(config))
        env = {"DBT_HOST": "https://x.dbt.com", "DBT_TOKEN": "tok"}
        with patch.dict("os.environ", env, clear=True):
            score = score_mcp(project_dir=tmp_path)
        assert score.server_configured is True
        assert str(tmp_path / "mcp.json") in score.config_paths_found


# ---------------------------------------------------------------- Overall Scorer
class TestReadinessScorer:
    def test_overall_score_is_bounded(self, project):
        with patch.dict("os.environ", {}, clear=True):
            scorer = ReadinessScorer(project)
            result = scorer.score()
        assert 0 <= result.overall <= 100

    def test_grade_assigned(self, project):
        with patch.dict("os.environ", {}, clear=True):
            scorer = ReadinessScorer(project)
            result = scorer.score()
        assert result.grade in {"Excellent", "Good", "Fair", "Poor", "Critical"}

    def test_insights_generated(self, project):
        with patch.dict("os.environ", {}, clear=True):
            scorer = ReadinessScorer(project)
            result = scorer.score()
        # At least one insight about MCP not being configured
        assert len(result.insights) > 0
        mcp_insights = [i for i in result.insights if i.dimension == "MCP Activation"]
        assert len(mcp_insights) > 0

    def test_as_dict_structure(self, project):
        with patch.dict("os.environ", {}, clear=True):
            scorer = ReadinessScorer(project)
            result = scorer.score()
        d = result.as_dict()
        assert "overall_score" in d
        assert "dimensions" in d
        assert set(d["dimensions"].keys()) == {"documentation", "testing", "semantic", "mcp"}
        assert "insights" in d

    def test_project_metadata_in_result(self, project):
        with patch.dict("os.environ", {}, clear=True):
            scorer = ReadinessScorer(project)
            result = scorer.score()
        assert result.project_name == "jaffle_shop"
        assert result.model_count == 3
        assert result.test_count == 3
