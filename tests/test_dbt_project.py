"""Tests for DbtProject artifact parsing."""

import json
from pathlib import Path

import pytest

from dbt_ai_readiness.dbt_project import DbtProject


FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def project(tmp_path):
    """Create a temporary dbt project with fixture manifest."""
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    manifest = json.loads((FIXTURES / "manifest_full.json").read_text())
    (target_dir / "manifest.json").write_text(json.dumps(manifest))
    import shutil
    shutil.copy(FIXTURES / "dbt_project.yml", tmp_path / "dbt_project.yml")
    return DbtProject(tmp_path)


def test_get_models_returns_only_models(project):
    models = project.get_models()
    assert all(m.resource_type in ("model", "snapshot") for m in models)
    assert len(models) == 3


def test_model_columns_parsed(project):
    models = project.get_models()
    stg_orders = next(m for m in models if m.name == "stg_orders")
    assert len(stg_orders.columns) == 4
    assert stg_orders.columns["order_id"].is_documented is True
    assert stg_orders.columns["status"].is_documented is False


def test_model_documentation_flags(project):
    models = project.get_models()
    stg_orders = next(m for m in models if m.name == "stg_orders")
    assert stg_orders.is_documented is True

    stg_customers = next(m for m in models if m.name == "stg_customers")
    assert stg_customers.is_documented is False


def test_column_doc_coverage(project):
    models = project.get_models()
    stg_orders = next(m for m in models if m.name == "stg_orders")
    assert stg_orders.documented_column_count == 3
    assert stg_orders.total_column_count == 4
    assert stg_orders.column_doc_coverage == pytest.approx(0.75)


def test_get_tests_parsed(project):
    tests = project.get_tests()
    assert len(tests) == 3
    test_names = {t.test_name for t in tests}
    assert "not_null" in test_names
    assert "unique" in test_names
    assert "accepted_values" in test_names


def test_tests_attached_to_correct_model(project):
    tests = project.get_tests()
    stg_orders_tests = [t for t in tests if t.attached_node == "model.jaffle_shop.stg_orders"]
    assert len(stg_orders_tests) == 2


def test_get_semantic_models(project):
    semantic_models = project.get_semantic_models()
    assert len(semantic_models) == 1
    sm = semantic_models[0]
    assert sm.name == "orders"
    assert len(sm.entities) == 2
    assert len(sm.measures) == 2


def test_get_metrics(project):
    metrics = project.get_metrics()
    assert len(metrics) == 2
    metric_names = {m.name for m in metrics}
    assert "order_count" in metric_names
    assert "revenue" in metric_names


def test_get_sources(project):
    sources = project.get_sources()
    assert len(sources) == 1
    assert sources[0].name == "orders"


def test_project_info(project):
    info = project.get_project_info()
    assert info.name == "jaffle_shop"
    assert info.version == "1.0.0"
