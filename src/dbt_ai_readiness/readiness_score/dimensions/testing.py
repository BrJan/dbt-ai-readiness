"""Test coverage scoring dimension."""

from __future__ import annotations

from dataclasses import dataclass, field

from dbt_ai_readiness.dbt_project import DbtProject

# Tests that signal data quality intent — critical for AI reliability
QUALITY_TESTS = {"not_null", "unique", "accepted_values", "relationships"}


@dataclass
class TestingScore:
    raw: float  # 0-100
    model_test_coverage: float  # % of models with >= 1 test
    column_test_coverage: float  # % of columns with >= 1 test (among documented cols)
    quality_test_variety: float  # % of QUALITY_TESTS in use (0-100)
    untested_models: list[str] = field(default_factory=list)
    test_type_counts: dict[str, int] = field(default_factory=dict)


def score_testing(project: DbtProject) -> TestingScore:
    """
    Score test coverage across models and columns.

    Weighting:
      - Model test coverage (at least 1 test): 50%
      - Column test coverage:                  30%
      - Quality test variety:                  20%
    """
    project_info = project.get_project_info()
    models = [
        m for m in project.get_models()
        if m.package_name == project_info.name
    ]
    tests = project.get_tests()

    if not models:
        return TestingScore(
            raw=0.0,
            model_test_coverage=0.0,
            column_test_coverage=0.0,
            quality_test_variety=0.0,
        )

    # Map tests to their attached model
    model_ids = {m.unique_id for m in models}
    tests_by_model: dict[str, list] = {m.unique_id: [] for m in models}
    column_tests: dict[str, set[str]] = {}  # model_uid -> set of tested column names
    test_type_counts: dict[str, int] = {}

    for test in tests:
        if test.attached_node not in model_ids:
            continue
        tests_by_model[test.attached_node].append(test)
        test_type_counts[test.test_name] = test_type_counts.get(test.test_name, 0) + 1
        if test.column_name:
            cols = column_tests.setdefault(test.attached_node, set())
            cols.add(test.column_name)

    # Model test coverage
    tested_model_ids = {uid for uid, t in tests_by_model.items() if t}
    model_test_coverage = len(tested_model_ids) / len(models) * 100
    untested_models = [
        m.name for m in models if m.unique_id not in tested_model_ids
    ]

    # Column test coverage (across all columns that exist in the manifest)
    total_columns = sum(m.total_column_count for m in models)
    tested_columns = sum(len(v) for v in column_tests.values())
    column_test_coverage = (
        tested_columns / total_columns * 100 if total_columns > 0 else 0.0
    )

    # Quality test variety: which of the 4 key test types are in use?
    used_quality_tests = QUALITY_TESTS & set(test_type_counts.keys())
    quality_test_variety = len(used_quality_tests) / len(QUALITY_TESTS) * 100

    raw = (
        model_test_coverage * 0.50
        + column_test_coverage * 0.30
        + quality_test_variety * 0.20
    )

    return TestingScore(
        raw=round(raw, 1),
        model_test_coverage=round(model_test_coverage, 1),
        column_test_coverage=round(column_test_coverage, 1),
        quality_test_variety=round(quality_test_variety, 1),
        untested_models=untested_models,
        test_type_counts=test_type_counts,
    )
