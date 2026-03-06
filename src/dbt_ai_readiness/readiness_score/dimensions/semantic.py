"""Semantic Layer scoring dimension."""

from __future__ import annotations

from dataclasses import dataclass, field

from dbt_ai_readiness.dbt_project import DbtProject


@dataclass
class SemanticScore:
    raw: float  # 0-100
    semantic_model_coverage: float  # % of mart/final models exposed as semantic models
    metric_count: int
    has_entities: bool
    has_measures: bool
    models_without_semantic: list[str] = field(default_factory=list)


def score_semantic(project: DbtProject) -> SemanticScore:
    """
    Score Semantic Layer readiness.

    Weighting:
      - Semantic model coverage of mart-tier models: 50%
      - At least 1 metric defined:                   20%
      - Entities present across semantic models:      15%
      - Measures present across semantic models:      15%

    Mart-tier models are identified by convention: names starting with
    'fct_', 'dim_', 'mart_', or tagged with 'mart'.
    """
    project_info = project.get_project_info()
    models = [
        m for m in project.get_models()
        if m.package_name == project_info.name
    ]
    semantic_models = project.get_semantic_models()
    metrics = project.get_metrics()

    # Identify mart-tier models by naming convention or tag
    mart_models = [
        m for m in models
        if (
            m.name.startswith(("fct_", "dim_", "mart_", "rpt_", "agg_"))
            or "mart" in m.tags
            or "semantic" in m.tags
        )
    ]

    # Fall back to all models if no mart-tier naming found
    reference_models = mart_models if mart_models else models

    semantic_model_names = {sm.name for sm in semantic_models}
    # Also match by the 'model' field (ref() pattern like "ref('my_model')")
    import re as _re
    _ref_pattern = _re.compile(r"""ref\(\s*['"](.+?)['"]\s*\)""")

    semantic_model_refs = set()
    for sm in semantic_models:
        ref = sm.model
        match = _ref_pattern.match(ref)
        if match:
            ref = match.group(1)
        semantic_model_refs.add(ref)

    covered = [
        m for m in reference_models
        if m.name in semantic_model_names or m.name in semantic_model_refs
    ]
    uncovered = [
        m.name for m in reference_models
        if m.name not in semantic_model_names and m.name not in semantic_model_refs
    ]

    coverage_pct = (
        len(covered) / len(reference_models) * 100 if reference_models else 0.0
    )

    has_entities = any(sm.entities for sm in semantic_models)
    has_measures = any(sm.measures for sm in semantic_models)
    metric_count = len(metrics)

    raw = (
        coverage_pct * 0.50
        + (min(metric_count, 1) * 20)  # 20 points for having >= 1 metric
        + (15 if has_entities else 0)
        + (15 if has_measures else 0)
    )
    raw = min(raw, 100.0)

    return SemanticScore(
        raw=round(raw, 1),
        semantic_model_coverage=round(coverage_pct, 1),
        metric_count=metric_count,
        has_entities=has_entities,
        has_measures=has_measures,
        models_without_semantic=uncovered,
    )
