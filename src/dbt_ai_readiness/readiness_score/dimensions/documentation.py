"""Documentation coverage scoring dimension."""

from __future__ import annotations

from dataclasses import dataclass, field

from dbt_ai_readiness.dbt_project import DbtProject


@dataclass
class DocumentationScore:
    raw: float  # 0-100
    model_coverage: float  # % of models with descriptions
    column_coverage: float  # % of columns with descriptions
    undocumented_models: list[str] = field(default_factory=list)
    undocumented_model_columns: dict[str, list[str]] = field(default_factory=dict)


def score_documentation(project: DbtProject) -> DocumentationScore:
    """
    Score documentation coverage across models and columns.

    Weighting:
      - Model description coverage: 40%
      - Column description coverage: 60%

    Only counts models owned by the project (excludes imported packages).
    """
    project_info = project.get_project_info()
    models = [
        m for m in project.get_models()
        if m.package_name == project_info.name
    ]

    if not models:
        return DocumentationScore(
            raw=0.0,
            model_coverage=0.0,
            column_coverage=0.0,
        )

    # Model-level coverage
    documented_models = [m for m in models if m.is_documented]
    undocumented_models = [m.name for m in models if not m.is_documented]
    model_coverage = len(documented_models) / len(models) * 100

    # Column-level coverage
    total_columns = 0
    documented_columns = 0
    undocumented_model_columns: dict[str, list[str]] = {}

    for model in models:
        if not model.columns:
            continue
        total_columns += model.total_column_count
        documented_columns += model.documented_column_count
        missing = [
            col.name for col in model.columns.values() if not col.is_documented
        ]
        if missing:
            undocumented_model_columns[model.name] = missing

    column_coverage = (
        documented_columns / total_columns * 100 if total_columns > 0 else 0.0
    )

    raw = (model_coverage * 0.40) + (column_coverage * 0.60)

    return DocumentationScore(
        raw=round(raw, 1),
        model_coverage=round(model_coverage, 1),
        column_coverage=round(column_coverage, 1),
        undocumented_models=undocumented_models,
        undocumented_model_columns=undocumented_model_columns,
    )
