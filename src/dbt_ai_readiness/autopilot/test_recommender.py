"""AI-powered test recommender using Claude claude-sonnet-4-6."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import anthropic

from dbt_ai_readiness.dbt_project import DbtNode, DbtTest

MODEL = "claude-sonnet-4-6"

_TEST_SYSTEM_PROMPT = """\
You are a dbt data quality expert. Given a dbt model's SQL and column information,
recommend dbt generic tests to ensure data quality and reliability.

Rules:
- Recommend only dbt built-in generic tests: not_null, unique, accepted_values, relationships
- For accepted_values: only recommend if you can infer likely values from column names/SQL
- For relationships: only recommend if the SQL clearly references another table/model
- Prioritize not_null for primary keys and required fields
- Prioritize unique for primary keys and natural keys
- Be conservative — only recommend tests you're confident are appropriate
- Output valid JSON only — no markdown, no extra text.

Output format:
{
  "model_tests": ["unique", "not_null"],  // model-level tests (rare)
  "column_tests": {
    "column_name": [
      {"test": "not_null"},
      {"test": "unique"},
      {"test": "accepted_values", "values": ["value1", "value2"]},
      {"test": "relationships", "to": "ref('other_model')", "field": "id"}
    ]
  }
}
"""

_TEST_PROMPT_TEMPLATE = """\
dbt model name: {model_name}
Model description: {model_description}

SQL:
```sql
{sql}
```

Columns:
{columns}

Existing tests (do not re-recommend these):
{existing_tests}

Recommend dbt generic tests for this model. Focus on data quality that matters for
AI-powered queries: primary keys must be unique and not_null, status fields should
have accepted_values, foreign keys should have relationships tests.

Output JSON only.
"""


@dataclass
class ColumnTestRecommendation:
    test: str
    values: list[str] = field(default_factory=list)
    to: str = ""
    field: str = ""

    def to_yaml_dict(self) -> dict:
        d: dict = {"name": self.test}
        if self.values:
            d["config"] = {}
            d = {"name": "accepted_values", "config": {"values": self.values}}
        if self.to and self.field:
            d = {
                "name": "relationships",
                "config": {"to": self.to, "field": self.field},
            }
        return d


@dataclass
class TestRecommendation:
    model_name: str
    model_tests: list[str] = field(default_factory=list)
    column_tests: dict[str, list[ColumnTestRecommendation]] = field(default_factory=dict)
    tokens_used: int = 0

    def to_patch_yaml(self) -> dict:
        """Render as a dbt schema.yml patch dict."""
        columns = []
        for col_name, tests in self.column_tests.items():
            col_entry: dict = {"name": col_name}
            if tests:
                col_entry["data_tests"] = [t.to_yaml_dict() for t in tests]
            columns.append(col_entry)

        model_entry: dict = {"name": self.model_name}
        if self.model_tests:
            model_entry["data_tests"] = [{"name": t} for t in self.model_tests]
        if columns:
            model_entry["columns"] = columns

        return model_entry


class TestRecommender:
    """Uses Claude to recommend dbt tests for models."""

    def __init__(self, api_key: str | None = None):
        self.client = anthropic.Anthropic(api_key=api_key)

    def recommend(
        self,
        model: DbtNode,
        existing_tests: list[DbtTest] | None = None,
    ) -> TestRecommendation:
        """Generate test recommendations for a dbt model."""
        existing_tests = existing_tests or []
        existing_by_column: dict[str, list[str]] = {}
        for test in existing_tests:
            if test.column_name:
                existing_by_column.setdefault(test.column_name, []).append(test.test_name)

        columns_text = "\n".join(
            f"- {col.name}"
            + (f" ({col.data_type})" if col.data_type else "")
            + (f": {col.description}" if col.description else "")
            for col in model.columns.values()
        ) or "(columns not listed in manifest — infer from SQL)"

        existing_text = "\n".join(
            f"- {col}: {', '.join(tests)}"
            for col, tests in existing_by_column.items()
        ) or "None"

        prompt = _TEST_PROMPT_TEMPLATE.format(
            model_name=model.name,
            model_description=model.description or "(no description)",
            sql=model.raw_code[:4000] if model.raw_code else "(SQL not available)",
            columns=columns_text,
            existing_tests=existing_text,
        )

        response = self.client.messages.create(
            model=MODEL,
            max_tokens=2000,
            system=_TEST_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return TestRecommendation(
                model_name=model.name,
                tokens_used=response.usage.input_tokens + response.usage.output_tokens,
            )

        column_tests: dict[str, list[ColumnTestRecommendation]] = {}
        for col_name, tests in data.get("column_tests", {}).items():
            recs = []
            for t in tests:
                recs.append(
                    ColumnTestRecommendation(
                        test=t.get("test", ""),
                        values=t.get("values", []),
                        to=t.get("to", ""),
                        field=t.get("field", ""),
                    )
                )
            column_tests[col_name] = recs

        return TestRecommendation(
            model_name=model.name,
            model_tests=data.get("model_tests", []),
            column_tests=column_tests,
            tokens_used=response.usage.input_tokens + response.usage.output_tokens,
        )

    def recommend_batch(
        self,
        models: list[DbtNode],
        tests_by_model: dict[str, list[DbtTest]] | None = None,
        on_progress: callable | None = None,
    ) -> list[TestRecommendation]:
        tests_by_model = tests_by_model or {}
        results = []
        for i, model in enumerate(models):
            if on_progress:
                on_progress(i, len(models), model.name)
            result = self.recommend(model, tests_by_model.get(model.unique_id, []))
            results.append(result)
        return results
