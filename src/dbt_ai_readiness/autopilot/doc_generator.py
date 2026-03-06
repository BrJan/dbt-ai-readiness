"""AI-powered documentation generator using Claude claude-sonnet-4-6."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import anthropic

from dbt_ai_readiness.dbt_project import DbtColumn, DbtNode

MODEL = "claude-sonnet-4-6"

_DOC_SYSTEM_PROMPT = """\
You are a dbt documentation expert. Given a dbt model's SQL code and column names,
generate clear, concise, business-friendly documentation.

Rules:
- Model descriptions: 1-2 sentences. Explain business purpose, not SQL mechanics.
- Column descriptions: 1 sentence. Explain what the value means, not the SQL type.
- Use present tense. No jargon. Write for a business analyst audience.
- Do not mention SQL, CTEs, joins, or technical implementation details.
- Output valid JSON only — no markdown, no extra text.
"""

_COLUMN_PROMPT_TEMPLATE = """\
dbt model name: {model_name}
Model description: {model_description}

SQL:
```sql
{sql}
```

Columns in this model:
{columns}

Generate a JSON object with:
1. "model_description": A 1-2 sentence business description of this model (if not already provided).
2. "columns": An object mapping each column name to a 1-sentence description.

Only include columns that are missing descriptions (listed above).
Output JSON only.
"""


@dataclass
class GeneratedDocs:
    model_name: str
    model_description: str = ""
    column_descriptions: dict[str, str] = field(default_factory=dict)
    tokens_used: int = 0


class DocGenerator:
    """Uses Claude to generate model and column documentation."""

    def __init__(self, api_key: str | None = None):
        self.client = anthropic.Anthropic(api_key=api_key)

    def generate(
        self,
        model: DbtNode,
        regenerate_model_desc: bool = False,
        regenerate_all_columns: bool = False,
    ) -> GeneratedDocs:
        """
        Generate missing documentation for a dbt model.

        Args:
            model: The dbt model node.
            regenerate_model_desc: Force regeneration even if description exists.
            regenerate_all_columns: Regenerate all column docs, not just missing ones.
        """
        needs_model_desc = regenerate_model_desc or not model.is_documented
        missing_columns = [
            col for col in model.columns.values()
            if regenerate_all_columns or not col.is_documented
        ]

        if not needs_model_desc and not missing_columns:
            return GeneratedDocs(
                model_name=model.name,
                model_description=model.description,
            )

        columns_text = "\n".join(
            f"- {col.name}" + (f" ({col.data_type})" if col.data_type else "")
            for col in missing_columns
        ) or "(no specific columns — generate docs for all columns found in the SQL)"

        prompt = _COLUMN_PROMPT_TEMPLATE.format(
            model_name=model.name,
            model_description=model.description or "(no description yet)",
            sql=model.raw_code[:4000] if model.raw_code else "(SQL not available)",
            columns=columns_text,
        )

        response = self.client.messages.create(
            model=MODEL,
            max_tokens=1500,
            system=_DOC_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text.strip()
        # Strip markdown code fences if Claude wrapped the JSON
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # Graceful fallback: return what we can parse
            return GeneratedDocs(
                model_name=model.name,
                model_description=model.description,
                tokens_used=response.usage.input_tokens + response.usage.output_tokens,
            )

        return GeneratedDocs(
            model_name=model.name,
            model_description=data.get("model_description", model.description),
            column_descriptions=data.get("columns", {}),
            tokens_used=response.usage.input_tokens + response.usage.output_tokens,
        )

    def generate_batch(
        self,
        models: list[DbtNode],
        on_progress: callable | None = None,
    ) -> list[GeneratedDocs]:
        """Generate docs for a list of models sequentially with optional progress callback."""
        results = []
        for i, model in enumerate(models):
            if on_progress:
                on_progress(i, len(models), model.name)
            result = self.generate(model)
            results.append(result)
        return results
