"""AI Autopilot runner — orchestrates doc generation and test recommendations."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml  # type: ignore[import-untyped]

from dbt_ai_readiness.autopilot.doc_generator import DocGenerator, GeneratedDocs
from dbt_ai_readiness.autopilot.test_recommender import TestRecommendation, TestRecommender
from dbt_ai_readiness.dbt_project import DbtNode, DbtProject, DbtTest


@dataclass
class AutopilotResult:
    model_name: str
    doc_result: GeneratedDocs | None = None
    test_result: TestRecommendation | None = None
    patch_file_written: str = ""
    skipped: bool = False
    skip_reason: str = ""


@dataclass
class AutopilotSummary:
    models_processed: int = 0
    models_skipped: int = 0
    docs_generated: int = 0
    tests_recommended: int = 0
    files_written: list[str] = field(default_factory=list)
    total_tokens: int = 0
    results: list[AutopilotResult] = field(default_factory=list)


class AutopilotRunner:
    """
    Background AI Autopilot that:
    1. Identifies models missing docs or tests
    2. Uses Claude to generate documentation and recommend tests
    3. Writes patch YAML files alongside the model SQL files
    """

    def __init__(
        self,
        project: DbtProject,
        api_key: str | None = None,
        dry_run: bool = False,
        generate_docs: bool = True,
        generate_tests: bool = True,
        model_filter: list[str] | None = None,
    ):
        self.project = project
        self.dry_run = dry_run
        self.generate_docs = generate_docs
        self.generate_tests = generate_tests
        self.model_filter = model_filter
        self.doc_gen = DocGenerator(api_key=api_key)
        self.test_rec = TestRecommender(api_key=api_key)

    def run(
        self,
        on_progress: callable | None = None,
    ) -> AutopilotSummary:
        """
        Run the autopilot over all eligible models.

        Args:
            on_progress: Optional callback(step: str, model_name: str, current: int, total: int)
        """
        project_info = self.project.get_project_info()
        all_models = [
            m for m in self.project.get_models()
            if m.package_name == project_info.name
        ]

        # Apply model filter
        if self.model_filter:
            all_models = [m for m in all_models if m.name in self.model_filter]

        # Identify models needing docs or tests
        all_tests = self.project.get_tests()
        tests_by_model: dict[str, list[DbtTest]] = {}
        for test in all_tests:
            tests_by_model.setdefault(test.attached_node, []).append(test)

        tested_model_ids = {uid for uid, tests in tests_by_model.items() if tests}

        eligible_models = []
        for model in all_models:
            needs_docs = self.generate_docs and (
                not model.is_documented
                or any(not col.is_documented for col in model.columns.values())
            )
            needs_tests = self.generate_tests and model.unique_id not in tested_model_ids
            if needs_docs or needs_tests:
                eligible_models.append((model, needs_docs, needs_tests))

        summary = AutopilotSummary()
        summary.models_skipped = len(all_models) - len(eligible_models)
        total = len(eligible_models)

        for i, (model, needs_docs, needs_tests) in enumerate(eligible_models):
            result = AutopilotResult(model_name=model.name)

            # Generate docs
            if needs_docs:
                if on_progress:
                    on_progress("docs", model.name, i + 1, total)
                doc_result = self.doc_gen.generate(model)
                result.doc_result = doc_result
                summary.total_tokens += doc_result.tokens_used
                if doc_result.model_description or doc_result.column_descriptions:
                    summary.docs_generated += 1

            # Recommend tests
            if needs_tests:
                if on_progress:
                    on_progress("tests", model.name, i + 1, total)
                test_result = self.test_rec.recommend(
                    model,
                    existing_tests=tests_by_model.get(model.unique_id, []),
                )
                result.test_result = test_result
                summary.total_tokens += test_result.tokens_used
                if test_result.column_tests or test_result.model_tests:
                    summary.tests_recommended += 1

            # Write patch YAML
            patch_path = self._write_patch_yaml(model, result)
            if patch_path:
                result.patch_file_written = str(patch_path)
                if str(patch_path) not in summary.files_written:
                    summary.files_written.append(str(patch_path))

            summary.results.append(result)
            summary.models_processed += 1

        return summary

    def _write_patch_yaml(
        self,
        model: DbtNode,
        result: AutopilotResult,
    ) -> Path | None:
        """Write or merge a schema patch YAML file for the model."""
        if not result.doc_result and not result.test_result:
            return None

        # Determine patch file location — alongside the model SQL
        model_path = self.project.project_dir / model.original_file_path
        patch_path = model_path.parent / f"_{model.name}_ai_patch.yml"

        # Build the patch YAML structure
        model_entry: dict = {"name": model.name}

        # Apply docs
        if result.doc_result:
            doc = result.doc_result
            if doc.model_description and not model.is_documented:
                model_entry["description"] = doc.model_description

            existing_col_docs = {
                col_name: col.description
                for col_name, col in model.columns.items()
                if col.is_documented
            }
            all_col_docs = {**existing_col_docs, **doc.column_descriptions}

            if all_col_docs:
                model_entry.setdefault("columns", [])
                # Merge with existing column entries from test results
                col_map: dict[str, dict] = {}
                for col_name, desc in all_col_docs.items():
                    col_map[col_name] = {"name": col_name, "description": desc}
                model_entry["columns"] = list(col_map.values())

        # Apply tests
        if result.test_result and result.test_result.column_tests:
            col_map: dict[str, dict] = {
                col["name"]: col
                for col in model_entry.get("columns", [])
            }
            for col_name, tests in result.test_result.column_tests.items():
                if col_name not in col_map:
                    col_map[col_name] = {"name": col_name}
                col_map[col_name]["data_tests"] = [t.to_yaml_dict() for t in tests]
            model_entry["columns"] = list(col_map.values())

            if result.test_result.model_tests:
                model_entry["data_tests"] = result.test_result.model_tests

        patch_content = {
            "version": 2,
            "models": [model_entry],
        }

        if self.dry_run:
            return patch_path  # Report path but don't write

        patch_path.parent.mkdir(parents=True, exist_ok=True)
        with open(patch_path, "w") as f:
            yaml.dump(patch_content, f, default_flow_style=False, sort_keys=False)

        return patch_path
