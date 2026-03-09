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
    duplicates_resolved: dict[str, list[str]] = field(default_factory=dict)


class AutopilotRunner:
    """
    Background AI Autopilot that:
    1. Resolves any duplicate model definitions across project YAML files
    2. Identifies models missing docs or tests
    3. Uses Claude to generate documentation and recommend tests
    4. Writes patch YAML files alongside the model SQL files
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
        summary = AutopilotSummary()

        # Resolve any pre-existing duplicate model definitions before writing new patches
        summary.duplicates_resolved = self.resolve_project_duplicates()

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

    # ------------------------------------------------------------------
    # Duplicate resolution
    # ------------------------------------------------------------------

    def resolve_project_duplicates(self) -> dict[str, list[str]]:
        """
        Scan all YAML files in the project and resolve duplicate model definitions.

        For each model defined in multiple YAML files:
        - Picks a canonical file (prefers non-patch files, then schema.yml, then alphabetical)
        - Deep-merges all entries into the canonical file
        - Removes the model entry from every other file (deletes the file if it becomes empty)

        Returns: {model_name: [all file paths that were involved]}
        """
        definitions = self._scan_all_yaml_definitions()
        resolved: dict[str, list[str]] = {}

        for model_name, paths in definitions.items():
            if len(paths) <= 1:
                continue

            canonical = self._pick_canonical_file(paths)
            entries = [
                e
                for e in (self._read_yaml_model_entry(p, model_name) for p in paths)
                if e is not None
            ]
            merged = self._deep_merge_model_entries(entries)

            if not self.dry_run:
                self._update_model_entry_in_yaml(canonical, model_name, merged)
                for path in paths:
                    if path != canonical:
                        self._remove_model_entry_from_yaml(path, model_name)

            resolved[model_name] = [str(p) for p in paths]

        return resolved

    def _get_models_dir(self) -> Path:
        """Return the project's first model-paths entry, defaulting to 'models/'."""
        config = self.project._load_project_config()
        model_paths = config.get("model-paths", config.get("source-paths", ["models"]))
        return self.project.project_dir / (model_paths[0] if model_paths else "models")

    def _scan_all_yaml_definitions(self) -> dict[str, list[Path]]:
        """
        Walk every YAML file under the models directory and return
        {model_name: [yaml_path, ...]} for every model defined at least once.
        """
        models_dir = self._get_models_dir()
        if not models_dir.exists():
            return {}

        definitions: dict[str, list[Path]] = {}
        yaml_files = sorted(models_dir.rglob("*.yml")) + sorted(models_dir.rglob("*.yaml"))

        for yaml_file in yaml_files:
            try:
                with open(yaml_file) as f:
                    content = yaml.safe_load(f)
            except Exception:
                continue
            if not isinstance(content, dict):
                continue
            for entry in content.get("models", []) or []:
                if isinstance(entry, dict) and entry.get("name"):
                    definitions.setdefault(entry["name"], []).append(yaml_file)

        return definitions

    def _pick_canonical_file(self, paths: list[Path]) -> Path:
        """
        Choose which file to keep when collapsing duplicates.

        Priority:
        1. Non-patch files (name doesn't start with '_', except '_schema.yml')
        2. Files named 'schema.yml' or '_schema.yml'
        3. Alphabetically first (most stable tie-breaker)
        """
        non_patch = [p for p in paths if not p.name.startswith("_") or p.stem == "_schema"]
        candidates = non_patch if non_patch else paths
        schema_files = [p for p in candidates if p.stem.lstrip("_") == "schema"]
        return schema_files[0] if schema_files else candidates[0]

    def _read_yaml_model_entry(self, yaml_path: Path, model_name: str) -> dict | None:
        """Return the model entry dict for model_name from yaml_path, or None."""
        try:
            with open(yaml_path) as f:
                content = yaml.safe_load(f)
        except Exception:
            return None
        if not isinstance(content, dict):
            return None
        for entry in content.get("models", []) or []:
            if isinstance(entry, dict) and entry.get("name") == model_name:
                return entry
        return None

    def _deep_merge_model_entries(self, entries: list[dict]) -> dict:
        """
        Merge a list of model entry dicts into one, never discarding existing content.

        - description: first non-empty value wins
        - data_tests (model-level): union, preserving order
        - columns: merged by name; within each column, first non-empty description/data_tests wins
        """
        if not entries:
            return {}

        merged: dict = {"name": entries[0]["name"]}

        for entry in entries:
            if not merged.get("description") and entry.get("description"):
                merged["description"] = entry["description"]

            if entry.get("data_tests"):
                existing_tests: list = merged.get("data_tests", [])
                merged["data_tests"] = existing_tests + [
                    t for t in entry["data_tests"] if t not in existing_tests
                ]

            if entry.get("columns"):
                col_map: dict[str, dict] = {
                    c["name"]: dict(c)
                    for c in (merged.get("columns") or [])
                    if isinstance(c, dict) and c.get("name")
                }
                for col in entry["columns"]:
                    if not isinstance(col, dict) or not col.get("name"):
                        continue
                    col_name = col["name"]
                    if col_name not in col_map:
                        col_map[col_name] = dict(col)
                    else:
                        if not col_map[col_name].get("description") and col.get("description"):
                            col_map[col_name]["description"] = col["description"]
                        if not col_map[col_name].get("data_tests") and col.get("data_tests"):
                            col_map[col_name]["data_tests"] = col["data_tests"]
                merged["columns"] = list(col_map.values())

        return merged

    def _update_model_entry_in_yaml(
        self, yaml_path: Path, model_name: str, new_entry: dict
    ) -> None:
        """Replace the model entry for model_name in yaml_path with new_entry."""
        try:
            with open(yaml_path) as f:
                content = yaml.safe_load(f) or {}
        except Exception:
            return
        models_list = content.get("models", []) or []
        for i, entry in enumerate(models_list):
            if isinstance(entry, dict) and entry.get("name") == model_name:
                models_list[i] = new_entry
                break
        content["models"] = models_list
        with open(yaml_path, "w") as f:
            yaml.dump(content, f, default_flow_style=False, sort_keys=False)

    def _remove_model_entry_from_yaml(self, yaml_path: Path, model_name: str) -> None:
        """
        Remove the model entry for model_name from yaml_path.
        Deletes the file if it contains no remaining models and no other top-level keys
        besides 'version'.
        """
        try:
            with open(yaml_path) as f:
                content = yaml.safe_load(f) or {}
        except Exception:
            return
        models_list = [
            e
            for e in (content.get("models", []) or [])
            if not (isinstance(e, dict) and e.get("name") == model_name)
        ]
        if not models_list and set(content.keys()) <= {"version", "models"}:
            yaml_path.unlink(missing_ok=True)
            return
        content["models"] = models_list
        with open(yaml_path, "w") as f:
            yaml.dump(content, f, default_flow_style=False, sort_keys=False)

    # ------------------------------------------------------------------
    # Patch writing
    # ------------------------------------------------------------------

    def _find_existing_yaml_for_model(self, model_name: str) -> Path | None:
        """
        Search all project YAML files for an existing definition of model_name.
        After resolve_project_duplicates() runs first, there will be at most one result.
        """
        definitions = self._scan_all_yaml_definitions()
        paths = definitions.get(model_name, [])
        if not paths:
            return None
        return self._pick_canonical_file(paths)

    def _build_model_entry(self, model: DbtNode, result: AutopilotResult) -> dict:
        """Build the model entry dict from doc/test results."""
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

        return model_entry

    def _merge_into_existing_yaml(
        self, existing_path: Path, model_name: str, new_entry: dict
    ) -> None:
        """Merge new_entry into the existing YAML file's model definition."""
        with open(existing_path) as f:
            content = yaml.safe_load(f) or {}

        models_list = content.get("models", []) or []
        for i, entry in enumerate(models_list):
            if isinstance(entry, dict) and entry.get("name") == model_name:
                existing_entry = entry

                if "description" in new_entry and not existing_entry.get("description"):
                    existing_entry["description"] = new_entry["description"]

                if "columns" in new_entry:
                    existing_cols: dict[str, dict] = {
                        c["name"]: c
                        for c in (existing_entry.get("columns") or [])
                        if isinstance(c, dict)
                    }
                    for new_col in new_entry["columns"]:
                        col_name = new_col["name"]
                        if col_name not in existing_cols:
                            existing_cols[col_name] = new_col
                        else:
                            if "description" in new_col and not existing_cols[col_name].get("description"):
                                existing_cols[col_name]["description"] = new_col["description"]
                            if "data_tests" in new_col and not existing_cols[col_name].get("data_tests"):
                                existing_cols[col_name]["data_tests"] = new_col["data_tests"]
                    existing_entry["columns"] = list(existing_cols.values())

                if "data_tests" in new_entry and not existing_entry.get("data_tests"):
                    existing_entry["data_tests"] = new_entry["data_tests"]

                models_list[i] = existing_entry
                break

        content["models"] = models_list
        with open(existing_path, "w") as f:
            yaml.dump(content, f, default_flow_style=False, sort_keys=False)

    def _write_patch_yaml(
        self,
        model: DbtNode,
        result: AutopilotResult,
    ) -> Path | None:
        """Write or merge a schema patch YAML file for the model."""
        if not result.doc_result and not result.test_result:
            return None

        model_path = self.project.project_dir / model.original_file_path
        model_entry = self._build_model_entry(model, result)

        # After resolve_project_duplicates() there is at most one existing definition
        existing_yaml = self._find_existing_yaml_for_model(model.name)
        if existing_yaml:
            target_path = existing_yaml
            if not self.dry_run:
                self._merge_into_existing_yaml(existing_yaml, model.name, model_entry)
        else:
            target_path = model_path.parent / f"_{model.name}_ai_patch.yml"
            if not self.dry_run:
                patch_content = {
                    "version": 2,
                    "models": [model_entry],
                }
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with open(target_path, "w") as f:
                    yaml.dump(patch_content, f, default_flow_style=False, sort_keys=False)

        return target_path
