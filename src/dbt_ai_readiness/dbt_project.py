"""dbt project reader — parses manifest.json, catalog.json, and dbt_project.yml."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class DbtColumn:
    name: str
    description: str = ""
    data_type: str = ""
    tests: list[str] = field(default_factory=list)

    @property
    def is_documented(self) -> bool:
        return bool(self.description.strip())


@dataclass
class DbtNode:
    unique_id: str
    name: str
    resource_type: str
    description: str = ""
    raw_code: str = ""
    original_file_path: str = ""
    package_name: str = ""
    columns: dict[str, DbtColumn] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def is_documented(self) -> bool:
        return bool(self.description.strip())

    @property
    def documented_column_count(self) -> int:
        return sum(1 for c in self.columns.values() if c.is_documented)

    @property
    def total_column_count(self) -> int:
        return len(self.columns)

    @property
    def column_doc_coverage(self) -> float:
        if not self.columns:
            return 0.0
        return self.documented_column_count / self.total_column_count


@dataclass
class DbtTest:
    unique_id: str
    name: str
    test_type: str  # generic or singular
    test_name: str  # not_null, unique, accepted_values, relationships, etc.
    attached_node: str  # model unique_id this test belongs to
    column_name: str = ""
    severity: str = "error"


@dataclass
class SemanticModel:
    unique_id: str
    name: str
    model: str
    description: str = ""
    entities: list[dict] = field(default_factory=list)
    measures: list[dict] = field(default_factory=list)
    dimensions: list[dict] = field(default_factory=list)


@dataclass
class DbtMetric:
    unique_id: str
    name: str
    description: str = ""
    label: str = ""


@dataclass
class DbtProjectInfo:
    name: str
    version: str
    dbt_version_required: str
    project_dir: Path


class DbtProject:
    """Reads and exposes dbt project artifacts for AI readiness evaluation."""

    def __init__(self, project_dir: Path | str = "."):
        self.project_dir = Path(project_dir).resolve()
        self._manifest: dict[str, Any] | None = None
        self._catalog: dict[str, Any] | None = None
        self._project_config: dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # Artifact loading
    # ------------------------------------------------------------------

    def _load_manifest(self) -> dict[str, Any]:
        if self._manifest is None:
            candidates = [
                self.project_dir / "target" / "manifest.json",
                self.project_dir / "manifest.json",
            ]
            for path in candidates:
                if path.exists():
                    with open(path) as f:
                        self._manifest = json.load(f)
                    return self._manifest
            raise FileNotFoundError(
                f"manifest.json not found in {self.project_dir}. "
                "Run `dbt compile` or `dbt build` first."
            )
        return self._manifest

    def _load_catalog(self) -> dict[str, Any] | None:
        if self._catalog is None:
            candidates = [
                self.project_dir / "target" / "catalog.json",
                self.project_dir / "catalog.json",
            ]
            for path in candidates:
                if path.exists():
                    with open(path) as f:
                        self._catalog = json.load(f)
                    return self._catalog
        return self._catalog

    def _load_project_config(self) -> dict[str, Any]:
        if self._project_config is None:
            import yaml  # type: ignore[import-untyped]

            config_path = self.project_dir / "dbt_project.yml"
            if config_path.exists():
                with open(config_path) as f:
                    self._project_config = yaml.safe_load(f) or {}
            else:
                self._project_config = {}
        return self._project_config

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    def get_project_info(self) -> DbtProjectInfo:
        config = self._load_project_config()
        manifest = self._load_manifest()
        metadata = manifest.get("metadata", {})
        return DbtProjectInfo(
            name=config.get("name", metadata.get("project_name", "unknown")),
            version=str(config.get("version", "0.0.0")),
            dbt_version_required=config.get("require-dbt-version", ""),
            project_dir=self.project_dir,
        )

    def get_models(self) -> list[DbtNode]:
        manifest = self._load_manifest()
        nodes = []
        for uid, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") not in ("model", "snapshot"):
                continue
            columns = {}
            for col_name, col_data in node.get("columns", {}).items():
                columns[col_name] = DbtColumn(
                    name=col_data.get("name", col_name),
                    description=col_data.get("description", ""),
                    data_type=col_data.get("data_type", ""),
                )
            nodes.append(
                DbtNode(
                    unique_id=uid,
                    name=node["name"],
                    resource_type=node["resource_type"],
                    description=node.get("description", ""),
                    raw_code=node.get("raw_code", node.get("raw_sql", "")),
                    original_file_path=node.get("original_file_path", ""),
                    package_name=node.get("package_name", ""),
                    columns=columns,
                    tags=node.get("tags", []),
                    config=node.get("config", {}),
                )
            )
        return nodes

    def get_sources(self) -> list[DbtNode]:
        manifest = self._load_manifest()
        sources = []
        for uid, source in manifest.get("sources", {}).items():
            columns = {}
            for col_name, col_data in source.get("columns", {}).items():
                columns[col_name] = DbtColumn(
                    name=col_data.get("name", col_name),
                    description=col_data.get("description", ""),
                )
            sources.append(
                DbtNode(
                    unique_id=uid,
                    name=source["name"],
                    resource_type="source",
                    description=source.get("description", ""),
                    original_file_path=source.get("original_file_path", ""),
                    package_name=source.get("package_name", ""),
                    columns=columns,
                )
            )
        return sources

    def get_tests(self) -> list[DbtTest]:
        manifest = self._load_manifest()
        tests = []
        for uid, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") != "test":
                continue
            test_meta = node.get("test_metadata", {})
            test_name = test_meta.get("name", node["name"])
            test_type = "generic" if test_meta else "singular"
            attached = node.get("attached_node", "")
            # fallback: parse from depends_on
            if not attached:
                for dep in node.get("depends_on", {}).get("nodes", []):
                    if dep.startswith("model.") or dep.startswith("source."):
                        attached = dep
                        break
            tests.append(
                DbtTest(
                    unique_id=uid,
                    name=node["name"],
                    test_type=test_type,
                    test_name=test_name,
                    attached_node=attached,
                    column_name=test_meta.get("kwargs", {}).get("column_name", ""),
                    severity=node.get("config", {}).get("severity", "error"),
                )
            )
        return tests

    def get_semantic_models(self) -> list[SemanticModel]:
        manifest = self._load_manifest()
        semantic_models = []
        for uid, sm in manifest.get("semantic_models", {}).items():
            semantic_models.append(
                SemanticModel(
                    unique_id=uid,
                    name=sm["name"],
                    model=sm.get("model", ""),
                    description=sm.get("description", ""),
                    entities=sm.get("entities", []),
                    measures=sm.get("measures", []),
                    dimensions=sm.get("dimensions", []),
                )
            )
        return semantic_models

    def get_metrics(self) -> list[DbtMetric]:
        manifest = self._load_manifest()
        metrics = []
        for uid, metric in manifest.get("metrics", {}).items():
            metrics.append(
                DbtMetric(
                    unique_id=uid,
                    name=metric["name"],
                    description=metric.get("description", ""),
                    label=metric.get("label", ""),
                )
            )
        return metrics

    def get_catalog_columns(self, model_unique_id: str) -> list[DbtColumn]:
        """Fetch column types from catalog.json for a model (richer than manifest)."""
        catalog = self._load_catalog()
        if not catalog:
            return []
        # catalog keys are schema.table format; map via node name
        manifest = self._load_manifest()
        node = manifest.get("nodes", {}).get(model_unique_id, {})
        relation_name = node.get("relation_name", "")
        for key, entry in catalog.get("nodes", {}).items():
            if key == model_unique_id or (
                relation_name and entry.get("metadata", {}).get("name", "").lower()
                in relation_name.lower()
            ):
                return [
                    DbtColumn(
                        name=col["name"],
                        data_type=col.get("type", ""),
                    )
                    for col in entry.get("columns", {}).values()
                ]
        return []

    def get_mcp_config(self) -> dict[str, Any]:
        """Check for dbt MCP server configuration in the project."""
        config = self._load_project_config()
        return config.get("mcp", {})

    def get_model_by_name(self, name: str) -> DbtNode | None:
        for model in self.get_models():
            if model.name == name:
                return model
        return None
