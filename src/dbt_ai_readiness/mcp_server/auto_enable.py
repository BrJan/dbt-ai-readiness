"""MCP Server auto-enablement for new and existing dbt projects."""

from __future__ import annotations

import json
import os
from pathlib import Path

from dbt_ai_readiness.mcp_server.config import (
    MCPServerConfig,
    config_from_env,
    get_claude_desktop_config_path,
    merge_into_claude_desktop_config,
)


class MCPAutoEnabler:
    """
    Detects dbt projects and enables MCP server configuration
    for AI agent integration.

    Supports three target environments:
      - Claude Desktop (claude_desktop_config.json)
      - VS Code / Cursor (.vscode/mcp.json or .cursor/mcp.json)
      - Project-local (mcp.json in project root)
    """

    def __init__(
        self,
        project_dir: Path,
        config: MCPServerConfig | None = None,
        dry_run: bool = False,
    ):
        self.project_dir = project_dir
        self.config = config or config_from_env()
        self.dry_run = dry_run

    def enable_claude_desktop(self) -> tuple[bool, str, Path]:
        """
        Merge dbt MCP entry into Claude Desktop config.
        Returns (success, message, config_path).
        """
        if not self.config:
            return False, "No dbt credentials found. Set DBT_HOST and DBT_TOKEN.", Path()

        target = get_claude_desktop_config_path()
        if target is None:
            return False, "Could not determine Claude Desktop config path.", Path()

        merged = merge_into_claude_desktop_config(self.config, target)

        if self.dry_run:
            preview = json.dumps(merged, indent=2)
            return True, f"[dry-run] Would write to {target}:\n{preview}", target

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(merged, indent=2))
        return True, f"dbt MCP Server added to {target}", target

    def enable_vscode(self) -> tuple[bool, str, Path]:
        """Write .vscode/mcp.json for VS Code Copilot / Cursor integration."""
        if not self.config:
            return False, "No dbt credentials found. Set DBT_HOST and DBT_TOKEN.", Path()

        vscode_dir = self.project_dir / ".vscode"
        target = vscode_dir / "mcp.json"
        config_data = self.config.to_vscode_json()

        if target.exists():
            try:
                existing = json.loads(target.read_text())
                existing.setdefault("mcp", {}).setdefault("mcpServers", {})
                existing["mcp"]["mcpServers"]["dbt"] = (
                    config_data["mcp"]["mcpServers"]["dbt"]
                )
                config_data = existing
            except (json.JSONDecodeError, KeyError):
                pass

        if self.dry_run:
            return True, f"[dry-run] Would write to {target}:\n{json.dumps(config_data, indent=2)}", target

        vscode_dir.mkdir(exist_ok=True)
        target.write_text(json.dumps(config_data, indent=2))
        return True, f"dbt MCP Server added to {target}", target

    def enable_project_local(self) -> tuple[bool, str, Path]:
        """Write a project-local mcp.json to the project root."""
        if not self.config:
            return False, "No dbt credentials found. Set DBT_HOST and DBT_TOKEN.", Path()

        target = self.project_dir / "mcp.json"
        config_data = self.config.to_mcp_json()

        if self.dry_run:
            return True, f"[dry-run] Would write to {target}:\n{json.dumps(config_data, indent=2)}", target

        target.write_text(json.dumps(config_data, indent=2))
        return True, f"dbt MCP Server config written to {target}", target

    def enable_all(self) -> list[tuple[str, bool, str, Path]]:
        """Enable MCP for all supported targets. Returns list of (target, success, msg, path)."""
        results = []
        for target_name, method in [
            ("Claude Desktop", self.enable_claude_desktop),
            ("VS Code / Cursor", self.enable_vscode),
            ("Project Local", self.enable_project_local),
        ]:
            ok, msg, path = method()
            results.append((target_name, ok, msg, path))
        return results

    def generate_env_snippet(self) -> str:
        """Return a .env file snippet for the user to add credentials."""
        if self.config:
            return self.config.to_env_file()
        return "\n".join([
            "# Add these to your .env or shell profile:",
            'DBT_HOST="https://YOUR_ACCOUNT.us1.dbt.com"',
            'DBT_TOKEN="your_dbt_cloud_personal_access_token"',
            'DBT_ENVIRONMENT_ID="your_environment_id"  # optional',
            'DBT_PROJECT_ID="your_project_id"          # optional',
        ])
