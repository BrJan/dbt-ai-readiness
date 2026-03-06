"""MCP Server configuration generation for dbt Cloud."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class MCPServerConfig:
    dbt_host: str
    dbt_token: str
    environment_id: str = ""
    project_id: str = ""
    multicell_account_prefix: str = ""

    def to_mcp_json(self) -> dict:
        """Generate the MCP server config block for claude_desktop_config.json."""
        env: dict[str, str] = {
            "DBT_HOST": self.dbt_host,
            "DBT_TOKEN": self.dbt_token,
        }
        if self.environment_id:
            env["DBT_ENVIRONMENT_ID"] = self.environment_id
        if self.project_id:
            env["DBT_PROJECT_ID"] = self.project_id
        if self.multicell_account_prefix:
            env["DBT_MULTICELL_ACCOUNT_PREFIX"] = self.multicell_account_prefix

        return {
            "mcpServers": {
                "dbt": {
                    "command": "uvx",
                    "args": ["dbt-mcp"],
                    "env": env,
                }
            }
        }

    def to_vscode_json(self) -> dict:
        """Generate VS Code / Cursor MCP server settings block."""
        base = self.to_mcp_json()
        return {"mcp": base}

    def to_env_file(self) -> str:
        """Generate a .env snippet for dbt MCP environment variables."""
        lines = [
            f'DBT_HOST="{self.dbt_host}"',
            f'DBT_TOKEN="{self.dbt_token}"',
        ]
        if self.environment_id:
            lines.append(f'DBT_ENVIRONMENT_ID="{self.environment_id}"')
        if self.project_id:
            lines.append(f'DBT_PROJECT_ID="{self.project_id}"')
        return "\n".join(lines)


def config_from_env() -> MCPServerConfig | None:
    """Build MCPServerConfig from environment variables if available."""
    host = os.environ.get("DBT_HOST", "")
    token = os.environ.get("DBT_TOKEN", "")
    if not host or not token:
        return None
    return MCPServerConfig(
        dbt_host=host,
        dbt_token=token,
        environment_id=os.environ.get("DBT_ENVIRONMENT_ID", ""),
        project_id=os.environ.get("DBT_PROJECT_ID", ""),
        multicell_account_prefix=os.environ.get("DBT_MULTICELL_ACCOUNT_PREFIX", ""),
    )


def merge_into_claude_desktop_config(
    mcp_config: MCPServerConfig,
    config_path: Path,
) -> dict:
    """
    Merge dbt MCP server entry into an existing claude_desktop_config.json,
    or create a new one. Returns the merged config dict.
    """
    existing: dict = {}
    if config_path.exists():
        try:
            existing = json.loads(config_path.read_text())
        except json.JSONDecodeError:
            pass

    servers = existing.setdefault("mcpServers", {})
    new_block = mcp_config.to_mcp_json()["mcpServers"]["dbt"]
    servers["dbt"] = new_block
    return existing


def get_claude_desktop_config_path() -> Path | None:
    """Return the platform-appropriate Claude Desktop config path."""
    candidates = [
        Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json",
        Path.home() / ".config" / "Claude" / "claude_desktop_config.json",
        Path(os.environ.get("APPDATA", "")) / "Claude" / "claude_desktop_config.json",
    ]
    for p in candidates:
        if p.parent.exists():
            return p
    return candidates[0]  # default to macOS path
