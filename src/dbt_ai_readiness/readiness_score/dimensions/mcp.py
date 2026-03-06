"""MCP Server activation scoring dimension."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class MCPScore:
    raw: float  # 0-100
    server_configured: bool
    cloud_credentials_present: bool
    config_paths_found: list[str] = field(default_factory=list)
    missing_env_vars: list[str] = field(default_factory=list)


_REQUIRED_ENV_VARS = ["DBT_TOKEN", "DBT_HOST"]
_OPTIONAL_ENV_VARS = ["DBT_ENVIRONMENT_ID", "DBT_PROJECT_ID"]

_MCP_CONFIG_CANDIDATES = [
    Path.home() / ".dbt" / "mcp.json",
    Path.home() / ".config" / "dbt" / "mcp.json",
    Path(".") / "mcp.json",
    Path(".") / ".dbt" / "mcp.json",
    # Claude Desktop / VS Code / Cursor MCP config locations
    Path.home() / ".config" / "Claude" / "claude_desktop_config.json",
    Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json",
]


def score_mcp(project_dir: Path | None = None) -> MCPScore:
    """
    Score MCP Server activation.

    Weighting:
      - dbt MCP config file exists:          50%
      - dbt Cloud credentials in environment: 50%
        - Required env vars (DBT_TOKEN, DBT_HOST): 35%
        - Optional env vars (environment/project ID): 15%
    """
    config_paths_found: list[str] = []

    # Check for MCP config file
    candidates = list(_MCP_CONFIG_CANDIDATES)
    if project_dir:
        candidates.insert(0, project_dir / "mcp.json")
        candidates.insert(0, project_dir / ".dbt" / "mcp.json")

    for path in candidates:
        if path.exists():
            try:
                import json
                content = json.loads(path.read_text())
                # Confirm it references a dbt MCP server
                servers = content.get("mcpServers", content.get("mcp_servers", {}))
                if any("dbt" in str(k).lower() or "dbt" in str(v).lower()
                       for k, v in servers.items()):
                    config_paths_found.append(str(path))
            except Exception:
                # File exists but isn't parseable as JSON with MCP config
                pass

    server_configured = bool(config_paths_found)

    # Check environment variables
    missing_required = [v for v in _REQUIRED_ENV_VARS if not os.environ.get(v)]
    missing_optional = [v for v in _OPTIONAL_ENV_VARS if not os.environ.get(v)]
    all_missing = missing_required + missing_optional
    cloud_credentials_present = len(missing_required) == 0

    # Scoring
    config_score = 50 if server_configured else 0
    required_score = 35 if cloud_credentials_present else (
        35 * (1 - len(missing_required) / len(_REQUIRED_ENV_VARS))
    )
    optional_score = 15 * (1 - len(missing_optional) / len(_OPTIONAL_ENV_VARS))
    raw = config_score + required_score + optional_score

    return MCPScore(
        raw=round(raw, 1),
        server_configured=server_configured,
        cloud_credentials_present=cloud_credentials_present,
        config_paths_found=config_paths_found,
        missing_env_vars=all_missing,
    )
