"""Tests for MCP server config generation and auto-enablement."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dbt_ai_readiness.mcp_server.config import (
    MCPServerConfig,
    config_from_env,
    merge_into_claude_desktop_config,
)
from dbt_ai_readiness.mcp_server.auto_enable import MCPAutoEnabler


@pytest.fixture
def config():
    return MCPServerConfig(
        dbt_host="https://myaccount.us1.dbt.com",
        dbt_token="dbt_abc123",
        environment_id="12345",
        project_id="67890",
    )


class TestMCPServerConfig:
    def test_to_mcp_json_structure(self, config):
        result = config.to_mcp_json()
        assert "mcpServers" in result
        assert "dbt" in result["mcpServers"]
        dbt_entry = result["mcpServers"]["dbt"]
        assert dbt_entry["command"] == "uvx"
        assert "dbt-mcp" in dbt_entry["args"]
        assert dbt_entry["env"]["DBT_HOST"] == "https://myaccount.us1.dbt.com"
        assert dbt_entry["env"]["DBT_TOKEN"] == "dbt_abc123"
        assert dbt_entry["env"]["DBT_ENVIRONMENT_ID"] == "12345"

    def test_to_mcp_json_without_optional_fields(self):
        cfg = MCPServerConfig(dbt_host="https://x.dbt.com", dbt_token="tok")
        result = cfg.to_mcp_json()
        env = result["mcpServers"]["dbt"]["env"]
        assert "DBT_ENVIRONMENT_ID" not in env
        assert "DBT_PROJECT_ID" not in env

    def test_to_vscode_json_structure(self, config):
        result = config.to_vscode_json()
        assert "mcp" in result
        assert "mcpServers" in result["mcp"]

    def test_to_env_file_format(self, config):
        env_str = config.to_env_file()
        assert 'DBT_HOST="https://myaccount.us1.dbt.com"' in env_str
        assert 'DBT_TOKEN="dbt_abc123"' in env_str
        assert 'DBT_ENVIRONMENT_ID="12345"' in env_str

    def test_config_from_env(self):
        env = {
            "DBT_HOST": "https://myaccount.us1.dbt.com",
            "DBT_TOKEN": "dbt_tok",
            "DBT_ENVIRONMENT_ID": "999",
        }
        with patch.dict("os.environ", env, clear=True):
            cfg = config_from_env()
        assert cfg is not None
        assert cfg.dbt_host == "https://myaccount.us1.dbt.com"
        assert cfg.environment_id == "999"

    def test_config_from_env_missing_returns_none(self):
        with patch.dict("os.environ", {}, clear=True):
            cfg = config_from_env()
        assert cfg is None

    def test_merge_into_existing_config(self, config, tmp_path):
        existing = {
            "mcpServers": {
                "other-server": {"command": "npx", "args": ["other-mcp"]}
            }
        }
        config_path = tmp_path / "claude_desktop_config.json"
        config_path.write_text(json.dumps(existing))
        merged = merge_into_claude_desktop_config(config, config_path)
        assert "dbt" in merged["mcpServers"]
        assert "other-server" in merged["mcpServers"]

    def test_merge_creates_new_config(self, config, tmp_path):
        config_path = tmp_path / "claude_desktop_config.json"
        merged = merge_into_claude_desktop_config(config, config_path)
        assert "mcpServers" in merged
        assert "dbt" in merged["mcpServers"]


class TestMCPAutoEnabler:
    def test_enable_project_local_dry_run(self, config, tmp_path):
        enabler = MCPAutoEnabler(project_dir=tmp_path, config=config, dry_run=True)
        ok, msg, path = enabler.enable_project_local()
        assert ok is True
        assert "dry-run" in msg
        assert not (tmp_path / "mcp.json").exists()

    def test_enable_project_local_writes_file(self, config, tmp_path):
        enabler = MCPAutoEnabler(project_dir=tmp_path, config=config, dry_run=False)
        ok, msg, path = enabler.enable_project_local()
        assert ok is True
        assert (tmp_path / "mcp.json").exists()
        written = json.loads((tmp_path / "mcp.json").read_text())
        assert "mcpServers" in written
        assert "dbt" in written["mcpServers"]

    def test_enable_vscode_creates_vscode_dir(self, config, tmp_path):
        enabler = MCPAutoEnabler(project_dir=tmp_path, config=config, dry_run=False)
        ok, msg, path = enabler.enable_vscode()
        assert ok is True
        assert (tmp_path / ".vscode" / "mcp.json").exists()

    def test_enable_without_config_returns_failure(self, tmp_path):
        enabler = MCPAutoEnabler(project_dir=tmp_path, config=None, dry_run=False)
        ok, msg, path = enabler.enable_project_local()
        assert ok is False
        assert "credentials" in msg.lower() or "DBT_" in msg

    def test_generate_env_snippet_with_config(self, config, tmp_path):
        enabler = MCPAutoEnabler(project_dir=tmp_path, config=config)
        snippet = enabler.generate_env_snippet()
        assert "DBT_HOST" in snippet
        assert "DBT_TOKEN" in snippet

    def test_generate_env_snippet_without_config(self, tmp_path):
        enabler = MCPAutoEnabler(project_dir=tmp_path, config=None)
        snippet = enabler.generate_env_snippet()
        assert "DBT_HOST" in snippet
        assert "DBT_TOKEN" in snippet
