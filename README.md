# dbt AI Readiness Autopilot

Makes every dbt project AI-ready by default — automatically.

## Overview

Three integrated capabilities turn any dbt project into an AI-powered workflow:

1. **MCP Server Auto-Enable** — Configures the dbt MCP Server so AI agents (Claude, GitHub Copilot, Cursor) can securely query dbt models, lineage, and metadata from day one.

2. **AI Autopilot** — Runs Claude claude-sonnet-4-6 in the background to generate missing model and column documentation and recommend data quality tests, writing patch YAML files directly into your project.

3. **AI Readiness Score** — Evaluates your project across four dimensions (Documentation, Testing, Semantic Layer, MCP Activation) and delivers a 0-100 score with actionable insights on what gaps may block AI functionality.

## Installation

```bash
pip install dbt-ai-readiness
# or with uv:
uv add dbt-ai-readiness
```

## Quick Start

```bash
# 1. Score your project (run from dbt project root after `dbt compile`)
dbt-ai-readiness score

# 2. Enable the MCP Server for Claude Desktop + VS Code
export DBT_HOST="https://myaccount.us1.dbt.com"
export DBT_TOKEN="your_personal_access_token"
dbt-ai-readiness mcp enable

# 3. Run the AI Autopilot to fill documentation and test gaps
export ANTHROPIC_API_KEY="your_key"
dbt-ai-readiness autopilot --dry-run   # preview first
dbt-ai-readiness autopilot             # apply changes

# 4. Re-score to see improvement
dbt-ai-readiness score
```

## AI Readiness Score

The score evaluates four dimensions with the following weights:

| Dimension | Weight | What It Measures |
|-----------|--------|-----------------|
| Documentation | 30% | Model + column description coverage |
| Testing | 25% | Test coverage and data quality variety |
| Semantic Layer | 25% | MetricFlow semantic models and metrics |
| MCP Activation | 20% | MCP server config + dbt Cloud credentials |

### Score Grades

| Score | Grade | AI Capability |
|-------|-------|---------------|
| 90-100 | Excellent | Full AI-powered workflows available |
| 75-89 | Good | Most AI features functional |
| 50-74 | Fair | Limited AI quality; gaps reduce accuracy |
| 25-49 | Poor | AI interactions will be unreliable |
| 0-24 | Critical | AI workflows are blocked |

## CLI Reference

### `dbt-ai-readiness score`

```
Options:
  -p, --project-dir PATH     Path to dbt project root [default: .]
  -f, --format TEXT          Output format: terminal, json, markdown [default: terminal]
  -o, --output PATH          Write output to file
  --insights / --no-insights Show actionable insights [default: insights]
```

### `dbt-ai-readiness autopilot`

```
Options:
  -p, --project-dir PATH  Path to dbt project root [default: .]
  --dry-run               Preview without writing files
  --docs-only             Only generate documentation
  --tests-only            Only recommend tests
  -m, --models TEXT       Comma-separated model names to process
  --api-key TEXT          Anthropic API key (or set ANTHROPIC_API_KEY)
```

The autopilot writes patch YAML files (`_{model_name}_ai_patch.yml`) alongside your model SQL files. Review and commit them like any other schema change.

### `dbt-ai-readiness mcp enable`

```
Options:
  -p, --project-dir PATH       dbt project root [default: .]
  -t, --target TEXT            all, claude-desktop, vscode, project-local [default: all]
  --host TEXT                  dbt Cloud host (or set DBT_HOST)
  --token TEXT                 dbt Cloud token (or set DBT_TOKEN)
  --environment-id TEXT        dbt Cloud environment ID (optional)
  --project-id TEXT            dbt Cloud project ID (optional)
  --dry-run                    Preview config without writing
```

### `dbt-ai-readiness mcp status`

Check current MCP Server activation status.

### `dbt-ai-readiness report`

```
Options:
  -p, --project-dir PATH  dbt project root [default: .]
  -o, --output PATH       Output file [default: ai-readiness-report.md]
  -f, --format TEXT       markdown or json [default: markdown]
```

## Architecture

```
src/dbt_ai_readiness/
├── dbt_project.py              # Reads manifest.json, catalog.json, dbt_project.yml
├── cli.py                      # Typer CLI with Rich terminal output
├── mcp_server/
│   ├── config.py               # MCPServerConfig — generates JSON for all AI tools
│   └── auto_enable.py          # MCPAutoEnabler — writes configs to Claude/VS Code/project
├── autopilot/
│   ├── doc_generator.py        # Claude-powered model + column doc generation
│   ├── test_recommender.py     # Claude-powered dbt test recommendations
│   └── runner.py               # AutopilotRunner — orchestrates, writes patch YAMLs
└── readiness_score/
    ├── scorer.py               # ReadinessScorer — weighted aggregate score
    ├── insights.py             # InsightEngine — prioritized actionable recommendations
    └── dimensions/
        ├── documentation.py    # Model + column description coverage (weight: 30%)
        ├── testing.py          # Test coverage + quality variety (weight: 25%)
        ├── semantic.py         # Semantic Layer + MetricFlow coverage (weight: 25%)
        └── mcp.py              # MCP config + credential detection (weight: 20%)
```

## How the AI Autopilot Works

1. Reads `manifest.json` to find models missing documentation or tests
2. For each eligible model, sends the model SQL and column names to Claude claude-sonnet-4-6
3. Claude returns structured JSON with descriptions and recommended tests
4. The runner writes patch YAML files (e.g., `_stg_orders_ai_patch.yml`) alongside your model SQL
5. You review, adjust, and commit the YAML — it merges naturally with existing schema files

## Semantic Layer Scoring

The semantic dimension scores mart-tier models (identified by `fct_`, `dim_`, `mart_`, `rpt_`, `agg_` prefixes or `mart`/`semantic` tags). A project with no mart-tier models falls back to all models.

Full score (100) requires:
- All mart-tier models exposed as semantic models (50 pts)
- At least one MetricFlow metric defined (20 pts)
- Entities configured on semantic models (15 pts)
- Measures configured on semantic models (15 pts)

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"
# or
uv sync

# Run tests
pytest

# Run against a real dbt project
cd /path/to/your/dbt/project
dbt compile
dbt-ai-readiness score --project-dir .
```

## Requirements

- Python 3.11+
- dbt project with a compiled `target/manifest.json`
- Anthropic API key (for autopilot features only)
- dbt Cloud credentials (for MCP server features)
