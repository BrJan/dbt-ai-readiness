"""dbt AI Readiness CLI — score, autopilot, and MCP enablement commands."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.text import Text
from rich import box

from dbt_ai_readiness import __version__

app = typer.Typer(
    name="dbt-ai-readiness",
    help="Make your dbt project AI-ready by default.",
    add_completion=False,
    rich_markup_mode="rich",
)
mcp_app = typer.Typer(help="MCP Server management commands.")
app.add_typer(mcp_app, name="mcp")

console = Console()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _score_bar(score: float, width: int = 20) -> str:
    filled = round(score / 100 * width)
    empty = width - filled
    return "[green]" + "█" * filled + "[/green][dim]" + "░" * empty + "[/dim]"


def _grade_color(grade: str) -> str:
    return {
        "Excellent": "bright_green",
        "Good": "green",
        "Fair": "yellow",
        "Poor": "red",
        "Critical": "bright_red",
    }.get(grade, "white")


def _severity_color(severity: str) -> str:
    return {
        "critical": "bright_red",
        "high": "red",
        "medium": "yellow",
        "low": "cyan",
    }.get(severity, "white")


def _severity_icon(severity: str) -> str:
    return {
        "critical": "[bright_red]●[/bright_red]",
        "high": "[red]●[/red]",
        "medium": "[yellow]●[/yellow]",
        "low": "[cyan]●[/cyan]",
    }.get(severity, "●")


def _load_project(project_dir: str) -> "DbtProject":
    from dbt_ai_readiness.dbt_project import DbtProject
    return DbtProject(project_dir)


# ---------------------------------------------------------------------------
# score command
# ---------------------------------------------------------------------------

@app.command()
def score(
    project_dir: str = typer.Option(
        ".", "--project-dir", "-p", help="Path to dbt project root."
    ),
    output_format: str = typer.Option(
        "terminal", "--format", "-f", help="Output format: terminal, json, markdown."
    ),
    output_file: Optional[str] = typer.Option(
        None, "--output", "-o", help="Write output to file instead of stdout."
    ),
    show_insights: bool = typer.Option(
        True, "--insights/--no-insights", help="Show actionable insights."
    ),
):
    """
    Calculate the AI Readiness Score for a dbt project.

    Evaluates four dimensions: Documentation, Testing, Semantic Layer, and MCP Activation.
    """
    from dbt_ai_readiness.dbt_project import DbtProject
    from dbt_ai_readiness.readiness_score.scorer import ReadinessScorer

    project = DbtProject(project_dir)

    with console.status("[bold cyan]Analyzing dbt project...[/bold cyan]"):
        try:
            scorer = ReadinessScorer(project)
            result = scorer.score()
        except FileNotFoundError as e:
            console.print(f"[bold red]Error:[/bold red] {e}")
            raise typer.Exit(1)

    if output_format == "json":
        data = json.dumps(result.as_dict(), indent=2)
        if output_file:
            Path(output_file).write_text(data)
            console.print(f"[green]Report written to {output_file}[/green]")
        else:
            print(data)
        return

    if output_format == "markdown":
        md = _render_markdown(result)
        if output_file:
            Path(output_file).write_text(md)
            console.print(f"[green]Report written to {output_file}[/green]")
        else:
            print(md)
        return

    # Terminal output
    _render_terminal(result, show_insights=show_insights)

    if output_file:
        md = _render_markdown(result)
        Path(output_file).write_text(md)
        console.print(f"\n[dim]Report also written to {output_file}[/dim]")


def _render_terminal(result, show_insights: bool = True) -> None:
    from dbt_ai_readiness.readiness_score.insights import InsightSeverity

    grade_color = _grade_color(result.grade)
    overall_bar = _score_bar(result.overall)

    # Header panel
    header = (
        f"[bold]Project:[/bold] {result.project_name}   "
        f"[bold]Models:[/bold] {result.model_count}   "
        f"[bold]Tests:[/bold] {result.test_count}"
    )
    console.print()
    console.print(Panel(header, title="[bold cyan]dbt AI Readiness[/bold cyan]", box=box.ROUNDED))

    # Overall score
    overall_text = Text()
    overall_text.append(f"  Overall Score  ", style="bold")
    overall_text.append(f"{result.overall:5.1f}/100  ")
    overall_text.append(f"{_score_bar(result.overall)}  ", style="")
    overall_text.append(f"{result.grade}", style=f"bold {grade_color}")

    console.print(Panel(overall_text, box=box.HEAVY_EDGE))

    # Dimension table
    table = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold cyan")
    table.add_column("Dimension", min_width=20)
    table.add_column("Score", justify="right", min_width=8)
    table.add_column("Progress", min_width=22)
    table.add_column("Grade", min_width=10)
    table.add_column("Weight", justify="right", min_width=6)

    dimensions = [
        ("Documentation", result.documentation.raw, 0.30),
        ("Testing", result.testing.raw, 0.25),
        ("Semantic Layer", result.semantic.raw, 0.25),
        ("MCP Activation", result.mcp.raw, 0.20),
    ]
    for name, raw, weight in dimensions:
        grade = _grade_label(raw)
        gc = _grade_color(grade)
        table.add_row(
            name,
            f"{raw:.1f}",
            _score_bar(raw),
            f"[{gc}]{grade}[/{gc}]",
            f"{int(weight * 100)}%",
        )

    console.print(table)

    # Dimension detail panels
    _render_dimension_details(result)

    # Insights
    if show_insights and result.insights:
        console.print()
        console.print("[bold cyan]Actionable Insights[/bold cyan]")
        console.print()

        critical = [i for i in result.insights if i.severity == InsightSeverity.CRITICAL]
        others = [i for i in result.insights if i.severity != InsightSeverity.CRITICAL]

        for insight in critical + others[:6]:  # cap at 7 total
            icon = _severity_icon(insight.severity.value)
            sc = _severity_color(insight.severity.value)
            console.print(
                Panel(
                    f"{icon} [bold]{insight.title}[/bold]\n\n"
                    f"  [dim]{insight.detail}[/dim]\n\n"
                    f"  [bold {sc}]Action:[/bold {sc}] {insight.action}\n"
                    f"  [dim]AI Impact:[/dim] [italic]{insight.ai_impact}[/italic]",
                    title=f"[{sc}]{insight.severity.value.upper()}[/{sc}] — {insight.dimension}",
                    box=box.ROUNDED,
                    padding=(0, 1),
                )
            )

        remaining = len(result.insights) - 7
        if remaining > 0:
            console.print(
                f"\n  [dim]+ {remaining} more insight(s). "
                f"Run with --format json to see all.[/dim]"
            )

    # Quick wins banner
    if result.overall < 75:
        console.print()
        console.print(Panel(
            "[bold]Quick wins to improve your score:[/bold]\n\n"
            "  1. [cyan]dbt-ai-readiness mcp enable[/cyan]           — enable MCP Server (+20 pts potential)\n"
            "  2. [cyan]dbt-ai-readiness autopilot --dry-run[/cyan]   — preview AI-generated docs & tests\n"
            "  3. [cyan]dbt-ai-readiness autopilot[/cyan]             — apply docs & tests automatically\n"
            "  4. [cyan]dbt-ai-readiness score --format markdown -o report.md[/cyan]  — share report",
            title="[yellow]Improve AI Readiness[/yellow]",
            box=box.ROUNDED,
        ))

    console.print()


def _render_dimension_details(result) -> None:
    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    table.add_column("Metric", style="dim")
    table.add_column("Value", justify="right")

    console.print()
    console.print("[bold]Dimension Details[/bold]")

    # Documentation
    doc = result.documentation
    details_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    details_table.add_column("Metric", style="dim", min_width=35)
    details_table.add_column("Value", justify="right")
    details_table.add_row("Model description coverage", f"{doc.model_coverage:.1f}%")
    details_table.add_row("Column description coverage", f"{doc.column_coverage:.1f}%")
    if doc.undocumented_models:
        details_table.add_row(
            "Undocumented models",
            f"{len(doc.undocumented_models)} models"
        )
    console.print(Panel(details_table, title="Documentation", box=box.SIMPLE))

    # Testing
    tst = result.testing
    test_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    test_table.add_column("Metric", style="dim", min_width=35)
    test_table.add_column("Value", justify="right")
    test_table.add_row("Model test coverage (>=1 test)", f"{tst.model_test_coverage:.1f}%")
    test_table.add_row("Column test coverage", f"{tst.column_test_coverage:.1f}%")
    test_table.add_row("Data quality test variety", f"{tst.quality_test_variety:.1f}%")
    if tst.test_type_counts:
        test_table.add_row(
            "Test types in use",
            ", ".join(sorted(tst.test_type_counts.keys()))
        )
    console.print(Panel(test_table, title="Testing", box=box.SIMPLE))

    # Semantic Layer
    sem = result.semantic
    sem_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    sem_table.add_column("Metric", style="dim", min_width=35)
    sem_table.add_column("Value", justify="right")
    sem_table.add_row("Mart-tier models in Semantic Layer", f"{sem.semantic_model_coverage:.1f}%")
    sem_table.add_row("Metrics defined", str(sem.metric_count))
    sem_table.add_row("Entities configured", "[green]Yes[/green]" if sem.has_entities else "[red]No[/red]")
    sem_table.add_row("Measures configured", "[green]Yes[/green]" if sem.has_measures else "[red]No[/red]")
    console.print(Panel(sem_table, title="Semantic Layer", box=box.SIMPLE))

    # MCP
    mcp = result.mcp
    mcp_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    mcp_table.add_column("Metric", style="dim", min_width=35)
    mcp_table.add_column("Value", justify="right")
    mcp_table.add_row(
        "MCP Server configured",
        "[green]Yes[/green]" if mcp.server_configured else "[red]No[/red]"
    )
    mcp_table.add_row(
        "dbt Cloud credentials present",
        "[green]Yes[/green]" if mcp.cloud_credentials_present else "[red]No[/red]"
    )
    if mcp.config_paths_found:
        mcp_table.add_row("Config file", mcp.config_paths_found[0])
    if mcp.missing_env_vars:
        mcp_table.add_row("Missing env vars", ", ".join(mcp.missing_env_vars))
    console.print(Panel(mcp_table, title="MCP Activation", box=box.SIMPLE))


def _grade_label(score: float) -> str:
    if score >= 90:
        return "Excellent"
    if score >= 75:
        return "Good"
    if score >= 50:
        return "Fair"
    if score >= 25:
        return "Poor"
    return "Critical"


def _render_markdown(result) -> str:
    lines = [
        f"# dbt AI Readiness Report: {result.project_name}",
        "",
        f"**Overall Score:** {result.overall}/100 — {result.grade}",
        f"**Models:** {result.model_count} | **Tests:** {result.test_count}",
        "",
        "## Dimension Scores",
        "",
        "| Dimension | Score | Grade | Weight |",
        "|-----------|-------|-------|--------|",
        f"| Documentation | {result.documentation.raw:.1f}/100 | {_grade_label(result.documentation.raw)} | 30% |",
        f"| Testing | {result.testing.raw:.1f}/100 | {_grade_label(result.testing.raw)} | 25% |",
        f"| Semantic Layer | {result.semantic.raw:.1f}/100 | {_grade_label(result.semantic.raw)} | 25% |",
        f"| MCP Activation | {result.mcp.raw:.1f}/100 | {_grade_label(result.mcp.raw)} | 20% |",
        "",
        "## Documentation",
        f"- Model coverage: {result.documentation.model_coverage:.1f}%",
        f"- Column coverage: {result.documentation.column_coverage:.1f}%",
        "",
        "## Testing",
        f"- Model test coverage: {result.testing.model_test_coverage:.1f}%",
        f"- Column test coverage: {result.testing.column_test_coverage:.1f}%",
        f"- Quality test variety: {result.testing.quality_test_variety:.1f}%",
        "",
        "## Semantic Layer",
        f"- Mart-tier model coverage: {result.semantic.semantic_model_coverage:.1f}%",
        f"- Metrics defined: {result.semantic.metric_count}",
        f"- Entities: {'Yes' if result.semantic.has_entities else 'No'}",
        f"- Measures: {'Yes' if result.semantic.has_measures else 'No'}",
        "",
        "## MCP Activation",
        f"- Server configured: {'Yes' if result.mcp.server_configured else 'No'}",
        f"- Cloud credentials: {'Yes' if result.mcp.cloud_credentials_present else 'No'}",
        "",
    ]
    if result.insights:
        lines += ["## Actionable Insights", ""]
        for insight in result.insights:
            lines += [
                f"### [{insight.severity.value.upper()}] {insight.title}",
                f"**Dimension:** {insight.dimension}",
                "",
                insight.detail,
                "",
                f"**Action:** {insight.action}",
                f"**AI Impact:** {insight.ai_impact}",
                "",
            ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# autopilot command
# ---------------------------------------------------------------------------

@app.command()
def autopilot(
    project_dir: str = typer.Option(
        ".", "--project-dir", "-p", help="Path to dbt project root."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview changes without writing files."
    ),
    docs_only: bool = typer.Option(
        False, "--docs-only", help="Only generate documentation, skip test recommendations."
    ),
    tests_only: bool = typer.Option(
        False, "--tests-only", help="Only recommend tests, skip documentation."
    ),
    models: Optional[str] = typer.Option(
        None, "--models", "-m",
        help="Comma-separated list of model names to process (default: all eligible)."
    ),
    api_key: Optional[str] = typer.Option(
        None, "--api-key", envvar="ANTHROPIC_API_KEY",
        help="Anthropic API key. Falls back to ANTHROPIC_API_KEY env var."
    ),
):
    """
    Run the AI Autopilot to generate missing documentation and recommend tests.

    Uses Claude claude-sonnet-4-6 to analyze your dbt models and write patch YAML files
    with descriptions and test configurations.
    """
    from dbt_ai_readiness.autopilot.runner import AutopilotRunner
    from dbt_ai_readiness.dbt_project import DbtProject

    if not api_key and not os.environ.get("ANTHROPIC_API_KEY"):
        console.print(
            "[bold red]Error:[/bold red] Anthropic API key required.\n"
            "Set [cyan]ANTHROPIC_API_KEY[/cyan] or pass [cyan]--api-key[/cyan]."
        )
        raise typer.Exit(1)

    project = DbtProject(project_dir)
    model_filter = [m.strip() for m in models.split(",")] if models else None

    generate_docs = not tests_only
    generate_tests = not docs_only

    mode_label = "docs + tests"
    if docs_only:
        mode_label = "docs only"
    elif tests_only:
        mode_label = "tests only"

    dry_label = " [yellow](dry run)[/yellow]" if dry_run else ""
    console.print(
        f"\n[bold cyan]AI Autopilot[/bold cyan]{dry_label} — mode: [bold]{mode_label}[/bold]"
    )
    console.print(f"Project: [dim]{Path(project_dir).resolve()}[/dim]\n")

    runner = AutopilotRunner(
        project=project,
        api_key=api_key,
        dry_run=dry_run,
        generate_docs=generate_docs,
        generate_tests=generate_tests,
        model_filter=model_filter,
    )

    processed_count = [0]

    def on_progress(step: str, model_name: str, current: int, total: int):
        icon = "📝" if step == "docs" else "🧪"
        verb = "Generating docs" if step == "docs" else "Recommending tests"
        console.print(
            f"  [{current}/{total}] {verb} for [bold]{model_name}[/bold]..."
        )

    try:
        with console.status("[bold cyan]Running AI Autopilot...[/bold cyan]"):
            summary = runner.run(on_progress=None)  # Use simple console output instead
    except FileNotFoundError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)

    # Re-run with visible progress
    runner2 = AutopilotRunner(
        project=project,
        api_key=api_key,
        dry_run=dry_run,
        generate_docs=generate_docs,
        generate_tests=generate_tests,
        model_filter=model_filter,
    )

    console.print("[bold]Processing models:[/bold]\n")
    try:
        summary = runner2.run(on_progress=on_progress)
    except FileNotFoundError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)

    # Summary
    console.print()
    status = "[yellow]Dry run complete[/yellow]" if dry_run else "[green]Complete[/green]"
    console.print(Panel(
        f"[bold]{status}[/bold]\n\n"
        f"  Models processed:  {summary.models_processed}\n"
        f"  Models skipped:    {summary.models_skipped} (already documented/tested)\n"
        f"  Docs generated:    {summary.docs_generated}\n"
        f"  Tests recommended: {summary.tests_recommended}\n"
        f"  Tokens used:       {summary.total_tokens:,}\n"
        + (
            f"\n  [bold]Files written ({len(summary.files_written)}):[/bold]\n"
            + "\n".join(f"    [green]{f}[/green]" for f in summary.files_written)
            if summary.files_written and not dry_run else ""
        )
        + (
            f"\n  [yellow]Would write {len(summary.files_written)} file(s) — remove --dry-run to apply.[/yellow]"
            if dry_run and summary.files_written else ""
        ),
        title="[bold cyan]AI Autopilot Summary[/bold cyan]",
        box=box.ROUNDED,
    ))

    if not dry_run and summary.files_written:
        console.print(
            "\n[dim]Run [cyan]dbt compile[/cyan] to validate generated YAML, "
            "then [cyan]dbt-ai-readiness score[/cyan] to see your updated score.[/dim]"
        )


# ---------------------------------------------------------------------------
# mcp commands
# ---------------------------------------------------------------------------

@mcp_app.command("enable")
def mcp_enable(
    project_dir: str = typer.Option(
        ".", "--project-dir", "-p", help="Path to dbt project root."
    ),
    target: str = typer.Option(
        "all", "--target", "-t",
        help="Target to configure: all, claude-desktop, vscode, project-local."
    ),
    dbt_host: Optional[str] = typer.Option(
        None, "--host", envvar="DBT_HOST",
        help="dbt Cloud host (e.g. https://myaccount.us1.dbt.com)."
    ),
    dbt_token: Optional[str] = typer.Option(
        None, "--token", envvar="DBT_TOKEN",
        help="dbt Cloud personal access token."
    ),
    environment_id: Optional[str] = typer.Option(
        None, "--environment-id", envvar="DBT_ENVIRONMENT_ID",
        help="dbt Cloud environment ID (optional but recommended)."
    ),
    project_id: Optional[str] = typer.Option(
        None, "--project-id", envvar="DBT_PROJECT_ID",
        help="dbt Cloud project ID (optional)."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview config without writing files."
    ),
):
    """
    Enable the dbt MCP Server for AI agent integration.

    Generates and installs MCP server configuration for Claude Desktop,
    VS Code/Cursor, or as a project-local config.
    """
    from dbt_ai_readiness.mcp_server.auto_enable import MCPAutoEnabler
    from dbt_ai_readiness.mcp_server.config import MCPServerConfig, config_from_env

    # Build config from args or environment
    if dbt_host and dbt_token:
        config = MCPServerConfig(
            dbt_host=dbt_host,
            dbt_token=dbt_token,
            environment_id=environment_id or "",
            project_id=project_id or "",
        )
    else:
        config = config_from_env()

    if not config:
        console.print(
            "[bold red]Missing dbt credentials.[/bold red]\n\n"
            "Provide credentials via flags or environment variables:\n\n"
        )
        from dbt_ai_readiness.mcp_server.auto_enable import MCPAutoEnabler
        enabler_tmp = MCPAutoEnabler(Path(project_dir), config=None, dry_run=True)
        console.print(Panel(
            enabler_tmp.generate_env_snippet(),
            title="Required Environment Variables",
            box=box.ROUNDED,
        ))
        raise typer.Exit(1)

    enabler = MCPAutoEnabler(
        project_dir=Path(project_dir),
        config=config,
        dry_run=dry_run,
    )

    dry_label = " [yellow](dry run)[/yellow]" if dry_run else ""
    console.print(f"\n[bold cyan]MCP Server Enable[/bold cyan]{dry_label}\n")

    valid_targets = {"all", "claude-desktop", "vscode", "project-local"}
    if target not in valid_targets:
        console.print(f"[red]Unknown target: {target}. Choose from: {', '.join(sorted(valid_targets))}[/red]")
        raise typer.Exit(1)

    results = []
    if target in ("all", "claude-desktop"):
        ok, msg, path = enabler.enable_claude_desktop()
        results.append(("Claude Desktop", ok, msg, path))
    if target in ("all", "vscode"):
        ok, msg, path = enabler.enable_vscode()
        results.append(("VS Code / Cursor", ok, msg, path))
    if target in ("all", "project-local"):
        ok, msg, path = enabler.enable_project_local()
        results.append(("Project Local", ok, msg, path))

    for target_name, ok, msg, path in results:
        icon = "[green]✓[/green]" if ok else "[red]✗[/red]"
        console.print(f"  {icon} [bold]{target_name}:[/bold] {msg}")

    console.print()
    if all(ok for _, ok, _, _ in results):
        console.print(Panel(
            "[bold green]dbt MCP Server configured![/bold green]\n\n"
            "Your AI tools can now:\n"
            "  • Query dbt model lineage and metadata\n"
            "  • Run dbt commands through natural language\n"
            "  • Access Semantic Layer metrics and dimensions\n"
            "  • Generate and validate dbt models with AI assistance\n\n"
            "[dim]Restart your AI tool (Claude Desktop / VS Code) to activate.[/dim]",
            title="[green]MCP Activation Complete[/green]",
            box=box.ROUNDED,
        ))


@mcp_app.command("status")
def mcp_status(
    project_dir: str = typer.Option(
        ".", "--project-dir", "-p", help="Path to dbt project root."
    ),
):
    """Check the current MCP Server activation status."""
    from dbt_ai_readiness.readiness_score.dimensions.mcp import score_mcp

    mcp = score_mcp(project_dir=Path(project_dir))

    status_color = "green" if mcp.server_configured and mcp.cloud_credentials_present else "red"
    status_text = "Active" if mcp.server_configured and mcp.cloud_credentials_present else "Inactive"

    table = Table(box=box.SIMPLE_HEAD)
    table.add_column("Check", style="bold")
    table.add_column("Status")
    table.add_column("Detail")

    table.add_row(
        "MCP Config File",
        "[green]Found[/green]" if mcp.server_configured else "[red]Not found[/red]",
        mcp.config_paths_found[0] if mcp.config_paths_found else "No config detected",
    )
    table.add_row(
        "dbt Cloud Credentials",
        "[green]Present[/green]" if mcp.cloud_credentials_present else "[red]Missing[/red]",
        "DBT_TOKEN + DBT_HOST set" if mcp.cloud_credentials_present else f"Missing: {', '.join(mcp.missing_env_vars)}",
    )

    console.print()
    console.print(Panel(
        table,
        title=f"[{status_color}]MCP Status: {status_text}[/{status_color}]",
        box=box.ROUNDED,
    ))

    if not mcp.server_configured or not mcp.cloud_credentials_present:
        console.print(
            "\n  Run [cyan]dbt-ai-readiness mcp enable[/cyan] to configure the MCP Server.\n"
        )


# ---------------------------------------------------------------------------
# report command
# ---------------------------------------------------------------------------

@app.command()
def report(
    project_dir: str = typer.Option(
        ".", "--project-dir", "-p", help="Path to dbt project root."
    ),
    output_file: str = typer.Option(
        "ai-readiness-report.md", "--output", "-o", help="Output file path."
    ),
    format: str = typer.Option(
        "markdown", "--format", "-f", help="Report format: markdown or json."
    ),
):
    """
    Generate a full AI Readiness Report and write it to a file.
    """
    from dbt_ai_readiness.dbt_project import DbtProject
    from dbt_ai_readiness.readiness_score.scorer import ReadinessScorer

    project = DbtProject(project_dir)
    with console.status("[bold cyan]Generating report...[/bold cyan]"):
        try:
            scorer = ReadinessScorer(project)
            result = scorer.score()
        except FileNotFoundError as e:
            console.print(f"[bold red]Error:[/bold red] {e}")
            raise typer.Exit(1)

    if format == "json":
        content = json.dumps(result.as_dict(), indent=2)
    else:
        content = _render_markdown(result)

    Path(output_file).write_text(content)
    console.print(f"\n[green]Report written to[/green] [bold]{output_file}[/bold]")
    console.print(f"  Overall score: [bold]{result.overall}/100[/bold] — {result.grade}")


# ---------------------------------------------------------------------------
# version command
# ---------------------------------------------------------------------------

@app.command()
def version():
    """Show version information."""
    console.print(f"dbt-ai-readiness v{__version__}")
    console.print(f"Powered by Claude claude-sonnet-4-6 (Anthropic)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()
