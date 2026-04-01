"""CLI entry point for terralchemy."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel

from terralchemy import __version__
from terralchemy.dag import build_dag, execute_pipeline, get_execution_order, print_dag
from terralchemy.engine import SpatialEngine
from terralchemy.models import load_models
from terralchemy.project import ProjectConfig, find_project_file
from terralchemy.sources import load_sources
from terralchemy.testing import print_test_results, run_all_tests

app = typer.Typer(
    name="terralchemy",
    help="Terralchemy — declarative spatial data pipelines powered by DuckDB.",
    no_args_is_help=True,
)
console = Console()

TEMPLATES_DIR = Path(__file__).parent / "templates"


@app.command()
def init(
    name: str = typer.Argument("my_geo_project", help="Project name"),
    directory: Optional[str] = typer.Option(None, "--dir", "-d", help="Target directory"),
):
    """Initialize a new terralchemy project."""
    target = Path(directory) if directory else Path.cwd() / name
    target.mkdir(parents=True, exist_ok=True)

    # Copy template files
    for item in TEMPLATES_DIR.iterdir():
        dest = target / item.name
        if item.is_dir():
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(item, dest)
        else:
            content = item.read_text()
            content = content.replace("{{project_name}}", name)
            dest.write_text(content)

    # Create data and target dirs
    (target / "data").mkdir(exist_ok=True)
    (target / "target").mkdir(exist_ok=True)

    console.print(Panel(
        f"[bold green]Project '{name}' initialized at {target}[/bold green]\n\n"
        f"Structure:\n"
        f"  [cyan]{name}/[/cyan]\n"
        f"  ├── terralchemy_project.yml  [dim]# Project configuration[/dim]\n"
        f"  ├── sources/                 [dim]# Source definitions (YAML)[/dim]\n"
        f"  │   └── example.yml\n"
        f"  ├── models/                  [dim]# SQL models with spatial transforms[/dim]\n"
        f"  │   └── example.sql\n"
        f"  ├── tests/                   [dim]# Spatial tests (YAML)[/dim]\n"
        f"  │   └── example.yml\n"
        f"  ├── data/                    [dim]# Put your geo files here[/dim]\n"
        f"  └── target/                  [dim]# Pipeline outputs[/dim]\n\n"
        f"Next steps:\n"
        f"  1. Add geospatial files to [cyan]data/[/cyan]\n"
        f"  2. Define sources in [cyan]sources/*.yml[/cyan]\n"
        f"  3. Write models in [cyan]models/*.sql[/cyan]\n"
        f"  4. Run [bold]terralchemy run[/bold]",
        title="terralchemy",
    ))


@app.command()
def run(
    select: Optional[str] = typer.Option(
        None, "--select", "-s", help="Comma-separated model names to run"
    ),
    project_dir: Optional[str] = typer.Option(None, "--project-dir", "-p"),
):
    """Run the spatial pipeline."""
    project_file = find_project_file(Path(project_dir) if project_dir else None)
    proj_dir = project_file.parent
    config = ProjectConfig.from_file(project_file)

    console.print(f"[bold]terralchemy[/bold] v{__version__} — running project [cyan]{config.name}[/cyan]\n")

    # Load sources and models
    sources = load_sources(proj_dir / config.sources_path, proj_dir)
    models = load_models(proj_dir / config.models_path)

    if not models:
        console.print("[yellow]No models found. Add SQL files to the models/ directory.[/yellow]")
        raise typer.Exit(1)

    # Build DAG
    dag = build_dag(models, sources)
    order = get_execution_order(dag)
    console.print(f"[dim]Execution order: {' -> '.join(order)}[/dim]\n")

    # Execute
    select_list = [s.strip() for s in select.split(",")] if select else None
    target_dir = str(proj_dir / config.target_path)
    db_path = str(proj_dir / config.database)

    with SpatialEngine(db_path) as engine:
        outputs = execute_pipeline(
            engine=engine,
            dag=dag,
            models=models,
            sources=sources,
            target_dir=target_dir,
            select=select_list,
        )

    console.print(f"\n[bold green]Pipeline complete.[/bold green] {len(outputs)} models materialized.")


@app.command()
def test(
    project_dir: Optional[str] = typer.Option(None, "--project-dir", "-p"),
):
    """Run spatial tests on materialized models."""
    project_file = find_project_file(Path(project_dir) if project_dir else None)
    proj_dir = project_file.parent
    config = ProjectConfig.from_file(project_file)

    console.print(f"[bold]terralchemy[/bold] v{__version__} — testing project [cyan]{config.name}[/cyan]\n")

    # We need to re-run the pipeline to have views available, or connect to existing db
    sources = load_sources(proj_dir / config.sources_path, proj_dir)
    models = load_models(proj_dir / config.models_path)
    dag = build_dag(models, sources)

    db_path = str(proj_dir / config.database)
    target_dir = str(proj_dir / config.target_path)

    with SpatialEngine(db_path) as engine:
        # Re-materialize to ensure views exist
        execute_pipeline(
            engine=engine,
            dag=dag,
            models=models,
            sources=sources,
            target_dir=target_dir,
        )

        # Run tests
        results = run_all_tests(engine, proj_dir / config.tests_path)

    if not results:
        console.print("[yellow]No tests found. Add test definitions to tests/*.yml[/yellow]")
        raise typer.Exit(0)

    print_test_results(results)

    if any(not r.passed for r in results):
        raise typer.Exit(1)


@app.command(name="list")
def list_pipeline(
    project_dir: Optional[str] = typer.Option(None, "--project-dir", "-p"),
):
    """Show the pipeline DAG."""
    project_file = find_project_file(Path(project_dir) if project_dir else None)
    proj_dir = project_file.parent
    config = ProjectConfig.from_file(project_file)

    console.print(f"[bold]terralchemy[/bold] v{__version__} — project [cyan]{config.name}[/cyan]\n")

    sources = load_sources(proj_dir / config.sources_path, proj_dir)
    models = load_models(proj_dir / config.models_path)
    dag = build_dag(models, sources)

    print_dag(dag, models)


@app.command()
def version():
    """Show terralchemy version."""
    console.print(f"terralchemy v{__version__}")


if __name__ == "__main__":
    app()
