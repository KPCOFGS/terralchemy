"""DAG builder and executor for terralchemy pipelines."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import networkx as nx
from rich.console import Console
from rich.table import Table

from terralchemy.engine import SpatialEngine
from terralchemy.models import Model
from terralchemy.sources import Source

console = Console()


class DAGError(Exception):
    pass


def build_dag(
    models: dict[str, Model],
    sources: dict[str, Source],
) -> nx.DiGraph:
    """Build a directed acyclic graph from model dependencies."""
    dag = nx.DiGraph()

    # Add source nodes
    for name in sources:
        dag.add_node(f"source:{name}", type="source", name=name)

    # Add model nodes and edges
    for name, model in models.items():
        dag.add_node(f"model:{name}", type="model", name=name)

        for src_dep in model.source_dependencies:
            src_key = f"source:{src_dep}"
            if src_key not in dag:
                raise DAGError(
                    f"Model '{name}' depends on source '{src_dep}' which is not defined."
                )
            dag.add_edge(src_key, f"model:{name}")

        for ref_dep in model.ref_dependencies:
            ref_key = f"model:{ref_dep}"
            if ref_key not in dag:
                raise DAGError(
                    f"Model '{name}' depends on model '{ref_dep}' which is not defined."
                )
            dag.add_edge(ref_key, f"model:{name}")

    # Check for cycles
    if not nx.is_directed_acyclic_graph(dag):
        cycles = list(nx.simple_cycles(dag))
        raise DAGError(f"Circular dependencies detected: {cycles}")

    return dag


def get_execution_order(dag: nx.DiGraph) -> list[str]:
    """Return model names in topological execution order."""
    ordered = list(nx.topological_sort(dag))
    # Filter to only model nodes
    return [
        dag.nodes[n]["name"]
        for n in ordered
        if dag.nodes[n]["type"] == "model"
    ]


def execute_pipeline(
    engine: SpatialEngine,
    dag: nx.DiGraph,
    models: dict[str, Model],
    sources: dict[str, Source],
    target_dir: str = "target",
    select: Optional[list[str]] = None,
) -> dict[str, Path]:
    """Execute the full pipeline in dependency order."""
    # Load all sources
    source_views: dict[str, str] = {}
    for name, source in sources.items():
        console.print(f"  [blue]Loading source:[/blue] {name} ({source.path})")
        view_name = engine.load_source(name, source.path, source.crs)
        source_views[name] = view_name

    # Execute models in topological order
    execution_order = get_execution_order(dag)
    if select:
        # If specific models are selected, include their upstream dependencies
        execution_order = [m for m in execution_order if m in select or _is_upstream(dag, m, select)]

    model_views: dict[str, str] = {}
    outputs: dict[str, Path] = {}

    for model_name in execution_order:
        model = models[model_name]
        console.print(f"  [green]Running model:[/green] {model_name}")

        # Resolve SQL with actual view names
        resolved_sql = model.resolve_sql(source_views, model_views)

        # Materialize
        output_path = engine.materialize_model(
            name=model_name,
            sql=resolved_sql,
            output_format=model.output_format,
            target_dir=target_dir,
        )

        model_views[model_name] = f"__model__{model_name}"
        outputs[model_name] = output_path

        row_count = engine.query(f"SELECT COUNT(*) FROM __model__{model_name}")[0][0]
        console.print(f"    [dim]-> {output_path} ({row_count} rows)[/dim]")

    return outputs


def _is_upstream(dag: nx.DiGraph, model_name: str, targets: list[str]) -> bool:
    """Check if a model is an upstream dependency of any target."""
    model_key = f"model:{model_name}"
    for target in targets:
        target_key = f"model:{target}"
        if target_key in dag and nx.has_path(dag, model_key, target_key):
            return True
    return False


def print_dag(dag: nx.DiGraph, models: dict[str, Model]) -> None:
    """Print the DAG as a table."""
    table = Table(title="Pipeline DAG")
    table.add_column("Order", style="bold")
    table.add_column("Type", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Dependencies", style="yellow")
    table.add_column("Output Format", style="magenta")

    for i, node in enumerate(nx.topological_sort(dag)):
        node_data = dag.nodes[node]
        deps = [dag.nodes[p]["name"] for p in dag.predecessors(node)]
        dep_str = ", ".join(deps) if deps else "-"
        node_type = node_data["type"]
        name = node_data["name"]

        fmt = ""
        if node_type == "model" and name in models:
            fmt = models[name].output_format

        table.add_row(str(i + 1), node_type, name, dep_str, fmt)

    console.print(table)
