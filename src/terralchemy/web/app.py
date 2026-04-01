"""FastAPI web backend for terralchemy dashboard."""

from __future__ import annotations

import json
import traceback
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from terralchemy.dag import build_dag, execute_pipeline, get_execution_order
from terralchemy.engine import SpatialEngine
from terralchemy.models import load_models
from terralchemy.project import ProjectConfig
from terralchemy.sources import load_sources
from terralchemy.testing import run_all_tests

app = FastAPI(title="terralchemy", docs_url=None, redoc_url=None)

# Will be set when the server starts
_project_dir: Optional[Path] = None
_config: Optional[ProjectConfig] = None


def configure(project_dir: Path, config: ProjectConfig) -> None:
    global _project_dir, _config
    _project_dir = project_dir
    _config = config


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path(__file__).parent / "index.html"
    return HTMLResponse(html_path.read_text())


@app.get("/api/project")
async def get_project():
    return {
        "name": _config.name,
        "version": _config.version,
        "sources_path": _config.sources_path,
        "models_path": _config.models_path,
        "tests_path": _config.tests_path,
        "target_path": _config.target_path,
        "project_dir": str(_project_dir),
    }


@app.get("/api/sources")
async def get_sources():
    sources = load_sources(_project_dir / _config.sources_path, _project_dir)
    return [
        {
            "name": s.name,
            "path": s.path,
            "format": s.format,
            "crs": s.crs,
            "description": s.description,
        }
        for s in sources.values()
    ]


@app.get("/api/models")
async def get_models():
    models = load_models(_project_dir / _config.models_path)
    return [
        {
            "name": m.name,
            "sql_path": str(m.sql_path),
            "sql": m.raw_sql,
            "output_format": m.output_format,
            "crs": m.crs,
            "description": m.description,
            "ref_dependencies": m.ref_dependencies,
            "source_dependencies": m.source_dependencies,
        }
        for m in models.values()
    ]


@app.get("/api/dag")
async def get_dag():
    sources = load_sources(_project_dir / _config.sources_path, _project_dir)
    models = load_models(_project_dir / _config.models_path)
    dag = build_dag(models, sources)
    order = get_execution_order(dag)

    nodes = []
    edges = []

    for node_id in dag.nodes:
        data = dag.nodes[node_id]
        node = {
            "id": node_id,
            "label": data["name"],
            "type": data["type"],
        }
        if data["type"] == "model" and data["name"] in models:
            node["output_format"] = models[data["name"]].output_format
        nodes.append(node)

    for u, v in dag.edges:
        edges.append({"from": u, "to": v})

    return {"nodes": nodes, "edges": edges, "execution_order": order}


@app.post("/api/run")
async def run_pipeline(select: Optional[str] = None):
    try:
        sources = load_sources(_project_dir / _config.sources_path, _project_dir)
        models = load_models(_project_dir / _config.models_path)
        dag = build_dag(models, sources)

        select_list = [s.strip() for s in select.split(",")] if select else None
        target_dir = str(_project_dir / _config.target_path)
        db_path = str(_project_dir / _config.database)

        logs = []

        with SpatialEngine(db_path) as engine:
            # Load sources
            source_views = {}
            for name, source in sources.items():
                engine.load_source(name, source.path, source.crs)
                source_views[name] = f"__source__{name}"
                logs.append({"type": "source", "name": name, "status": "loaded"})

            # Execute models
            order = get_execution_order(dag)
            model_views = {}
            outputs = {}

            for model_name in order:
                if select_list and model_name not in select_list:
                    continue
                model = models[model_name]
                resolved_sql = model.resolve_sql(source_views, model_views)
                output_path = engine.materialize_model(
                    name=model_name,
                    sql=resolved_sql,
                    output_format=model.output_format,
                    target_dir=target_dir,
                )
                model_views[model_name] = f"__model__{model_name}"
                row_count = engine.query(f"SELECT COUNT(*) FROM __model__{model_name}")[0][0]
                outputs[model_name] = str(output_path)
                logs.append({
                    "type": "model",
                    "name": model_name,
                    "status": "completed",
                    "rows": row_count,
                    "output": str(output_path),
                })

        return {"success": True, "logs": logs, "outputs": outputs}

    except Exception as e:
        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.post("/api/test")
async def run_tests():
    try:
        sources = load_sources(_project_dir / _config.sources_path, _project_dir)
        models = load_models(_project_dir / _config.models_path)
        dag = build_dag(models, sources)

        db_path = str(_project_dir / _config.database)
        target_dir = str(_project_dir / _config.target_path)

        with SpatialEngine(db_path) as engine:
            # Re-materialize
            source_views = {}
            for name, source in sources.items():
                engine.load_source(name, source.path, source.crs)
                source_views[name] = f"__source__{name}"

            order = get_execution_order(dag)
            model_views = {}
            for model_name in order:
                model = models[model_name]
                resolved_sql = model.resolve_sql(source_views, model_views)
                engine.materialize_model(
                    name=model_name,
                    sql=resolved_sql,
                    output_format=model.output_format,
                    target_dir=target_dir,
                )
                model_views[model_name] = f"__model__{model_name}"

            results = run_all_tests(engine, _project_dir / _config.tests_path)

        return {
            "success": True,
            "results": [
                {
                    "name": r.name,
                    "model": r.model,
                    "test_type": r.test_type,
                    "passed": r.passed,
                    "message": r.message,
                    "failing_rows": r.failing_rows,
                }
                for r in results
            ],
            "passed": sum(1 for r in results if r.passed),
            "failed": sum(1 for r in results if not r.passed),
            "total": len(results),
        }

    except Exception as e:
        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/preview/{model_name}")
async def preview_model(model_name: str):
    """Return GeoJSON preview of a model's output for map display."""
    try:
        sources = load_sources(_project_dir / _config.sources_path, _project_dir)
        models = load_models(_project_dir / _config.models_path)
        dag = build_dag(models, sources)

        db_path = str(_project_dir / _config.database)
        target_dir = str(_project_dir / _config.target_path)

        with SpatialEngine(db_path) as engine:
            # Load and materialize everything needed
            source_views = {}
            for name, source in sources.items():
                engine.load_source(name, source.path, source.crs)
                source_views[name] = f"__source__{name}"

            order = get_execution_order(dag)
            model_views = {}
            for mn in order:
                m = models[mn]
                resolved = m.resolve_sql(source_views, model_views)
                engine.materialize_model(
                    name=mn, sql=resolved,
                    output_format=m.output_format, target_dir=target_dir,
                )
                model_views[mn] = f"__model__{mn}"

            # Get column info
            cols = [
                row[0]
                for row in engine.query(f"DESCRIBE __model__{model_name}")
            ]

            # Build GeoJSON
            has_geometry = "geometry" in cols
            if has_geometry:
                non_geom = [c for c in cols if c != "geometry"]
                props_sql = ", ".join(non_geom) if non_geom else "'no_props' AS _placeholder"
                rows = engine.query(
                    f"SELECT {props_sql}, ST_AsGeoJSON(geometry) AS __geojson "
                    f"FROM __model__{model_name} LIMIT 1000"
                )
                features = []
                for row in rows:
                    geojson = json.loads(row[-1])
                    props = {}
                    for i, col in enumerate(non_geom):
                        val = row[i]
                        # Convert non-serializable types
                        if isinstance(val, (int, float, str, bool, type(None))):
                            props[col] = val
                        else:
                            props[col] = str(val)
                    features.append({
                        "type": "Feature",
                        "properties": props,
                        "geometry": geojson,
                    })
                return {
                    "type": "FeatureCollection",
                    "features": features,
                    "meta": {"model": model_name, "total_rows": len(features)},
                }
            else:
                # No geometry — return tabular data
                rows = engine.query(
                    f"SELECT * FROM __model__{model_name} LIMIT 100"
                )
                return {
                    "type": "table",
                    "columns": cols,
                    "rows": [[str(v) for v in row] for row in rows],
                    "meta": {"model": model_name},
                }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SaveModelRequest(BaseModel):
    name: str
    sql: str


@app.post("/api/models/save")
async def save_model(req: SaveModelRequest):
    """Save or update a SQL model file."""
    models_dir = _project_dir / _config.models_path
    models_dir.mkdir(exist_ok=True)
    path = models_dir / f"{req.name}.sql"
    path.write_text(req.sql)
    return {"success": True, "path": str(path)}
