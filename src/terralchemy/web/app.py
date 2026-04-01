"""FastAPI web backend for terralchemy dashboard."""

from __future__ import annotations

import json
import os
import traceback
from pathlib import Path
from typing import Optional

import yaml
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from terralchemy.dag import build_dag, execute_pipeline, get_execution_order
from terralchemy.engine import SpatialEngine
from terralchemy.models import load_models
from terralchemy.project import ProjectConfig
from terralchemy.sources import load_sources
from terralchemy.testing import load_tests, run_all_tests

app = FastAPI(title="terralchemy", docs_url=None, redoc_url=None)

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


# ── Project ──────────────────────────────────────────────────────────

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


# ── Sources ──────────────────────────────────────────────────────────

@app.get("/api/sources")
async def get_sources():
    try:
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
    except Exception as e:
        return []


@app.post("/api/sources/upload")
async def upload_source(
    file: UploadFile = File(...),
    name: str = Form(...),
    crs: str = Form("EPSG:4326"),
    description: str = Form(""),
):
    """Upload a geo file and auto-register it as a source."""
    # Save file to data/
    data_dir = _project_dir / "data"
    data_dir.mkdir(exist_ok=True)
    file_path = data_dir / file.filename
    content = await file.read()
    file_path.write_bytes(content)

    # Detect format from extension
    ext = file_path.suffix.lower().lstrip(".")
    fmt_map = {
        "shp": "shapefile", "geojson": "geojson", "json": "geojson",
        "gpkg": "geopackage", "parquet": "geoparquet", "fgb": "flatgeobuf",
        "kml": "kml", "csv": "csv",
    }
    fmt = fmt_map.get(ext, ext)

    # Write source YAML
    sources_dir = _project_dir / _config.sources_path
    sources_dir.mkdir(exist_ok=True)
    source_yml = sources_dir / f"{name}.yml"

    source_def = {
        "sources": [{
            "name": name,
            "path": f"data/{file.filename}",
            "format": fmt,
            "crs": crs,
            "description": description,
        }]
    }
    source_yml.write_text(yaml.dump(source_def, default_flow_style=False))

    return {
        "success": True,
        "name": name,
        "file": file.filename,
        "format": fmt,
        "source_yml": str(source_yml),
    }


class CreateSourceRequest(BaseModel):
    name: str
    path: str
    crs: str = "EPSG:4326"
    description: str = ""


@app.post("/api/sources/create")
async def create_source(req: CreateSourceRequest):
    """Register an existing file as a source (no upload)."""
    ext = Path(req.path).suffix.lower().lstrip(".")
    fmt_map = {
        "shp": "shapefile", "geojson": "geojson", "json": "geojson",
        "gpkg": "geopackage", "parquet": "geoparquet", "fgb": "flatgeobuf",
        "kml": "kml", "csv": "csv",
    }
    fmt = fmt_map.get(ext, ext)

    sources_dir = _project_dir / _config.sources_path
    sources_dir.mkdir(exist_ok=True)
    source_yml = sources_dir / f"{req.name}.yml"

    source_def = {
        "sources": [{
            "name": req.name,
            "path": req.path,
            "format": fmt,
            "crs": req.crs,
            "description": req.description,
        }]
    }
    source_yml.write_text(yaml.dump(source_def, default_flow_style=False))
    return {"success": True, "name": req.name}


@app.delete("/api/sources/{name}")
async def delete_source(name: str):
    """Delete a source definition."""
    sources_dir = _project_dir / _config.sources_path
    yml_path = sources_dir / f"{name}.yml"
    if yml_path.exists():
        yml_path.unlink()
        return {"success": True}
    # May be inside a multi-source file — not handled for simplicity
    return {"success": False, "error": "Source file not found"}


# ── Models ───────────────────────────────────────────────────────────

@app.get("/api/models")
async def get_models():
    try:
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
    except Exception as e:
        return []


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


@app.delete("/api/models/{name}")
async def delete_model(name: str):
    """Delete a model."""
    models_dir = _project_dir / _config.models_path
    path = models_dir / f"{name}.sql"
    if path.exists():
        path.unlink()
        return {"success": True}
    return {"success": False, "error": "Model not found"}


@app.get("/api/sources/{name}/columns")
async def get_source_columns(name: str):
    """Return column names and types for a source (for the visual builder)."""
    try:
        sources = load_sources(_project_dir / _config.sources_path, _project_dir)
        if name not in sources:
            return {"columns": [], "error": f"Source '{name}' not found"}

        source = sources[name]
        db_path = ":memory:"
        with SpatialEngine(db_path) as engine:
            engine.load_source(name, source.path, source.crs)
            col_info = engine.query(f"DESCRIBE __source__{name}")
            columns = []
            for row in col_info:
                col_name, col_type = row[0], row[1]
                # Classify type for the UI
                t = col_type.upper().split("(")[0].strip()
                geom_types = {"GEOMETRY", "BLOB", "WKB_BLOB", "POINT", "LINESTRING",
                              "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON"}
                if t in geom_types:
                    kind = "geometry"
                elif t in ("INTEGER", "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL", "HUGEINT"):
                    kind = "number"
                else:
                    kind = "text"
                columns.append({"name": col_name, "type": col_type, "kind": kind})
            return {"columns": columns}
    except Exception as e:
        return {"columns": [], "error": str(e)}


class BuildModelRequest(BaseModel):
    name: str
    source: str
    operations: list[dict]
    output_format: str = "geoparquet"
    description: str = ""


@app.post("/api/models/build")
async def build_model(req: BuildModelRequest):
    """Generate SQL from a visual pipeline definition and save it."""
    try:
        sql_parts = []
        select_cols = ["*"]
        from_clause = f"{{{{ source('{req.source}') }}}}"
        where_clauses = []
        group_by = None
        join_clause = None
        order_by = None

        # Track column replacements for geometry operations
        extra_selects = []
        replace_geometry = None

        for op in req.operations:
            op_type = op.get("type")

            if op_type == "filter":
                col = op["column"]
                condition = op["condition"]
                value = op["value"]
                # Try to detect if value is numeric
                try:
                    float(value)
                    val_str = value
                except (ValueError, TypeError):
                    val_str = f"'{value}'"

                cond_map = {
                    "equals": f"{col} = {val_str}",
                    "not_equals": f"{col} != {val_str}",
                    "greater_than": f"{col} > {val_str}",
                    "less_than": f"{col} < {val_str}",
                    "greater_equal": f"{col} >= {val_str}",
                    "less_equal": f"{col} <= {val_str}",
                    "contains": f"{col} LIKE '%{value}%'",
                    "starts_with": f"{col} LIKE '{value}%'",
                }
                where_clauses.append(cond_map.get(condition, f"{col} = {val_str}"))

            elif op_type == "buffer":
                distance = op.get("distance", 0.1)
                replace_geometry = f"ST_Buffer(geometry, {distance}) AS geometry"

            elif op_type == "centroid":
                replace_geometry = "ST_Centroid(geometry) AS geometry"

            elif op_type == "area":
                extra_selects.append("ST_Area(geometry) AS area")

            elif op_type == "length":
                extra_selects.append("ST_Length(geometry) AS length")

            elif op_type == "spatial_join":
                join_source = op.get("join_source", "")
                join_type = op.get("join_type", "within")
                join_map = {
                    "within": "ST_Within(a.geometry, b.geometry)",
                    "intersects": "ST_Intersects(a.geometry, b.geometry)",
                    "contains": "ST_Contains(a.geometry, b.geometry)",
                }
                join_cond = join_map.get(join_type, join_map["within"])
                from_clause = f"{{{{ source('{req.source}') }}}} a"
                join_clause = f"JOIN {{{{ source('{join_source}') }}}} b ON {join_cond}"
                select_cols = ["a.*"]

            elif op_type == "select_columns":
                cols = op.get("columns", [])
                if cols:
                    # Always keep geometry
                    if "geometry" not in cols:
                        cols.append("geometry")
                    select_cols = cols

            elif op_type == "aggregate":
                group_col = op.get("group_by", "")
                agg_col = op.get("agg_column", "")
                agg_func = op.get("agg_function", "count")
                func_map = {
                    "count": f"COUNT(*) AS count",
                    "sum": f"SUM({agg_col}) AS total_{agg_col}" if agg_col else "COUNT(*) AS count",
                    "avg": f"AVG({agg_col}) AS avg_{agg_col}" if agg_col else "COUNT(*) AS count",
                    "min": f"MIN({agg_col}) AS min_{agg_col}" if agg_col else "COUNT(*) AS count",
                    "max": f"MAX({agg_col}) AS max_{agg_col}" if agg_col else "COUNT(*) AS count",
                }
                select_cols = [group_col, func_map.get(agg_func, "COUNT(*) AS count"),
                               "ST_Union(geometry) AS geometry"]
                group_by = group_col

            elif op_type == "sort":
                sort_col = op.get("column", "")
                sort_dir = op.get("direction", "DESC")
                if sort_col:
                    order_by = f"{sort_col} {sort_dir}"

        # Build the SELECT columns
        if replace_geometry:
            # Replace geometry in select list
            if select_cols == ["*"]:
                # Need to enumerate non-geom columns + the replacement
                cols_str = f"* REPLACE({replace_geometry})"
            else:
                cols_str = ", ".join(c if c != "geometry" else replace_geometry for c in select_cols)
        else:
            cols_str = ", ".join(select_cols)

        if extra_selects:
            cols_str += ",\n    " + ",\n    ".join(extra_selects)

        # Assemble SQL
        sql = f"-- description: {req.description}\n-- output_format: {req.output_format}\n\n"
        sql += f"SELECT\n    {cols_str}\nFROM {from_clause}"

        if join_clause:
            sql += f"\n{join_clause}"
        if where_clauses:
            sql += f"\nWHERE {' AND '.join(where_clauses)}"
        if group_by:
            sql += f"\nGROUP BY {group_by}"
        if order_by:
            sql += f"\nORDER BY {order_by}"

        # Save the model
        models_dir = _project_dir / _config.models_path
        models_dir.mkdir(exist_ok=True)
        path = models_dir / f"{req.name}.sql"
        path.write_text(sql + "\n")

        return {"success": True, "sql": sql, "path": str(path)}

    except Exception as e:
        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


# ── Tests ────────────────────────────────────────────────────────────

@app.get("/api/tests")
async def get_tests():
    tests_dir = _project_dir / _config.tests_path
    tests = load_tests(tests_dir)
    return tests


class SaveTestsRequest(BaseModel):
    filename: str
    tests: list[dict]


@app.post("/api/tests/save")
async def save_tests(req: SaveTestsRequest):
    """Save test definitions to a YAML file."""
    tests_dir = _project_dir / _config.tests_path
    tests_dir.mkdir(exist_ok=True)
    path = tests_dir / f"{req.filename}.yml"
    path.write_text(yaml.dump({"tests": req.tests}, default_flow_style=False))
    return {"success": True, "path": str(path)}


# ── DAG ──────────────────────────────────────────────────────────────

@app.get("/api/dag")
async def get_dag():
    try:
        sources = load_sources(_project_dir / _config.sources_path, _project_dir)
        models = load_models(_project_dir / _config.models_path)
        dag = build_dag(models, sources)
        order = get_execution_order(dag)

        nodes = []
        edges = []

        for node_id in dag.nodes:
            data = dag.nodes[node_id]
            node = {"id": node_id, "label": data["name"], "type": data["type"]}
            if data["type"] == "model" and data["name"] in models:
                node["output_format"] = models[data["name"]].output_format
            nodes.append(node)

        for u, v in dag.edges:
            edges.append({"from": u, "to": v})

        return {"nodes": nodes, "edges": edges, "execution_order": order}
    except Exception as e:
        return {"nodes": [], "edges": [], "execution_order": [], "error": str(e)}


# ── Run / Test ───────────────────────────────────────────────────────

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
            source_views = {}
            for name, source in sources.items():
                engine.load_source(name, source.path, source.crs)
                source_views[name] = f"__source__{name}"
                logs.append({"type": "source", "name": name, "status": "loaded"})

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
                    "type": "model", "name": model_name,
                    "status": "completed", "rows": row_count,
                    "output": str(output_path),
                })

        return {"success": True, "logs": logs, "outputs": outputs}
    except Exception as e:
        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.post("/api/test")
async def run_tests_endpoint():
    try:
        sources = load_sources(_project_dir / _config.sources_path, _project_dir)
        models = load_models(_project_dir / _config.models_path)
        dag = build_dag(models, sources)

        db_path = str(_project_dir / _config.database)
        target_dir = str(_project_dir / _config.target_path)

        with SpatialEngine(db_path) as engine:
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
                    name=model_name, sql=resolved_sql,
                    output_format=model.output_format, target_dir=target_dir,
                )
                model_views[model_name] = f"__model__{model_name}"

            results = run_all_tests(engine, _project_dir / _config.tests_path)

        return {
            "success": True,
            "results": [
                {
                    "name": r.name, "model": r.model, "test_type": r.test_type,
                    "passed": r.passed, "message": r.message, "failing_rows": r.failing_rows,
                }
                for r in results
            ],
            "passed": sum(1 for r in results if r.passed),
            "failed": sum(1 for r in results if not r.passed),
            "total": len(results),
        }
    except Exception as e:
        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


# ── Map Preview ──────────────────────────────────────────────────────

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

            col_info = engine.query(f"DESCRIBE __model__{model_name}")
            cols = [row[0] for row in col_info]
            col_types = {row[0]: row[1] for row in col_info}

            # Find the primary geometry column and skip all geometry/blob columns from properties
            # DuckDB types can include CRS suffix like GEOMETRY('EPSG:4326'), so check prefix
            geom_prefixes = ("GEOMETRY", "BLOB", "WKB_BLOB", "POINT", "LINESTRING", "POLYGON",
                             "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION")

            def is_geom_type(type_str):
                t = type_str.upper().split("(")[0].strip()
                return t in geom_prefixes

            geom_col = None
            for c in cols:
                if is_geom_type(col_types[c]):
                    if geom_col is None:
                        geom_col = c
            has_geometry = geom_col is not None

            if has_geometry:
                # Exclude ALL geometry/blob columns from properties
                non_geom = [c for c in cols if not is_geom_type(col_types[c])]
                props_sql = ", ".join(non_geom) if non_geom else "'no_props' AS _placeholder"
                rows = engine.query(
                    f"SELECT {props_sql}, ST_AsGeoJSON({geom_col}) AS __geojson "
                    f"FROM __model__{model_name} LIMIT 1000"
                )
                features = []
                for row in rows:
                    geojson = json.loads(row[-1])
                    props = {}
                    for i, col in enumerate(non_geom):
                        val = row[i]
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
                rows = engine.query(f"SELECT * FROM __model__{model_name} LIMIT 100")
                return {
                    "type": "table", "columns": cols,
                    "rows": [[str(v) for v in row] for row in rows],
                    "meta": {"model": model_name},
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
