"""DuckDB spatial execution engine for terralchemy."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import duckdb


FORMAT_READERS = {
    ".shp": "ST_Read('{path}')",
    ".geojson": "ST_Read('{path}')",
    ".json": "ST_Read('{path}')",
    ".gpkg": "ST_Read('{path}')",
    ".geoparquet": "read_parquet('{path}')",
    ".parquet": "read_parquet('{path}')",
    ".fgb": "ST_Read('{path}')",
    ".kml": "ST_Read('{path}')",
    ".csv": "read_csv('{path}')",
}

FORMAT_WRITERS = {
    "geoparquet": "COPY ({query}) TO '{path}' (FORMAT PARQUET)",
    "parquet": "COPY ({query}) TO '{path}' (FORMAT PARQUET)",
    "geojson": "COPY ({query}) TO '{path}' (FORMAT GDAL, DRIVER 'GeoJSON')",
    "csv": "COPY ({query}) TO '{path}' (FORMAT CSV, HEADER TRUE)",
}


class SpatialEngine:
    """Manages a DuckDB connection with the spatial extension loaded."""

    def __init__(self, database: str = ":memory:") -> None:
        self.database = database
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> None:
        if self.database != ":memory:":
            os.makedirs(Path(self.database).parent, exist_ok=True)
        self.conn = duckdb.connect(self.database)
        self.conn.execute("INSTALL spatial; LOAD spatial;")

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> "SpatialEngine":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def execute(self, sql: str) -> duckdb.DuckDBPyRelation:
        return self.conn.execute(sql)

    def load_source(self, name: str, path: str, crs: Optional[str] = None) -> str:
        """Load a geospatial file as a DuckDB view. Returns the view name."""
        filepath = Path(path)
        suffix = filepath.suffix.lower()

        reader = FORMAT_READERS.get(suffix)
        if not reader:
            raise ValueError(
                f"Unsupported format '{suffix}' for source '{name}'. "
                f"Supported: {', '.join(FORMAT_READERS.keys())}"
            )

        read_expr = reader.format(path=str(filepath.resolve()))

        # Create a view for this source, normalizing geometry column to "geometry"
        view_name = f"__source__{name}"

        # Inspect columns from the source to check geometry column naming
        cols = [row[0] for row in self.conn.execute(f"DESCRIBE SELECT * FROM {read_expr}").fetchall()]
        if "geom" in cols and "geometry" not in cols:
            col_exprs = [f"geom AS geometry" if c == "geom" else c for c in cols]
            sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(col_exprs)} FROM {read_expr}"
        else:
            sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {read_expr}"
        self.conn.execute(sql)

        # If a target CRS is specified and differs, reproject
        if crs and suffix not in (".csv", ".parquet"):
            try:
                current_srid = self.conn.execute(
                    f"SELECT ST_SRID(geometry) FROM {view_name} LIMIT 1"
                ).fetchone()
                if current_srid and current_srid[0]:
                    target_srid = int(crs.split(":")[1]) if ":" in crs else int(crs)
                    if current_srid[0] != target_srid:
                        self.conn.execute(
                            f"CREATE OR REPLACE VIEW {view_name} AS "
                            f"SELECT * REPLACE(ST_Transform(geometry, 'EPSG:{current_srid[0]}', '{crs}') AS geometry) "
                            f"FROM {read_expr}"
                        )
            except Exception:
                pass  # Not all sources have geometry or SRID metadata

        return view_name

    def materialize_model(
        self,
        name: str,
        sql: str,
        output_format: str = "geoparquet",
        target_dir: str = "target",
    ) -> Path:
        """Execute a model's SQL and materialize the result."""
        # Create as a view first for downstream models to reference
        view_name = f"__model__{name}"
        self.conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")

        # Write output file
        os.makedirs(target_dir, exist_ok=True)

        ext_map = {
            "geoparquet": ".parquet",
            "parquet": ".parquet",
            "geojson": ".geojson",
            "csv": ".csv",
        }
        ext = ext_map.get(output_format, ".parquet")
        output_path = Path(target_dir) / f"{name}{ext}"

        writer_template = FORMAT_WRITERS.get(output_format)
        if not writer_template:
            raise ValueError(f"Unsupported output format: {output_format}")

        write_sql = writer_template.format(
            query=f"SELECT * FROM {view_name}",
            path=str(output_path.resolve()),
        )
        self.conn.execute(write_sql)

        return output_path

    def query(self, sql: str):
        """Run a query and return all rows."""
        return self.conn.execute(sql).fetchall()

    def query_df(self, sql: str):
        """Run a query and return a DuckDB relation."""
        return self.conn.execute(sql)
