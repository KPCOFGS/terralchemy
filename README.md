# terralchemy

**Transforming raw geodata into gold — declarative spatial data pipelines powered by DuckDB.**

terralchemy is the **dbt for geodata** — a code-first, open-source framework for building reproducible geospatial data pipelines. Define your sources in YAML, write spatial transformations in SQL, and let terralchemy handle dependency resolution, format conversion, CRS management, and spatial testing.

```
sources/             models/                    target/
┌──────────┐    ┌──────────────────┐     ┌──────────────┐
│ .shp     │───>│ large_countries   │────>│ .parquet     │
│ .geojson │    └────────┬─────────┘     └──────────────┘
│ .gpkg    │             │
│ .parquet │    ┌────────▼─────────┐     ┌──────────────┐
│ .fgb     │───>│ major_cities      │────>│ .parquet     │
│ .kml     │    └────────┬─────────┘     └──────────────┘
└──────────┘             │
                ┌────────▼─────────┐     ┌──────────────┐
                │ city_density      │────>│ .parquet     │
                └──────────────────┘     └──────────────┘
```

## Why terralchemy?

| Pain point | How terralchemy solves it |
|---|---|
| Every geo pipeline is hand-rolled GDAL scripts | Declarative YAML sources + SQL models |
| CRS mismatches cause silent bugs | CRS-aware source loading and spatial tests |
| Dozens of incompatible formats | Reads 8+ formats, outputs GeoParquet by default |
| No dependency tracking between spatial transforms | DAG with `ref()` and `source()` like dbt |
| No spatial-specific testing | Built-in tests: geometry validity, bounds, emptiness, duplicates |
| Pipelines aren't version-controllable | Everything is YAML + SQL — git-friendly |

## Installation

```bash
# Install from GitHub
pip install git+https://github.com/KPCOFGS/terralchemy.git

# Or clone and install locally
git clone https://github.com/KPCOFGS/terralchemy.git
cd terralchemy
pip install -e .
```

## Quickstart

### 1. Initialize a project

```bash
terralchemy init my_project
cd my_project
```

This creates:

```
my_project/
├── terralchemy_project.yml  # Project configuration
├── sources/                 # Source definitions (YAML)
│   └── example.yml
├── models/                  # SQL models with spatial transforms
│   └── example.sql
├── tests/                   # Spatial tests (YAML)
│   └── example.yml
├── data/                    # Your geospatial files
└── target/                  # Pipeline outputs
```

### 2. Define sources

```yaml
# sources/boundaries.yml
sources:
  - name: countries
    path: data/countries.geojson
    format: geojson
    crs: EPSG:4326
    description: Country boundaries
```

### 3. Write models

Models are SQL files with `{{ source() }}` and `{{ ref() }}` for dependency resolution:

```sql
-- models/large_countries.sql
-- description: Countries with population over 50M
-- output_format: geoparquet

SELECT
    name,
    population,
    ST_Area(geometry) AS area,
    geometry
FROM {{ source('countries') }}
WHERE population > 50000000
```

```sql
-- models/country_centroids.sql
-- description: Centroids of large countries

SELECT
    name,
    population,
    ST_Centroid(geometry) AS geometry
FROM {{ ref('large_countries') }}
```

### 4. Add tests

```yaml
# tests/spatial.yml
tests:
  - name: valid_geometry
    model: large_countries
    test: geometry_is_valid

  - name: within_bounds
    model: large_countries
    test: bounds_check

  - name: has_data
    model: country_centroids
    test: row_count_positive
```

### 5. Run

```bash
# View the pipeline DAG
terralchemy list

# Run the pipeline
terralchemy run

# Run spatial tests
terralchemy test

# Run specific models
terralchemy run --select large_countries,country_centroids
```

## Supported Formats

| Input | Output |
|---|---|
| Shapefile (.shp) | GeoParquet (.parquet) |
| GeoJSON (.geojson) | GeoJSON (.geojson) |
| GeoPackage (.gpkg) | CSV (.csv) |
| GeoParquet (.parquet) | |
| FlatGeobuf (.fgb) | |
| KML (.kml) | |
| CSV (.csv) | |

## Built-in Spatial Tests

| Test | Description |
|---|---|
| `geometry_is_valid` | All geometries pass `ST_IsValid` |
| `geometry_not_empty` | No empty geometries |
| `geometry_not_null` | No NULL geometries |
| `no_duplicate_geometries` | No exact duplicate geometries |
| `bounds_check` | All geometries within WGS84 bounds |
| `row_count_positive` | Model has at least one row |
| `custom_sql` | Your own SQL assertion |

## Spatial SQL Functions

terralchemy uses DuckDB's spatial extension, giving you access to 100+ spatial functions:

```sql
-- Spatial joins
ST_Within(point, polygon)
ST_Intersects(a, b)
ST_Contains(a, b)

-- Geometry operations
ST_Buffer(geometry, distance)
ST_Centroid(geometry)
ST_Union(geometry)
ST_Intersection(a, b)

-- Measurements
ST_Area(geometry)
ST_Distance(a, b)
ST_Length(geometry)

-- Coordinate transforms
ST_Transform(geometry, source_crs, target_crs)

-- And many more: https://duckdb.org/docs/extensions/spatial.html
```

## Architecture

```
                    ┌─────────────────┐
                    │      CLI        │  terralchemy run / test / list
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │     Project     │  terralchemy_project.yml
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
       ┌──────▼──────┐ ┌────▼───┐ ┌────────▼──────┐
       │   Sources   │ │ Models │ │    Tests      │
       │   (YAML)    │ │ (SQL)  │ │    (YAML)     │
       └──────┬──────┘ └────┬───┘ └────────┬──────┘
              │              │              │
              └──────────────┼──────────────┘
                             │
                    ┌────────▼────────┐
                    │   DAG Engine    │  NetworkX dependency graph
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │     DuckDB      │  Spatial extension
                    │     Engine      │  100+ ST_ functions
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   GeoParquet    │  Output files
                    └─────────────────┘
```

## License

MIT

## Contributing

Contributions welcome! Please open an issue first to discuss what you'd like to change.
