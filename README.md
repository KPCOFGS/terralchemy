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
# Install from GitHub (with web dashboard)
pip install "terralchemy[ui] @ git+https://github.com/KPCOFGS/terralchemy.git"

# Or clone and install locally
git clone https://github.com/KPCOFGS/terralchemy.git
cd terralchemy
pip install -e ".[ui]"
```

> The `[ui]` extra installs the web dashboard. If you only want the CLI, drop it: `pip install git+https://...`

## Web Dashboard

**Don't want to use the terminal? No problem.** terralchemy comes with a full visual interface.

```bash
terralchemy ui
```

This opens a browser dashboard where you can:

- See your entire pipeline as an **interactive visual graph** (drag, zoom, explore)
- **Run the pipeline** and **run tests** with a single click
- **Edit SQL models** directly in the browser with a code editor
- **Preview results on a map** — points, polygons, and lines rendered on OpenStreetMap
- Click on features to see their properties in a popup
- View detailed **test results** with pass/fail indicators
- **Create new models** from the UI without touching the file system

The dashboard works with any terralchemy project. Just `cd` into your project folder and run `terralchemy ui`.

## Quickstart — Full Walkthrough

This walkthrough takes you from zero to a working spatial pipeline in about 5 minutes. We'll use a small GeoJSON file with Florida cities as our example data.

### Step 1: Create a new project

```bash
terralchemy init florida_demo
cd florida_demo
```

You'll see this folder structure get created:

```
florida_demo/
├── terralchemy_project.yml  # Project config (you usually don't need to touch this)
├── sources/                 # Tell terralchemy where your geo files are
│   └── example.yml
├── models/                  # Your SQL transforms go here (one file per transform)
│   └── example.sql
├── tests/                   # Define quality checks on your output
│   └── example.yml
├── data/                    # Drop your shapefiles, geojson, etc. here
└── target/                  # terralchemy writes output files here
```

### Step 2: Add your geospatial data

Drop any geo file into the `data/` folder. For this demo, create a small GeoJSON file:

```bash
cat > data/florida_cities.geojson << 'EOF'
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {"name": "Jacksonville", "population": 949611, "county": "Duval"},
      "geometry": {"type": "Point", "coordinates": [-81.6557, 30.3322]}
    },
    {
      "type": "Feature",
      "properties": {"name": "Miami", "population": 449514, "county": "Miami-Dade"},
      "geometry": {"type": "Point", "coordinates": [-80.1918, 25.7617]}
    },
    {
      "type": "Feature",
      "properties": {"name": "Tampa", "population": 384959, "county": "Hillsborough"},
      "geometry": {"type": "Point", "coordinates": [-82.4572, 27.9506]}
    },
    {
      "type": "Feature",
      "properties": {"name": "Orlando", "population": 307573, "county": "Orange"},
      "geometry": {"type": "Point", "coordinates": [-81.3789, 28.5383]}
    },
    {
      "type": "Feature",
      "properties": {"name": "Tallahassee", "population": 196169, "county": "Leon"},
      "geometry": {"type": "Point", "coordinates": [-84.2807, 30.4383]}
    }
  ]
}
EOF
```

> You can also use your own Shapefiles (.shp), GeoPackage (.gpkg), GeoParquet (.parquet), FlatGeobuf (.fgb), or KML (.kml) — terralchemy reads them all the same way.

### Step 3: Tell terralchemy about your data (sources)

Open `sources/example.yml` and replace its contents with:

```yaml
# sources/florida.yml  (you can rename the file to anything you want)

sources:
  - name: florida_cities          # A short name you'll reference in SQL
    path: data/florida_cities.geojson
    format: geojson
    crs: EPSG:4326                # WGS84 — standard lat/lon coordinates
    description: Major Florida cities with population data
```

**What's happening here:**
- `name` — this is how you'll refer to this data in your SQL models (like a table alias)
- `path` — relative path to the file inside your project folder
- `format` — the file type (terralchemy can usually auto-detect this, but it's good to be explicit)
- `crs` — the coordinate reference system your data uses. EPSG:4326 (WGS84) is the most common for lat/lon data

### Step 4: Write your first model (SQL transform)

Models are just `.sql` files in the `models/` folder. Each one takes some input data, transforms it, and produces an output file.

Delete the example model and create a new one:

```bash
rm models/example.sql
```

**Model 1** — Filter to cities with population over 300k:

Create `models/big_cities.sql`:

```sql
-- description: Florida cities with population over 300,000
-- output_format: geoparquet

SELECT
    name,
    population,
    county,
    geometry
FROM {{ source('florida_cities') }}
WHERE population > 300000
ORDER BY population DESC
```

**What's happening here:**
- The `-- description:` and `-- output_format:` comments at the top are config. terralchemy reads these to know what format to save the output in.
- `{{ source('florida_cities') }}` tells terralchemy to pull in the source you defined in Step 3. terralchemy replaces this with the actual table reference at runtime.
- Everything else is standard SQL. You can use any SQL you already know.

**Model 2** — Create a 0.1-degree buffer zone around each big city:

Create `models/city_buffers.sql`:

```sql
-- description: Buffer zones around big Florida cities
-- output_format: geojson

SELECT
    name,
    population,
    ST_Buffer(geometry, 0.1) AS geometry
FROM {{ ref('big_cities') }}
```

**What's happening here:**
- `{{ ref('big_cities') }}` references the output of your first model. terralchemy automatically figures out that `city_buffers` depends on `big_cities` and runs them in the right order.
- `ST_Buffer(geometry, 0.1)` is a spatial function that creates a polygon around each point. The 0.1 is in degrees (roughly 11km at this latitude).
- This model outputs GeoJSON instead of GeoParquet — you control the format per model.

> **Tip:** You can use any of DuckDB's 100+ spatial functions in your models — `ST_Within`, `ST_Intersects`, `ST_Area`, `ST_Distance`, `ST_Centroid`, `ST_Union`, and many more. Full list: https://duckdb.org/docs/extensions/spatial.html

### Step 5: Add quality checks (tests)

Tests catch problems like invalid geometries, empty results, or coordinates that are out of bounds. You define them in YAML.

Replace `tests/example.yml` with:

```yaml
# tests/florida_tests.yml

tests:
  # Make sure the big_cities model actually returned some rows
  - name: big_cities_not_empty
    model: big_cities
    test: row_count_positive

  # Make sure all geometries are valid (no self-intersections, etc.)
  - name: big_cities_valid_geometry
    model: big_cities
    test: geometry_is_valid

  # Make sure no coordinates are outside normal lat/lon bounds
  - name: big_cities_within_bounds
    model: big_cities
    test: bounds_check

  # Same checks for the buffer model
  - name: buffers_not_empty
    model: city_buffers
    test: row_count_positive

  - name: buffers_valid_geometry
    model: city_buffers
    test: geometry_is_valid
```

**Available built-in tests:**

| Test name | What it checks |
|---|---|
| `row_count_positive` | The model produced at least 1 row |
| `geometry_is_valid` | Every geometry passes OGC validity rules (no self-intersections, etc.) |
| `geometry_not_empty` | No empty/blank geometries |
| `geometry_not_null` | No rows where the geometry column is NULL |
| `no_duplicate_geometries` | No two rows have the exact same geometry |
| `bounds_check` | All coordinates are within -180 to 180 longitude, -90 to 90 latitude |

You can also write custom SQL tests:

```yaml
  - name: population_sanity_check
    model: big_cities
    test: custom_sql
    query: "SELECT COUNT(*) FROM {{ model }} WHERE population < 0"
    expect: zero
```

### Step 6: See the pipeline DAG

Before running anything, you can preview what terralchemy will do:

```bash
terralchemy list
```

Output:

```
                              Pipeline DAG
┏━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Order ┃ Type   ┃ Name            ┃ Dependencies    ┃ Output Format ┃
┡━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ 1     │ source │ florida_cities  │ -               │               │
│ 2     │ model  │ big_cities      │ florida_cities  │ geoparquet    │
│ 3     │ model  │ city_buffers    │ big_cities      │ geojson       │
└───────┴────────┴─────────────────┴─────────────────┴───────────────┘
```

This shows the execution order. terralchemy reads `florida_cities` first, then runs `big_cities`, then `city_buffers`.

### Step 7: Run the pipeline

```bash
terralchemy run
```

Output:

```
terralchemy v0.1.0 — running project florida_demo

Execution order: big_cities -> city_buffers

  Loading source: florida_cities (data/florida_cities.geojson)
  Running model: big_cities
    -> target/big_cities.parquet (4 rows)
  Running model: city_buffers
    -> target/city_buffers.geojson (4 rows)

Pipeline complete. 2 models materialized.
```

Your output files are now in `target/`:
- `target/big_cities.parquet` — GeoParquet file with the 4 cities over 300k
- `target/city_buffers.geojson` — GeoJSON file with buffer polygons (you can open this in QGIS, kepler.gl, or geojson.io to see the result)

### Step 8: Run the tests

```bash
terralchemy test
```

Output:

```
                                  Test Results
┏━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Status ┃ Test                    ┃ Model        ┃ Type              ┃ Message       ┃
┡━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ PASS   │ big_cities_not_empty    │ big_cities   │ row_count_positive│ Row count: 4  │
│ PASS   │ big_cities_valid_geom   │ big_cities   │ geometry_is_valid │ All rows pass │
│ PASS   │ big_cities_within_bounds│ big_cities   │ bounds_check      │ All rows pass │
│ PASS   │ buffers_not_empty       │ city_buffers │ row_count_positive│ Row count: 4  │
│ PASS   │ buffers_valid_geometry  │ city_buffers │ geometry_is_valid │ All rows pass │
└────────┴─────────────────────────┴──────────────┴───────────────────┴───────────────┘

All 5 tests passed.
```

### Step 9: Run only specific models (optional)

If you only want to re-run one model (and its dependencies), use `--select`:

```bash
terralchemy run --select city_buffers
```

This will run `big_cities` first (because `city_buffers` depends on it), then `city_buffers` — but skip any other models you might have.

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
