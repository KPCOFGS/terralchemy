-- description: Buffer example points by 0.01 degrees
-- output_format: geoparquet
-- crs: EPSG:4326

SELECT
    *,
    ST_Buffer(geometry, 0.01) AS buffered_geometry
FROM {{ source('example_points') }}
