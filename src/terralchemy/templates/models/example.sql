-- description: Florida cities with population over 200,000
-- output_format: geoparquet

SELECT
    name,
    population,
    state,
    geometry
FROM {{ source('example_points') }}
WHERE population > 200000
ORDER BY population DESC
