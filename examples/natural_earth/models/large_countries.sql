-- description: Countries with population over 50 million
-- output_format: geoparquet
-- crs: EPSG:4326

SELECT
    NAME AS name,
    POP_EST AS population,
    CONTINENT AS continent,
    GDP_MD AS gdp_millions,
    ST_Area(geometry) AS area_sq_deg,
    geometry
FROM {{ source('countries') }}
WHERE POP_EST > 50000000
ORDER BY POP_EST DESC
