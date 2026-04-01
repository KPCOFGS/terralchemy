-- description: Cities in large countries with spatial join
-- output_format: geoparquet
-- crs: EPSG:4326

SELECT
    c.NAME AS city_name,
    c.POP_MAX AS city_population,
    lc.name AS country_name,
    lc.continent,
    c.geometry
FROM {{ source('cities') }} c
JOIN {{ ref('large_countries') }} lc
    ON ST_Within(c.geometry, lc.geometry)
WHERE c.POP_MAX > 1000000
ORDER BY c.POP_MAX DESC
