-- description: City count and total urban population per large country
-- output_format: geoparquet
-- crs: EPSG:4326

SELECT
    mc.country_name,
    mc.continent,
    COUNT(*) AS major_city_count,
    SUM(mc.city_population) AS total_urban_pop,
    lc.population AS total_population,
    ROUND(SUM(mc.city_population) * 100.0 / lc.population, 1) AS urban_pct,
    lc.geometry
FROM {{ ref('major_cities') }} mc
JOIN {{ ref('large_countries') }} lc
    ON mc.country_name = lc.name
GROUP BY mc.country_name, mc.continent, lc.population, lc.geometry
ORDER BY major_city_count DESC
