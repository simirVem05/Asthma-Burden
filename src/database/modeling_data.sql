DROP MATERIALIZED VIEW IF EXISTS modeling_data;
CREATE MATERIALIZED VIEW modeling_data AS
WITH pollution_agg AS (
    SELECT
        monitor_id,
        EXTRACT(YEAR FROM timestamp) as year,
        AVG(CASE WHEN pollutant = 'pm25' THEN value END) as pm25_mean,
        AVG(CASE WHEN pollutant = 'no2' THEN value END) as no2_mean
    FROM pollution_readings
    GROUP BY monitor_id, year
),
tract_pollution AS (
    SELECT
        t.geo_id,
        p.year,
        AVG(p.pm25_mean) as pm25_mean,
        AVG(p.no2_mean) as no2_mean
    FROM tracts t
    JOIN pollution_monitors m ON ST_Contains(t.geom, m.geom)
    JOIN pollution_agg p ON m.monitor_id = p.monitor_id
    GROUP BY t.geo_id, p.year
),
nearest_road AS (
    SELECT
        t.geo_id,
        MIN(ST_Distance(
            t.geom::geography,
            h.geom::geography
        )) as dist_primary_road_meters
    FROM tracts t
    CROSS JOIN highways h
    WHERE h.mtfcc = 'S1100'
    GROUP BY t.geo_id
)
SELECT
    t.geo_id,
    t.state_code,
    t.county_code,
    t.asthma_prev,
    t.poverty_rate,
    t.population_density,
    t.svi_ranking,
    tp.year,
    tp.pm25_mean,
    tp.no2_mean,
    nr.dist_primary_road_meters,
    t.population
FROM tracts t
LEFT JOIN tract_pollution tp ON t.geo_id = tp.geo_id
LEFT JOIN nearest_road nr ON t.geo_id = nr.geo_id;
CREATE INDEX IF NOT EXISTS idx_modeling_data_geo_id ON modeling_data (geo_id);
CREATE INDEX IF NOT EXISTS idx_modeling_data_year ON modeling_data (year);
