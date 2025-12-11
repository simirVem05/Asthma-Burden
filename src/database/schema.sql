CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE IF NOT EXISTS tracts (
    geo_id VARCHAR(11) PRIMARY KEY,
    state_code VARCHAR(2),
    county_code VARCHAR(3),
    name VARCHAR(100),
    population INTEGER,
    population_density DOUBLE PRECISION,
    svi_ranking DOUBLE PRECISION,
    poverty_rate DOUBLE PRECISION,
    asthma_prev DOUBLE PRECISION,
    geom GEOMETRY(MultiPolygon, 4326)
);
CREATE INDEX IF NOT EXISTS idx_tracts_geom ON tracts USING GIST (geom);
CREATE TABLE IF NOT EXISTS highways (
    gid SERIAL PRIMARY KEY,
    linear_id VARCHAR(22),
    fullname VARCHAR(100),
    mtfcc VARCHAR(10),
    type VARCHAR(50),
    traffic_volume INTEGER,
    geom GEOMETRY(MultiLineString, 4326)
);
CREATE INDEX IF NOT EXISTS idx_highways_geom ON highways USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_highways_mtfcc ON highways (mtfcc);
CREATE TABLE IF NOT EXISTS pollution_monitors (
    monitor_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255),
    source VARCHAR(50),
    sensor_type VARCHAR(50),
    geom GEOMETRY(Point, 4326)
);
CREATE INDEX IF NOT EXISTS idx_monitors_geom ON pollution_monitors USING GIST (geom);
CREATE TABLE IF NOT EXISTS pollution_readings (
    id BIGSERIAL PRIMARY KEY,
    monitor_id VARCHAR(100) REFERENCES pollution_monitors(monitor_id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    pollutant VARCHAR(20),
    value DOUBLE PRECISION,
    unit VARCHAR(20),
    CONSTRAINT unique_reading UNIQUE (monitor_id, timestamp, pollutant)
);
CREATE INDEX IF NOT EXISTS idx_readings_monitor_time ON pollution_readings (monitor_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_readings_monitor_param ON pollution_readings (monitor_id, pollutant);
CREATE TABLE IF NOT EXISTS daily_covariates (
    date DATE PRIMARY KEY,
    avg_temp_celsius DECIMAL(5, 2),
    avg_humidity DECIMAL(5, 2),
    pollen_level VARCHAR(20),
    smoke_surge BOOLEAN DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS interventions (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    cost_per_unit DECIMAL(10, 2),
    reduction_efficiency DECIMAL(3, 2)
);
CREATE TABLE IF NOT EXISTS tract_intervention (
    id SERIAL PRIMARY KEY,
    tract_geo_id VARCHAR(11) REFERENCES tracts(geo_id),
    intervention_id INTEGER REFERENCES interventions(id),
    status VARCHAR(20) DEFAULT 'proposed'
);
INSERT INTO interventions (name, cost_per_unit, reduction_efficiency) VALUES
('Vegetative Barrier', 5000.00, 0.20),
('Pollution Cap Enforcement', 10000.00, 0.00),
('Home Air Filtration', 200.00, 0.10)
ON CONFLICT DO NOTHING;
