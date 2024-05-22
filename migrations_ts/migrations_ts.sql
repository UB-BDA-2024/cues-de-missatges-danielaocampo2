
-- #TODO: Create new TS hypertable

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS sensor_data (
    sensor_id INT,
    velocity DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    battery_level DOUBLE PRECISION,
    last_seen TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sensor_id, last_seen)
);

SELECT create_hypertable('sensor_data', 'last_seen');
