
-- Hourly aggregates using materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS hour_aggregates
WITH (timescaledb.continuous) AS
SELECT 
  sensor_id,
  time_bucket( INTERVAL '1 hour', last_seen) AS hour,
  AVG(velocity) AS avg_velocity,
  AVG(temperature) AS avg_temperature,
  AVG(humidity) AS avg_humidity,
  AVG(battery_level) AS avg_battery
FROM 
  sensor_data
GROUP BY 
  sensor_id, hour; 

SELECT add_continuous_aggregate_policy('hour_aggregates',
  start_offset => NULL,   
  end_offset => INTERVAL '1 h',      
  schedule_interval => INTERVAL '1 minute');  


-- Daily aggregates using materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS day_aggregates
WITH (timescaledb.continuous) AS
SELECT 
  sensor_id,
  time_bucket( '1 day', last_seen) AS day,
  AVG(velocity) AS avg_velocity,
  AVG(temperature) AS avg_temperature,
  AVG(humidity) AS avg_humidity,
  AVG(battery_level) AS avg_battery
FROM 
  sensor_data
GROUP BY 
  sensor_id, day;

SELECT add_continuous_aggregate_policy('day_aggregates',
  start_offset => NULL,   
  end_offset => INTERVAL '1 h',      
  schedule_interval => INTERVAL '2 minutes');  


-- Weekly aggregates using materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS week_aggregates
WITH (timescaledb.continuous) AS
SELECT 
  sensor_id,
  time_bucket('1 week', last_seen) AS week,
  AVG(velocity) AS avg_velocity,
  AVG(temperature) AS avg_temperature,
  AVG(humidity) AS avg_humidity,
  AVG(battery_level) AS avg_battery
FROM 
  sensor_data
GROUP BY 
  sensor_id, week;

SELECT add_continuous_aggregate_policy('week_aggregates',
  start_offset => NULL,   
  end_offset => INTERVAL '1 h',      
  schedule_interval => INTERVAL '1 h');  


-- Monthly aggregates using materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS month_aggregates
WITH (timescaledb.continuous) AS
SELECT 
  sensor_id,
  time_bucket('1 month', last_seen) AS month,
  AVG(velocity) AS avg_velocity,
  AVG(temperature) AS avg_temperature,
  AVG(humidity) AS avg_humidity,
  AVG(battery_level) AS avg_battery
FROM 
  sensor_data
GROUP BY 
  sensor_id, month;


SELECT add_continuous_aggregate_policy('month_aggregates',
  start_offset => NULL,   
  end_offset => INTERVAL '1 h',      
  schedule_interval => INTERVAL '1 h');  



-- Yearly aggregates using materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS year_aggregates
WITH (timescaledb.continuous) AS
SELECT 
  sensor_id,
  time_bucket('1 year', last_seen) AS year,
  AVG(velocity) AS avg_velocity,
  AVG(temperature) AS avg_temperature,
  AVG(humidity) AS avg_humidity,
  AVG(battery_level) AS avg_battery
FROM 
  sensor_data
GROUP BY 
  sensor_id, year;

SELECT add_continuous_aggregate_policy('year_aggregates',
  start_offset => NULL,   
  end_offset => INTERVAL '1 h',      
  schedule_interval => INTERVAL '1 year'); 