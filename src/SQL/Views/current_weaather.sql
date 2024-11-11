CREATE OR REPLACE VIEW "current_weather" AS 
WITH
  latest_timestamps AS (
   SELECT
     region
   , MAX(CAST(timestamp AS timestamp)) latest_timestamp
   FROM
     forecast_view
   GROUP BY region
) 
, filled_temps AS (
   SELECT
     f.*
   , COALESCE(temperature_celsius, LAG(temperature_celsius, 1) OVER (PARTITION BY region ORDER BY CAST(timestamp AS timestamp) ASC)) filled_temp_celsius
   , COALESCE(temperature_fahrenheit, LAG(temperature_fahrenheit, 1) OVER (PARTITION BY region ORDER BY CAST(timestamp AS timestamp) ASC)) filled_temp_fahrenheit
   FROM
     forecast_view f
) 
SELECT
  f.timestamp
, f.region
, COALESCE(f.temperature_celsius, f.filled_temp_celsius) temperature_celsius
, COALESCE(f.temperature_fahrenheit, f.filled_temp_fahrenheit) temperature_fahrenheit
, f.humidity
, f.wind_speed_ms
, f.wind_direction
, f.barometric_pressure
, f.visibility
, f.dew_point
, f.heat_index
, f.wind_chill
, f.present_weather
, f.forecast_temp
, f.short_forecast
, f.detailed_forecast
, f.snow_level
, f.ice_accumulation
, f.precipitation_probability
, f.max_temperature
, f.min_temperature
, f.uv_index
, f.has_alerts
FROM
  (filled_temps f
INNER JOIN latest_timestamps lt ON ((f.region = lt.region) AND (CAST(f.timestamp AS timestamp) = lt.latest_timestamp)))
