CREATE OR REPLACE VIEW "extreme_weather_alerts" AS 
SELECT
  DATE(timestamp) date
, region
, (CASE WHEN (temperature_celsius >= 35) THEN 'Extreme Heat' WHEN (temperature_celsius <= 0) THEN 'Freezing' ELSE 'Normal' END) temperature_condition
, (CASE WHEN (wind_speed_ms >= 20) THEN 'High Wind' ELSE 'Normal' END) wind_condition
, (CASE WHEN (snow_level > 0) THEN 'Snow Present' WHEN (ice_accumulation > 0) THEN 'Ice Present' ELSE 'None' END) precipitation_condition
, has_alerts
, detailed_forecast
FROM
  forecast_view
WHERE ((temperature_celsius >= 35) OR (temperature_celsius <= 0) OR (wind_speed_ms >= 20) OR (snow_level > 0) OR (ice_accumulation > 0) OR (has_alerts = 'Yes'))
ORDER BY date DESC
