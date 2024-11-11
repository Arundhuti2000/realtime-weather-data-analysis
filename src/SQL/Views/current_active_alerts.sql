CREATE OR REPLACE VIEW "current_active_alerts" AS 
SELECT
  region
, timestamp
, ARRAY_AGG(alert_type) active_alerts
FROM
  (
   SELECT
     region
   , timestamp
   , (CASE WHEN (temperature_fahrenheit >= 95) THEN 'Extreme Heat' WHEN (temperature_fahrenheit <= 32) THEN 'Freeze Warning' WHEN (wind_speed_ms >= 1.341E1) THEN 'High Wind' WHEN (precipitation_probability >= 60) THEN 'Precipitation' END) alert_type
   FROM
     forecast_view
   WHERE ((temperature_fahrenheit >= 95) OR (temperature_fahrenheit <= 32) OR (wind_speed_ms >= 1.341E1) OR (precipitation_probability >= 60) OR (has_alerts = 'Yes'))
) 
WHERE (alert_type IS NOT NULL)
GROUP BY region, timestamp
