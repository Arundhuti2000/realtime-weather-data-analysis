CREATE OR REPLACE VIEW "alert_severity_levels" AS 
SELECT
  timestamp
, region
, (CASE WHEN ((temperature_fahrenheit >= 95) OR (temperature_fahrenheit <= 20) OR (wind_speed_ms >= 2.012E1) OR (precipitation_probability >= 80)) THEN 'Critical' WHEN ((temperature_fahrenheit >= 85) OR (temperature_fahrenheit <= 32) OR (wind_speed_ms >= 1.341E1) OR (precipitation_probability >= 60)) THEN 'Warning' WHEN (has_alerts = 'Yes') THEN 'Advisory' ELSE 'Normal' END) severity_level
, temperature_fahrenheit
, wind_speed_ms
, precipitation_probability
, present_weather
, short_forecast
FROM
  forecast_view
