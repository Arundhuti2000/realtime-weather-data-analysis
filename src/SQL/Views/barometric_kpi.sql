CREATE OR REPLACE VIEW "barometric_pressure_kpi" AS 
SELECT
  DATE_TRUNC('hour', CAST(timestamp AS timestamp)) trend_hour
, ROUND(AVG((CAST(barometric_pressure AS decimal) * 2.953E-4)), 2) current_pressure_inhg
, 2.992E1 target_pressure_inhg
, COUNT(*) reading_count
, (CASE WHEN (AVG((CAST(barometric_pressure AS decimal) * 2.953E-4)) >= 3.02E1) THEN 'High' WHEN (AVG((CAST(barometric_pressure AS decimal) * 2.953E-4)) <= 2.98E1) THEN 'Low' ELSE 'Normal' END) pressure_status
, ROUND((((AVG((CAST(barometric_pressure AS decimal) * 2.953E-4)) - 2.992E1) / 2.992E1) * 100), 2) target_difference_percentage
FROM
  forecast_view
WHERE ((timestamp >= (current_date - INTERVAL  '24' HOUR)) AND (barometric_pressure IS NOT NULL))
GROUP BY DATE_TRUNC('hour', CAST(timestamp AS timestamp))
ORDER BY trend_hour ASC
