CREATE OR REPLACE VIEW "forecast_word_cloud" AS 
SELECT
  (CASE WHEN ((short_forecast IS NOT NULL) AND (short_forecast <> '{}')) THEN short_forecast WHEN ((detailed_forecast IS NOT NULL) AND (detailed_forecast <> '{}')) THEN detailed_forecast END) forecast_text
, COUNT(*) word_count
, region
FROM
  forecast_view
WHERE ((timestamp >= (current_date - INTERVAL  '7' DAY)) AND (((short_forecast IS NOT NULL) AND (short_forecast <> '{}')) OR ((detailed_forecast IS NOT NULL) AND (detailed_forecast <> '{}'))))
GROUP BY (CASE WHEN ((short_forecast IS NOT NULL) AND (short_forecast <> '{}')) THEN short_forecast WHEN ((detailed_forecast IS NOT NULL) AND (detailed_forecast <> '{}')) THEN detailed_forecast END), region
HAVING ((CASE WHEN ((short_forecast IS NOT NULL) AND (short_forecast <> '{}')) THEN short_forecast WHEN ((detailed_forecast IS NOT NULL) AND (detailed_forecast <> '{}')) THEN detailed_forecast END) IS NOT NULL)
