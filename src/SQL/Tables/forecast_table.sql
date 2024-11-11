CREATE EXTERNAL TABLE `forecast`(
  `timestamp` string, 
  `region` string, 
  `temperature_celsius` double, 
  `temperature_fahrenheit` double, 
  `humidity` double, 
  `wind_speed_ms` double, 
  `wind_direction` int, 
  `barometric_pressure` double, 
  `visibility` double, 
  `dew_point` double, 
  `heat_index` double, 
  `wind_chill` double, 
  `present_weather` string, 
  `forecast_temp` int, 
  `short_forecast` string, 
  `detailed_forecast` string, 
  `snow_level` double, 
  `ice_accumulation` double, 
  `precipitation_probability` double, 
  `max_temperature` double, 
  `min_temperature` double, 
  `uv_index` double, 
  `has_alerts` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://weather-processed-data-2024/data'
TBLPROPERTIES (
  'skip.header.line.count'='1', 
  'transient_lastDdlTime'='1731177280')