import json
import boto3
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging
from botocore.exceptions import ClientError
import csv
import io
import time
from collections.abc import Mapping

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS S3 client
s3 = boto3.client('s3')

# Constants
BUCKET_NAME: str = 'weather-processed-data-2024'
NWS_API_BASE: str = 'https://api.weather.gov'

# Define fieldnames globally for consistency
FIELDNAMES = [
    'timestamp',
    'region',
    'temperature_celsius',
    'temperature_fahrenheit',
    'humidity',
    'wind_speed_ms',
    'wind_direction',
    'barometric_pressure',
    'visibility',
    'dew_point',
    'heat_index',
    'wind_chill',
    'present_weather',
    'forecast_temp',
    'short_forecast',
    'detailed_forecast',
    'snow_level',
    'ice_accumulation',
    'precipitation_probability',
    'max_temperature',
    'min_temperature',
    'uv_index',
    'has_alerts'
]

# Regions with type annotation
REGIONS: List[Dict[str, str]] = [
    # Northeast (varied seasons, coastal effects)
    {'name': 'UMass_Dartmouth', 'lat': '41.6297', 'lon': '-71.0068', 'description': 'Base location - varied seasonal weather'},
    {'name': 'NYC_Central_Park', 'lat': '40.7829', 'lon': '-73.9654', 'description': 'Urban heat island, coastal effects'},
    {'name': 'Mount_Washington_NH', 'lat': '44.2706', 'lon': '-71.3033', 'description': 'Extreme wind, alpine conditions'},
    
    # Southeast (tropical systems, severe weather)
    {'name': 'Miami_FL', 'lat': '25.7617', 'lon': '-80.1918', 'description': 'Tropical weather, hurricanes'},
    {'name': 'New_Orleans_LA', 'lat': '29.9511', 'lon': '-90.0715', 'description': 'Gulf Coast weather, high humidity'},
    
    # Midwest (severe weather corridor)
    {'name': 'Oklahoma_City_OK', 'lat': '35.4676', 'lon': '-97.5164', 'description': 'Tornado alley, severe storms'},
    {'name': 'Chicago_IL', 'lat': '41.8781', 'lon': '-87.6298', 'description': 'Lake effect weather'},
    
    # Mountain Regions (elevation effects)
    {'name': 'Denver_CO', 'lat': '39.7392', 'lon': '-104.9903', 'description': 'High altitude, mountain weather'},
    {'name': 'Salt_Lake_City_UT', 'lat': '40.7608', 'lon': '-111.8910', 'description': 'Lake effect snow'},
    
    # West Coast (marine influence)
    {'name': 'Seattle_WA', 'lat': '47.6062', 'lon': '-122.3321', 'description': 'Marine climate, persistent clouds'},
    {'name': 'San_Francisco_CA', 'lat': '37.7749', 'lon': '-122.4194', 'description': 'Marine layer, microclimate'},
    
    # Desert Southwest
    {'name': 'Phoenix_AZ', 'lat': '33.4484', 'lon': '-112.0740', 'description': 'Extreme heat, monsoon'},
    
    # Alaska (extreme conditions)
    {'name': 'Anchorage_AK', 'lat': '61.2181', 'lon': '-149.9003', 'description': 'Subarctic conditions'},
    
    # Hawaii (tropical patterns)
    {'name': 'Honolulu_HI', 'lat': '21.3069', 'lon': '-157.8583', 'description': 'Tropical climate'}
]

def get_station_data(lat: str, lon: str) -> Dict[str, str]:
    """Get the nearest weather station data."""
    try:
        points_url = f'{NWS_API_BASE}/points/{lat},{lon}'
        headers = {
            'User-Agent': '(UMass Dartmouth Weather Project, adas5@umassd.edu)'
        }
        
        response = requests.get(points_url, headers=headers, timeout=10)
        response.raise_for_status()
        grid_data = response.json()
        
        # Get both station and grid endpoints
        station_data = {
            'station_id': grid_data['properties']['observationStations'],
            'forecast_grid': grid_data['properties']['forecastGridData'],
            'forecast_hourly': grid_data['properties']['forecastHourly'],
            'forecast': grid_data['properties']['forecast']
        }
        
        # Get nearest station
        stations_response = requests.get(station_data['station_id'], headers=headers, timeout=10)
        stations_response.raise_for_status()
        stations_json = stations_response.json()
        
        if not stations_json.get('features'):
            raise ValueError("No weather stations found in response")
            
        station_data['station_id'] = stations_json['features'][0]['properties']['stationIdentifier']
        
        return station_data
        
    except Exception as e:
        logger.error(f"Error getting station data: {str(e)}")
        raise

def get_weather_alerts(lat: str, lon: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Get active weather alerts for a region."""
    try:
        # Get the zone ID
        points_url = f'{NWS_API_BASE}/points/{lat},{lon}'
        response = requests.get(points_url, headers=headers, timeout=10)
        response.raise_for_status()
        zone_id = response.json()['properties']['forecastZone'].split('/')[-1]
        
        # Get alerts for that zone
        alerts_url = f'{NWS_API_BASE}/alerts/active/zone/{zone_id}'
        alerts_response = requests.get(alerts_url, headers=headers, timeout=10)
        alerts_response.raise_for_status()
        return alerts_response.json()
        
    except Exception as e:
        logger.error(f"Error getting alerts: {str(e)}")
        return {'features': []}

def get_weather_data(station_data: Dict[str, str], headers: Dict[str, str]) -> Dict[str, Any]:
    """Get comprehensive weather data including forecasts."""
    try:
        weather_data: Dict[str, Any] = {}
        
        # Current observations
        obs_url = f'{NWS_API_BASE}/stations/{station_data["station_id"]}/observations/latest'
        current_response = requests.get(obs_url, headers=headers, timeout=10)
        current_response.raise_for_status()
        weather_data['current'] = current_response.json()
        
        # Collect other forecasts
        endpoints = {
            'hourly': station_data['forecast_hourly'],
            'forecast': station_data['forecast'],
            'grid': station_data['forecast_grid']
        }
        
        for data_type, url in endpoints.items():
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                weather_data[data_type] = response.json()
            except Exception as e:
                logger.error(f"Error getting {data_type} data: {str(e)}")
                weather_data[data_type] = {}
        
        return weather_data
        
    except Exception as e:
        logger.error(f"Error getting weather data: {str(e)}")
        raise

def process_weather_data(raw_data: Dict[str, Any], region_name: str) -> Dict[str, Any]:
    """Process weather data into unified format."""
    try:
        current = raw_data.get('current', {}).get('properties', {})
        forecast_periods = raw_data.get('forecast', {}).get('properties', {}).get('periods', [])
        forecast = forecast_periods[0] if forecast_periods else {}
        grid = raw_data.get('grid', {}).get('properties', {})
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        def safe_get_value(data: Optional[Mapping], *keys: str, default: Any = '') -> Any:
            try:
                result = data or {}
                for key in keys:
                    if not isinstance(result, Mapping):
                        return default
                    result = result.get(key, {})
                return result if result is not None else default
            except (AttributeError, TypeError):
                return default

        def safe_get_grid_value(grid_data: dict, property_name: str) -> Any:
            try:
                values = grid_data.get(property_name, {}).get('values', [])
                return values[0].get('value', '') if values else ''
            except (IndexError, AttributeError, TypeError):
                return ''

        def format_number(value: Any) -> Any:
            if value is None or value == '':
                return ''
            try:
                float_val = float(value)
                return float_val if float_val != int(float_val) else int(float_val)
            except (ValueError, TypeError):
                return value

        # Initialize all fields with empty strings
        processed_data = {field: '' for field in FIELDNAMES}
        
        # Update with actual values
        processed_data.update({
            'timestamp': timestamp,
            'region': region_name,
            'temperature_celsius': format_number(safe_get_value(current, 'temperature', 'value')),
            'temperature_fahrenheit': format_number(
                safe_get_value(current, 'temperature', 'value', default=0) * 9/5 + 32 
                if safe_get_value(current, 'temperature', 'value') is not None else ''
            ),
            'humidity': format_number(safe_get_value(current, 'relativeHumidity', 'value')),
            'wind_speed_ms': format_number(safe_get_value(current, 'windSpeed', 'value')),
            'wind_direction': format_number(safe_get_value(current, 'windDirection', 'value')),
            'barometric_pressure': format_number(safe_get_value(current, 'barometricPressure', 'value')),
            'visibility': format_number(safe_get_value(current, 'visibility', 'value')),
            'dew_point': format_number(safe_get_value(current, 'dewpoint', 'value')),
            'heat_index': format_number(safe_get_value(current, 'heatIndex', 'value')),
            'wind_chill': format_number(safe_get_value(current, 'windChill', 'value')),
            'present_weather': str(safe_get_value(current, 'textDescription')),
            'forecast_temp': format_number(safe_get_value(forecast, 'temperature')),
            'short_forecast': str(safe_get_value(forecast, 'shortForecast')),
            'detailed_forecast': str(safe_get_value(forecast, 'detailedForecast')).strip('"').replace(',', '|'),
            'snow_level': format_number(safe_get_grid_value(grid, 'snowLevel')),
            'ice_accumulation': format_number(safe_get_grid_value(grid, 'iceAccumulation')),
            'precipitation_probability': format_number(safe_get_grid_value(grid, 'probabilityOfPrecipitation')),
            'max_temperature': format_number(safe_get_grid_value(grid, 'maxTemperature')),
            'min_temperature': format_number(safe_get_grid_value(grid, 'minTemperature')),
            'uv_index': format_number(safe_get_grid_value(grid, 'maxDaytimeUVIndex')),
            'has_alerts': 'Yes' if raw_data.get('alerts', {}).get('features') else 'No'
        })
        
        return processed_data
        
    except Exception as e:
        logger.error(f"Error processing weather data for region {region_name}: {str(e)}")
        raise

def save_consolidated_data(data: Dict[str, Any], bucket_name: str) -> None:
    """Save weather data to a single consolidated CSV file."""
    try:
        current_timestamp = datetime.now()
        file_name = f'weather_data_{current_timestamp.strftime("%Y-%m-%d")}.csv'
        
        # Clean data function
        def clean_value(value: Any) -> str:
            if value is None:
                return ''
            if isinstance(value, bool):
                return str(value)
            if isinstance(value, (int, float)):
                return str(value)
            if isinstance(value, str):
                # Remove any problematic characters and extra whitespace
                cleaned = value.strip().replace('\n', ' ').replace('\r', '')
                # Remove multiple quotes
                while '""' in cleaned:
                    cleaned = cleaned.replace('""', '"')
                # Remove leading/trailing quotes if they exist
                cleaned = cleaned.strip('"')
                # If the string contains commas or quotes, properly quote it
                if ',' in cleaned or '"' in cleaned:
                    return f'"{cleaned.replace("`", "")}"'
                return cleaned
            return str(value)

        output = io.StringIO()
        existing_entries = set()  # To track unique entries

        try:
            # Try to get existing file
            existing_obj = s3.get_object(Bucket=bucket_name, Key=file_name)
            existing_csv = existing_obj['Body'].read().decode('utf-8')
            
            # Check if the existing file is empty or corrupted
            if existing_csv.strip() and ',' in existing_csv:
                reader = csv.DictReader(io.StringIO(existing_csv))
                rows = []
                for row in reader:
                    # Create a unique key for each entry
                    entry_key = f"{row['timestamp']}_{row['region']}"
                    if entry_key not in existing_entries:
                        existing_entries.add(entry_key)
                        rows.append(row)
                
                # Write existing data
                writer = csv.DictWriter(output, fieldnames=FIELDNAMES)
                writer.writeheader()
                for row in rows:
                    cleaned_row = {k: clean_value(row.get(k, '')) for k in FIELDNAMES}
                    writer.writerow(cleaned_row)
                    
        except (s3.exceptions.NoSuchKey, csv.Error):
            # Create new file if it doesn't exist or if there's an error
            writer = csv.DictWriter(output, fieldnames=FIELDNAMES)
            writer.writeheader()

        # Check if this is a duplicate entry
        new_entry_key = f"{data['timestamp']}_{data['region']}"
        if new_entry_key not in existing_entries:
            # Clean and write new data
            cleaned_data = {k: clean_value(data.get(k, '')) for k in FIELDNAMES}
            if not output.getvalue():  # If file is empty, write header
                writer = csv.DictWriter(output, fieldnames=FIELDNAMES)
                writer.writeheader()
            writer = csv.DictWriter(output, fieldnames=FIELDNAMES)
            writer.writerow(cleaned_data)
            # Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=output.getvalue(),
            ContentType='text/csv'
        )
        logger.info(f"Successfully saved data to s3://{bucket_name}/{file_name}")
        
    except Exception as e:
        logger.error(f"Error saving to S3: {str(e)}")
        raise

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler function."""
    # Add execution tracking
    execution_start = datetime.now()
    logger.info(f"Starting execution at: {execution_start}")

    try:
        headers = {
            'User-Agent': '(UMass Dartmouth Weather Project, adas5@umassd.edu)'
        }
        
        processed_regions = 0
        failed_regions = 0
        results = {
            'successful_regions': [],
            'failed_regions': []
        }
        
        # Process each region only once
        for region in REGIONS:
            try:
                logger.info(f"Processing region: {region['name']} at {datetime.now()}")
                
                # Add delay for rate limiting (NWS API requirement)
                if processed_regions > 0:  # Don't delay on first request
                    time.sleep(1)
                
                # Get station and weather data
                station_data = get_station_data(region['lat'], region['lon'])
                raw_weather_data = get_weather_data(station_data, headers)
                alerts_data = get_weather_alerts(region['lat'], region['lon'], headers)
                raw_weather_data['alerts'] = alerts_data
                
                # Process and save data
                processed_data = process_weather_data(raw_weather_data, region['name'])
                
                # Add execution timestamp to track duplicates
                processed_data['execution_timestamp'] = execution_start.strftime('%Y-%m-%d %H:%M:%S')
                
                save_consolidated_data(processed_data, BUCKET_NAME)
                
                processed_regions += 1
                results['successful_regions'].append(region['name'])
                
            except Exception as e:
                failed_regions += 1
                results['failed_regions'].append({
                    'region': region['name'],
                    'error': str(e)
                })
                logger.error(f"Error processing region {region['name']}: {str(e)}")
                continue
        
        logger.info(f"Completed execution at: {datetime.now()}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weather data collection completed',
                'execution_start': execution_start.strftime('%Y-%m-%d %H:%M:%S'),
                'execution_end': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'processed_regions': processed_regions,
                'failed_regions': failed_regions,
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f"Lambda execution failed: {str(e)}",
                'execution_start': execution_start.strftime('%Y-%m-%d %H:%M:%S'),
                'results': results
            })
        }
