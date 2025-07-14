"""
Fitbit Google Takeout JSON parser for extracting and converting data.
"""

import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import re
from dateutil.parser import parse as parse_datetime

from .models import (
    FitbitUserData, ActivityData, SleepData, DailyMetrics, BodyComposition,
    HeartRateData, HeartRateVariability, StressData, TemperatureData,
    SpO2Data, ActiveZoneMinutes, ActivityType, HeartRateZone
)

logger = logging.getLogger(__name__)


class FitbitParser:
    """Parser for Fitbit Google Takeout data."""
    
    def __init__(self, takeout_path: Union[str, Path]):
        """Initialize parser with path to extracted Google Takeout data."""
        self.takeout_path = Path(takeout_path)
        
        # Try different possible Fitbit paths
        possible_paths = [
            self.takeout_path / "Takeout" / "Fitbit",
            self.takeout_path / "Takeout 2" / "Fitbit", 
            self.takeout_path / "Fitbit",
            self.takeout_path  # If takeout_path already points to Fitbit directory
        ]
        
        self.fitbit_path = None
        for path in possible_paths:
            if path.exists():
                self.fitbit_path = path
                break
        
        if not self.fitbit_path:
            raise FileNotFoundError(f"Fitbit data not found. Tried: {[str(p) for p in possible_paths]}")
        
        logger.info(f"Initialized Fitbit parser for: {self.fitbit_path}")
        
        # Discover all subdirectories
        self.data_directories = self._discover_data_directories()
    
    def _discover_data_directories(self) -> Dict[str, Path]:
        """Discover all data directories in the Fitbit export."""
        directories = {}
        
        # Map directory names to data types
        directory_mapping = {
            'Global Export Data': 'global_export',
            'Activities': 'activities',
            'Sleep': 'sleep',
            'Sleep Score': 'sleep_score',
            'Heart Rate Variability': 'hrv',
            'Biometrics': 'biometrics',
            'Active Zone Minutes (AZM)': 'active_zone_minutes',
            'Daily Readiness': 'daily_readiness',
            'Stress Journal': 'stress',
            'Oxygen Saturation (SpO2)': 'spo2',
            'Temperature': 'temperature',
            'Physical Activity_GoogleData': 'physical_activity',
            'Snore and Noise Detect': 'snore_noise',
            'Mindfulness': 'mindfulness',
            'Menstrual Health': 'menstrual_health'
        }
        
        for subdir in self.fitbit_path.iterdir():
            if subdir.is_dir():
                dir_name = subdir.name
                if dir_name in directory_mapping:
                    directories[directory_mapping[dir_name]] = subdir
                    logger.debug(f"Found data directory: {dir_name} -> {subdir}")
        
        logger.info(f"Discovered {len(directories)} data directories: {list(directories.keys())}")
        return directories
    
    def parse_all_data(self) -> FitbitUserData:
        """Parse all available Fitbit data and return structured data."""
        logger.info("Starting to parse all Fitbit data")
        print("🔍 Discovering data directories...")
        
        user_data = FitbitUserData()
        
        # Parse different data types with progress reporting
        print("📊 Parsing daily metrics and activities...")
        user_data.activities = self._parse_activities()
        user_data.daily_metrics = self._parse_daily_metrics()
        
        print("😴 Parsing sleep data...")
        user_data.sleep_data = self._parse_sleep_data()
        
        print("❤️ Parsing heart rate data...")
        user_data.heart_rate_data = self._parse_heart_rate_data()
        
        print("🏋️ Parsing body composition...")
        user_data.body_composition = self._parse_body_composition()
        
        print("📈 Parsing additional health metrics...")
        user_data.heart_rate_variability = self._parse_heart_rate_variability()
        user_data.stress_data = self._parse_stress_data()
        user_data.temperature_data = self._parse_temperature_data()
        user_data.spo2_data = self._parse_spo2_data()
        user_data.active_zone_minutes = self._parse_active_zone_minutes()
        
        logger.info(f"Parsed data summary: {user_data.total_activities} activities, "
                   f"{user_data.total_sleep_records} sleep records, "
                   f"{user_data.total_daily_records} daily records")
        
        print(f"✅ Parsing complete! Found {user_data.total_activities} activities, "
              f"{user_data.total_sleep_records} sleep records, "
              f"{user_data.total_daily_records} daily records")
        
        return user_data
    
    def _parse_activities(self) -> List[ActivityData]:
        """Parse activity data from Fitbit exports."""
        activities = []
        
        # Check for activities in different possible locations
        activity_paths = []
        
        # Add discovered directories
        if 'activities' in self.data_directories:
            activity_paths.append(self.data_directories['activities'])
        if 'global_export' in self.data_directories:
            activity_paths.append(self.data_directories['global_export'])
        if 'physical_activity' in self.data_directories:
            activity_paths.append(self.data_directories['physical_activity'])
        
        for activity_path in activity_paths:
            if activity_path.exists():
                print(f"  📁 Processing activities from: {activity_path.name}")
                activities.extend(self._parse_activities_from_path(activity_path))
        
        logger.info(f"Parsed {len(activities)} activities")
        return activities
    
    def _parse_activities_from_path(self, path: Path) -> List[ActivityData]:
        """Parse activities from a specific path."""
        activities = []
        
        # Look for activity JSON files
        json_files = list(path.glob("**/*.json"))
        activity_files = []
        
        for json_file in json_files:
            filename = json_file.name.lower()
            if any(keyword in filename for keyword in ['exercise', 'activity', 'workout']):
                activity_files.append(json_file)
        
        print(f"    📋 Found {len(activity_files)} activity files")
        
        # Optional: Limit processing for performance
        # activity_files = activity_files[:10]  # Uncomment to process only first 10 files
        
        for json_file in activity_files:
            try:
                print(f"    📄 Processing: {json_file.name}")
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, list):
                    for item in data:
                        activity = self._parse_single_activity(item)
                        if activity:
                            activities.append(activity)
                elif isinstance(data, dict):
                    activity = self._parse_single_activity(data)
                    if activity:
                        activities.append(activity)
                        
            except (json.JSONDecodeError, Exception) as e:
                logger.warning(f"Error parsing activity file {json_file}: {e}")
        
        return activities
    
    def _parse_single_activity(self, data: Dict[str, Any]) -> Optional[ActivityData]:
        """Parse a single activity from JSON data."""
        try:
            # Extract basic activity information
            log_id = data.get('logId', 0)
            activity_name = data.get('activityName', 'Unknown')
            
            # Parse start time
            start_time_str = data.get('startTime', data.get('originalStartTime', ''))
            if start_time_str:
                start_time = parse_datetime(start_time_str)
            else:
                logger.warning(f"No start time found for activity {log_id}")
                return None
            
            # Parse duration
            duration_ms = data.get('activeDuration', data.get('duration', 0))
            
            # Parse activity type
            activity_type = self._map_activity_type(activity_name)
            
            # Parse optional fields
            calories = data.get('calories')
            distance = data.get('distance')
            steps = data.get('steps')
            
            # Parse heart rate zones
            heart_rate_zones = []
            if 'heartRateZones' in data:
                for zone in data['heartRateZones']:
                    hr_zone = HeartRateZone(
                        name=zone.get('name', 'Unknown'),
                        min_bpm=zone.get('min', 0),
                        max_bpm=zone.get('max', 0),
                        minutes=zone.get('minutes', 0),
                        calories_out=zone.get('caloriesOut')
                    )
                    heart_rate_zones.append(hr_zone)
            
            # Parse average/max heart rate
            average_heart_rate = data.get('averageHeartRate')
            max_heart_rate = data.get('maxHeartRate')
            
            # Parse GPS data if available
            gps_data = data.get('gpsData')
            tcx_data = data.get('tcxData', data.get('tcxLink'))
            
            return ActivityData(
                log_id=log_id,
                activity_name=activity_name,
                activity_type=activity_type,
                start_time=start_time,
                duration_ms=duration_ms,
                calories=calories,
                distance=distance,
                steps=steps,
                heart_rate_zones=heart_rate_zones,
                average_heart_rate=average_heart_rate,
                max_heart_rate=max_heart_rate,
                gps_data=gps_data,
                tcx_data=tcx_data
            )
            
        except Exception as e:
            logger.warning(f"Error parsing activity data: {e}")
            return None
    
    def _map_activity_type(self, activity_name: str) -> ActivityType:
        """Map Fitbit activity name to our ActivityType enum."""
        name_lower = activity_name.lower()
        
        if any(word in name_lower for word in ['run', 'jog']):
            return ActivityType.RUN
        elif any(word in name_lower for word in ['walk', 'walking']):
            return ActivityType.WALK
        elif any(word in name_lower for word in ['bike', 'cycling', 'bicycle']):
            return ActivityType.BIKE
        elif any(word in name_lower for word in ['hike', 'hiking']):
            return ActivityType.HIKE
        elif any(word in name_lower for word in ['swim', 'swimming']):
            return ActivityType.SWIM
        elif any(word in name_lower for word in ['workout', 'exercise']):
            return ActivityType.WORKOUT
        elif any(word in name_lower for word in ['yoga', 'pilates']):
            return ActivityType.YOGA
        elif any(word in name_lower for word in ['weight', 'strength']):
            return ActivityType.WEIGHTS
        elif any(word in name_lower for word in ['sport', 'tennis', 'basketball']):
            return ActivityType.SPORT
        else:
            return ActivityType.OTHER
    
    def _parse_sleep_data(self) -> List[SleepData]:
        """Parse sleep data from Fitbit exports."""
        sleep_data = []
        
        sleep_paths = []
        
        # Add discovered directories
        if 'sleep' in self.data_directories:
            sleep_paths.append(self.data_directories['sleep'])
        if 'sleep_score' in self.data_directories:
            sleep_paths.append(self.data_directories['sleep_score'])
        if 'global_export' in self.data_directories:
            sleep_paths.append(self.data_directories['global_export'])
        
        for sleep_path in sleep_paths:
            if sleep_path.exists():
                print(f"  📁 Processing sleep data from: {sleep_path.name}")
                
                # Look for sleep-related files
                json_files = list(sleep_path.glob("**/*.json"))
                sleep_files = [f for f in json_files if "sleep" in f.name.lower()]
                
                print(f"    📋 Found {len(sleep_files)} sleep files")
                
                for json_file in sleep_files:
                    try:
                        print(f"    📄 Processing: {json_file.name}")
                        with open(json_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        if isinstance(data, list):
                            for item in data:
                                sleep = self._parse_single_sleep_record(item)
                                if sleep:
                                    sleep_data.append(sleep)
                    except (json.JSONDecodeError, Exception) as e:
                        logger.warning(f"Error parsing sleep file {json_file}: {e}")
        
        logger.info(f"Parsed {len(sleep_data)} sleep records")
        return sleep_data
    
    def _parse_single_sleep_record(self, data: Dict[str, Any]) -> Optional[SleepData]:
        """Parse a single sleep record from JSON data."""
        try:
            log_id = data.get('logId', 0)
            date_of_sleep = parse_datetime(data.get('dateOfSleep', '')).date()
            start_time = parse_datetime(data.get('startTime', ''))
            end_time = parse_datetime(data.get('endTime', ''))
            duration_ms = data.get('duration', 0)
            
            return SleepData(
                log_id=log_id,
                date_of_sleep=date_of_sleep,
                start_time=start_time,
                end_time=end_time,
                duration_ms=duration_ms,
                efficiency=data.get('efficiency'),
                minutes_awake=data.get('minutesAwake'),
                minutes_asleep=data.get('minutesAsleep'),
                minutes_to_fall_asleep=data.get('minutesToFallAsleep'),
                minutes_after_wakeup=data.get('minutesAfterWakeup'),
                time_in_bed=data.get('timeInBed'),
                sleep_stages=data.get('levels', {}).get('data', [])
            )
        except Exception as e:
            logger.warning(f"Error parsing sleep record: {e}")
            return None
    
    def _parse_daily_metrics(self) -> List[DailyMetrics]:
        """Parse daily metrics from various Fitbit data files."""
        daily_metrics = []
        
        # Parse from different data sources
        metrics_paths = []
        
        # Add discovered directories
        if 'global_export' in self.data_directories:
            metrics_paths.append(self.data_directories['global_export'])
        
        for metrics_path in metrics_paths:
            if metrics_path.exists():
                print(f"  📁 Processing daily metrics from: {metrics_path.name}")
                daily_metrics.extend(self._parse_daily_metrics_from_path(metrics_path))
        
        logger.info(f"Parsed {len(daily_metrics)} daily metric records")
        return daily_metrics
    
    def _parse_daily_metrics_from_path(self, path: Path) -> List[DailyMetrics]:
        """Parse daily metrics from a specific path."""
        metrics = []
        
        # Look for daily metrics files
        json_files = list(path.glob("**/*.json"))
        
        # Filter for daily metrics files
        daily_files = []
        for json_file in json_files:
            file_name = json_file.name.lower()
            
            # Include files that contain daily metrics
            if any(keyword in file_name for keyword in ['steps', 'distance', 'calories', 'floors', 'elevation']):
                daily_files.append(json_file)
            # Skip activity, sleep, and heart rate files
            elif any(skip in file_name for skip in ['exercise', 'activity', 'sleep', 'heart_rate']):
                continue
            else:
                # Include other potential daily files
                daily_files.append(json_file)
        
        print(f"    📋 Found {len(daily_files)} daily metrics files")
        
        for json_file in daily_files:
            try:
                print(f"    📄 Processing: {json_file.name}")
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, list):
                    for item in data:
                        metric = self._parse_single_daily_metric(item)
                        if metric:
                            metrics.append(metric)
                elif isinstance(data, dict):
                    metric = self._parse_single_daily_metric(data)
                    if metric:
                        metrics.append(metric)
                        
            except (json.JSONDecodeError, Exception) as e:
                logger.warning(f"Error parsing daily metrics file {json_file}: {e}")
        
        return metrics
    
    def _parse_single_daily_metric(self, data: Dict[str, Any]) -> Optional[DailyMetrics]:
        """Parse a single daily metric record."""
        try:
            date_str = data.get('dateTime', data.get('date', ''))
            if not date_str:
                return None
                
            record_date = parse_datetime(date_str).date()
            
            return DailyMetrics(
                date=record_date,
                steps=data.get('steps'),
                distance=data.get('distance'),
                calories_burned=data.get('caloriesOut'),
                calories_bmr=data.get('caloriesBMR'),
                active_minutes=data.get('activeMinutes'),
                sedentary_minutes=data.get('sedentaryMinutes'),
                lightly_active_minutes=data.get('lightlyActiveMinutes'),
                fairly_active_minutes=data.get('fairlyActiveMinutes'),
                very_active_minutes=data.get('veryActiveMinutes'),
                floors=data.get('floors'),
                elevation=data.get('elevation'),
                resting_heart_rate=data.get('restingHeartRate')
            )
        except Exception as e:
            logger.warning(f"Error parsing daily metric: {e}")
            return None
    
    def _parse_heart_rate_data(self) -> List[HeartRateData]:
        """Parse heart rate data from Fitbit exports."""
        heart_rate_data = []
        
        if 'global_export' in self.data_directories:
            hr_path = self.data_directories['global_export']
            print(f"  📁 Processing heart rate data from: {hr_path.name}")
            
            hr_files = list(hr_path.glob("heart_rate*.json"))
            print(f"    📋 Found {len(hr_files)} heart rate files")
            
            # Optional: Limit processing for performance
            # hr_files = hr_files[:10]  # Uncomment to process only first 10 files
            
            for json_file in hr_files:
                try:
                    print(f"    📄 Processing: {json_file.name}")
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if isinstance(data, list):
                        for item in data:
                            hr_data = self._parse_single_heart_rate(item)
                            if hr_data:
                                heart_rate_data.append(hr_data)
                                
                except (json.JSONDecodeError, Exception) as e:
                    logger.warning(f"Error parsing heart rate file {json_file}: {e}")
        
        logger.info(f"Parsed {len(heart_rate_data)} heart rate records")
        return heart_rate_data
    
    def _parse_single_heart_rate(self, data: Dict[str, Any]) -> Optional[HeartRateData]:
        """Parse a single heart rate record."""
        try:
            datetime_str = data.get('dateTime', '')
            if not datetime_str:
                return None
            
            # Handle the format like "03/10/22 16:44:00"
            if datetime_str:
                # Try different datetime formats
                try:
                    # Try MM/DD/YY format first
                    dt = datetime.strptime(datetime_str, "%m/%d/%y %H:%M:%S")
                except ValueError:
                    try:
                        # Try other common formats
                        dt = parse_datetime(datetime_str)
                    except:
                        logger.warning(f"Could not parse datetime: {datetime_str}")
                        return None
            else:
                return None
                
            value = data.get('value', {})
            
            return HeartRateData(
                datetime=dt,
                bpm=value.get('bpm', 0),
                confidence=value.get('confidence', 0)
            )
        except Exception as e:
            logger.warning(f"Error parsing heart rate record: {e}")
            return None
    
    def _parse_body_composition(self) -> List[BodyComposition]:
        """Parse body composition data."""
        # Placeholder for body composition parsing
        return []
    
    def _parse_heart_rate_variability(self) -> List[HeartRateVariability]:
        """Parse heart rate variability data."""
        # Placeholder for HRV parsing
        return []
    
    def _parse_stress_data(self) -> List[StressData]:
        """Parse stress data."""
        # Placeholder for stress data parsing
        return []
    
    def _parse_temperature_data(self) -> List[TemperatureData]:
        """Parse temperature data."""
        # Placeholder for temperature data parsing
        return []
    
    def _parse_spo2_data(self) -> List[SpO2Data]:
        """Parse SpO2 data."""
        # Placeholder for SpO2 data parsing
        return []
    
    def _parse_active_zone_minutes(self) -> List[ActiveZoneMinutes]:
        """Parse active zone minutes data."""
        # Placeholder for active zone minutes parsing
        return []