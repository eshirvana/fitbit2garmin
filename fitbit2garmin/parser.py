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
from tqdm import tqdm
import ijson  # For streaming JSON parsing
import pandas as pd

from .models import (
    FitbitUserData,
    ActivityData,
    SleepData,
    DailyMetrics,
    BodyComposition,
    HeartRateData,
    HeartRateVariability,
    StressData,
    TemperatureData,
    SpO2Data,
    ActiveZoneMinutes,
    ActivityType,
    HeartRateZone,
)
from .utils import ParallelProcessor, ResumeManager, process_json_file_worker
from .heart_rate_zones import HeartRateZoneCalculator, UserProfile

logger = logging.getLogger(__name__)


class FitbitParser:
    """Parser for Fitbit Google Takeout data."""

    def __init__(
        self,
        takeout_path: Union[str, Path],
        enable_resume: bool = True,
        enable_parallel: bool = True,
        max_workers: Optional[int] = None,
    ):
        """Initialize parser with path to extracted Google Takeout data."""
        self.takeout_path = Path(takeout_path)
        self.enable_resume = enable_resume
        self.enable_parallel = enable_parallel

        # Try different possible Fitbit paths
        possible_paths = [
            self.takeout_path / "Takeout" / "Fitbit",
            self.takeout_path / "Takeout 2" / "Fitbit",
            self.takeout_path / "Fitbit",
            self.takeout_path,  # If takeout_path already points to Fitbit directory
        ]

        self.fitbit_path = None
        for path in possible_paths:
            if path.exists():
                self.fitbit_path = path
                break

        if not self.fitbit_path:
            raise FileNotFoundError(
                f"Fitbit data not found. Tried: {[str(p) for p in possible_paths]}"
            )

        logger.info(f"Initialized Fitbit parser for: {self.fitbit_path}")

        # Initialize parallel processor and resume manager
        if self.enable_parallel:
            self.parallel_processor = ParallelProcessor(max_workers)

        # Discover all subdirectories
        self.data_directories = self._discover_data_directories()

    def _discover_data_directories(self) -> Dict[str, Path]:
        """Discover all data directories in the Fitbit export."""
        directories = {}

        # Map directory names to data types
        directory_mapping = {
            "Global Export Data": "global_export",
            "Activities": "activities",
            "Sleep": "sleep",
            "Sleep Score": "sleep_score",
            "Heart Rate Variability": "hrv",
            "Biometrics": "biometrics",
            "Active Zone Minutes (AZM)": "active_zone_minutes",
            "Daily Readiness": "daily_readiness",
            "Stress Journal": "stress",
            "Oxygen Saturation (SpO2)": "spo2",
            "Temperature": "temperature",
            "Physical Activity_GoogleData": "physical_activity",
            "Snore and Noise Detect": "snore_noise",
            "Mindfulness": "mindfulness",
            "Menstrual Health": "menstrual_health",
        }

        for subdir in self.fitbit_path.iterdir():
            if subdir.is_dir():
                dir_name = subdir.name
                if dir_name in directory_mapping:
                    directories[directory_mapping[dir_name]] = subdir
                    logger.debug(f"Found data directory: {dir_name} -> {subdir}")

        logger.info(
            f"Discovered {len(directories)} data directories: {list(directories.keys())}"
        )
        return directories

    def _enhance_heart_rate_zones(self, user_data: FitbitUserData) -> FitbitUserData:
        """Enhance heart rate zones with advanced calculations and Garmin compatibility."""
        try:
            # Estimate user profile from data
            hr_calculator = HeartRateZoneCalculator()
            user_profile = hr_calculator.estimate_user_profile_from_data(
                user_data.activities, user_data.daily_metrics
            )

            logger.info(
                f"Estimated user profile: resting HR = {user_profile.resting_heart_rate}, "
                f"max HR = {user_profile.max_heart_rate}, fitness = {user_profile.fitness_level}"
            )

            # Update calculator with estimated profile
            hr_calculator.user_profile = user_profile

            # Enhance each activity with recalculated heart rate zones
            enhanced_activities = []
            for activity in user_data.activities:
                try:
                    enhanced_activity = hr_calculator.recalculate_activity_zones(
                        activity
                    )
                    enhanced_activities.append(enhanced_activity)
                except Exception as e:
                    logger.warning(
                        f"Error enhancing heart rate zones for activity {activity.log_id}: {e}"
                    )
                    enhanced_activities.append(activity)

            user_data.activities = enhanced_activities

            # Log statistics
            activities_with_zones = len(
                [a for a in user_data.activities if a.heart_rate_zones]
            )
            activities_with_recalc_zones = len(
                [a for a in user_data.activities if a.recalculated_hr_zones]
            )

            logger.info(
                f"Enhanced {activities_with_recalc_zones} activities with recalculated heart rate zones "
                f"(original zones: {activities_with_zones})"
            )

            if activities_with_recalc_zones > 0:
                print(
                    f"  ✅ Enhanced {activities_with_recalc_zones} activities with improved heart rate zones"
                )

            # Validate enhanced zones
            validation_issues = 0
            for activity in user_data.activities:
                if activity.recalculated_hr_zones:
                    issues = hr_calculator.validate_heart_rate_zones(
                        activity.recalculated_hr_zones
                    )
                    if issues:
                        validation_issues += 1
                        logger.debug(
                            f"Activity {activity.log_id} zone validation issues: {issues}"
                        )

            if validation_issues > 0:
                logger.warning(
                    f"Found validation issues in {validation_issues} activities"
                )

            return user_data

        except Exception as e:
            logger.error(f"Error enhancing heart rate zones: {e}")
            # Return original data if enhancement fails
            return user_data

    def _parse_json_file_efficiently(self, file_path: Path) -> Any:
        """Parse JSON file efficiently, using streaming for large files."""
        file_size = file_path.stat().st_size

        # Use streaming parser for files larger than 10MB
        if file_size > 10 * 1024 * 1024:  # 10MB
            try:
                with open(file_path, "rb") as f:
                    # For large files, try to stream parse as array
                    items = []
                    parser = ijson.items(f, "item")
                    for item in parser:
                        items.append(item)
                    return items
            except (ijson.JSONError, ValueError):
                # Fallback to regular parsing if streaming fails
                pass

        # Regular JSON parsing for smaller files or when streaming fails
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

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

        logger.info(
            f"Parsed data summary: {user_data.total_activities} activities, "
            f"{user_data.total_sleep_records} sleep records, "
            f"{user_data.total_daily_records} daily records"
        )

        print(
            f"✅ Parsing complete! Found {user_data.total_activities} activities, "
            f"{user_data.total_sleep_records} sleep records, "
            f"{user_data.total_daily_records} daily records"
        )

        # Enhance heart rate zones with advanced calculations
        print("⚡ Enhancing heart rate zones with advanced calculations...")
        user_data = self._enhance_heart_rate_zones(user_data)

        return user_data

    def _parse_activities(self) -> List[ActivityData]:
        """Parse activity data from Fitbit exports."""
        activities = []

        # Check for activities in different possible locations
        activity_paths = []

        # Add discovered directories
        if "activities" in self.data_directories:
            activity_paths.append(self.data_directories["activities"])
        if "global_export" in self.data_directories:
            activity_paths.append(self.data_directories["global_export"])
        if "physical_activity" in self.data_directories:
            activity_paths.append(self.data_directories["physical_activity"])

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
            if any(
                keyword in filename for keyword in ["exercise", "activity", "workout"]
            ):
                activity_files.append(json_file)

        print(f"    📋 Found {len(activity_files)} activity files")

        # Optional: Limit processing for performance
        # activity_files = activity_files[:10]  # Uncomment to process only first 10 files

        with tqdm(total=len(activity_files), desc="    🏃 Processing activity files", leave=False) as pbar:
            for json_file in activity_files:
                pbar.set_description(f"    🏃 Processing {json_file.name}")
                try:
                    data = self._parse_json_file_efficiently(json_file)

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
                pbar.update(1)

        return activities

    def _parse_single_activity(self, data: Dict[str, Any]) -> Optional[ActivityData]:
        """Parse a single activity from JSON data."""
        try:
            # Extract basic activity information
            log_id = data.get("logId", 0)
            activity_name = data.get("activityName", "Unknown")
            activity_type_id = data.get("activityTypeId")

            # Parse start time
            start_time_str = data.get("startTime", data.get("originalStartTime", ""))
            if start_time_str:
                start_time = parse_datetime(start_time_str)
            else:
                logger.warning(f"No start time found for activity {log_id}")
                return None

            # Parse duration
            duration_ms = data.get("activeDuration", data.get("duration", 0))

            # Parse activity type using both name and ID
            activity_type = self._map_activity_type(activity_name, activity_type_id)

            # Parse optional fields
            calories = data.get("calories")
            distance = data.get("distance")
            steps = data.get("steps")

            # Parse heart rate zones
            heart_rate_zones = []
            if "heartRateZones" in data:
                for zone in data["heartRateZones"]:
                    hr_zone = HeartRateZone(
                        name=zone.get("name", "Unknown"),
                        min_bpm=zone.get("min", 0),
                        max_bpm=zone.get("max", 0),
                        minutes=zone.get("minutes", 0),
                        calories_out=zone.get("caloriesOut"),
                    )
                    heart_rate_zones.append(hr_zone)

            # Parse average/max heart rate
            average_heart_rate = data.get("averageHeartRate")
            max_heart_rate = data.get("maxHeartRate")

            # Parse GPS data if available - enhanced processing
            gps_data = self._parse_gps_data(data.get("gpsData"))
            tcx_data = data.get("tcxData", data.get("tcxLink"))

            # Extract additional comprehensive data
            pace = data.get("pace")
            speed = data.get("speed")
            elevation_gain = data.get("elevationGain")
            min_heart_rate = data.get("minHeartRate")
            active_duration = data.get("activeDuration")
            has_gps = gps_data is not None and len(gps_data) > 0 if gps_data else False
            manual_values_specified = data.get("manualValuesSpecified")
            source_data = data.get("source")
            source = json.dumps(source_data) if isinstance(source_data, dict) else source_data
            is_favorite = data.get("isFavorite")
            activity_parent_id = data.get("activityParentId")
            activity_parent_name = data.get("activityParentName")

            # Parse last modified timestamp
            last_modified = None
            if "lastModified" in data:
                try:
                    last_modified = parse_datetime(data["lastModified"])
                except:
                    pass

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
                tcx_data=tcx_data,
                activity_type_id=activity_type_id,
                original_activity_name=activity_name,
                # Additional comprehensive fields
                pace=pace,
                speed=speed,
                elevation_gain=elevation_gain,
                min_heart_rate=min_heart_rate,
                active_duration=active_duration,
                has_gps=has_gps,
                manual_values_specified=manual_values_specified,
                source=source,
                is_favorite=is_favorite,
                activity_parent_id=activity_parent_id,
                activity_parent_name=activity_parent_name,
                last_modified=last_modified,
            )

        except Exception as e:
            logger.warning(f"Error parsing activity data: {e}")
            return None

    def _map_activity_type(
        self, activity_name: str, activity_type_id: Optional[int] = None
    ) -> ActivityType:
        """Map Fitbit activity name and ID to our ActivityType enum."""

        # First try mapping by Fitbit activity type ID (most accurate)
        if activity_type_id:
            fitbit_id_mapping = {
                # Running & Walking
                90009: ActivityType.RUN,  # Run
                90019: ActivityType.RUN,  # Outdoor Run
                20049: ActivityType.TREADMILL,  # Treadmill
                90013: ActivityType.WALK,  # Walk
                90014: ActivityType.WALK,  # Outdoor Walk
                90012: ActivityType.HIKE,  # Hike
                # Cycling
                90001: ActivityType.BIKE,  # Bike
                1071: ActivityType.BIKE,  # Outdoor Bike
                20008: ActivityType.BIKE,  # Stationary Bike
                # Water Sports
                90024: ActivityType.SWIM,  # Swimming
                90026: ActivityType.SWIM,  # Pool Swimming
                90025: ActivityType.SWIM,  # Open Water Swimming
                # Gym Equipment
                20047: ActivityType.ELLIPTICAL,  # Elliptical
                20001: ActivityType.OTHER,  # Stair Climber
                20010: ActivityType.ROWING,  # Rowing Machine
                # Strength Training
                91045: ActivityType.WEIGHTS,  # Weights
                3000: ActivityType.WORKOUT,  # Workout
                3001: ActivityType.AEROBIC,  # Aerobic Workout
                2131: ActivityType.CROSSFIT,  # CrossFit
                3101: ActivityType.ABS,  # 10 Minute Abs
                3102: ActivityType.AEROBIC,  # Warm It Up
                3013: ActivityType.CROSSFIT,  # HIIT
                3014: ActivityType.CROSSFIT,  # Bootcamp
                90004: ActivityType.WORKOUT,  # Interval Workout
                # Sports
                15675: ActivityType.TENNIS,  # Tennis
                15000: ActivityType.SPORT,  # Sport
                15020: ActivityType.BASKETBALL,  # Basketball
                15030: ActivityType.SOCCER,  # Soccer
                15040: ActivityType.VOLLEYBALL,  # Volleyball
                15050: ActivityType.FOOTBALL,  # Football
                15060: ActivityType.GOLF,  # Golf
                15070: ActivityType.SKIING,  # Skiing
                15080: ActivityType.SNOWBOARDING,  # Snowboarding
                15090: ActivityType.MARTIAL_ARTS,  # Martial Arts
                15100: ActivityType.BOXING,  # Boxing
                15120: ActivityType.CLIMBING,  # Rock Climbing
                # Mind-Body
                15110: ActivityType.YOGA,  # Yoga
                15130: ActivityType.YOGA,  # Pilates
                # Dance & Other
                20030: ActivityType.DANCE,  # Dance
                90005: ActivityType.DANCE,  # Zumba
                90006: ActivityType.MARTIAL_ARTS,  # Kickboxing
                90007: ActivityType.AEROBIC,  # Step
                90008: ActivityType.AEROBIC,  # Cardio
                # Additional Common IDs
                1: ActivityType.WALK,  # Walk
                2: ActivityType.RUN,  # Run
                3: ActivityType.BIKE,  # Bike
                4: ActivityType.SWIM,  # Swim
                5: ActivityType.HIKE,  # Hike
                6: ActivityType.WEIGHTS,  # Weights
                7: ActivityType.WORKOUT,  # Workout
                8: ActivityType.YOGA,  # Yoga
                9: ActivityType.SPORT,  # Sport
                10: ActivityType.TENNIS,  # Tennis
            }

            if activity_type_id in fitbit_id_mapping:
                return fitbit_id_mapping[activity_type_id]

        # Fallback to enhanced activity name mapping
        name_lower = activity_name.lower()

        # Running variations
        if any(word in name_lower for word in ["run", "jog", "running", "jogging"]):
            return ActivityType.RUN
        # Walking variations
        elif any(word in name_lower for word in ["walk", "walking"]):
            return ActivityType.WALK
        # Cycling variations
        elif any(
            word in name_lower
            for word in ["bike", "cycling", "bicycle", "biking", "cycle"]
        ):
            return ActivityType.BIKE
        # Hiking variations
        elif any(word in name_lower for word in ["hike", "hiking", "trail"]):
            return ActivityType.HIKE
        # Swimming variations
        elif any(word in name_lower for word in ["swim", "swimming", "pool", "water"]):
            return ActivityType.SWIM
        # Treadmill variations
        elif any(word in name_lower for word in ["treadmill", "tread mill"]):
            return ActivityType.TREADMILL
        # Elliptical variations
        elif any(word in name_lower for word in ["elliptical", "cross trainer"]):
            return ActivityType.ELLIPTICAL
        # Rowing variations
        elif any(word in name_lower for word in ["rowing", "row"]):
            return ActivityType.ROWING
        # Tennis variations
        elif any(word in name_lower for word in ["tennis", "racquet", "racket"]):
            return ActivityType.TENNIS
        # Basketball variations
        elif any(word in name_lower for word in ["basketball", "bball"]):
            return ActivityType.BASKETBALL
        # Soccer variations
        elif any(
            word in name_lower
            for word in ["soccer", "football" if "american" not in name_lower else ""]
        ):
            return ActivityType.SOCCER
        # American Football variations
        elif any(word in name_lower for word in ["american football", "nfl"]):
            return ActivityType.FOOTBALL
        # Volleyball variations
        elif any(word in name_lower for word in ["volleyball", "vball"]):
            return ActivityType.VOLLEYBALL
        # Golf variations
        elif any(word in name_lower for word in ["golf"]):
            return ActivityType.GOLF
        # Skiing variations
        elif any(word in name_lower for word in ["skiing", "ski"]):
            return ActivityType.SKIING
        # Snowboarding variations
        elif any(word in name_lower for word in ["snowboarding", "snowboard"]):
            return ActivityType.SNOWBOARDING
        # Dance variations
        elif any(word in name_lower for word in ["dance", "zumba", "dancing"]):
            return ActivityType.DANCE
        # Martial Arts variations
        elif any(
            word in name_lower
            for word in ["martial arts", "karate", "taekwondo", "judo", "kickboxing"]
        ):
            return ActivityType.MARTIAL_ARTS
        # Boxing variations
        elif any(word in name_lower for word in ["boxing", "box"]):
            return ActivityType.BOXING
        # Climbing variations
        elif any(word in name_lower for word in ["climbing", "climb", "rock climbing"]):
            return ActivityType.CLIMBING
        # Aerobic variations
        elif any(word in name_lower for word in ["aerobic", "cardio", "step"]):
            return ActivityType.AEROBIC
        # Ab workout variations
        elif any(word in name_lower for word in ["abs", "core", "abdominal"]):
            return ActivityType.ABS
        # CrossFit variations
        elif any(
            word in name_lower for word in ["crossfit", "cross fit", "hiit", "bootcamp"]
        ):
            return ActivityType.CROSSFIT
        # Weight training variations
        elif any(
            word in name_lower
            for word in ["weight", "strength", "lifting", "barbell", "dumbbell"]
        ):
            return ActivityType.WEIGHTS
        # Yoga variations
        elif any(
            word in name_lower
            for word in ["yoga", "pilates", "stretching", "meditation"]
        ):
            return ActivityType.YOGA
        # Generic sports variations
        elif any(
            word in name_lower for word in ["sport", "baseball", "hockey", "cricket"]
        ):
            return ActivityType.SPORT
        # Generic workout variations
        elif any(
            word in name_lower
            for word in ["workout", "exercise", "training", "fitness"]
        ):
            return ActivityType.WORKOUT
        else:
            return ActivityType.OTHER

    def _parse_sleep_data(self) -> List[SleepData]:
        """Parse sleep data from Fitbit exports."""
        sleep_data = []

        sleep_paths = []

        # Add discovered directories
        if "sleep" in self.data_directories:
            sleep_paths.append(self.data_directories["sleep"])
        if "sleep_score" in self.data_directories:
            sleep_paths.append(self.data_directories["sleep_score"])
        if "global_export" in self.data_directories:
            sleep_paths.append(self.data_directories["global_export"])

        for sleep_path in sleep_paths:
            if sleep_path.exists():
                print(f"  📁 Processing sleep data from: {sleep_path.name}")

                # Look for sleep-related files (JSON and CSV)
                json_files = list(sleep_path.glob("**/*.json"))
                csv_files = list(sleep_path.glob("**/*.csv"))
                sleep_files = [f for f in json_files if "sleep" in f.name.lower()]
                sleep_csv_files = [f for f in csv_files if "sleep" in f.name.lower()]

                print(
                    f"    📋 Found {len(sleep_files)} sleep JSON files and {len(sleep_csv_files)} sleep CSV files"
                )

                # Process JSON files
                with tqdm(total=len(sleep_files), desc="    😴 Processing sleep files", leave=False) as pbar:
                    for json_file in sleep_files:
                        pbar.set_description(f"    😴 Processing {json_file.name}")
                        try:
                            with open(json_file, "r", encoding="utf-8") as f:
                                data = json.load(f)

                            if isinstance(data, list):
                                for item in data:
                                    sleep = self._parse_single_sleep_record(item)
                                    if sleep:
                                        sleep_data.append(sleep)
                        except (json.JSONDecodeError, Exception) as e:
                            logger.warning(f"Error parsing sleep file {json_file}: {e}")
                        pbar.update(1)

                # Process CSV files (Sleep Score data)
                for csv_file in sleep_csv_files:
                    try:
                        df = pd.read_csv(csv_file)
                        for _, row in df.iterrows():
                            sleep = self._parse_sleep_score_record(row)
                            if sleep:
                                sleep_data.append(sleep)
                    except Exception as e:
                        logger.warning(f"Error parsing sleep CSV file {csv_file}: {e}")

        logger.info(f"Parsed {len(sleep_data)} sleep records")
        return sleep_data

    def _parse_single_sleep_record(self, data: Dict[str, Any]) -> Optional[SleepData]:
        """Parse a single sleep record from JSON data with comprehensive metrics."""
        try:
            log_id = data.get("logId", 0)
            date_of_sleep = parse_datetime(data.get("dateOfSleep", "")).date()
            start_time = parse_datetime(data.get("startTime", ""))
            end_time = parse_datetime(data.get("endTime", ""))
            duration_ms = data.get("duration", 0)

            # Parse sleep stages and calculate stage-specific metrics
            sleep_stages = data.get("levels", {}).get("data", [])
            (
                minutes_rem,
                minutes_light,
                minutes_deep,
                minutes_wake,
            ) = self._calculate_sleep_stages(sleep_stages)

            # Extract additional sleep metrics
            sleep_score = data.get("sleepScore") or data.get("overall_score")
            restlessness = data.get("restlessness")
            awakening_count = data.get("awakeningCount") or data.get(
                "awakeningSummary", {}
            ).get("count")

            # Heart rate data during sleep
            heart_rate_avg = data.get("averageHeartRate") or data.get(
                "heartRate", {}
            ).get("average")
            heart_rate_min = data.get("minimumHeartRate") or data.get(
                "heartRate", {}
            ).get("minimum")

            # Additional biometric data
            breathing_rate = data.get("breathingRate")
            temperature_variation = data.get("skinTemperatureVariation")
            sleep_type = data.get("type")
            info_code = data.get("infoCode")

            return SleepData(
                log_id=log_id,
                date_of_sleep=date_of_sleep,
                start_time=start_time,
                end_time=end_time,
                duration_ms=duration_ms,
                efficiency=data.get("efficiency"),
                minutes_awake=data.get("minutesAwake"),
                minutes_asleep=data.get("minutesAsleep"),
                minutes_to_fall_asleep=data.get("minutesToFallAsleep"),
                minutes_after_wakeup=data.get("minutesAfterWakeup"),
                time_in_bed=data.get("timeInBed"),
                sleep_stages=sleep_stages,
                # Enhanced metrics
                sleep_score=sleep_score,
                restlessness=restlessness,
                awakening_count=awakening_count,
                minutes_rem=minutes_rem,
                minutes_light=minutes_light,
                minutes_deep=minutes_deep,
                minutes_wake=minutes_wake,
                heart_rate_avg=heart_rate_avg,
                heart_rate_min=heart_rate_min,
                breathing_rate=breathing_rate,
                temperature_variation=temperature_variation,
                type=sleep_type,
                info_code=info_code,
            )
        except Exception as e:
            logger.warning(f"Error parsing sleep record: {e}")
            return None

    def _calculate_sleep_stages(
        self, sleep_stages: List[Dict[str, Any]]
    ) -> tuple[int, int, int, int]:
        """Calculate minutes spent in each sleep stage."""
        minutes_rem = 0
        minutes_light = 0
        minutes_deep = 0
        minutes_wake = 0

        try:
            for stage in sleep_stages:
                level = stage.get("level", "").lower()
                seconds = stage.get("seconds", 0)
                minutes = seconds / 60

                if level == "rem":
                    minutes_rem += minutes
                elif level == "light":
                    minutes_light += minutes
                elif level == "deep":
                    minutes_deep += minutes
                elif level in ["wake", "awake"]:
                    minutes_wake += minutes
                elif level == "restless":
                    # Treat restless as light sleep
                    minutes_light += minutes
                elif level == "asleep":
                    # Generic asleep - treat as light sleep
                    minutes_light += minutes
        except Exception as e:
            logger.warning(f"Error calculating sleep stages: {e}")

        return (
            int(minutes_rem),
            int(minutes_light),
            int(minutes_deep),
            int(minutes_wake),
        )

    def _parse_sleep_score_record(self, row) -> Optional[SleepData]:
        """Parse a sleep score record from CSV row."""
        try:
            # Sleep Score CSV usually has timestamp and overall_score columns
            timestamp_str = str(row.get("timestamp", ""))
            if not timestamp_str or timestamp_str == "nan":
                return None

            # Parse timestamp
            try:
                timestamp = parse_datetime(timestamp_str)
                record_date = timestamp.date()
            except:
                return None

            # Create a basic sleep record with score information
            return SleepData(
                log_id=int(timestamp.timestamp()),  # Use timestamp as ID
                date_of_sleep=record_date,
                start_time=timestamp,
                end_time=timestamp,  # Will be updated if duration is available
                duration_ms=0,  # Will be updated if available
                efficiency=int(row.get("overall_score", 0))
                if pd.notna(row.get("overall_score"))
                else None,
                minutes_awake=0,
                minutes_asleep=0,
                minutes_to_fall_asleep=0,
                minutes_after_wakeup=0,
                time_in_bed=0,
                sleep_stages=[],
            )
        except Exception as e:
            logger.warning(f"Error parsing sleep score record: {e}")
            return None

    def _parse_daily_metrics(self) -> List[DailyMetrics]:
        """Parse daily metrics from various Fitbit data files."""
        daily_metrics = []

        # Parse from different data sources
        metrics_paths = []

        # Add discovered directories
        if "global_export" in self.data_directories:
            metrics_paths.append(self.data_directories["global_export"])

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
            if any(
                keyword in file_name
                for keyword in ["steps", "distance", "calories", "floors", "elevation"]
            ):
                daily_files.append(json_file)
            # Skip activity, sleep, and heart rate files
            elif any(
                skip in file_name
                for skip in ["exercise", "activity", "sleep", "heart_rate"]
            ):
                continue
            else:
                # Include other potential daily files
                daily_files.append(json_file)

        print(f"    📋 Found {len(daily_files)} daily metrics files")

        with tqdm(total=len(daily_files), desc="    📄 Processing daily metrics files", leave=False) as pbar:
            for json_file in daily_files:
                pbar.set_description(f"    📄 Processing {json_file.name}")
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
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
                pbar.update(1)

        return metrics

    def _parse_single_daily_metric(
        self, data: Dict[str, Any]
    ) -> Optional[DailyMetrics]:
        """Parse a single daily metric record."""
        try:
            date_str = data.get("dateTime", data.get("date", ""))
            if not date_str:
                return None

            record_date = parse_datetime(date_str).date()

            return DailyMetrics(
                date=record_date,
                steps=data.get("steps"),
                distance=data.get("distance"),
                calories_burned=data.get("caloriesOut"),
                calories_bmr=data.get("caloriesBMR"),
                active_minutes=data.get("activeMinutes"),
                sedentary_minutes=data.get("sedentaryMinutes"),
                lightly_active_minutes=data.get("lightlyActiveMinutes"),
                fairly_active_minutes=data.get("fairlyActiveMinutes"),
                very_active_minutes=data.get("veryActiveMinutes"),
                floors=data.get("floors"),
                elevation=data.get("elevation"),
                resting_heart_rate=data.get("restingHeartRate"),
            )
        except Exception as e:
            logger.warning(f"Error parsing daily metric: {e}")
            return None

    def _parse_heart_rate_data(self) -> List[HeartRateData]:
        """Parse heart rate data from Fitbit exports."""
        heart_rate_data = []

        if "global_export" in self.data_directories:
            hr_path = self.data_directories["global_export"]
            print(f"  📁 Processing heart rate data from: {hr_path.name}")

            hr_files = list(hr_path.glob("heart_rate*.json"))
            print(f"    📋 Found {len(hr_files)} heart rate files")

            # Optional: Limit processing for performance
            # hr_files = hr_files[:10]  # Uncomment to process only first 10 files

            if self.enable_parallel and len(hr_files) > 2:
                # Use parallel processing for large number of files
                print("    🚀 Using parallel processing for heart rate data")

                # Process files in chunks to manage memory
                all_file_data = self.parallel_processor.process_in_chunks(
                    hr_files,
                    process_json_file_worker,
                    chunk_size=100,  # Process 100 files at a time
                    description="💓 Processing HR files",
                )

                # Parse all collected data
                for item in all_file_data:
                    hr_data = self._parse_single_heart_rate(item)
                    if hr_data:
                        heart_rate_data.append(hr_data)
            else:
                # Sequential processing for small number of files
                for json_file in tqdm(
                    hr_files, desc="    💓 Processing HR files", leave=False
                ):
                    try:
                        file_data = self._parse_json_file_efficiently(json_file)

                        if isinstance(file_data, list):
                            for item in file_data:
                                hr_data = self._parse_single_heart_rate(item)
                                if hr_data:
                                    heart_rate_data.append(hr_data)

                    except (json.JSONDecodeError, Exception) as e:
                        logger.warning(
                            f"Error parsing heart rate file {json_file}: {e}"
                        )

        logger.info(f"Parsed {len(heart_rate_data)} heart rate records")
        print(f"    ✅ Parsed {len(heart_rate_data)} heart rate records")
        return heart_rate_data

    def _parse_single_heart_rate(self, data: Dict[str, Any]) -> Optional[HeartRateData]:
        """Parse a single heart rate record."""
        try:
            datetime_str = data.get("dateTime", "")
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

            value = data.get("value", {})

            return HeartRateData(
                datetime=dt,
                bpm=value.get("bpm", 0),
                confidence=value.get("confidence", 0),
            )
        except Exception as e:
            logger.warning(f"Error parsing heart rate record: {e}")
            return None

    def _parse_body_composition(self) -> List[BodyComposition]:
        """Parse body composition data from Fitbit exports."""
        body_data = []

        if "global_export" in self.data_directories:
            body_path = self.data_directories["global_export"]
            print(f"  📁 Processing body composition from: {body_path.name}")

            # Look for weight files
            weight_files = list(body_path.glob("weight*.json"))
            print(f"    📋 Found {len(weight_files)} weight files")

            # Process files with progress bar
            for json_file in tqdm(
                weight_files, desc="    ⚖️ Processing weight files", leave=False
            ):
                try:
                    file_data = self._parse_json_file_efficiently(json_file)

                    if isinstance(file_data, list):
                        for item in file_data:
                            body_comp = self._parse_single_body_composition(item)
                            if body_comp:
                                body_data.append(body_comp)

                except (json.JSONDecodeError, Exception) as e:
                    logger.warning(
                        f"Error parsing body composition file {json_file}: {e}"
                    )

        logger.info(f"Parsed {len(body_data)} body composition records")
        print(f"    ✅ Parsed {len(body_data)} body composition records")
        return body_data

    def _parse_single_body_composition(
        self, data: Dict[str, Any]
    ) -> Optional[BodyComposition]:
        """Parse a single body composition record."""
        try:
            # Parse date from format like "04/08/25"
            date_str = data.get("date", "")
            if not date_str:
                return None

            # Handle MM/DD/YY format
            try:
                record_date = datetime.strptime(date_str, "%m/%d/%y").date()
            except ValueError:
                try:
                    record_date = parse_datetime(date_str).date()
                except:
                    logger.warning(f"Could not parse date: {date_str}")
                    return None

            return BodyComposition(
                date=record_date,
                weight=data.get("weight"),
                bmi=data.get("bmi"),
                body_fat_percentage=data.get("fat"),
                lean_mass=data.get("leanMass"),
                muscle_mass=data.get("muscleMass"),
                bone_mass=data.get("boneMass"),
                water_percentage=data.get("water"),
            )
        except Exception as e:
            logger.warning(f"Error parsing body composition record: {e}")
            return None

    def _parse_heart_rate_variability(self) -> List[HeartRateVariability]:
        """Parse heart rate variability data from CSV files."""
        hrv_data = []

        if "hrv" in self.data_directories:
            hrv_path = self.data_directories["hrv"]
            print(f"  📁 Processing HRV data from: {hrv_path.name}")

            # Look for HRV CSV files
            hrv_files = list(hrv_path.glob("*.csv"))
            print(f"    📋 Found {len(hrv_files)} HRV files")

            # Process files with progress bar
            for csv_file in tqdm(
                hrv_files, desc="    💓 Processing HRV files", leave=False
            ):
                try:
                    import pandas as pd

                    df = pd.read_csv(csv_file)

                    for _, row in df.iterrows():
                        hrv_record = self._parse_single_hrv_record(row)
                        if hrv_record:
                            hrv_data.append(hrv_record)

                except Exception as e:
                    logger.warning(f"Error parsing HRV file {csv_file}: {e}")

        logger.info(f"Parsed {len(hrv_data)} HRV records")
        print(f"    ✅ Parsed {len(hrv_data)} HRV records")
        return hrv_data

    def _parse_single_hrv_record(self, row) -> Optional[HeartRateVariability]:
        """Parse a single HRV record from CSV row."""
        try:
            timestamp_str = str(row.get("timestamp", ""))
            if not timestamp_str or timestamp_str == "nan":
                return None

            # Parse timestamp
            try:
                timestamp = parse_datetime(timestamp_str)
                record_date = timestamp.date()
            except:
                return None

            return HeartRateVariability(
                date=record_date,
                rmssd=float(row.get("rmssd", 0))
                if pd.notna(row.get("rmssd"))
                else None,
                coverage=float(row.get("coverage", 0))
                if pd.notna(row.get("coverage"))
                else None,
                low_frequency=float(row.get("low_frequency", 0))
                if pd.notna(row.get("low_frequency"))
                else None,
                high_frequency=float(row.get("high_frequency", 0))
                if pd.notna(row.get("high_frequency"))
                else None,
                timestamp=timestamp,
            )
        except Exception as e:
            logger.warning(f"Error parsing HRV record: {e}")
            return None

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
        """Parse active zone minutes data from CSV files."""
        azm_data = []

        if "active_zone_minutes" in self.data_directories:
            azm_path = self.data_directories["active_zone_minutes"]
            print(f"  📁 Processing Active Zone Minutes from: {azm_path.name}")

            # Look for AZM CSV files
            azm_files = list(azm_path.glob("*.csv"))
            print(f"    📋 Found {len(azm_files)} AZM files")

            # Process files with progress bar
            for csv_file in tqdm(
                azm_files, desc="    🔥 Processing AZM files", leave=False
            ):
                try:
                    import pandas as pd

                    df = pd.read_csv(csv_file)

                    # Group by date and aggregate zone minutes
                    daily_azm = {}

                    for _, row in df.iterrows():
                        azm_record = self._parse_single_azm_record(row, daily_azm)

                    # Convert aggregated data to ActiveZoneMinutes objects
                    for date_key, zones in daily_azm.items():
                        azm_obj = ActiveZoneMinutes(
                            date=date_key,
                            fat_burn_minutes=zones.get("FAT_BURN", 0),
                            cardio_minutes=zones.get("CARDIO", 0),
                            peak_minutes=zones.get("PEAK", 0),
                            total_minutes=sum(zones.values()),
                        )
                        azm_data.append(azm_obj)

                except Exception as e:
                    logger.warning(f"Error parsing AZM file {csv_file}: {e}")

        logger.info(f"Parsed {len(azm_data)} Active Zone Minutes records")
        print(f"    ✅ Parsed {len(azm_data)} Active Zone Minutes records")
        return azm_data

    def _parse_single_azm_record(self, row, daily_azm: Dict):
        """Parse a single AZM record and aggregate by date."""
        try:
            date_time_str = str(row.get("date_time", ""))
            zone = str(row.get("heart_zone_id", ""))
            minutes = int(row.get("total_minutes", 0))

            if not date_time_str or date_time_str == "nan":
                return

            # Parse date from datetime
            try:
                dt = parse_datetime(date_time_str)
                date_key = dt.date()
            except:
                return

            # Aggregate minutes by date and zone
            if date_key not in daily_azm:
                daily_azm[date_key] = {}

            if zone not in daily_azm[date_key]:
                daily_azm[date_key][zone] = 0

            daily_azm[date_key][zone] += minutes

        except Exception as e:
            logger.warning(f"Error parsing AZM record: {e}")

    def _parse_gps_data(self, raw_gps_data) -> Optional[List[Dict[str, Any]]]:
        """Enhanced GPS data parsing with timestamps, speed, and elevation."""
        if not raw_gps_data:
            return None

        try:
            # Handle different GPS data formats
            if isinstance(raw_gps_data, str):
                # GPS data might be a string reference to a file
                return None

            if not isinstance(raw_gps_data, list):
                return None

            enhanced_gps_data = []

            for i, point in enumerate(raw_gps_data):
                if not isinstance(point, dict):
                    continue

                enhanced_point = {}

                # Basic coordinates
                if "latitude" in point and "longitude" in point:
                    enhanced_point["latitude"] = float(point["latitude"])
                    enhanced_point["longitude"] = float(point["longitude"])
                else:
                    continue  # Skip points without coordinates

                # Altitude/elevation
                if "altitude" in point:
                    enhanced_point["altitude"] = float(point["altitude"])
                elif "elevation" in point:
                    enhanced_point["altitude"] = float(point["elevation"])

                # Timestamp
                if "time" in point:
                    try:
                        enhanced_point["time"] = parse_datetime(
                            point["time"]
                        ).isoformat()
                    except:
                        enhanced_point["time"] = point["time"]
                elif "timestamp" in point:
                    try:
                        enhanced_point["time"] = parse_datetime(
                            point["timestamp"]
                        ).isoformat()
                    except:
                        enhanced_point["time"] = point["timestamp"]

                # Speed
                if "speed" in point:
                    enhanced_point["speed"] = float(point["speed"])

                # Distance (cumulative)
                if "distance" in point:
                    enhanced_point["distance"] = float(point["distance"])

                # Heart rate at this point
                if "heartRate" in point:
                    enhanced_point["heart_rate"] = int(point["heartRate"])
                elif "hr" in point:
                    enhanced_point["heart_rate"] = int(point["hr"])

                # Accuracy information
                if "accuracy" in point:
                    enhanced_point["accuracy"] = float(point["accuracy"])

                # Calculate speed if not provided but we have previous point
                if "speed" not in enhanced_point and i > 0:
                    prev_point = enhanced_gps_data[i - 1]
                    if "latitude" in prev_point and "longitude" in prev_point:
                        try:
                            distance = self._calculate_gps_distance(
                                prev_point["latitude"],
                                prev_point["longitude"],
                                enhanced_point["latitude"],
                                enhanced_point["longitude"],
                            )
                            # Estimate time interval (assuming regular intervals)
                            time_interval = 1  # 1 second default
                            if "time" in enhanced_point and "time" in prev_point:
                                try:
                                    current_time = parse_datetime(
                                        enhanced_point["time"]
                                    )
                                    prev_time = parse_datetime(prev_point["time"])
                                    time_interval = (
                                        current_time - prev_time
                                    ).total_seconds()
                                except:
                                    pass

                            if time_interval > 0:
                                enhanced_point["speed"] = (
                                    distance / time_interval
                                )  # m/s
                        except:
                            pass

                enhanced_gps_data.append(enhanced_point)

            logger.debug(
                f"Enhanced GPS data: {len(enhanced_gps_data)} points with enriched metadata"
            )
            return enhanced_gps_data if enhanced_gps_data else None

        except Exception as e:
            logger.warning(f"Error parsing GPS data: {e}")
            return raw_gps_data if isinstance(raw_gps_data, list) else None

    def _calculate_gps_distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """Calculate distance between two GPS points using Haversine formula."""
        import math

        # Convert latitude and longitude from degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))

        # Radius of earth in meters
        r = 6371000

        return c * r
