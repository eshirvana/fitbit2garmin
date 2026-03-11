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
        memory_limit_mb: Optional[int] = None,
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

        # Memory management — default to 75% of available RAM, min 1 GB.
        if memory_limit_mb is not None:
            self.memory_limit_mb = memory_limit_mb
        else:
            try:
                import psutil
                available_mb = psutil.virtual_memory().available // (1024 * 1024)
                self.memory_limit_mb = max(1024, int(available_mb * 0.75))
            except Exception:
                self.memory_limit_mb = 1024

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

    def _check_memory_usage(self) -> bool:
        """Check if memory usage is within limits."""
        try:
            import psutil

            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            if memory_mb > self.memory_limit_mb:
                logger.warning(
                    f"Memory usage ({memory_mb:.1f}MB) exceeds limit ({self.memory_limit_mb}MB)"
                )
                return False
            return True
        except ImportError:
            # psutil not available, assume memory is OK
            return True
        except Exception:
            # Error checking memory, assume it's OK
            return True

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

        # Skip files that are unreasonably large (500 MB).
        if file_size > 500 * 1024 * 1024:
            logger.warning(f"Skipping very large file {file_path} ({file_size / (1024*1024):.1f}MB)")
            return []

        # Use streaming parser for files larger than 10MB to keep per-file
        # memory low.  NOTE: do NOT break early — read every item so no data
        # is silently dropped from large exports.
        if file_size > 10 * 1024 * 1024:  # 10MB
            logger.debug(f"Using streaming parser for large file {file_path} ({file_size / (1024*1024):.1f}MB)")
            try:
                with open(file_path, "rb") as f:
                    items = []
                    try:
                        parser = ijson.items(f, "item")
                        for item in parser:
                            items.append(item)
                    except ijson.JSONError:
                        # If ijson fails, fall back to reading entire file
                        f.seek(0)
                        import orjson
                        data = orjson.loads(f.read())
                        if isinstance(data, list):
                            items = data
                        else:
                            items = [data]
                    return items
            except (ijson.JSONError, ValueError, Exception) as e:
                logger.warning(f"Streaming parser failed for {file_path}: {e}, trying standard parser")
                # Fallback to regular parsing if streaming fails
                pass

        # Regular JSON parsing — orjson is 2-5× faster than stdlib json
        try:
            import orjson
            with open(file_path, "rb") as f:
                return orjson.loads(f.read())
        except Exception:
            pass
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"Error parsing JSON file {file_path}: {e}")
            return []

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
        hr_data, hr_daily_stats = self._parse_heart_rate_data()
        user_data.heart_rate_data = hr_data
        user_data.heart_rate_daily_stats = hr_daily_stats

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

        # GPS data in Fitbit Google Takeout lives in separate .tcx files inside
        # the Activities/ directory — not embedded in the exercise JSON files.
        # Attach GPS trackpoints to matching activities now.
        self._attach_gps_from_tcx_files(activities)

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

        with tqdm(
            total=len(activity_files),
            desc="    🏃 Processing activity files",
            leave=False,
        ) as pbar:
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
            steps = data.get("steps")

            # Normalize distance to km.
            # Fitbit stores distance in the user's locale unit (miles for US accounts,
            # km for metric accounts).  "distanceKm" is always km when present.
            _MILES_TO_KM = 1.60934
            distance_km = data.get("distanceKm")
            if distance_km is not None:
                distance = float(distance_km)
            else:
                distance_raw = data.get("distance")
                distance_unit = (data.get("distanceUnit") or "Kilometer").strip()
                if distance_raw is not None:
                    distance = (
                        float(distance_raw) * _MILES_TO_KM
                        if distance_unit.lower() in ("mile", "miles")
                        else float(distance_raw)
                    )
                else:
                    distance = None

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
            source = (
                json.dumps(source_data)
                if isinstance(source_data, dict)
                else source_data
            )
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
                90001: ActivityType.BIKE,  # Bike / Outdoor Bike
                1071: ActivityType.BIKE,  # Outdoor Bike
                20008: ActivityType.INDOOR_CYCLING,  # Stationary Bike / Spin
                90003: ActivityType.INDOOR_CYCLING,  # Indoor Cycling
                # Water Sports
                90024: ActivityType.SWIM,  # Swimming
                90026: ActivityType.SWIM,  # Pool Swimming
                90025: ActivityType.SWIM,  # Open Water Swimming
                # Gym Equipment
                20047: ActivityType.ELLIPTICAL,  # Elliptical
                20001: ActivityType.STAIR_CLIMBING,  # Stair Climber
                20010: ActivityType.ROWING,  # Rowing Machine
                20002: ActivityType.STAIR_CLIMBING,  # Stair Stepper
                # Strength & Conditioning
                91045: ActivityType.WEIGHTS,  # Weights
                3000: ActivityType.WORKOUT,  # Workout
                3001: ActivityType.AEROBIC,  # Aerobic Workout
                2131: ActivityType.CROSSFIT,  # CrossFit
                3101: ActivityType.ABS,  # 10 Minute Abs
                3102: ActivityType.AEROBIC,  # Warm It Up
                3013: ActivityType.HIIT,  # HIIT
                3014: ActivityType.HIIT,  # Bootcamp
                90004: ActivityType.WORKOUT,  # Interval Workout
                90016: ActivityType.WEIGHTS,  # Circuit Training
                90017: ActivityType.AEROBIC,  # Kickboxing (cardio variant)
                90020: ActivityType.WORKOUT,  # Functional Strength Training
                # Sports
                15675: ActivityType.TENNIS,  # Tennis
                15000: ActivityType.SPORT,  # Sport
                15020: ActivityType.BASKETBALL,  # Basketball
                15030: ActivityType.SOCCER,  # Soccer
                15040: ActivityType.VOLLEYBALL,  # Volleyball
                15050: ActivityType.FOOTBALL,  # Football
                15060: ActivityType.GOLF,  # Golf
                15070: ActivityType.SKIING,  # Alpine Skiing
                15080: ActivityType.SNOWBOARDING,  # Snowboarding
                15090: ActivityType.MARTIAL_ARTS,  # Martial Arts
                15100: ActivityType.BOXING,  # Boxing
                15120: ActivityType.CLIMBING,  # Rock Climbing
                15140: ActivityType.SPORT,  # Baseball
                15150: ActivityType.SPORT,  # Hockey
                15160: ActivityType.SPORT,  # Cricket
                15170: ActivityType.TENNIS,  # Racquetball
                15180: ActivityType.TENNIS,  # Squash
                15190: ActivityType.TENNIS,  # Badminton
                15200: ActivityType.SPORT,  # Rugby
                15210: ActivityType.SPORT,  # Lacrosse
                15220: ActivityType.SPORT,  # Archery
                15230: ActivityType.SPORT,  # Fencing
                15240: ActivityType.SPORT,  # Table Tennis
                15250: ActivityType.SPORT,  # Polo
                15260: ActivityType.PADDLE_SPORTS,  # Kayaking
                15270: ActivityType.PADDLE_SPORTS,  # Paddleboarding
                15280: ActivityType.PADDLE_SPORTS,  # Canoeing
                15290: ActivityType.SPORT,  # Surfing
                # Mind-Body
                15110: ActivityType.YOGA,  # Yoga
                15130: ActivityType.PILATES,  # Pilates
                15300: ActivityType.YOGA,  # Tai Chi
                15310: ActivityType.YOGA,  # Barre
                # Dance
                20030: ActivityType.DANCE,  # Dance
                90005: ActivityType.DANCE,  # Zumba
                90011: ActivityType.DANCE,  # Aerobic Dance
                # Cardio & Other
                90006: ActivityType.MARTIAL_ARTS,  # Kickboxing
                90007: ActivityType.AEROBIC,  # Step Aerobics
                90008: ActivityType.AEROBIC,  # Cardio
                90010: ActivityType.HIIT,  # Boot Camp
                90015: ActivityType.AEROBIC,  # Jump Rope
                # Skiing variants
                90021: ActivityType.SKIING,  # Cross-Country Skiing
                90022: ActivityType.SKIING,  # Downhill Skiing
                90023: ActivityType.SNOWBOARDING,  # Snowboarding
                # Additional Common / Low-value IDs
                1: ActivityType.WALK,
                2: ActivityType.RUN,
                3: ActivityType.BIKE,
                4: ActivityType.SWIM,
                5: ActivityType.HIKE,
                6: ActivityType.WEIGHTS,
                7: ActivityType.WORKOUT,
                8: ActivityType.YOGA,
                9: ActivityType.SPORT,
                10: ActivityType.TENNIS,
            }

            if activity_type_id in fitbit_id_mapping:
                return fitbit_id_mapping[activity_type_id]

        # Fallback to activity name mapping (ordered most-specific first)
        name_lower = activity_name.lower()

        # Treadmill (before "run" to avoid partial match)
        if any(w in name_lower for w in ["treadmill", "tread mill"]):
            return ActivityType.TREADMILL
        # Running
        elif any(w in name_lower for w in ["run", "jog", "running", "jogging", "sprint"]):
            return ActivityType.RUN
        # Hiking (before "walk" to avoid partial match)
        elif any(w in name_lower for w in ["hike", "hiking", "trail run", "trekking"]):
            return ActivityType.HIKE
        # Walking
        elif any(w in name_lower for w in ["walk", "walking", "stroll"]):
            return ActivityType.WALK
        # Indoor Cycling / Spin (before generic "bike")
        elif any(w in name_lower for w in ["spin", "indoor cycling", "stationary bike", "indoor bike"]):
            return ActivityType.INDOOR_CYCLING
        # Cycling
        elif any(w in name_lower for w in ["bike", "cycling", "bicycle", "biking", "cycle"]):
            return ActivityType.BIKE
        # Swimming
        elif any(w in name_lower for w in ["swim", "swimming", "pool swim", "open water"]):
            return ActivityType.SWIM
        # Elliptical
        elif any(w in name_lower for w in ["elliptical", "cross trainer"]):
            return ActivityType.ELLIPTICAL
        # Stair Climbing
        elif any(w in name_lower for w in ["stair", "step mill", "stairmaster", "step climber"]):
            return ActivityType.STAIR_CLIMBING
        # Rowing
        elif any(w in name_lower for w in ["rowing", "row machine", "ergometer", "erg"]):
            return ActivityType.ROWING
        # Paddle Sports
        elif any(w in name_lower for w in ["kayak", "paddle", "canoe", "sup", "paddleboard"]):
            return ActivityType.PADDLE_SPORTS
        # Tennis / Racquet sports
        elif any(w in name_lower for w in ["tennis", "racquet", "racket", "squash", "badminton", "racquetball"]):
            return ActivityType.TENNIS
        # Basketball
        elif any(w in name_lower for w in ["basketball", "bball"]):
            return ActivityType.BASKETBALL
        # American Football (before "football" / "soccer" to avoid confusion)
        elif any(w in name_lower for w in ["american football", "nfl", "flag football"]):
            return ActivityType.FOOTBALL
        # Soccer
        elif any(w in name_lower for w in ["soccer", "football"]):
            return ActivityType.SOCCER
        # Volleyball
        elif any(w in name_lower for w in ["volleyball", "vball", "beach volleyball"]):
            return ActivityType.VOLLEYBALL
        # Golf
        elif "golf" in name_lower:
            return ActivityType.GOLF
        # Snowboarding (before "ski" to avoid partial match)
        elif any(w in name_lower for w in ["snowboard", "snowboarding"]):
            return ActivityType.SNOWBOARDING
        # Skiing
        elif any(w in name_lower for w in ["ski", "skiing", "cross-country ski"]):
            return ActivityType.SKIING
        # Dance
        elif any(w in name_lower for w in ["dance", "zumba", "dancing", "aerobic dance"]):
            return ActivityType.DANCE
        # Pilates (before "yoga")
        elif any(w in name_lower for w in ["pilates", "barre"]):
            return ActivityType.PILATES
        # Yoga / stretching / mindfulness
        elif any(w in name_lower for w in ["yoga", "tai chi", "stretching", "meditation", "flexibility"]):
            return ActivityType.YOGA
        # Boxing
        elif any(w in name_lower for w in ["boxing", "box", "punching"]):
            return ActivityType.BOXING
        # Martial Arts / Kickboxing
        elif any(w in name_lower for w in ["martial arts", "karate", "taekwondo", "judo", "kickboxing", "mma", "jiu-jitsu", "kung fu"]):
            return ActivityType.MARTIAL_ARTS
        # Climbing
        elif any(w in name_lower for w in ["climb", "climbing", "rock climb", "bouldering"]):
            return ActivityType.CLIMBING
        # HIIT / Bootcamp (before "crossfit")
        elif any(w in name_lower for w in ["hiit", "bootcamp", "boot camp", "interval training", "tabata"]):
            return ActivityType.HIIT
        # CrossFit
        elif any(w in name_lower for w in ["crossfit", "cross fit"]):
            return ActivityType.CROSSFIT
        # Ab workouts
        elif any(w in name_lower for w in ["abs", "core", "abdominal", "crunch"]):
            return ActivityType.ABS
        # Weight / Strength training
        elif any(w in name_lower for w in ["weight", "strength", "lifting", "barbell", "dumbbell", "resistance", "circuit"]):
            return ActivityType.WEIGHTS
        # Generic aerobic / cardio
        elif any(w in name_lower for w in ["aerobic", "cardio", "step aerobic", "jump rope"]):
            return ActivityType.AEROBIC
        # Other sports (baseball, hockey, rugby, etc.)
        elif any(w in name_lower for w in ["sport", "baseball", "hockey", "cricket", "rugby", "lacrosse", "handball"]):
            return ActivityType.SPORT
        # Generic workout / training
        elif any(w in name_lower for w in ["workout", "exercise", "training", "fitness", "gym"]):
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
                with tqdm(
                    total=len(sleep_files),
                    desc="    😴 Processing sleep files",
                    leave=False,
                ) as pbar:
                    for json_file in sleep_files:
                        pbar.set_description(f"    😴 Processing {json_file.name}")
                        try:
                            data = self._parse_json_file_efficiently(json_file)
                            if isinstance(data, list):
                                for item in data:
                                    sleep = self._parse_single_sleep_record(item)
                                    if sleep:
                                        sleep_data.append(sleep)
                        except Exception as e:
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

        # Only process files that are known Fitbit daily metric exports.
        # Using an explicit allowlist avoids wasting time on the hundreds of other
        # JSON files in Global Export Data (weight, oxygen, stress, HRV, etc.).
        DAILY_METRIC_PREFIXES = (
            "steps-",
            "distance-",
            "calories-",
            "lightly_active_minutes-",
            "fairly_active_minutes-",
            "very_active_minutes-",
            "minutessedentary-",
            "sedentary_minutes-",
            "floors-",
            "elevation-",
            "resting_heart_rate-",
            "active_minutes-",
            "minuteslightlyactive-",
            "minutesfairlyactive-",
            "minutesveryactive-",
        )
        json_files = list(path.glob("*.json"))  # no recursion needed; flat directory
        daily_files = [
            f for f in json_files
            if f.name.lower().startswith(DAILY_METRIC_PREFIXES)
        ]

        print(f"    📋 Found {len(daily_files)} daily metrics files")

        with tqdm(
            total=len(daily_files),
            desc="    📄 Processing daily metrics files",
            leave=False,
        ) as pbar:
            for json_file in daily_files:
                pbar.set_description(f"    📄 Processing {json_file.name}")
                try:
                    data = self._parse_json_file_efficiently(json_file)
                    if isinstance(data, list):
                        for item in data:
                            metric = self._parse_single_daily_metric(item)
                            if metric:
                                metrics.append(metric)
                    elif isinstance(data, dict):
                        metric = self._parse_single_daily_metric(data)
                        if metric:
                            metrics.append(metric)
                except Exception as e:
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

    def _parse_heart_rate_data(self):
        """Parse heart rate data from Fitbit exports.

        Performance design: Fitbit records HR every ~5 seconds. Over 3 years that is
        ~19 million readings. Storing each as a Pydantic object (~400 B) would consume
        ~8 GB of RAM only to compute a daily-summary CSV at the end.

        Instead we stream each file and accumulate lightweight per-day aggregates in a
        plain dict. The result is a list of daily-summary dicts (~1 KB total per year).

        Returns:
            (List[HeartRateData], List[Dict]) — the HeartRateData list is always empty
            (kept for backward compatibility); the Dict list contains one entry per day
            with precomputed avg/min/max/resting statistics ready for the exporter.
        """
        import gc
        import time
        import psutil

        # daily_agg[date_str] = {sum, count, min, max, hc_min, hc_count}
        # Using plain ints/floats — no Pydantic overhead at all.
        daily_agg: Dict[str, Dict] = {}
        hr_files = []

        if "global_export" in self.data_directories:
            hr_path = self.data_directories["global_export"]
            print(f"  📁 Processing heart rate data from: {hr_path.name}")
            hr_files = list(hr_path.glob("heart_rate*.json"))
            print(f"    📋 Found {len(hr_files)} heart rate files")

            try:
                memory_gb = psutil.virtual_memory().available / (1024 ** 3)
                total_size_mb = sum(f.stat().st_size for f in hr_files) / (1024 ** 2)
                print(f"    📊 {len(hr_files)} files ({total_size_mb:.1f} MB), "
                      f"{memory_gb:.1f} GB available memory")
            except Exception:
                pass

            use_parallel = self.enable_parallel and len(hr_files) > 4

            start_time = time.time()

            if use_parallel:
                print("    🚀 Using parallel processing for heart rate data")
                # Process all files with a SINGLE ProcessPoolExecutor.
                # Aggregate each file's records immediately as futures complete —
                # never accumulate the full result set in RAM simultaneously.
                from concurrent.futures import ProcessPoolExecutor, as_completed

                workers = self.parallel_processor.max_workers
                with tqdm(total=len(hr_files), desc="    💓 Processing HR files",
                          unit="files") as pbar:
                    with ProcessPoolExecutor(max_workers=workers) as executor:
                        future_to_file = {
                            executor.submit(process_json_file_worker, fp): fp
                            for fp in hr_files
                        }
                        completed = 0
                        for future in as_completed(future_to_file):
                            try:
                                items = future.result(timeout=120)
                                if items:
                                    for item in items:
                                        if isinstance(item, dict):
                                            self._aggregate_hr_item(item, daily_agg)
                            except Exception as e:
                                fp = future_to_file[future]
                                logger.warning(f"Error processing {fp.name}: {e}")
                            finally:
                                future_to_file.pop(future, None)
                                completed += 1
                                pbar.update(1)
                                if completed % 50 == 0:
                                    elapsed = time.time() - start_time
                                    rate = completed / elapsed if elapsed > 0 else 0
                                    pbar.set_postfix({
                                        "days": len(daily_agg),
                                        "rate": f"{rate:.1f} f/s",
                                    })
                                    gc.collect()
            else:
                with tqdm(total=len(hr_files), desc="    💓 Processing HR files",
                          unit="files", leave=False) as pbar:
                    for i, json_file in enumerate(hr_files):
                        try:
                            file_data = self._parse_json_file_efficiently(json_file)
                            if isinstance(file_data, list):
                                for item in file_data:
                                    if isinstance(item, dict):
                                        self._aggregate_hr_item(item, daily_agg)
                        except Exception as e:
                            logger.warning(f"Error parsing heart rate file {json_file}: {e}")
                        finally:
                            pbar.update(1)

                        elapsed = time.time() - start_time
                        rate = (i + 1) / elapsed if elapsed > 0 else 0
                        pbar.set_postfix({"days": len(daily_agg), "rate": f"{rate:.1f} f/s"})

        total_time = time.time() - start_time if hr_files else 0
        total_readings = sum(v["count"] for v in daily_agg.values())
        logger.info(f"Aggregated {total_readings} HR readings into {len(daily_agg)} "
                    f"daily summaries from {len(hr_files)} files in {total_time:.1f}s")
        print(f"    ✅ Aggregated {total_readings:,} readings → {len(daily_agg)} daily summaries "
              f"({total_time:.1f}s)")

        # Convert aggregated dict to the list of daily-stat dicts expected by the exporter
        daily_stats = []
        for date_str, agg in sorted(daily_agg.items()):
            resting = agg["hc_min"] if agg["hc_count"] > 0 else agg["min"]
            daily_stats.append({
                "date": date_str,
                "avg_bpm": round(agg["sum"] / agg["count"]),
                "min_bpm": agg["min"],
                "max_bpm": agg["max"],
                "resting_bpm": resting,
                "total_readings": agg["count"],
                "hc_readings": agg["hc_count"],
            })

        return [], daily_stats  # empty HeartRateData list + precomputed daily stats

    def _aggregate_hr_item(self, item: Dict[str, Any], daily_agg: Dict) -> None:
        """Aggregate a single raw HR JSON record into the per-day accumulator dict.

        Avoids creating any Pydantic objects — just updates plain int totals.
        """
        datetime_str = item.get("dateTime", "")
        if not datetime_str:
            return
        try:
            try:
                dt = datetime.strptime(datetime_str, "%m/%d/%y %H:%M:%S")
            except ValueError:
                dt = parse_datetime(datetime_str)
        except Exception:
            return

        value = item.get("value", {})
        if not isinstance(value, dict):
            return
        bpm = value.get("bpm", 0)
        confidence = value.get("confidence", 0)
        if not bpm or bpm <= 0:
            return

        day = dt.strftime("%Y-%m-%d")
        if day not in daily_agg:
            daily_agg[day] = {"sum": 0, "count": 0, "min": 9999, "max": 0,
                               "hc_min": 9999, "hc_count": 0}
        agg = daily_agg[day]
        agg["sum"] += bpm
        agg["count"] += 1
        if bpm < agg["min"]:
            agg["min"] = bpm
        if bpm > agg["max"]:
            agg["max"] = bpm
        if confidence >= 2:
            agg["hc_count"] += 1
            if bpm < agg["hc_min"]:
                agg["hc_min"] = bpm

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
        """Parse stress management score data from CSV files."""
        stress_data = []

        if "stress" not in self.data_directories:
            return stress_data

        stress_path = self.data_directories["stress"]
        print(f"  📁 Processing stress data from: {stress_path.name}")

        csv_files = list(stress_path.glob("**/*.csv"))
        print(f"    📋 Found {len(csv_files)} stress files")

        for csv_file in tqdm(csv_files, desc="    🧘 Processing stress files", leave=False):
            try:
                df = pd.read_csv(csv_file)
                for _, row in df.iterrows():
                    # Fitbit stress CSV columns: DATE, STRESS_SCORE (column names vary)
                    date_str = str(row.get("DATE", row.get("date", row.get("timestamp", ""))))
                    if not date_str or date_str == "nan":
                        continue
                    try:
                        record_date = parse_datetime(date_str).date()
                    except Exception:
                        continue

                    stress_score = None
                    for col_name in ["STRESS_SCORE", "stress_score", "Stress Score", "score"]:
                        val = row.get(col_name)
                        if val is not None and pd.notna(val):
                            try:
                                stress_score = int(float(val))
                            except (ValueError, TypeError):
                                pass
                            break

                    stress_data.append(StressData(
                        date=record_date,
                        stress_score=stress_score,
                    ))
            except Exception as e:
                logger.warning(f"Error parsing stress file {csv_file}: {e}")

        logger.info(f"Parsed {len(stress_data)} stress records")
        print(f"    ✅ Parsed {len(stress_data)} stress records")
        return stress_data

    def _parse_temperature_data(self) -> List[TemperatureData]:
        """Parse skin temperature deviation data from Fitbit exports.

        Note: Fitbit records nightly skin temperature as a deviation from the
        user's personal baseline (not absolute temperature). Positive = warmer,
        negative = cooler. Requires a Fitbit Sense, Versa 3+, or Charge 5+.
        """
        temperature_data = []

        if "temperature" not in self.data_directories:
            return temperature_data

        temp_path = self.data_directories["temperature"]
        print(f"  📁 Processing temperature data from: {temp_path.name}")

        csv_files = list(temp_path.glob("**/*.csv"))
        json_files = list(temp_path.glob("**/*.json"))
        print(f"    📋 Found {len(csv_files)} CSV and {len(json_files)} JSON temperature files")

        for csv_file in tqdm(csv_files, desc="    🌡️ Processing temperature CSV files", leave=False):
            try:
                df = pd.read_csv(csv_file)
                for _, row in df.iterrows():
                    date_str = str(row.get("date_time", row.get("dateTime", row.get("date", ""))))
                    if not date_str or date_str == "nan":
                        continue
                    try:
                        record_date = parse_datetime(date_str).date()
                    except Exception:
                        continue

                    temp_deviation = None
                    for col_name in ["temperature_celsius", "nightlyRelative", "temperature", "Temperature", "value"]:
                        val = row.get(col_name)
                        if val is not None and pd.notna(val):
                            try:
                                temp_deviation = float(val)
                            except (ValueError, TypeError):
                                pass
                            break

                    temperature_data.append(TemperatureData(
                        date=record_date,
                        temperature_celsius=temp_deviation,  # deviation from baseline, not absolute
                    ))
            except Exception as e:
                logger.warning(f"Error parsing temperature CSV file {csv_file}: {e}")

        for json_file in tqdm(json_files, desc="    🌡️ Processing temperature JSON files", leave=False):
            try:
                data = self._parse_json_file_efficiently(json_file)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    date_str = item.get("dateTime", "")
                    if not date_str:
                        continue
                    try:
                        record_date = parse_datetime(date_str).date()
                    except Exception:
                        continue

                    value = item.get("value", {})
                    if isinstance(value, dict):
                        temp_deviation = value.get("nightlyRelative")
                    else:
                        try:
                            temp_deviation = float(value) if value is not None else None
                        except (ValueError, TypeError):
                            temp_deviation = None

                    if temp_deviation is not None:
                        temperature_data.append(TemperatureData(
                            date=record_date,
                            temperature_celsius=float(temp_deviation),
                        ))
            except Exception as e:
                logger.warning(f"Error parsing temperature JSON file {json_file}: {e}")

        logger.info(f"Parsed {len(temperature_data)} temperature records")
        print(f"    ✅ Parsed {len(temperature_data)} temperature records")
        return temperature_data

    def _parse_spo2_data(self) -> List[SpO2Data]:
        """Parse blood oxygen saturation (SpO2) data from Fitbit exports.

        Fitbit records SpO2 during sleep. A reading of 50 is an invalid/error marker.
        Both daily summaries (avg/min/max) and minute-level files may be present.
        """
        spo2_data = []

        if "spo2" not in self.data_directories:
            return spo2_data

        spo2_path = self.data_directories["spo2"]
        print(f"  📁 Processing SpO2 data from: {spo2_path.name}")

        csv_files = list(spo2_path.glob("**/*.csv"))
        json_files = list(spo2_path.glob("**/*.json"))
        print(f"    📋 Found {len(csv_files)} CSV and {len(json_files)} JSON SpO2 files")

        for csv_file in tqdm(csv_files, desc="    🩸 Processing SpO2 CSV files", leave=False):
            try:
                df = pd.read_csv(csv_file)
                for _, row in df.iterrows():
                    date_str = str(row.get("timestamp", row.get("dateTime", row.get("date", ""))))
                    if not date_str or date_str == "nan":
                        continue
                    try:
                        dt = parse_datetime(date_str)
                        record_date = dt.date()
                    except Exception:
                        continue

                    # Find the SpO2 value — column names vary across Fitbit export versions
                    spo2_value = None
                    for col in df.columns:
                        col_lower = col.lower()
                        if "spo2" in col_lower or "oxygen" in col_lower or "avg" in col_lower:
                            val = row.get(col)
                            if val is not None and pd.notna(val):
                                try:
                                    candidate = float(val)
                                    if candidate != 50.0:  # 50.0 is Fitbit's invalid reading marker
                                        spo2_value = candidate
                                        break
                                except (ValueError, TypeError):
                                    pass

                    if spo2_value is not None:
                        spo2_data.append(SpO2Data(
                            date=record_date,
                            spo2_percentage=spo2_value,
                            timestamp=dt,
                        ))
            except Exception as e:
                logger.warning(f"Error parsing SpO2 CSV file {csv_file}: {e}")

        for json_file in tqdm(json_files, desc="    🩸 Processing SpO2 JSON files", leave=False):
            try:
                data = self._parse_json_file_efficiently(json_file)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    date_str = item.get("dateTime", "")
                    if not date_str:
                        continue
                    try:
                        record_date = parse_datetime(date_str).date()
                    except Exception:
                        continue

                    value = item.get("value", {})
                    if isinstance(value, dict):
                        avg_val = value.get("avg")
                        if avg_val is not None:
                            spo2_data.append(SpO2Data(
                                date=record_date,
                                spo2_percentage=float(avg_val),
                            ))
            except Exception as e:
                logger.warning(f"Error parsing SpO2 JSON file {json_file}: {e}")

        logger.info(f"Parsed {len(spo2_data)} SpO2 records")
        print(f"    ✅ Parsed {len(spo2_data)} SpO2 records")
        return spo2_data

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

                # Timestamp — TCX/GPX requires ISO 8601 UTC format with trailing 'Z'
                if "time" in point:
                    try:
                        enhanced_point["time"] = parse_datetime(
                            point["time"]
                        ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    except Exception:
                        enhanced_point["time"] = point["time"]
                elif "timestamp" in point:
                    try:
                        enhanced_point["time"] = parse_datetime(
                            point["timestamp"]
                        ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    except Exception:
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

    def _parse_tcx_gps_points(self, tcx_path: Path):
        """Parse GPS trackpoints and activity start time from a Fitbit TCX file.

        Returns (gps_points, start_time_str).  gps_points is a list of dicts
        with keys latitude, longitude, altitude, distance, time, heart_rate.
        """
        import xml.etree.ElementTree as ET

        TCX_NS = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"

        def _find(elem, tag):
            """Try namespaced then bare tag lookup.
            NOTE: must use 'is not None' — leaf Elements are falsy in Python.
            """
            result = elem.find(f"{{{TCX_NS}}}{tag}")
            if result is None:
                result = elem.find(tag)
            return result

        try:
            tree = ET.parse(tcx_path)
            root = tree.getroot()

            # Get the activity start time from <Id> element.
            # Must check 'is not None' because a childless Element is falsy.
            start_time_str = None
            id_elem = root.find(f".//{{{TCX_NS}}}Id")
            if id_elem is None:
                id_elem = root.find(".//Id")
            if id_elem is not None and id_elem.text:
                start_time_str = id_elem.text.strip()

            gps_points = []
            # Walk all Trackpoints across all laps
            trackpoints = root.findall(f".//{{{TCX_NS}}}Trackpoint")
            if not trackpoints:
                trackpoints = root.findall(".//Trackpoint")

            for tp in trackpoints:
                point = {}

                # Time
                time_elem = _find(tp, "Time")
                if time_elem is not None and time_elem.text:
                    point["time"] = time_elem.text.strip()

                # Position (required for GPS)
                pos_elem = _find(tp, "Position")
                if pos_elem is not None:
                    lat_elem = _find(pos_elem, "LatitudeDegrees")
                    lon_elem = _find(pos_elem, "LongitudeDegrees")
                    try:
                        if lat_elem is not None and lon_elem is not None:
                            point["latitude"] = float(lat_elem.text)
                            point["longitude"] = float(lon_elem.text)
                    except (ValueError, TypeError):
                        pass

                if "latitude" not in point or "longitude" not in point:
                    continue  # Skip points without coordinates

                # Altitude
                alt_elem = _find(tp, "AltitudeMeters")
                if alt_elem is not None and alt_elem.text:
                    try:
                        point["altitude"] = float(alt_elem.text)
                    except (ValueError, TypeError):
                        pass

                # Cumulative distance
                dist_elem = _find(tp, "DistanceMeters")
                if dist_elem is not None and dist_elem.text:
                    try:
                        point["distance"] = float(dist_elem.text)
                    except (ValueError, TypeError):
                        pass

                # Heart rate — nested element, find with explicit path
                hr_val = tp.find(f".//{{{TCX_NS}}}HeartRateBpm/{{{TCX_NS}}}Value")
                if hr_val is None:
                    hr_val = tp.find(".//HeartRateBpm/Value")
                if hr_val is not None and hr_val.text:
                    try:
                        point["heart_rate"] = int(hr_val.text)
                    except (ValueError, TypeError):
                        pass

                gps_points.append(point)

            return (gps_points if gps_points else None), start_time_str

        except Exception as e:
            logger.warning(f"Error parsing TCX file {tcx_path}: {e}")
            return None, None

    def _attach_gps_from_tcx_files(self, activities: List[ActivityData]) -> None:
        """Match Fitbit TCX files from the Activities directory to parsed activities
        and attach GPS trackpoints to each matching activity.

        Fitbit stores GPS data in separate .tcx files rather than embedding it in
        the exercise JSON files.  Matching is attempted in two ways:
          1. If the TCX filename stem is purely numeric it is treated as a logId.
          2. Otherwise the TCX file is parsed for its <Id> start-time and matched
             against activity start times (within a 60-second tolerance).
        """
        if "activities" not in self.data_directories:
            return

        tcx_dir = self.data_directories["activities"]
        if not tcx_dir.exists():
            return

        tcx_files = list(tcx_dir.glob("**/*.tcx"))
        if not tcx_files:
            return

        print(f"  📍 Found {len(tcx_files)} TCX file(s) with potential GPS data")

        # Build lookup maps (only for activities that don't already have GPS)
        log_id_map: Dict[int, ActivityData] = {
            a.log_id: a for a in activities if a.log_id and not a.gps_data
        }
        start_ts_map: Dict[int, ActivityData] = {
            int(a.start_time.timestamp()): a
            for a in activities
            if not a.gps_data
        }

        attached = 0
        for tcx_file in tcx_files:
            try:
                activity = None
                gps_points = None
                start_time_str = None

                # Strategy 1: filename is the logId (e.g. "12345678.tcx")
                stem = tcx_file.stem
                if stem.lstrip("-").isdigit():
                    activity = log_id_map.get(int(stem))

                # Strategy 2: parse the TCX and match by start time
                if not activity:
                    gps_points, start_time_str = self._parse_tcx_gps_points(tcx_file)
                    if start_time_str:
                        try:
                            tcx_ts = int(parse_datetime(start_time_str).timestamp())
                            # Allow ±60 s tolerance for clock/timezone drift
                            for offset in range(-60, 61):
                                activity = start_ts_map.get(tcx_ts + offset)
                                if activity:
                                    break
                        except Exception:
                            pass

                if not activity:
                    continue

                # Parse GPS points if strategy 1 was used (not yet parsed)
                if gps_points is None:
                    gps_points, _ = self._parse_tcx_gps_points(tcx_file)

                if gps_points:
                    activity.gps_data = gps_points
                    activity.has_gps = True
                    attached += 1
                    logger.debug(
                        f"Attached {len(gps_points)} GPS points to activity "
                        f"{activity.log_id} from {tcx_file.name}"
                    )

            except Exception as e:
                logger.warning(f"Error attaching GPS from {tcx_file.name}: {e}")

        if attached:
            print(f"  ✅ Attached GPS data to {attached} activities from TCX files")
