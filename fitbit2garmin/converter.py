"""
Data converter for transforming Fitbit data to Garmin-compatible formats.
Handles TCX, GPX, and FIT file generation for activities.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import gpxpy
import gpxpy.gpx
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

from .models import ActivityData, FitbitUserData, HeartRateData

logger = logging.getLogger(__name__)


class DataConverter:
    """Convert Fitbit data to Garmin-compatible formats."""

    def __init__(self, output_dir: Union[str, Path]):
        """Initialize converter with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Initialized data converter with output directory: {self.output_dir}"
        )

    def convert_activities_to_tcx(self, activities: List[ActivityData]) -> List[str]:
        """Convert activities to TCX format."""
        tcx_files = []

        for activity in activities:
            if activity.gps_data or activity.heart_rate_zones:
                tcx_file = self._generate_tcx_file(activity)
                if tcx_file:
                    tcx_files.append(tcx_file)

        logger.info(f"Generated {len(tcx_files)} TCX files")
        return tcx_files

    def convert_activities_to_gpx(self, activities: List[ActivityData]) -> List[str]:
        """Convert activities with GPS data to GPX format."""
        gpx_files = []

        for activity in activities:
            if activity.gps_data:
                gpx_file = self._generate_gpx_file(activity)
                if gpx_file:
                    gpx_files.append(gpx_file)

        logger.info(f"Generated {len(gpx_files)} GPX files")
        return gpx_files

    def _generate_tcx_file(self, activity: ActivityData) -> Optional[str]:
        """Generate a TCX file for a single activity."""
        try:
            # Create TCX root element
            tcx_root = Element("TrainingCenterDatabase")
            tcx_root.set(
                "xmlns", "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
            )
            tcx_root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
            tcx_root.set(
                "xsi:schemaLocation",
                "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2 "
                "http://www.garmin.com/xmlschemas/TrainingCenterDatabasev2.xsd",
            )

            # Activities element
            activities_elem = SubElement(tcx_root, "Activities")
            activity_elem = SubElement(activities_elem, "Activity")
            activity_elem.set(
                "Sport", self._map_activity_type_to_tcx(activity.activity_type)
            )

            # Add detailed activity information in notes for better Garmin Connect recognition
            notes_elem = SubElement(activity_elem, "Notes")
            notes_parts = [
                f"Fitbit Activity: {activity.activity_name}",
                f"Activity Type: {activity.activity_type.value}",
                f"Log ID: {activity.log_id}",
            ]

            # Add original Fitbit activity type ID if available
            if hasattr(activity, "activity_type_id") and activity.activity_type_id:
                notes_parts.append(f"Fitbit Type ID: {activity.activity_type_id}")

            # Add original activity name if different from processed name
            if (
                hasattr(activity, "original_activity_name")
                and activity.original_activity_name
                and activity.original_activity_name != activity.activity_name
            ):
                notes_parts.append(f"Original: {activity.original_activity_name}")

            notes_elem.text = " | ".join(notes_parts)

            # Activity ID
            id_elem = SubElement(activity_elem, "Id")
            id_elem.text = activity.start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            # Create a lap
            lap_elem = SubElement(activity_elem, "Lap")
            lap_elem.set(
                "StartTime", activity.start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            )

            # Lap totals
            total_time_elem = SubElement(lap_elem, "TotalTimeSeconds")
            total_time_elem.text = str(activity.duration_ms / 1000)

            distance_elem = SubElement(lap_elem, "DistanceMeters")
            distance_elem.text = str(
                (activity.distance or 0) * 1000
            )  # Convert km to meters

            if activity.calories:
                calories_elem = SubElement(lap_elem, "Calories")
                calories_elem.text = str(activity.calories)

            if activity.average_heart_rate:
                avg_hr_elem = SubElement(lap_elem, "AverageHeartRateBpm")
                avg_hr_value = SubElement(avg_hr_elem, "Value")
                avg_hr_value.text = str(activity.average_heart_rate)

            if activity.max_heart_rate:
                max_hr_elem = SubElement(lap_elem, "MaximumHeartRateBpm")
                max_hr_value = SubElement(max_hr_elem, "Value")
                max_hr_value.text = str(activity.max_heart_rate)

            # Add maximum speed if available
            if activity.speed:
                max_speed_elem = SubElement(lap_elem, "MaximumSpeed")
                max_speed_elem.text = str(activity.speed)

            # Intensity
            intensity_elem = SubElement(lap_elem, "Intensity")
            intensity_elem.text = "Active"

            # Add heart rate zones if available (use recalculated zones if available)
            zones_to_use = (
                activity.recalculated_hr_zones
                if activity.recalculated_hr_zones
                else activity.heart_rate_zones
            )
            if zones_to_use:
                extensions_elem = SubElement(lap_elem, "Extensions")
                lx_elem = SubElement(extensions_elem, "LX")
                lx_elem.set(
                    "xmlns", "http://www.garmin.com/xmlschemas/ActivityExtension/v2"
                )

                for zone in zones_to_use:
                    if zone.minutes > 0:  # Only include zones with time spent
                        zone_elem = SubElement(lx_elem, "HeartRateZone")
                        zone_elem.set("Index", str(zone.zone_index or 0))
                        zone_elem.set("Name", zone.garmin_zone_name or zone.name)
                        zone_elem.set("Low", str(zone.min_bpm))
                        zone_elem.set("High", str(zone.max_bpm))
                        zone_elem.set("Minutes", str(zone.minutes))

            # Track (for GPS data or time-based data)
            track_elem = SubElement(lap_elem, "Track")

            # If we have GPS data, add trackpoints
            if activity.gps_data:
                self._add_gps_trackpoints(track_elem, activity)
            else:
                # Create basic trackpoints for time-based data
                self._add_time_trackpoints(track_elem, activity)

            # Add Creator information for better Garmin Connect compatibility
            creator_elem = SubElement(activity_elem, "Creator")
            creator_elem.set("xsi:type", "Device_t")

            name_elem = SubElement(creator_elem, "Name")
            name_elem.text = "Fitbit2Garmin Converter"

            unit_id_elem = SubElement(creator_elem, "UnitId")
            unit_id_elem.text = str(activity.log_id)

            product_id_elem = SubElement(creator_elem, "ProductID")
            product_id_elem.text = "65534"  # Generic product ID

            version_elem = SubElement(creator_elem, "Version")
            version_major = SubElement(version_elem, "VersionMajor")
            version_major.text = "1"
            version_minor = SubElement(version_elem, "VersionMinor")
            version_minor.text = "0"

            # Generate filename with better naming
            activity_type_name = activity.activity_type.value.replace("_", "-")
            filename = f"{activity_type_name}_{activity.log_id}_{activity.start_time.strftime('%Y%m%d_%H%M%S')}.tcx"
            filepath = self.output_dir / filename

            # Write TCX file
            rough_string = tostring(tcx_root, "utf-8")
            reparsed = minidom.parseString(rough_string)
            pretty_xml = reparsed.toprettyxml(indent="  ")

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(pretty_xml)

            logger.info(f"Generated TCX file: {filepath}")
            return str(filepath)

        except Exception as e:
            logger.error(
                f"Error generating TCX file for activity {activity.log_id}: {e}"
            )
            return None

    def _generate_gpx_file(self, activity: ActivityData) -> Optional[str]:
        """Generate a GPX file for a single activity with GPS data."""
        try:
            # Create GPX object
            gpx = gpxpy.gpx.GPX()

            # Create track
            gpx_track = gpxpy.gpx.GPXTrack()
            gpx_track.name = f"{activity.activity_name} - {activity.start_time.strftime('%Y-%m-%d %H:%M')}"
            gpx_track.type = activity.activity_type.value
            gpx.tracks.append(gpx_track)

            # Create track segment
            gpx_segment = gpxpy.gpx.GPXTrackSegment()
            gpx_track.segments.append(gpx_segment)

            # Add GPS points if available
            if activity.gps_data:
                for gps_point in activity.gps_data:
                    if (
                        isinstance(gps_point, dict)
                        and "latitude" in gps_point
                        and "longitude" in gps_point
                    ):
                        point = gpxpy.gpx.GPXTrackPoint(
                            latitude=gps_point["latitude"],
                            longitude=gps_point["longitude"],
                            elevation=gps_point.get("altitude"),
                            time=gps_point.get("time"),
                        )
                        gpx_segment.points.append(point)

            # Generate filename
            filename = f"activity_{activity.log_id}_{activity.start_time.strftime('%Y%m%d_%H%M%S')}.gpx"
            filepath = self.output_dir / filename

            # Write GPX file
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(gpx.to_xml())

            logger.info(f"Generated GPX file: {filepath}")
            return str(filepath)

        except Exception as e:
            logger.error(
                f"Error generating GPX file for activity {activity.log_id}: {e}"
            )
            return None

    def _add_gps_trackpoints(self, track_elem: Element, activity: ActivityData):
        """Add GPS trackpoints to TCX track element."""
        if not activity.gps_data:
            return

        for i, gps_point in enumerate(activity.gps_data):
            if not isinstance(gps_point, dict):
                continue

            trackpoint = SubElement(track_elem, "Trackpoint")

            # Time
            time_elem = SubElement(trackpoint, "Time")
            if "time" in gps_point:
                time_elem.text = gps_point["time"]
            else:
                # Estimate time based on duration and point index
                estimated_time = activity.start_time + timedelta(
                    seconds=(activity.duration_ms / 1000) * i / len(activity.gps_data)
                )
                time_elem.text = estimated_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            # Position
            if "latitude" in gps_point and "longitude" in gps_point:
                position = SubElement(trackpoint, "Position")
                lat_elem = SubElement(position, "LatitudeDegrees")
                lat_elem.text = str(gps_point["latitude"])
                lon_elem = SubElement(position, "LongitudeDegrees")
                lon_elem.text = str(gps_point["longitude"])

            # Altitude
            if "altitude" in gps_point:
                alt_elem = SubElement(trackpoint, "AltitudeMeters")
                alt_elem.text = str(gps_point["altitude"])

            # Distance (if available)
            if "distance" in gps_point:
                dist_elem = SubElement(trackpoint, "DistanceMeters")
                dist_elem.text = str(gps_point["distance"])

            # Heart rate (if available)
            if "heart_rate" in gps_point:
                hr_elem = SubElement(trackpoint, "HeartRateBpm")
                hr_value = SubElement(hr_elem, "Value")
                hr_value.text = str(gps_point["heart_rate"])

            # Speed (if available)
            if "speed" in gps_point:
                extensions = SubElement(trackpoint, "Extensions")
                tpx_elem = SubElement(extensions, "TPX")
                tpx_elem.set(
                    "xmlns", "http://www.garmin.com/xmlschemas/ActivityExtension/v2"
                )
                speed_elem = SubElement(tpx_elem, "Speed")
                speed_elem.text = str(gps_point["speed"])

    def _add_time_trackpoints(self, track_elem: Element, activity: ActivityData):
        """Add time-based trackpoints to TCX track element."""
        # Create trackpoints at regular intervals
        num_points = min(
            100, max(10, activity.duration_ms // 60000)
        )  # 1 point per minute, max 100
        interval_ms = activity.duration_ms / num_points

        for i in range(num_points):
            trackpoint = SubElement(track_elem, "Trackpoint")

            # Time
            time_elem = SubElement(trackpoint, "Time")
            point_time = activity.start_time + timedelta(milliseconds=i * interval_ms)
            time_elem.text = point_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            # Add heart rate if available
            if activity.average_heart_rate:
                hr_elem = SubElement(trackpoint, "HeartRateBpm")
                hr_value = SubElement(hr_elem, "Value")
                # Vary heart rate slightly around average
                hr_variation = int(activity.average_heart_rate * 0.1)  # 10% variation
                import random

                hr_value.text = str(
                    activity.average_heart_rate
                    + random.randint(-hr_variation, hr_variation)
                )

    def _map_activity_type_to_tcx(self, activity_type) -> str:
        """Map our activity type to TCX sport type with comprehensive Garmin Connect compatibility."""
        # Comprehensive mapping based on official Garmin TCX schema and Connect import behavior
        # These are the sport types that Garmin Connect recognizes and properly categorizes
        mapping = {
            "running": "Running",
            "walking": "Walking",
            "biking": "Biking",
            "hiking": "Walking",  # Garmin Connect imports hiking as walking
            "swimming": "Swimming",
            "treadmill": "Running",  # Treadmill runs are imported as running
            "elliptical": "Other",  # Elliptical gets better recognition as Other
            "rowing": "Other",  # Rowing gets better recognition as Other
            "workout": "Other",  # Generic workouts
            "yoga": "Other",  # Yoga gets better recognition as Other
            "weights": "Other",  # Weight training gets better recognition as Other
            "sport": "Other",  # Generic sports
            "tennis": "Other",  # Tennis gets better recognition as Other
            "basketball": "Other",  # Basketball gets better recognition as Other
            "soccer": "Other",  # Soccer gets better recognition as Other
            "football": "Other",  # Football gets better recognition as Other
            "volleyball": "Other",  # Volleyball gets better recognition as Other
            "golf": "Other",  # Golf gets better recognition as Other
            "skiing": "Other",  # Skiing gets better recognition as Other
            "snowboarding": "Other",  # Snowboarding gets better recognition as Other
            "dance": "Other",  # Dance gets better recognition as Other
            "martial_arts": "Other",  # Martial arts gets better recognition as Other
            "boxing": "Other",  # Boxing gets better recognition as Other
            "climbing": "Other",  # Climbing gets better recognition as Other
            "aerobic": "Other",  # Aerobic gets better recognition as Other
            "crossfit": "Other",  # CrossFit gets better recognition as Other
            "abs": "Other",  # Ab workouts get better recognition as Other
            "other": "Other",
        }

        # Get the mapped sport type
        sport_type = mapping.get(activity_type.value, "Other")

        return sport_type

    def convert_activities_to_fit(self, activities: List[ActivityData]) -> List[str]:
        """Convert activities to FIT format using fit-tool library."""
        fit_files = []

        try:
            from fit_tool.fit_file_builder import FitFileBuilder
            from fit_tool.profile.messages.activity_message import ActivityMessage
            from fit_tool.profile.messages.lap_message import LapMessage
            from fit_tool.profile.messages.record_message import RecordMessage
            from fit_tool.profile.messages.session_message import SessionMessage
            from fit_tool.profile.messages.file_id_message import FileIdMessage
            from fit_tool.profile.profile_type import Sport, SubSport, FileType
            from datetime import datetime, timezone

            for activity in activities:
                fit_file = self._generate_fit_file(activity)
                if fit_file:
                    fit_files.append(fit_file)

        except ImportError:
            logger.error(
                "fit-tool library not available. Install with: pip install fit-tool"
            )
            return []
        except Exception as e:
            logger.error(f"Error generating FIT files: {e}")
            return []

        logger.info(f"Generated {len(fit_files)} FIT files")
        return fit_files

    def _generate_fit_file(self, activity: ActivityData) -> Optional[str]:
        """Generate a FIT file for a single activity."""
        try:
            from fit_tool.fit_file_builder import FitFileBuilder
            from fit_tool.profile.messages.activity_message import ActivityMessage
            from fit_tool.profile.messages.lap_message import LapMessage
            from fit_tool.profile.messages.record_message import RecordMessage
            from fit_tool.profile.messages.session_message import SessionMessage
            from fit_tool.profile.messages.file_id_message import FileIdMessage
            from fit_tool.profile.profile_type import (
                Sport,
                SubSport,
                FileType,
                Manufacturer,
            )
            from datetime import datetime, timezone

            # Create FIT file builder
            builder = FitFileBuilder()

            # File ID message
            file_id = FileIdMessage()
            file_id.type = FileType.ACTIVITY
            file_id.manufacturer = Manufacturer.FITBIT
            file_id.product = 1
            file_id.time_created = int(activity.start_time.timestamp())
            builder.add_message(file_id)

            # Map activity type to FIT sport
            sport, sub_sport = self._map_activity_to_fit_sport(activity.activity_type)

            # Session message
            session = SessionMessage()
            session.sport = sport
            session.sub_sport = sub_sport
            session.start_time = int(activity.start_time.timestamp())
            session.timestamp = int(activity.start_time.timestamp()) + (
                activity.duration_ms // 1000
            )
            session.total_elapsed_time = activity.duration_ms / 1000
            session.total_timer_time = (
                activity.active_duration / 1000
                if activity.active_duration
                else activity.duration_ms / 1000
            )

            if activity.distance:
                session.total_distance = (
                    activity.distance * 1000
                )  # Convert km to meters
            if activity.calories:
                session.total_calories = activity.calories
            if activity.steps:
                session.total_steps = activity.steps
            if activity.average_heart_rate:
                session.avg_heart_rate = activity.average_heart_rate
            if activity.max_heart_rate:
                session.max_heart_rate = activity.max_heart_rate
            if activity.min_heart_rate:
                session.min_heart_rate = activity.min_heart_rate
            if activity.elevation_gain:
                session.total_ascent = activity.elevation_gain

            # Add heart rate zones to session (use recalculated zones if available)
            zones_to_use = (
                activity.recalculated_hr_zones
                if activity.recalculated_hr_zones
                else activity.heart_rate_zones
            )
            if zones_to_use:
                # Calculate zone time distribution in seconds
                zone_times = []
                for zone in zones_to_use:
                    zone_times.append(zone.minutes * 60)  # Convert minutes to seconds

                # FIT format supports up to 5 zones
                while len(zone_times) < 5:
                    zone_times.append(0)

                # Add zone times to session
                if len(zone_times) >= 5:
                    session.time_in_hr_zone = zone_times[:5]

            builder.add_message(session)

            # Lap message
            lap = LapMessage()
            lap.sport = sport
            lap.sub_sport = sub_sport
            lap.start_time = int(activity.start_time.timestamp())
            lap.timestamp = int(activity.start_time.timestamp()) + (
                activity.duration_ms // 1000
            )
            lap.total_elapsed_time = activity.duration_ms / 1000
            lap.total_timer_time = (
                activity.active_duration / 1000
                if activity.active_duration
                else activity.duration_ms / 1000
            )

            if activity.distance:
                lap.total_distance = activity.distance * 1000
            if activity.calories:
                lap.total_calories = activity.calories
            if activity.steps:
                lap.total_steps = activity.steps
            if activity.average_heart_rate:
                lap.avg_heart_rate = activity.average_heart_rate
            if activity.max_heart_rate:
                lap.max_heart_rate = activity.max_heart_rate
            if activity.min_heart_rate:
                lap.min_heart_rate = activity.min_heart_rate
            if activity.elevation_gain:
                lap.total_ascent = activity.elevation_gain

            # Add heart rate zones to lap as well
            if zones_to_use:
                zone_times = []
                for zone in zones_to_use:
                    zone_times.append(zone.minutes * 60)  # Convert minutes to seconds

                # FIT format supports up to 5 zones
                while len(zone_times) < 5:
                    zone_times.append(0)

                # Add zone times to lap
                if len(zone_times) >= 5:
                    lap.time_in_hr_zone = zone_times[:5]

            builder.add_message(lap)

            # Add GPS trackpoints if available
            if activity.gps_data and isinstance(activity.gps_data, list):
                self._add_fit_trackpoints(builder, activity)
            else:
                # Add time-based records for non-GPS activities
                self._add_fit_time_records(builder, activity)

            # Activity message
            activity_msg = ActivityMessage()
            activity_msg.timestamp = int(activity.start_time.timestamp()) + (
                activity.duration_ms // 1000
            )
            activity_msg.type = 0  # Manual
            activity_msg.event = 26  # Activity
            activity_msg.local_timestamp = int(activity.start_time.timestamp()) + (
                activity.duration_ms // 1000
            )
            builder.add_message(activity_msg)

            # Generate filename
            activity_type_name = activity.activity_type.value.replace("_", "-")
            filename = f"{activity_type_name}_{activity.log_id}_{activity.start_time.strftime('%Y%m%d_%H%M%S')}.fit"
            filepath = self.output_dir / filename

            # Build and save FIT file
            fit_file = builder.build()

            with open(filepath, "wb") as f:
                f.write(fit_file.to_bytes())

            logger.info(f"Generated FIT file: {filepath}")
            return str(filepath)

        except Exception as e:
            logger.error(
                f"Error generating FIT file for activity {activity.log_id}: {e}"
            )
            return None

    def _map_activity_to_fit_sport(self, activity_type):
        """Map activity type to FIT Sport and SubSport enums."""
        try:
            from fit_tool.profile.profile_type import Sport, SubSport

            # Comprehensive mapping for FIT sports
            mapping = {
                "running": (Sport.RUNNING, SubSport.GENERIC),
                "walking": (Sport.WALKING, SubSport.GENERIC),
                "biking": (Sport.CYCLING, SubSport.ROAD),
                "hiking": (Sport.HIKING, SubSport.GENERIC),
                "swimming": (Sport.SWIMMING, SubSport.LAP_SWIMMING),
                "treadmill": (Sport.RUNNING, SubSport.TREADMILL),
                "elliptical": (Sport.FITNESS_EQUIPMENT, SubSport.ELLIPTICAL),
                "rowing": (Sport.ROWING, SubSport.GENERIC),
                "workout": (Sport.TRAINING, SubSport.CARDIO_TRAINING),
                "yoga": (Sport.YOGA, SubSport.GENERIC),
                "weights": (Sport.STRENGTH_TRAINING, SubSport.GENERIC),
                "tennis": (Sport.TENNIS, SubSport.GENERIC),
                "basketball": (Sport.BASKETBALL, SubSport.GENERIC),
                "soccer": (Sport.SOCCER, SubSport.GENERIC),
                "football": (Sport.AMERICAN_FOOTBALL, SubSport.GENERIC),
                "volleyball": (Sport.VOLLEYBALL, SubSport.GENERIC),
                "golf": (Sport.GOLF, SubSport.GENERIC),
                "skiing": (Sport.ALPINE_SKIING, SubSport.GENERIC),
                "snowboarding": (Sport.SNOWBOARDING, SubSport.GENERIC),
                "dance": (Sport.DANCING, SubSport.GENERIC),
                "martial_arts": (Sport.MARTIAL_ARTS, SubSport.GENERIC),
                "boxing": (Sport.BOXING, SubSport.GENERIC),
                "climbing": (Sport.ROCK_CLIMBING, SubSport.GENERIC),
                "aerobic": (Sport.FITNESS_EQUIPMENT, SubSport.CARDIO_TRAINING),
                "crossfit": (Sport.TRAINING, SubSport.CROSS_TRAINING),
            }

            return mapping.get(activity_type.value, (Sport.GENERIC, SubSport.GENERIC))

        except ImportError:
            # Fallback if fit-tool not available
            return (0, 0)  # Generic sport/subsport

    def _add_fit_trackpoints(self, builder, activity: ActivityData):
        """Add GPS trackpoints to FIT file."""
        try:
            from fit_tool.profile.messages.record_message import RecordMessage
            from datetime import timedelta

            if not activity.gps_data:
                return

            # Calculate time intervals between points
            duration_seconds = activity.duration_ms / 1000
            num_points = len(activity.gps_data)
            time_interval = duration_seconds / num_points if num_points > 0 else 1

            cumulative_distance = 0

            for i, gps_point in enumerate(activity.gps_data):
                if not isinstance(gps_point, dict):
                    continue

                record = RecordMessage()

                # Timestamp
                point_time = activity.start_time + timedelta(seconds=i * time_interval)
                record.timestamp = int(point_time.timestamp())

                # Position (convert to semicircles)
                if "latitude" in gps_point and "longitude" in gps_point:
                    record.position_lat = int(
                        gps_point["latitude"] * 11930465
                    )  # Convert to semicircles
                    record.position_long = int(gps_point["longitude"] * 11930465)

                # Altitude
                if "altitude" in gps_point:
                    record.altitude = gps_point["altitude"]

                # Distance (cumulative)
                if "distance" in gps_point:
                    cumulative_distance = (
                        gps_point["distance"] * 1000
                    )  # Convert km to meters
                elif i > 0 and "latitude" in gps_point and "longitude" in gps_point:
                    # Calculate distance from previous point if not provided
                    prev_point = activity.gps_data[i - 1]
                    if (
                        isinstance(prev_point, dict)
                        and "latitude" in prev_point
                        and "longitude" in prev_point
                    ):
                        distance_increment = self._calculate_distance(
                            prev_point["latitude"],
                            prev_point["longitude"],
                            gps_point["latitude"],
                            gps_point["longitude"],
                        )
                        cumulative_distance += distance_increment

                record.distance = cumulative_distance

                # Speed
                if "speed" in gps_point:
                    record.speed = gps_point["speed"]

                # Heart rate
                if "heart_rate" in gps_point:
                    record.heart_rate = gps_point["heart_rate"]
                elif activity.average_heart_rate:
                    # Use average heart rate with some variation
                    import random

                    variation = int(activity.average_heart_rate * 0.1)
                    record.heart_rate = activity.average_heart_rate + random.randint(
                        -variation, variation
                    )

                builder.add_message(record)

        except Exception as e:
            logger.warning(f"Error adding FIT trackpoints: {e}")

    def _add_fit_time_records(self, builder, activity: ActivityData):
        """Add time-based records for non-GPS activities."""
        try:
            from fit_tool.profile.messages.record_message import RecordMessage
            from datetime import timedelta

            # Create records at regular intervals
            duration_seconds = activity.duration_ms / 1000
            num_records = min(
                100, max(10, int(duration_seconds / 60))
            )  # 1 record per minute, max 100
            time_interval = duration_seconds / num_records

            for i in range(num_records):
                record = RecordMessage()

                # Timestamp
                point_time = activity.start_time + timedelta(seconds=i * time_interval)
                record.timestamp = int(point_time.timestamp())

                # Distance (if available, spread over time)
                if activity.distance:
                    record.distance = (
                        activity.distance * 1000 * i
                    ) / num_records  # Convert km to meters

                # Heart rate
                if activity.average_heart_rate:
                    import random

                    variation = int(activity.average_heart_rate * 0.1)
                    record.heart_rate = activity.average_heart_rate + random.randint(
                        -variation, variation
                    )

                # Steps (for walking/running activities)
                if activity.steps:
                    record.cadence = int(
                        (activity.steps / 2) / (duration_seconds / 60)
                    )  # Steps per minute / 2

                builder.add_message(record)

        except Exception as e:
            logger.warning(f"Error adding FIT time records: {e}")

    def _calculate_distance(
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

    def batch_convert_activities(
        self, user_data: FitbitUserData
    ) -> Dict[str, List[str]]:
        """Convert all activities to multiple formats."""
        logger.info(
            f"Starting batch conversion of {len(user_data.activities)} activities"
        )

        results = {"tcx_files": [], "gpx_files": [], "fit_files": []}

        # Convert to TCX
        if user_data.activities:
            print("  🏃 Converting activities to TCX format...")
            results["tcx_files"] = self.convert_activities_to_tcx(user_data.activities)

        # Convert to GPX (only GPS activities)
        gps_activities = [a for a in user_data.activities if a.gps_data]
        if gps_activities:
            print(
                f"  🗺️ Converting {len(gps_activities)} GPS activities to GPX format..."
            )
            results["gpx_files"] = self.convert_activities_to_gpx(gps_activities)

        # Convert to FIT format
        if user_data.activities:
            print(
                f"  📁 Converting {len(user_data.activities)} activities to FIT format..."
            )
            results["fit_files"] = self.convert_activities_to_fit(user_data.activities)

        logger.info(
            f"Batch conversion completed: {len(results['tcx_files'])} TCX, "
            f"{len(results['gpx_files'])} GPX, {len(results['fit_files'])} FIT files"
        )

        print(
            f"  ✅ Created {len(results['tcx_files'])} TCX files, "
            f"{len(results['gpx_files'])} GPX files, {len(results['fit_files'])} FIT files"
        )

        return results
