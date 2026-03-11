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

# fit-tool handles the FIT-protocol epoch (Dec 31 1989) internally.
# All timestamp fields must be supplied as Unix milliseconds (int).

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
            # Generate TCX for all activities — not just those with GPS or HR zones.
            # Activities without GPS get time-based trackpoints; without HR zones
            # they still carry lap-level average/max HR and step counts.
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

            # TCX schema order: Id → Lap(s) → Notes → Creator
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

            # Intensity
            intensity_elem = SubElement(lap_elem, "Intensity")
            intensity_elem.text = "Active"

            # Lap Extensions — valid fields per ActivityExtensionv2.xsd:
            # AvgSpeed (m/s), MaxBikeCadence, AvgRunCadence, MaxRunCadence, Steps, AvgWatts, MaxWatts
            # HeartRateZone is NOT a valid LX child element and must not be used here.
            lx_fields = {}
            if activity.speed:
                # Fitbit exports speed in km/h; TCX AvgSpeed must be in m/s
                lx_fields["AvgSpeed"] = round(activity.speed / 3.6, 4)
            if activity.steps and activity.activity_type.value in ("running", "walking", "treadmill", "hiking"):
                lx_fields["Steps"] = activity.steps

            if lx_fields:
                extensions_elem = SubElement(lap_elem, "Extensions")
                lx_elem = SubElement(extensions_elem, "LX")
                lx_elem.set(
                    "xmlns", "http://www.garmin.com/xmlschemas/ActivityExtension/v2"
                )
                for field_name, field_value in lx_fields.items():
                    field_elem = SubElement(lx_elem, field_name)
                    field_elem.text = str(field_value)

            # Track (for GPS data or time-based data)
            track_elem = SubElement(lap_elem, "Track")

            # If we have GPS data, add trackpoints
            if activity.gps_data:
                self._add_gps_trackpoints(track_elem, activity)
            else:
                # Create basic trackpoints for time-based data
                self._add_time_trackpoints(track_elem, activity)

            # Notes element must come after all Lap elements per TCX schema
            garmin_sport = self._garmin_sport_name(activity.activity_type)
            notes_parts = [
                f"Fitbit Activity: {activity.activity_name}",
                f"Garmin Sport: {garmin_sport}",
                f"Log ID: {activity.log_id}",
            ]
            # Warn when TCX must show "Other" but FIT has a proper sport type
            tcx_sport = self._map_activity_type_to_tcx(activity.activity_type)
            if tcx_sport == "Other" and garmin_sport != "Generic":
                notes_parts.append("NOTE: Import .fit file to get correct sport type in Garmin Connect")
            if activity.activity_type_id:
                notes_parts.append(f"Fitbit Type ID: {activity.activity_type_id}")
            if (
                activity.original_activity_name
                and activity.original_activity_name != activity.activity_name
            ):
                notes_parts.append(f"Original: {activity.original_activity_name}")
            notes_elem = SubElement(activity_elem, "Notes")
            notes_elem.text = " | ".join(notes_parts)

            # Creator element (last per TCX schema)
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

            # Add the recorded average heart rate (do not fabricate variation)
            if activity.average_heart_rate:
                hr_elem = SubElement(trackpoint, "HeartRateBpm")
                hr_value = SubElement(hr_elem, "Value")
                hr_value.text = str(activity.average_heart_rate)

    def _map_activity_type_to_tcx(self, activity_type) -> str:
        """Map our activity type to TCX sport type with comprehensive Garmin Connect compatibility."""
        # TCX schema only officially supports Running, Biking, Other;
        # Garmin Connect also accepts Walking and Swimming in practice.
        # All other sports must be "Other" in TCX — use FIT files for correct sport types.
        mapping = {
            "running": "Running",
            "walking": "Walking",
            "biking": "Biking",
            "indoor_cycling": "Biking",
            "hiking": "Walking",
            "swimming": "Swimming",
            "treadmill": "Running",
            "elliptical": "Other",
            "rowing": "Other",
            "workout": "Other",
            "yoga": "Other",
            "pilates": "Other",
            "weights": "Other",
            "abs": "Other",
            "crossfit": "Other",
            "hiit": "Other",
            "aerobic": "Other",
            "dance": "Other",
            "martial_arts": "Other",
            "boxing": "Other",
            "climbing": "Other",
            "stair_climbing": "Other",
            "paddle_sports": "Other",
            "sport": "Other",
            "tennis": "Other",
            "basketball": "Other",
            "soccer": "Other",
            "football": "Other",
            "volleyball": "Other",
            "golf": "Other",
            "skiing": "Other",
            "snowboarding": "Other",
            "other": "Other",
        }

        return mapping.get(activity_type.value, "Other")

    def _garmin_sport_name(self, activity_type) -> str:
        """Return a human-readable Garmin sport name for the given activity type."""
        mapping = {
            "running": "Running",
            "walking": "Walking",
            "biking": "Cycling - Road",
            "indoor_cycling": "Cycling - Indoor",
            "hiking": "Hiking",
            "swimming": "Swimming - Lap",
            "treadmill": "Running - Treadmill",
            "elliptical": "Elliptical",
            "rowing": "Rowing",
            "workout": "Training - Cardio",
            "yoga": "Training - Yoga",
            "pilates": "Training - Pilates",
            "weights": "Training - Strength",
            "abs": "Training - Strength",
            "crossfit": "Training - Strength",
            "hiit": "Training - Cardio",
            "aerobic": "Training - Cardio",
            "dance": "Training - Cardio",
            "martial_arts": "Training - Cardio",
            "boxing": "Boxing",
            "climbing": "Rock Climbing",
            "stair_climbing": "Stair Climbing",
            "paddle_sports": "Paddling",
            "tennis": "Tennis",
            "basketball": "Basketball",
            "soccer": "Soccer",
            "football": "American Football",
            "volleyball": "Volleyball",
            "golf": "Golf",
            "skiing": "Alpine Skiing",
            "snowboarding": "Snowboarding",
            "sport": "Generic Sport",
            "other": "Generic",
        }
        return mapping.get(activity_type.value, "Generic")

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

    def _compute_gps_stats(self, gps_data: list) -> dict:
        """Compute aggregate stats from a GPS trackpoint list.

        Returns a dict that may contain:
          start_lat, start_lon       — first trackpoint position
          nec_lat/long, swc_lat/long — bounding box (NE and SW corners)
          avg_altitude, max_altitude, min_altitude
          total_descent              — sum of elevation drops (metres)
          max_speed, avg_speed       — m/s from speed field when present
        """
        lats, lons, alts, speeds = [], [], [], []
        for p in gps_data:
            if not isinstance(p, dict):
                continue
            if "latitude" in p:
                lats.append(p["latitude"])
            if "longitude" in p:
                lons.append(p["longitude"])
            if "altitude" in p:
                alts.append(float(p["altitude"]))
            if "speed" in p:
                speeds.append(float(p["speed"]))

        stats = {}
        if lats and lons:
            stats["start_lat"] = lats[0]
            stats["start_lon"] = lons[0]
            stats["nec_lat"] = max(lats)
            stats["nec_long"] = max(lons)
            stats["swc_lat"] = min(lats)
            stats["swc_long"] = min(lons)

        if alts:
            stats["avg_altitude"] = sum(alts) / len(alts)
            stats["max_altitude"] = max(alts)
            stats["min_altitude"] = min(alts)
            total_descent = sum(
                alts[i - 1] - alts[i]
                for i in range(1, len(alts))
                if alts[i - 1] > alts[i]
            )
            if total_descent > 0:
                stats["total_descent"] = total_descent

        if speeds:
            stats["max_speed"] = max(speeds)
            stats["avg_speed"] = sum(speeds) / len(speeds)

        return stats

    def _apply_gps_stats_to_message(self, msg, gps_stats: dict, avg_speed_fallback=None):
        """Apply GPS-derived stats to a Session or Lap message."""
        if gps_stats.get("start_lat") is not None:
            msg.start_position_lat = gps_stats["start_lat"]
            msg.start_position_long = gps_stats["start_lon"]
        if gps_stats.get("avg_altitude") is not None:
            msg.avg_altitude = gps_stats["avg_altitude"]
            msg.max_altitude = gps_stats["max_altitude"]
            msg.min_altitude = gps_stats["min_altitude"]
        if gps_stats.get("total_descent") is not None:
            msg.total_descent = gps_stats["total_descent"]
        if gps_stats.get("max_speed") is not None:
            msg.max_speed = gps_stats["max_speed"]
        # avg_speed: prefer Fitbit-provided value, fall back to GPS-computed
        if avg_speed_fallback is not None:
            msg.avg_speed = avg_speed_fallback
        elif gps_stats.get("avg_speed") is not None:
            msg.avg_speed = gps_stats["avg_speed"]

    def _generate_fit_file(self, activity: ActivityData) -> Optional[str]:
        """Generate a FIT file for a single activity with all available data."""
        try:
            from fit_tool.fit_file_builder import FitFileBuilder
            from fit_tool.profile.messages.activity_message import ActivityMessage
            from fit_tool.profile.messages.event_message import EventMessage
            from fit_tool.profile.messages.lap_message import LapMessage
            from fit_tool.profile.messages.session_message import SessionMessage
            from fit_tool.profile.messages.file_id_message import FileIdMessage
            from fit_tool.profile.messages.sport_message import SportMessage
            from fit_tool.profile.profile_type import (
                Sport,
                SubSport,
                FileType,
                Manufacturer,
                Event,
                EventType,
            )

            builder = FitFileBuilder()
            builder.auto_define = True

            # fit-tool expects ALL timestamps as Unix milliseconds.
            start_ms = int(activity.start_time.timestamp() * 1000)
            end_ms = start_ms + activity.duration_ms
            timer_time = (
                activity.active_duration / 1000
                if activity.active_duration
                else activity.duration_ms / 1000
            )

            # ── FileId ──────────────────────────────────────────────────────
            file_id = FileIdMessage()
            file_id.type = FileType.ACTIVITY
            file_id.manufacturer = Manufacturer.DEVELOPMENT
            file_id.product = 1
            file_id.time_created = start_ms
            builder.add(file_id)

            # ── Sport (explicit sport/sub-sport declaration) ─────────────────
            sport, sub_sport = self._map_activity_to_fit_sport(activity.activity_type)
            sport_msg = SportMessage()
            sport_msg.sport = sport
            sport_msg.sub_sport = sub_sport
            sport_msg.sport_name = self._garmin_sport_name(activity.activity_type)
            builder.add(sport_msg)

            # ── Timer-start event ────────────────────────────────────────────
            start_evt = EventMessage()
            start_evt.timestamp = start_ms
            start_evt.event = Event.TIMER
            start_evt.event_type = EventType.START
            builder.add(start_evt)

            # ── Pre-compute GPS stats (used by both Session and Lap) ──────────
            has_gps = bool(activity.gps_data and isinstance(activity.gps_data, list))
            gps_stats = self._compute_gps_stats(activity.gps_data) if has_gps else {}

            # avg_speed from Fitbit field (km/h → m/s), or derived from distance/time
            if activity.speed:
                avg_speed_ms = activity.speed / 3.6
            elif activity.distance and activity.duration_ms:
                avg_speed_ms = (activity.distance * 1000) / (activity.duration_ms / 1000)
            else:
                avg_speed_ms = None

            # Running/walking cadence from step count
            avg_cadence = None
            total_strides = None
            if activity.steps and activity.duration_ms:
                duration_min = activity.duration_ms / 60000
                if activity.activity_type.value in ("running", "treadmill", "walking", "hiking"):
                    avg_cadence = int(activity.steps / duration_min / 2)  # strides/min
                    total_strides = activity.steps // 2

            # HR zones
            zones_to_use = (
                activity.recalculated_hr_zones or activity.heart_rate_zones
            )
            zone_times = None
            if zones_to_use:
                zt = [z.minutes * 60 for z in zones_to_use]
                while len(zt) < 5:
                    zt.append(0)
                zone_times = zt[:5]

            # ── Record messages (GPS or time-based) — MUST precede Lap/Session ──
            if has_gps:
                self._add_fit_trackpoints(builder, activity)
            else:
                self._add_fit_time_records(builder, activity)

            # ── Timer-stop event ─────────────────────────────────────────────
            stop_evt = EventMessage()
            stop_evt.timestamp = end_ms
            stop_evt.event = Event.TIMER
            stop_evt.event_type = EventType.STOP_ALL
            builder.add(stop_evt)

            # ── Lap ──────────────────────────────────────────────────────────
            lap = LapMessage()
            lap.sport = sport
            lap.sub_sport = sub_sport
            lap.start_time = start_ms
            lap.timestamp = end_ms
            lap.total_elapsed_time = activity.duration_ms / 1000
            lap.total_timer_time = timer_time
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
            if avg_cadence is not None:
                lap.avg_running_cadence = avg_cadence
            if total_strides is not None:
                lap.total_strides = total_strides
            if zone_times:
                lap.time_in_hr_zone = zone_times
            self._apply_gps_stats_to_message(lap, gps_stats, avg_speed_ms)
            builder.add(lap)

            # ── Session ──────────────────────────────────────────────────────
            session = SessionMessage()
            session.sport = sport
            session.sub_sport = sub_sport
            session.start_time = start_ms
            session.timestamp = end_ms
            session.total_elapsed_time = activity.duration_ms / 1000
            session.total_timer_time = timer_time
            session.num_laps = 1
            if activity.distance:
                session.total_distance = activity.distance * 1000
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
            if avg_cadence is not None:
                session.avg_running_cadence = avg_cadence
            if total_strides is not None:
                session.total_strides = total_strides
            if zone_times:
                session.time_in_hr_zone = zone_times
            if gps_stats.get("nec_lat") is not None:
                session.nec_lat = gps_stats["nec_lat"]
                session.nec_long = gps_stats["nec_long"]
                session.swc_lat = gps_stats["swc_lat"]
                session.swc_long = gps_stats["swc_long"]
            self._apply_gps_stats_to_message(session, gps_stats, avg_speed_ms)
            builder.add(session)

            # ── Activity ─────────────────────────────────────────────────────
            activity_msg = ActivityMessage()
            activity_msg.timestamp = end_ms
            activity_msg.num_sessions = 1
            activity_msg.type = 0        # Manual
            activity_msg.event = 26      # Activity
            activity_msg.local_timestamp = end_ms // 1000  # Unix seconds
            builder.add(activity_msg)

            # ── Write file ───────────────────────────────────────────────────
            activity_type_name = activity.activity_type.value.replace("_", "-")
            filename = f"{activity_type_name}_{activity.log_id}_{activity.start_time.strftime('%Y%m%d_%H%M%S')}.fit"
            filepath = self.output_dir / filename

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

            # Comprehensive mapping for FIT sports.
            # Note: Sport.YOGA, Sport.STRENGTH_TRAINING, Sport.DANCING,
            # Sport.MARTIAL_ARTS, and SubSport.CROSS_TRAINING do NOT exist in
            # fit-tool — use Training sport with appropriate sub-sport instead.
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
                "yoga": (Sport.TRAINING, SubSport.YOGA),
                "pilates": (Sport.TRAINING, SubSport.PILATES),
                "weights": (Sport.TRAINING, SubSport.STRENGTH_TRAINING),
                "abs": (Sport.TRAINING, SubSport.STRENGTH_TRAINING),
                "crossfit": (Sport.TRAINING, SubSport.STRENGTH_TRAINING),
                "hiit": (Sport.TRAINING, SubSport.CARDIO_TRAINING),
                "aerobic": (Sport.TRAINING, SubSport.CARDIO_TRAINING),
                "dance": (Sport.TRAINING, SubSport.CARDIO_TRAINING),
                "martial_arts": (Sport.TRAINING, SubSport.CARDIO_TRAINING),
                "indoor_cycling": (Sport.CYCLING, SubSport.INDOOR_CYCLING),
                "stair_climbing": (Sport.FITNESS_EQUIPMENT, SubSport.STAIR_CLIMBING),
                "paddle_sports": (Sport.PADDLING, SubSport.GENERIC),
                "tennis": (Sport.TENNIS, SubSport.GENERIC),
                "basketball": (Sport.BASKETBALL, SubSport.GENERIC),
                "soccer": (Sport.SOCCER, SubSport.GENERIC),
                "football": (Sport.AMERICAN_FOOTBALL, SubSport.GENERIC),
                "volleyball": (Sport.GENERIC, SubSport.MATCH),
                "golf": (Sport.GOLF, SubSport.GENERIC),
                "skiing": (Sport.ALPINE_SKIING, SubSport.GENERIC),
                "snowboarding": (Sport.SNOWBOARDING, SubSport.GENERIC),
                "boxing": (Sport.BOXING, SubSport.GENERIC),
                "climbing": (Sport.ROCK_CLIMBING, SubSport.GENERIC),
                "sport": (Sport.GENERIC, SubSport.GENERIC),
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

                # fit-tool expects Unix milliseconds for all timestamps
                if "time" in gps_point:
                    try:
                        from dateutil.parser import parse as _parse_dt
                        point_ms = int(_parse_dt(gps_point["time"]).timestamp() * 1000)
                    except Exception:
                        point_time = activity.start_time + timedelta(seconds=i * time_interval)
                        point_ms = int(point_time.timestamp() * 1000)
                else:
                    point_time = activity.start_time + timedelta(seconds=i * time_interval)
                    point_ms = int(point_time.timestamp() * 1000)
                record.timestamp = point_ms

                # fit-tool expects degrees (it handles the semicircle conversion internally).
                # Do NOT pre-convert to semicircles — that causes a double conversion
                # and overflows the 32-bit signed int range.
                if "latitude" in gps_point and "longitude" in gps_point:
                    record.position_lat = gps_point["latitude"]
                    record.position_long = gps_point["longitude"]

                # Altitude
                if "altitude" in gps_point:
                    record.altitude = gps_point["altitude"]

                # Distance (cumulative, already in meters — from TCX <DistanceMeters>
                # or from the Haversine accumulator in _parse_gps_data)
                if "distance" in gps_point:
                    cumulative_distance = gps_point["distance"]
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

                # Only use actual recorded heart rate — never fabricate values
                if "heart_rate" in gps_point:
                    record.heart_rate = gps_point["heart_rate"]

                builder.add(record)

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

                # fit-tool expects Unix milliseconds
                point_time = activity.start_time + timedelta(seconds=i * time_interval)
                record.timestamp = int(point_time.timestamp() * 1000)

                # Distance (if available, spread over time)
                if activity.distance:
                    record.distance = (
                        activity.distance * 1000 * i
                    ) / num_records  # Convert km to meters

                # Only use actual recorded average heart rate — do not fabricate variation
                if activity.average_heart_rate:
                    record.heart_rate = activity.average_heart_rate

                # Steps (for walking/running activities)
                if activity.steps:
                    record.cadence = int(
                        (activity.steps / 2) / (duration_seconds / 60)
                    )  # Steps per minute / 2

                builder.add(record)

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
