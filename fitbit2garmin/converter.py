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

    def __init__(self, output_dir: Union[str, Path], hr_data_dir: Optional[Union[str, Path]] = None):
        """Initialize converter with output directory and optional HR data directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.hr_data_dir = Path(hr_data_dir) if hr_data_dir else None
        # Cache loaded HR data by date string to avoid re-reading the same file
        self._hr_cache: Dict[str, List] = {}

        logger.info(
            f"Initialized data converter with output directory: {self.output_dir}"
        )
        if self.hr_data_dir:
            logger.info(f"HR data directory: {self.hr_data_dir}")

    def _load_day_hr_data(self, date_str: str) -> List:
        """Load HR readings for a specific date from Fitbit's heart_rate-YYYY-MM-DD.json.

        Returns a list of (unix_ms, bpm) tuples sorted by timestamp.
        Results are cached by date to avoid re-reading the same file for
        multiple activities on the same day.
        """
        if date_str in self._hr_cache:
            return self._hr_cache[date_str]

        if not self.hr_data_dir or not self.hr_data_dir.exists():
            self._hr_cache[date_str] = []
            return []

        # Try the canonical file name first, then glob for variants
        candidate = self.hr_data_dir / f"heart_rate-{date_str}.json"
        if not candidate.exists():
            matches = sorted(self.hr_data_dir.glob(f"heart_rate-{date_str}*.json"))
            if not matches:
                self._hr_cache[date_str] = []
                return []
            candidate = matches[0]

        readings = []
        try:
            try:
                import orjson
                with open(candidate, "rb") as f:
                    data = orjson.loads(f.read())
            except Exception:
                import json as _json
                with open(candidate, "r", encoding="utf-8") as f:
                    data = _json.load(f)

            for record in data:
                if not isinstance(record, dict):
                    continue
                dt_str = record.get("dateTime", "")
                value = record.get("value", {})
                bpm = value.get("bpm", 0) if isinstance(value, dict) else 0
                if not dt_str or not bpm:
                    continue
                try:
                    # Fitbit HR format: "03/10/22 16:44:00"
                    dt = datetime.strptime(dt_str, "%m/%d/%y %H:%M:%S")
                    readings.append((int(dt.timestamp() * 1000), int(bpm)))
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"Could not load HR data for {date_str}: {e}")

        readings.sort(key=lambda x: x[0])
        self._hr_cache[date_str] = readings
        return readings

    def _get_activity_hr_data(self, activity: ActivityData) -> List:
        """Return HR readings within the activity's time window.

        Returns sorted list of (unix_ms, bpm) tuples.
        """
        start_ms = int(activity.start_time.timestamp() * 1000)
        end_ms = start_ms + activity.duration_ms

        date_str = activity.start_time.strftime("%Y-%m-%d")
        readings = list(self._load_day_hr_data(date_str))

        # If activity crosses midnight load the next day too
        from datetime import timedelta
        end_dt = activity.start_time + timedelta(milliseconds=activity.duration_ms)
        end_date_str = end_dt.strftime("%Y-%m-%d")
        if end_date_str != date_str:
            readings = readings + list(self._load_day_hr_data(end_date_str))

        return [(ts, bpm) for ts, bpm in readings if start_ms <= ts <= end_ms]

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
        lats, lons, alts, speeds, distances = [], [], [], [], []
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
            if "distance" in p:
                distances.append(float(p["distance"]))

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
            total_ascent = sum(
                alts[i] - alts[i - 1]
                for i in range(1, len(alts))
                if alts[i] > alts[i - 1]
            )
            if total_ascent > 0:
                stats["total_ascent"] = total_ascent
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

        # Total distance: take the maximum cumulative distance value from GPS points
        # (GPS points have a cumulative "distance" field in metres from TCX parsing)
        if distances:
            gps_total = max(distances)
            if gps_total > 0:
                stats["total_distance"] = gps_total

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
        if gps_stats.get("total_ascent") is not None:
            msg.total_ascent = gps_stats["total_ascent"]
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
            file_id.product_name = "Fitbit"
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
            start_evt.data = 0
            builder.add(start_evt)

            # ── Pre-compute GPS stats (used by both Session and Lap) ──────────
            has_gps = bool(activity.gps_data and isinstance(activity.gps_data, list))
            gps_stats = self._compute_gps_stats(activity.gps_data) if has_gps else {}

            # avg_speed from Fitbit field (km/h → m/s), or derived from distance/time
            if activity.speed:
                avg_speed_ms = activity.speed / 3.6
            elif activity.distance is not None and activity.distance > 0 and activity.duration_ms:
                avg_speed_ms = (activity.distance * 1000) / (activity.duration_ms / 1000)
            elif gps_stats.get("total_distance") and activity.duration_ms:
                avg_speed_ms = gps_stats["total_distance"] / (activity.duration_ms / 1000)
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
            stop_evt.data = 0
            builder.add(stop_evt)

            # ── Lap ──────────────────────────────────────────────────────────
            # Effective total distance: prefer Fitbit activity field; fall back to GPS-derived
            if activity.distance is not None and activity.distance > 0:
                effective_distance_m = activity.distance * 1000  # km → m
            elif gps_stats.get("total_distance"):
                effective_distance_m = gps_stats["total_distance"]  # already metres
            else:
                effective_distance_m = None

            lap = LapMessage()
            lap.sport = sport
            lap.sub_sport = sub_sport
            lap.start_time = start_ms
            lap.timestamp = end_ms
            lap.total_elapsed_time = activity.duration_ms / 1000
            lap.total_timer_time = timer_time
            # Effective elevation: prefer Fitbit JSON (barometric altimeter, accurate)
            # over GPS-computed (noisy). Fall back to GPS if JSON doesn't have it.
            effective_ascent_m: Optional[float] = None
            if activity.elevation_gain is not None and activity.elevation_gain > 0:
                effective_ascent_m = float(activity.elevation_gain)  # metres (Fitbit stores in m)
            elif gps_stats.get("total_ascent"):
                effective_ascent_m = gps_stats["total_ascent"]

            if effective_distance_m is not None:
                lap.total_distance = effective_distance_m
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
            if effective_ascent_m is not None:
                lap.total_ascent = effective_ascent_m
            if avg_cadence is not None:
                lap.avg_running_cadence = avg_cadence
            if total_strides is not None:
                lap.total_strides = total_strides
            if zone_times:
                lap.time_in_hr_zone = zone_times
            # GPS-derived stats (avg/max speed, altitude, descent, start position)
            # Pass None for total_ascent so _apply_gps_stats doesn't overwrite our choice
            self._apply_gps_stats_to_message(lap, gps_stats, avg_speed_ms)
            if effective_ascent_m is not None:
                lap.total_ascent = effective_ascent_m  # Re-apply — _apply_gps_stats may override
            # Add GPS start/end positions to lap
            if gps_stats.get("start_lat") is not None:
                lap.start_position_lat = gps_stats["start_lat"]
                lap.start_position_long = gps_stats["start_lon"]
            if has_gps and activity.gps_data:
                last_pt = next(
                    (p for p in reversed(activity.gps_data)
                     if isinstance(p, dict) and "latitude" in p and "longitude" in p),
                    None,
                )
                if last_pt:
                    lap.end_position_lat = last_pt["latitude"]
                    lap.end_position_long = last_pt["longitude"]
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
            if effective_distance_m is not None:
                session.total_distance = effective_distance_m
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
            if effective_ascent_m is not None:
                session.total_ascent = effective_ascent_m
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
            if effective_ascent_m is not None:
                session.total_ascent = effective_ascent_m  # Re-apply after GPS stats
            builder.add(session)

            # ── Activity ─────────────────────────────────────────────────────
            activity_msg = ActivityMessage()
            activity_msg.timestamp = end_ms
            activity_msg.num_sessions = 1
            activity_msg.total_timer_time = timer_time
            activity_msg.type = 0              # Manual
            activity_msg.event = Event.ACTIVITY
            activity_msg.event_type = EventType.STOP
            # local_timestamp must be Unix seconds (fit-tool does not auto-convert this field)
            activity_msg.local_timestamp = end_ms // 1000
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
        """Add GPS trackpoints to FIT file, enriched with intraday HR when available."""
        try:
            from fit_tool.profile.messages.record_message import RecordMessage
            from datetime import timedelta

            if not activity.gps_data:
                return

            # Pre-load intraday HR data for this activity (keyed by unix_ms)
            intraday_hr = self._get_activity_hr_data(activity)
            # Build a lookup: for each GPS point, find the nearest HR reading within ±30s
            hr_lookup: Dict[int, int] = {ts: bpm for ts, bpm in intraday_hr}

            def _nearest_hr(point_ms: int) -> Optional[int]:
                """Find the nearest intraday HR reading within ±30 000 ms."""
                if not hr_lookup:
                    return None
                best_ts = min(hr_lookup, key=lambda t: abs(t - point_ms))
                if abs(best_ts - point_ms) <= 30000:
                    return hr_lookup[best_ts]
                return None

            # Calculate time intervals between points
            duration_seconds = activity.duration_ms / 1000
            num_points = len(activity.gps_data)
            time_interval = duration_seconds / num_points if num_points > 0 else 1

            cumulative_distance = 0

            for i, gps_point in enumerate(activity.gps_data):
                if not isinstance(gps_point, dict):
                    continue

                record = RecordMessage()

                # fit-tool expects Unix milliseconds for all timestamps.
                # If the GPS time string has no timezone (naive), fall back to
                # offset-based timing so records stay within the session window.
                point_ms = None
                if "time" in gps_point:
                    try:
                        from dateutil.parser import parse as _parse_dt
                        parsed_t = _parse_dt(gps_point["time"])
                        if parsed_t.tzinfo is not None:
                            # Timezone-aware: safe to use directly
                            point_ms = int(parsed_t.timestamp() * 1000)
                        else:
                            # Timezone-naive: assume UTC (Fitbit TCX uses UTC with Z)
                            from datetime import timezone as _tz
                            point_ms = int(
                                parsed_t.replace(tzinfo=_tz.utc).timestamp() * 1000
                            )
                    except Exception:
                        pass
                if point_ms is None:
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

                # Heart rate: prefer embedded GPS point HR, then intraday HR lookup
                if "heart_rate" in gps_point:
                    record.heart_rate = gps_point["heart_rate"]
                else:
                    intraday_bpm = _nearest_hr(point_ms)
                    if intraday_bpm:
                        record.heart_rate = intraday_bpm

                builder.add(record)

        except Exception as e:
            logger.warning(f"Error adding FIT trackpoints: {e}")

    def _add_fit_time_records(self, builder, activity: ActivityData):
        """Add time-based records for non-GPS activities, using intraday HR when available."""
        try:
            from fit_tool.profile.messages.record_message import RecordMessage
            from datetime import timedelta

            duration_seconds = activity.duration_ms / 1000
            total_distance_m = (activity.distance or 0) * 1000  # km → m

            # Load intraday HR for this activity
            intraday_hr = self._get_activity_hr_data(activity)

            if intraday_hr:
                # We have per-second HR data — emit one record per HR sample (≤600 records)
                # Limit to every 6th sample when very dense (Fitbit records every ~5s)
                step = max(1, len(intraday_hr) // 600)
                samples = intraday_hr[::step]
                start_ms = int(activity.start_time.timestamp() * 1000)
                end_ms = start_ms + activity.duration_ms

                for ts_ms, bpm in samples:
                    record = RecordMessage()
                    record.timestamp = ts_ms
                    record.heart_rate = bpm

                    # Interpolate distance linearly over time
                    if total_distance_m > 0 and end_ms > start_ms:
                        frac = (ts_ms - start_ms) / (end_ms - start_ms)
                        record.distance = max(0.0, min(total_distance_m, frac * total_distance_m))

                    builder.add(record)
            else:
                # No intraday HR available — create one record per minute
                num_records = min(100, max(10, int(duration_seconds / 60)))
                time_interval = duration_seconds / num_records

                for i in range(num_records):
                    record = RecordMessage()
                    point_time = activity.start_time + timedelta(seconds=i * time_interval)
                    record.timestamp = int(point_time.timestamp() * 1000)

                    # Spread distance linearly
                    if total_distance_m > 0:
                        record.distance = total_distance_m * i / num_records

                    # Use average HR as a constant fallback
                    if activity.average_heart_rate:
                        record.heart_rate = activity.average_heart_rate

                    # Running/walking cadence
                    if activity.steps:
                        record.cadence = int(
                            (activity.steps / 2) / (duration_seconds / 60)
                        )

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

    def convert_sleep_to_fit(self, sleep_data: List) -> Optional[str]:
        """Generate sleep.fit from SleepData records (MONITORING_B file).

        Each sleep stage segment becomes a MonitoringMessage with:
          activity_type=SEDENTARY, activity_level encoding the stage.
        Returns output path or None.
        """
        if not sleep_data:
            return None

        try:
            from fit_tool.fit_file_builder import FitFileBuilder
            from fit_tool.profile.messages.file_id_message import FileIdMessage
            from fit_tool.profile.messages.monitoring_info_message import MonitoringInfoMessage
            from fit_tool.profile.messages.monitoring_message import MonitoringMessage
            from fit_tool.profile.profile_type import (
                FileType, Manufacturer, ActivityType as FitActivityType, ActivityLevel,
            )
        except ImportError:
            logger.warning("fit-tool not available; skipping sleep FIT export")
            return None

        from datetime import timezone as _tz

        FIT_EPOCH_MS = 631065600000

        # Map Fitbit stage names → ActivityLevel
        STAGE_TO_LEVEL = {
            "deep":    ActivityLevel.LOW,
            "light":   ActivityLevel.MEDIUM,
            "rem":     ActivityLevel.MEDIUM,
            "wake":    ActivityLevel.HIGH,
            "awake":   ActivityLevel.HIGH,
            "asleep":  ActivityLevel.LOW,
            "restless": ActivityLevel.MEDIUM,
        }

        builder = FitFileBuilder(auto_define=True, min_string_size=50)
        now_ms = int(datetime.now(tz=_tz.utc).timestamp() * 1000)

        file_id = FileIdMessage()
        file_id.type = FileType.MONITORING_B
        file_id.manufacturer = Manufacturer.DEVELOPMENT
        file_id.time_created = now_ms
        builder.add(file_id)

        info = MonitoringInfoMessage()
        info.timestamp = now_ms
        info.local_timestamp = (now_ms - FIT_EPOCH_MS) // 1000
        info.activity_type = [FitActivityType.SEDENTARY]
        builder.add(info)

        added = skipped = 0

        for entry in sorted(sleep_data, key=lambda e: e.start_time):
            if entry.duration_ms <= 0:
                skipped += 1
                continue

            stages = entry.sleep_stages  # list of {dateTime, level, seconds}

            if stages:
                for seg in stages:
                    raw_dt = seg.get("dateTime", "")
                    level_str = seg.get("level", "").lower()
                    seg_secs = int(seg.get("seconds", 0))
                    if not raw_dt or seg_secs <= 0:
                        continue

                    try:
                        seg_start = datetime.fromisoformat(raw_dt)
                        if seg_start.tzinfo is None:
                            seg_start = seg_start.replace(tzinfo=_tz.utc)
                    except Exception:
                        continue

                    seg_end_ms = int((seg_start.timestamp() + seg_secs) * 1000)

                    msg = MonitoringMessage()
                    msg.timestamp = seg_end_ms
                    msg.activity_type = FitActivityType.SEDENTARY
                    msg.active_time = float(seg_secs)
                    act_level = STAGE_TO_LEVEL.get(level_str, ActivityLevel.MEDIUM)
                    msg.activity_level = act_level
                    if entry.heart_rate_avg:
                        msg.heart_rate = int(entry.heart_rate_avg)
                    builder.add(msg)
                    added += 1
            else:
                # No stage detail — write one summary record for the whole session
                end_ms = int(entry.end_time.timestamp() * 1000)
                if entry.end_time.tzinfo is None:
                    end_ms = int(entry.end_time.replace(tzinfo=_tz.utc).timestamp() * 1000)

                msg = MonitoringMessage()
                msg.timestamp = end_ms
                msg.activity_type = FitActivityType.SEDENTARY
                msg.active_time = float(entry.duration_ms / 1000)
                msg.activity_level = ActivityLevel.LOW
                if entry.heart_rate_avg:
                    msg.heart_rate = int(entry.heart_rate_avg)
                builder.add(msg)
                added += 1

        if added == 0:
            logger.warning("No sleep records had usable data; skipping sleep.fit")
            return None

        out_path = self.output_dir / "sleep.fit"
        builder.build().to_file(str(out_path))
        logger.info(f"Wrote {added} sleep stage records to {out_path} (skipped {skipped})")
        return str(out_path)

    def convert_spo2_to_fit(self, spo2_data: List) -> Optional[str]:
        """Generate spo2.fit from SpO2Data records (ACTIVITY file).

        Each daily SpO2 reading becomes a RecordMessage with
        saturated_hemoglobin_percent at noon UTC.
        Returns output path or None.
        """
        if not spo2_data:
            return None

        try:
            from fit_tool.fit_file_builder import FitFileBuilder
            from fit_tool.profile.messages.file_id_message import FileIdMessage
            from fit_tool.profile.messages.event_message import EventMessage
            from fit_tool.profile.messages.record_message import RecordMessage
            from fit_tool.profile.messages.lap_message import LapMessage
            from fit_tool.profile.messages.session_message import SessionMessage
            from fit_tool.profile.messages.activity_message import ActivityMessage
            from fit_tool.profile.profile_type import (
                FileType, Manufacturer, Event, EventType, Sport, SubSport,
            )
        except ImportError:
            logger.warning("fit-tool not available; skipping spo2 FIT export")
            return None

        from datetime import timezone as _tz

        valid = [e for e in spo2_data if e.spo2_percentage is not None]
        if not valid:
            return None

        builder = FitFileBuilder(auto_define=True, min_string_size=50)
        now_ms = int(datetime.now(tz=_tz.utc).timestamp() * 1000)

        file_id = FileIdMessage()
        file_id.type = FileType.ACTIVITY
        file_id.manufacturer = Manufacturer.DEVELOPMENT
        file_id.time_created = now_ms
        builder.add(file_id)

        for entry in sorted(valid, key=lambda e: e.date):
            if entry.timestamp is not None:
                ts_dt = entry.timestamp
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=_tz.utc)
                ts_ms = int(ts_dt.timestamp() * 1000)
            else:
                ts_ms = int(
                    datetime(entry.date.year, entry.date.month, entry.date.day,
                             12, 0, 0, tzinfo=_tz.utc).timestamp() * 1000
                )

            day_start_ms = int(
                datetime(entry.date.year, entry.date.month, entry.date.day,
                         0, 0, 0, tzinfo=_tz.utc).timestamp() * 1000
            )

            ev_start = EventMessage()
            ev_start.timestamp = day_start_ms
            ev_start.event = Event.TIMER
            ev_start.event_type = EventType.START
            builder.add(ev_start)

            rec = RecordMessage()
            rec.timestamp = ts_ms
            rec.saturated_hemoglobin_percent = float(entry.spo2_percentage)
            builder.add(rec)

            ev_stop = EventMessage()
            ev_stop.timestamp = ts_ms + 1000
            ev_stop.event = Event.TIMER
            ev_stop.event_type = EventType.STOP_ALL
            builder.add(ev_stop)

            lap = LapMessage()
            lap.timestamp = ts_ms
            lap.start_time = day_start_ms
            lap.total_elapsed_time = (ts_ms - day_start_ms) / 1000.0
            builder.add(lap)

            session = SessionMessage()
            session.timestamp = ts_ms
            session.start_time = day_start_ms
            session.sport = Sport.GENERIC
            session.sub_sport = SubSport.GENERIC
            session.total_elapsed_time = (ts_ms - day_start_ms) / 1000.0
            session.num_laps = 1
            builder.add(session)

        act = ActivityMessage()
        act.timestamp = now_ms
        act.num_sessions = len(valid)
        act.total_timer_time = 0.0
        builder.add(act)

        out_path = self.output_dir / "spo2.fit"
        builder.build().to_file(str(out_path))
        logger.info(f"Wrote {len(valid)} SpO2 records to {out_path}")
        return str(out_path)

    def convert_hrv_to_fit(self, hrv_data: List) -> Optional[str]:
        """Generate hrv.fit from HeartRateVariability records (ACTIVITY file).

        Daily RMSSD (ms) is stored in HrvMessage.time as a single-element list
        [rmssd_ms / 1000.0] (seconds), which is the FIT HRV field's native unit.
        Returns output path or None.
        """
        if not hrv_data:
            return None

        try:
            from fit_tool.fit_file_builder import FitFileBuilder
            from fit_tool.profile.messages.file_id_message import FileIdMessage
            from fit_tool.profile.messages.event_message import EventMessage
            from fit_tool.profile.messages.hrv_message import HrvMessage
            from fit_tool.profile.messages.lap_message import LapMessage
            from fit_tool.profile.messages.session_message import SessionMessage
            from fit_tool.profile.messages.activity_message import ActivityMessage
            from fit_tool.profile.profile_type import (
                FileType, Manufacturer, Event, EventType, Sport, SubSport,
            )
        except ImportError:
            logger.warning("fit-tool not available; skipping HRV FIT export")
            return None

        from datetime import timezone as _tz

        valid = [e for e in hrv_data if e.rmssd is not None and e.rmssd > 0]
        if not valid:
            return None

        builder = FitFileBuilder(auto_define=True, min_string_size=50)
        now_ms = int(datetime.now(tz=_tz.utc).timestamp() * 1000)

        file_id = FileIdMessage()
        file_id.type = FileType.ACTIVITY
        file_id.manufacturer = Manufacturer.DEVELOPMENT
        file_id.time_created = now_ms
        builder.add(file_id)

        for entry in sorted(valid, key=lambda e: e.date):
            if entry.timestamp is not None:
                ts_dt = entry.timestamp
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=_tz.utc)
                ts_ms = int(ts_dt.timestamp() * 1000)
            else:
                # Morning measurement at 06:00 UTC
                ts_ms = int(
                    datetime(entry.date.year, entry.date.month, entry.date.day,
                             6, 0, 0, tzinfo=_tz.utc).timestamp() * 1000
                )

            ev_start = EventMessage()
            ev_start.timestamp = ts_ms
            ev_start.event = Event.TIMER
            ev_start.event_type = EventType.START
            builder.add(ev_start)

            hrv_msg = HrvMessage()
            # Store RMSSD (ms) as seconds — FIT HRV time field unit is seconds
            hrv_msg.time = [entry.rmssd / 1000.0]
            builder.add(hrv_msg)

            ev_stop = EventMessage()
            ev_stop.timestamp = ts_ms + 1000
            ev_stop.event = Event.TIMER
            ev_stop.event_type = EventType.STOP_ALL
            builder.add(ev_stop)

            lap = LapMessage()
            lap.timestamp = ts_ms
            lap.start_time = ts_ms
            lap.total_elapsed_time = 1.0
            builder.add(lap)

            session = SessionMessage()
            session.timestamp = ts_ms
            session.start_time = ts_ms
            session.sport = Sport.GENERIC
            session.sub_sport = SubSport.GENERIC
            session.total_elapsed_time = 1.0
            session.num_laps = 1
            builder.add(session)

        act = ActivityMessage()
        act.timestamp = now_ms
        act.num_sessions = len(valid)
        act.total_timer_time = 0.0
        builder.add(act)

        out_path = self.output_dir / "hrv.fit"
        builder.build().to_file(str(out_path))
        logger.info(f"Wrote {len(valid)} HRV records to {out_path}")
        return str(out_path)

    def convert_body_composition_to_fit(
        self,
        body_composition: List,
    ) -> Optional[str]:
        """Generate a single weight.fit file from all BodyComposition records.

        Returns the output file path, or None if no data / fit-tool unavailable.
        """
        if not body_composition:
            return None

        try:
            from fit_tool.fit_file_builder import FitFileBuilder
            from fit_tool.profile.messages.file_id_message import FileIdMessage
            from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
            from fit_tool.profile.profile_type import FileType, Manufacturer
        except ImportError:
            logger.warning("fit-tool not available; skipping weight FIT export")
            return None

        from datetime import timezone as _tz

        builder = FitFileBuilder(auto_define=True, min_string_size=50)

        file_id = FileIdMessage()
        file_id.type = FileType.WEIGHT
        file_id.manufacturer = Manufacturer.DEVELOPMENT
        file_id.time_created = int(datetime.now(tz=_tz.utc).timestamp() * 1000)
        builder.add(file_id)

        skipped = 0
        added = 0
        for entry in sorted(body_composition, key=lambda e: e.date):
            if entry.weight is None or entry.weight <= 0:
                skipped += 1
                continue

            # Convert date to a noon-UTC timestamp so Garmin Connect
            # assigns the measurement to the correct calendar day.
            dt = datetime(
                entry.date.year, entry.date.month, entry.date.day,
                12, 0, 0, tzinfo=_tz.utc,
            )
            ts_ms = int(dt.timestamp() * 1000)

            ws = WeightScaleMessage()
            ws.timestamp = ts_ms
            ws.weight = float(entry.weight)  # kg

            if entry.body_fat_percentage is not None:
                ws.percent_fat = float(entry.body_fat_percentage)
            if entry.water_percentage is not None:
                ws.percent_hydration = float(entry.water_percentage)
            if entry.muscle_mass is not None:
                ws.muscle_mass = float(entry.muscle_mass)
            if entry.bone_mass is not None:
                ws.bone_mass = float(entry.bone_mass)

            builder.add(ws)
            added += 1

        if added == 0:
            logger.warning("No body composition records had valid weight data; skipping weight.fit")
            return None

        out_path = self.output_dir / "weight.fit"
        fit_file = builder.build()
        fit_file.to_file(str(out_path))
        logger.info(f"Wrote {added} weight records to {out_path} (skipped {skipped})")
        return str(out_path)

    def convert_daily_steps_to_fit(
        self,
        daily_metrics: List,
    ) -> Optional[str]:
        """Generate a monitoring.fit file from DailyMetrics (steps, distance, calories).

        Returns the output file path, or None if no data / fit-tool unavailable.
        """
        if not daily_metrics:
            return None

        try:
            from fit_tool.fit_file_builder import FitFileBuilder
            from fit_tool.profile.messages.file_id_message import FileIdMessage
            from fit_tool.profile.messages.monitoring_info_message import MonitoringInfoMessage
            from fit_tool.profile.messages.monitoring_message import MonitoringMessage
            from fit_tool.profile.profile_type import FileType, Manufacturer
            from fit_tool.profile.profile_type import ActivityType as FitActivityType
        except ImportError:
            logger.warning("fit-tool not available; skipping steps FIT export")
            return None

        from datetime import timezone as _tz

        FIT_EPOCH_MS = 631065600000  # 1989-12-31 00:00:00 UTC in unix ms

        builder = FitFileBuilder(auto_define=True, min_string_size=50)

        now_ms = int(datetime.now(tz=_tz.utc).timestamp() * 1000)
        file_id = FileIdMessage()
        file_id.type = FileType.MONITORING_B
        file_id.manufacturer = Manufacturer.DEVELOPMENT
        file_id.time_created = now_ms
        builder.add(file_id)

        info = MonitoringInfoMessage()
        info.timestamp = now_ms
        info.local_timestamp = (now_ms - FIT_EPOCH_MS) // 1000
        info.activity_type = [FitActivityType.WALKING]
        builder.add(info)

        skipped = 0
        added = 0
        for entry in sorted(daily_metrics, key=lambda e: e.date):
            if entry.steps is None or entry.steps == 0:
                skipped += 1
                continue

            # End-of-day timestamp (23:59:59 UTC) so Garmin assigns to the correct date
            dt = datetime(
                entry.date.year, entry.date.month, entry.date.day,
                23, 59, 59, tzinfo=_tz.utc,
            )
            ts_ms = int(dt.timestamp() * 1000)

            msg = MonitoringMessage()
            msg.timestamp = ts_ms
            msg.steps = int(entry.steps)
            msg.activity_type = FitActivityType.WALKING

            if entry.distance is not None and entry.distance > 0:
                msg.distance = float(entry.distance) * 1000.0  # km → m

            if entry.calories_burned is not None and entry.calories_burned > 0:
                msg.calories = int(entry.calories_burned)

            if entry.active_minutes is not None and entry.active_minutes > 0:
                msg.moderate_activity_minutes = int(
                    (entry.lightly_active_minutes or 0) + (entry.fairly_active_minutes or 0)
                ) or None
                msg.vigorous_activity_minutes = int(
                    entry.very_active_minutes or 0
                ) or None

            builder.add(msg)
            added += 1

        if added == 0:
            logger.warning("No daily metrics records had step data; skipping monitoring.fit")
            return None

        out_path = self.output_dir / "monitoring.fit"
        fit_file = builder.build()
        fit_file.to_file(str(out_path))
        logger.info(f"Wrote {added} daily step records to {out_path} (skipped {skipped})")
        return str(out_path)

    def batch_convert_activities(
        self,
        user_data: FitbitUserData,
        formats: Optional[List[str]] = None,
    ) -> Dict[str, List[str]]:
        """Convert all activities to the requested formats.

        formats: list of strings from {"tcx", "gpx", "fit"}.
                 Defaults to all three when None.
        """
        if formats is None:
            formats = ["tcx", "gpx", "fit"]

        logger.info(
            f"Starting batch conversion of {len(user_data.activities)} activities "
            f"(formats: {formats})"
        )

        # Report what data is available per activity
        gps_count = sum(1 for a in user_data.activities if a.gps_data)
        hr_count = sum(1 for a in user_data.activities if a.average_heart_rate)
        dist_count = sum(
            1 for a in user_data.activities
            if a.distance is not None and a.distance > 0
        )
        elev_count = sum(
            1 for a in user_data.activities
            if (a.elevation_gain is not None and a.elevation_gain > 0)
            or (a.gps_data and any("altitude" in p for p in a.gps_data if isinstance(p, dict)))
        )
        print(f"  📊 Activity data availability ({len(user_data.activities)} total):")
        print(f"     • {gps_count}/{len(user_data.activities)} have GPS tracks")
        print(f"     • {hr_count}/{len(user_data.activities)} have heart rate data")
        print(f"     • {dist_count}/{len(user_data.activities)} have distance")
        print(f"     • {elev_count}/{len(user_data.activities)} have elevation data")
        if self.hr_data_dir and self.hr_data_dir.exists():
            hr_files = list(self.hr_data_dir.glob("heart_rate*.json"))
            print(f"     • {len(hr_files)} intraday HR files for per-second HR graphs")

        # Per-type breakdown to help diagnose missing data (e.g. walking distance)
        from collections import Counter
        type_dist_missing = Counter()
        type_gps_missing = Counter()
        for a in user_data.activities:
            atype = a.activity_type.value if a.activity_type else "unknown"
            if a.distance is None or a.distance == 0:
                type_dist_missing[atype] += 1
            if not a.gps_data:
                type_gps_missing[atype] += 1
        if type_dist_missing:
            top = type_dist_missing.most_common(5)
            missing_str = ", ".join(f"{t}:{n}" for t, n in top)
            print(f"     ℹ️  Activities without distance (top types): {missing_str}")

        results = {"tcx_files": [], "gpx_files": [], "fit_files": []}

        if "tcx" in formats and user_data.activities:
            print("  🏃 Converting activities to TCX format...")
            results["tcx_files"] = self.convert_activities_to_tcx(user_data.activities)

        if "gpx" in formats:
            gps_activities = [a for a in user_data.activities if a.gps_data]
            if gps_activities:
                print(
                    f"  🗺️ Converting {len(gps_activities)} GPS activities to GPX format..."
                )
                results["gpx_files"] = self.convert_activities_to_gpx(gps_activities)

        if "fit" in formats and user_data.activities:
            print(
                f"  📁 Converting {len(user_data.activities)} activities to FIT format..."
            )
            results["fit_files"] = self.convert_activities_to_fit(user_data.activities)

        created = []
        if results["tcx_files"]:
            created.append(f"{len(results['tcx_files'])} TCX")
        if results["gpx_files"]:
            created.append(f"{len(results['gpx_files'])} GPX")
        if results["fit_files"]:
            created.append(f"{len(results['fit_files'])} FIT")

        logger.info(f"Batch conversion completed: {', '.join(created) or 'nothing'}")
        print(f"  ✅ Created: {', '.join(created) or 'no activity files'}")

        return results
