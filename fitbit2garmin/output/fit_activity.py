"""Generate one FIT ACTIVITY file per row in the canonical `activity` table.

FIT is the authoritative activity output (see project plan) -- TCX's sport field
is capped at Running/Walking/Biking/Swimming/Other, so correct sport typing for
this user's real data (Tennis, CrossFit, Elliptical, Hike, ...) requires FIT.

Carries forward the old converter.py's hard-learned encoding fixes as day-one
requirements rather than reactive patches (see git history: ab25e9e, 1861a57):
raw lat/lon passed to fit-tool as degrees (never pre-converted to semicircles),
altitude/speed clamped to their FIT UINT16-encodable ranges before assignment,
and one bad point never aborts the whole file.
"""

import logging
import math
import sqlite3
from datetime import datetime
from pathlib import Path

from fit_tool.fit_file_builder import FitFileBuilder
from fit_tool.profile.messages.activity_message import ActivityMessage
from fit_tool.profile.messages.event_message import EventMessage
from fit_tool.profile.messages.file_id_message import FileIdMessage
from fit_tool.profile.messages.lap_message import LapMessage
from fit_tool.profile.messages.record_message import RecordMessage
from fit_tool.profile.messages.session_message import SessionMessage
from fit_tool.profile.messages.sport_message import SportMessage
from fit_tool.profile.profile_type import Event, EventType, FileType, Manufacturer, Sport, SubSport

logger = logging.getLogger(__name__)

# FIT UINT16-encodable ranges (see project plan / old repo's fixed bug history).
_ALT_MIN = -499.0    # offset=500, scale=5
_SPD_MAX = 65.0      # scale=1000

# A single-activity Record stream hitting the UINT16 message-count ceiling
# (65535) is not expected for real per-activity GPS tracks, but guard it anyway
# rather than silently truncating -- see the FIT_MAX_RECORDS_PER_FILE check below.
_FIT_MAX_RECORDS_PER_FILE = 65535


def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _parse_utc_ms(ts: str) -> int:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def _sport_name(activity_name_raw: str) -> str:
    return activity_name_raw[:16]  # FIT string field length guard


def _load_gps_points(conn: sqlite3.Connection, activity_uid: str) -> list[sqlite3.Row]:
    return conn.execute(
        """SELECT gp.point_time_utc, gp.latitude, gp.longitude, gp.altitude_m,
                  gp.distance_m, gp.heart_rate
           FROM activity_gps_point agp
           JOIN gps_point gp ON gp.id = agp.gps_point_id
           WHERE agp.activity_uid = ?
           ORDER BY agp.seq""",
        (activity_uid,),
    ).fetchall()


def _add_gps_records(builder: FitFileBuilder, points: list[sqlite3.Row]) -> dict:
    """Returns aggregate stats (bounds, ascent/descent, distance/speed) used by
    Lap/Session, and a `points_written` count for the QA report."""
    stats = {
        "points_written": 0, "points_skipped": 0,
        "start_lat": None, "start_lon": None, "end_lat": None, "end_lon": None,
        "min_lat": None, "max_lat": None, "min_lon": None, "max_lon": None,
        "total_distance_m": None, "total_ascent_m": 0.0, "total_descent_m": 0.0,
        "avg_altitude_m": None, "max_altitude_m": None, "min_altitude_m": None,
        "max_speed_ms": None,
    }
    if len(points) > _FIT_MAX_RECORDS_PER_FILE:
        logger.warning(
            "Activity has %d GPS points, exceeding the FIT UINT16 record-count "
            "ceiling (%d) -- truncating. This should not happen for real "
            "per-activity tracks; investigate if seen.",
            len(points), _FIT_MAX_RECORDS_PER_FILE,
        )
        points = points[:_FIT_MAX_RECORDS_PER_FILE]

    cumulative_distance = 0.0
    altitudes = []
    prev_lat = prev_lon = prev_ms = None

    for i, pt in enumerate(points):
        try:
            record = RecordMessage()
            point_ms = _parse_utc_ms(pt["point_time_utc"])
            record.timestamp = point_ms

            lat, lon = float(pt["latitude"]), float(pt["longitude"])
            record.position_lat = lat   # fit-tool accepts degrees, converts internally
            record.position_long = lon

            if stats["start_lat"] is None:
                stats["start_lat"], stats["start_lon"] = lat, lon
            stats["end_lat"], stats["end_lon"] = lat, lon
            stats["min_lat"] = lat if stats["min_lat"] is None else min(stats["min_lat"], lat)
            stats["max_lat"] = lat if stats["max_lat"] is None else max(stats["max_lat"], lat)
            stats["min_lon"] = lon if stats["min_lon"] is None else min(stats["min_lon"], lon)
            stats["max_lon"] = lon if stats["max_lon"] is None else max(stats["max_lon"], lon)

            if pt["altitude_m"] is not None:
                alt = max(_ALT_MIN, float(pt["altitude_m"]))
                record.altitude = alt
                altitudes.append(alt)
                if len(altitudes) > 1:
                    delta = altitudes[-1] - altitudes[-2]
                    if delta > 0:
                        stats["total_ascent_m"] += delta
                    else:
                        stats["total_descent_m"] += -delta

            if pt["distance_m"] is not None:
                cumulative_distance = float(pt["distance_m"])
            elif prev_lat is not None:
                cumulative_distance += _haversine_m(prev_lat, prev_lon, lat, lon)
            record.distance = cumulative_distance

            if prev_ms is not None and point_ms > prev_ms and prev_lat is not None:
                # instantaneous speed from consecutive-point distance delta / dt
                dt_s = (point_ms - prev_ms) / 1000.0
                d_delta = _haversine_m(prev_lat, prev_lon, lat, lon)
                if dt_s > 0:
                    speed = min(d_delta / dt_s, _SPD_MAX)
                    record.speed = speed
                    stats["max_speed_ms"] = speed if stats["max_speed_ms"] is None else max(stats["max_speed_ms"], speed)

            if pt["heart_rate"] is not None:
                record.heart_rate = int(pt["heart_rate"])

            builder.add(record)
            stats["points_written"] += 1
            prev_lat, prev_lon, prev_ms = lat, lon, point_ms

        except Exception as exc:
            stats["points_skipped"] += 1
            logger.warning("Skipping GPS point %d: %s", i, exc)

    if points:
        stats["total_distance_m"] = cumulative_distance
    if altitudes:
        stats["avg_altitude_m"] = sum(altitudes) / len(altitudes)
        stats["max_altitude_m"] = max(altitudes)
        stats["min_altitude_m"] = min(altitudes)

    return stats


def _add_minimal_records(builder: FitFileBuilder, start_ms: int, end_ms: int) -> None:
    """Two bare start/end records -- the minimal-valid-session guarantee for
    sparse AUTO_DETECTED fragments with no tracker metrics and no GPS."""
    for ts in (start_ms, end_ms):
        record = RecordMessage()
        record.timestamp = ts
        builder.add(record)


def build_activity_fit(conn: sqlite3.Connection, activity_uid: str) -> tuple[bytes, dict]:
    """Build the FIT bytes for one activity. Returns (fit_bytes, report) where
    report carries points_written/points_skipped for the QA summary."""
    activity = conn.execute(
        "SELECT * FROM activity WHERE activity_uid = ?", (activity_uid,)
    ).fetchone()
    if activity is None:
        raise ValueError(f"No activity found for {activity_uid}")

    start_ms = _parse_utc_ms(activity["start_time_utc"])
    end_ms = _parse_utc_ms(activity["end_time_utc"])
    sport = Sport(activity["fit_sport"])
    sub_sport = SubSport(activity["fit_sub_sport"]) if activity["fit_sub_sport"] is not None else SubSport.GENERIC

    builder = FitFileBuilder(auto_define=True, min_string_size=50)

    file_id = FileIdMessage()
    file_id.type = FileType.ACTIVITY
    file_id.manufacturer = Manufacturer.DEVELOPMENT
    file_id.product = 1
    file_id.product_name = "Fitbit2Garmin"
    file_id.time_created = start_ms
    builder.add(file_id)

    sport_msg = SportMessage()
    sport_msg.sport = sport
    sport_msg.sub_sport = sub_sport
    sport_msg.sport_name = _sport_name(activity["activity_name_raw"])
    builder.add(sport_msg)

    start_evt = EventMessage()
    start_evt.timestamp = start_ms
    start_evt.event = Event.TIMER
    start_evt.event_type = EventType.START
    start_evt.data = 0
    builder.add(start_evt)

    gps_points = _load_gps_points(conn, activity_uid) if activity["gps_source"] != "none" else []
    if gps_points:
        gps_stats = _add_gps_records(builder, gps_points)
    else:
        _add_minimal_records(builder, start_ms, end_ms)
        gps_stats = {"points_written": 0, "points_skipped": 0}

    stop_evt = EventMessage()
    stop_evt.timestamp = end_ms
    stop_evt.event = Event.TIMER
    stop_evt.event_type = EventType.STOP_ALL
    stop_evt.data = 0
    builder.add(stop_evt)

    def _apply_common_fields(msg):
        msg.sport = sport
        msg.sub_sport = sub_sport
        msg.start_time = start_ms
        msg.timestamp = end_ms
        msg.total_elapsed_time = activity["duration_s"]
        msg.total_timer_time = activity["duration_s"]
        if activity["calories"] is not None:
            msg.total_calories = activity["calories"]
        if activity["steps"] is not None:
            # FIT has no total_steps field on Session/Lap -- steps are represented
            # as strides (1 stride = 2 steps). Confirmed against the real fit-tool
            # message schema: setting a non-existent attribute silently no-ops and
            # would otherwise drop step data from the file without any error.
            msg.total_strides = activity["steps"] // 2
        if activity["avg_heart_rate"] is not None:
            msg.avg_heart_rate = activity["avg_heart_rate"]
        if activity["peak_heart_rate"] is not None:
            msg.max_heart_rate = activity["peak_heart_rate"]

        distance_m = activity["distance_m"] if activity["distance_m"] is not None else gps_stats.get("total_distance_m")
        if distance_m is not None:
            msg.total_distance = distance_m

        elevation_gain_m = activity["elevation_gain_m"] if activity["elevation_gain_m"] is not None else (
            gps_stats.get("total_ascent_m") or None
        )
        if elevation_gain_m is not None:
            msg.total_ascent = elevation_gain_m

        if gps_stats.get("start_lat") is not None:
            msg.start_position_lat = gps_stats["start_lat"]
            msg.start_position_long = gps_stats["start_lon"]
        if gps_stats.get("avg_altitude_m") is not None:
            msg.avg_altitude = gps_stats["avg_altitude_m"]
            msg.max_altitude = gps_stats["max_altitude_m"]
            msg.min_altitude = gps_stats["min_altitude_m"]
        if gps_stats.get("max_speed_ms") is not None:
            msg.max_speed = gps_stats["max_speed_ms"]

    lap = LapMessage()
    _apply_common_fields(lap)
    if gps_stats.get("end_lat") is not None:
        lap.end_position_lat = gps_stats["end_lat"]
        lap.end_position_long = gps_stats["end_lon"]
    builder.add(lap)

    session = SessionMessage()
    _apply_common_fields(session)
    session.num_laps = 1
    if gps_stats.get("min_lat") is not None:
        session.swc_lat, session.swc_long = gps_stats["min_lat"], gps_stats["min_lon"]
        session.nec_lat, session.nec_long = gps_stats["max_lat"], gps_stats["max_lon"]
    builder.add(session)

    activity_msg = ActivityMessage()
    activity_msg.timestamp = end_ms
    activity_msg.num_sessions = 1
    activity_msg.total_timer_time = activity["duration_s"]
    activity_msg.type = 0  # Manual
    activity_msg.event = Event.ACTIVITY
    activity_msg.event_type = EventType.STOP
    activity_msg.local_timestamp = end_ms // 1000
    builder.add(activity_msg)

    fit_file = builder.build()
    report = {
        "activity_uid": activity_uid,
        "points_written": gps_stats.get("points_written", 0),
        "points_skipped": gps_stats.get("points_skipped", 0),
    }
    return fit_file.to_bytes(), report


def write_activity_fit(conn: sqlite3.Connection, activity_uid: str, output_dir: Path) -> tuple[Path, dict]:
    activity = conn.execute(
        "SELECT activity_name_raw, start_time_utc FROM activity WHERE activity_uid = ?",
        (activity_uid,),
    ).fetchone()
    fit_bytes, report = build_activity_fit(conn, activity_uid)

    safe_name = activity["activity_name_raw"].replace(" ", "-").replace("/", "-")
    start_compact = activity["start_time_utc"].replace(":", "").replace("-", "").rstrip("Z")
    filename = f"{safe_name}_{activity_uid.replace(':', '_')}_{start_compact}.fit"
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename
    with open(filepath, "wb") as f:
        f.write(fit_bytes)
    return filepath, report
