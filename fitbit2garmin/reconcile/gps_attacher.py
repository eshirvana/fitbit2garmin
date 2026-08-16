"""GPS attachment: exact TCX match first, then a day-window fallback against the
continuous gps_location_csv log, per the approved plan's algorithm (step 9).
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .activity_type_map import GPS_PLAUSIBLE_NAMES, GPS_PLAUSIBLE_TYPE_IDS

_WINDOW_TOLERANCE = timedelta(seconds=30)
_MIN_POINTS = 2


@dataclass
class GpsResult:
    gps_source: str          # 'tcx' | 'gps_location_csv' | 'none'
    gps_confidence: str      # 'exact' | 'windowed' | 'flagged_no_data' | 'not_expected'
    point_ids: list[int]     # ordered gps_point.id values to link via activity_gps_point


def _parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _is_gps_plausible(activity_name: str, activity_type_id: int | None, has_gps_flag: bool) -> bool:
    if has_gps_flag:
        return True
    if activity_type_id is not None and activity_type_id in GPS_PLAUSIBLE_TYPE_IDS:
        return True
    return activity_name in GPS_PLAUSIBLE_NAMES


def _try_tcx(conn: sqlite3.Connection, log_id: int) -> list[int]:
    rows = conn.execute(
        """SELECT id FROM gps_point
           WHERE source='tcx' AND source_key=?
           ORDER BY sequence_in_source""",
        (str(log_id),),
    ).fetchall()
    return [r["id"] for r in rows]


def _try_gps_location_csv(
    conn: sqlite3.Connection, start_utc: datetime, end_utc: datetime
) -> list[int]:
    window_start = start_utc - _WINDOW_TOLERANCE
    window_end = end_utc + _WINDOW_TOLERANCE

    day_keys = set()
    d = window_start.date()
    while d <= window_end.date():
        day_keys.add(d.isoformat())
        d += timedelta(days=1)

    if not day_keys:
        return []

    placeholders = ",".join("?" for _ in day_keys)
    rows = conn.execute(
        f"""SELECT id, point_time_utc FROM gps_point
            WHERE source='gps_location_csv' AND source_key IN ({placeholders})
            AND point_time_utc BETWEEN ? AND ?
            ORDER BY point_time_utc""",
        (*day_keys, window_start.strftime("%Y-%m-%dT%H:%M:%SZ"), window_end.strftime("%Y-%m-%dT%H:%M:%SZ")),
    ).fetchall()
    return [r["id"] for r in rows]


def attach_gps(
    conn: sqlite3.Connection,
    exercise_json_log_id: int | None,
    activity_name: str,
    activity_type_id: int | None,
    has_gps_flag: bool,
    start_time_utc: str,
    end_time_utc: str,
) -> GpsResult:
    if exercise_json_log_id is not None:
        point_ids = _try_tcx(conn, exercise_json_log_id)
        if len(point_ids) >= _MIN_POINTS:
            return GpsResult("tcx", "exact", point_ids)
        # else: TCX file exists but is a confirmed-empty stub (real data has 8 of
        # these) -- fall through to the day-window fallback below, don't stop here.

    plausible = _is_gps_plausible(activity_name, activity_type_id, has_gps_flag)
    if not plausible:
        return GpsResult("none", "not_expected", [])

    start_utc = _parse_utc(start_time_utc)
    end_utc = _parse_utc(end_time_utc)
    point_ids = _try_gps_location_csv(conn, start_utc, end_utc)
    if len(point_ids) >= _MIN_POINTS:
        return GpsResult("gps_location_csv", "windowed", point_ids)

    return GpsResult("none", "flagged_no_data", [])
