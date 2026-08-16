"""Ingest Activities/*.tcx -- per-activity GPS track files.

Confirmed by direct inspection: filename (numeric) == exercise-*.json's logId,
exact match, no fuzzy/time-window matching needed for these files. source_key is
that logId as a string, used by reconcile/gps_attacher.py for the exact-match path.
"""

import logging
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from . import file_registry

logger = logging.getLogger(__name__)

SOURCE_GROUP = "tcx"

# Confirmed by direct inspection of a real exported file: Fitbit's Takeout TCX uses
# "xmlschemas" (plural), not the "xmlschema" (singular) used by Garmin's own TCX --
# a real, easy-to-get-wrong discrepancy between the two.
_NS = {
    "tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2",
    "ax": "http://www.garmin.com/xmlschemas/ActivityExtension/v2",
}


def _to_utc_z(iso_with_offset: str) -> str:
    """TCX <Time> carries a real local UTC offset (e.g. '...-04:00'), unlike the
    naive-but-UTC strings in exercise-*.json -- must actually convert here, not
    just relabel."""
    dt = datetime.fromisoformat(iso_with_offset)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_trackpoints(root: ET.Element):
    for tp in root.iter("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Trackpoint"):
        time_el = tp.find("tcx:Time", _NS)
        pos_el = tp.find("tcx:Position", _NS)
        if time_el is None or pos_el is None:
            continue
        lat_el = pos_el.find("tcx:LatitudeDegrees", _NS)
        lon_el = pos_el.find("tcx:LongitudeDegrees", _NS)
        if lat_el is None or lon_el is None:
            continue
        alt_el = tp.find("tcx:AltitudeMeters", _NS)
        dist_el = tp.find("tcx:DistanceMeters", _NS)
        hr_el = tp.find("tcx:HeartRateBpm/tcx:Value", _NS)

        yield {
            "time": _to_utc_z(time_el.text),
            "latitude": float(lat_el.text),
            "longitude": float(lon_el.text),
            "altitude_m": float(alt_el.text) if alt_el is not None and alt_el.text else None,
            "distance_m": float(dist_el.text) if dist_el is not None and dist_el.text else None,
            "heart_rate": int(hr_el.text) if hr_el is not None and hr_el.text else None,
        }


def ingest_file(conn: sqlite3.Connection, takeout_root: Path, tcx_path: Path) -> int:
    relative_path = str(tcx_path.relative_to(takeout_root))
    status = file_registry.check_file(conn, relative_path, tcx_path)
    if not status.needs_ingest:
        return 0

    file_registry.begin_ingest(conn, relative_path, SOURCE_GROUP, tcx_path, status.content_hash)
    file_registry.clear_prior_rows(conn, "gps_point", relative_path)

    source_key = tcx_path.stem  # numeric logId as string
    row_count = 0
    try:
        tree = ET.parse(tcx_path)
        root = tree.getroot()
        rows = []
        for seq, pt in enumerate(_parse_trackpoints(root)):
            rows.append((
                "tcx",
                relative_path,
                source_key,
                pt["time"],
                pt["latitude"],
                pt["longitude"],
                pt["altitude_m"],
                pt["distance_m"],
                pt["heart_rate"],
                seq,
            ))
        conn.executemany(
            """INSERT INTO gps_point
               (source, source_file, source_key, point_time_utc, latitude, longitude,
                altitude_m, distance_m, heart_rate, sequence_in_source)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        row_count = len(rows)
        conn.commit()
        file_registry.finish_ingest_ok(conn, relative_path, row_count)
    except Exception as exc:
        conn.rollback()
        file_registry.finish_ingest_error(conn, relative_path, str(exc))
        logger.error("Failed to ingest %s: %s", tcx_path, exc)
        raise
    return row_count


def ingest_all(conn: sqlite3.Connection, takeout_root: Path, activities_dir: Path) -> int:
    total = 0
    for tcx_path in sorted(activities_dir.glob("*.tcx")):
        total += ingest_file(conn, takeout_root, tcx_path)
    return total
